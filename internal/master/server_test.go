package master_test

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"hive/gen/hivepb"
	"hive/internal/master"
)

const bufSize = 1 << 20 // 1 MiB

func newTestServer(t *testing.T, tables []string) hivepb.HiveMasterClient {
	t.Helper()

	db := newTestDB(t)
	snapDir := t.TempDir()
	srv := master.NewServer(db, tables, snapDir, testLogger(t))

	lis := bufconn.Listen(bufSize)
	grpcSrv := grpc.NewServer()
	hivepb.RegisterHiveMasterServer(grpcSrv, srv)

	go func() {
		if err := grpcSrv.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Logf("grpc serve: %v", err)
		}
	}()
	t.Cleanup(func() { grpcSrv.GracefulStop() })

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	return hivepb.NewHiveMasterClient(conn)
}

func TestServer_Execute_DDLAndDML(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := newTestServer(t, []string{"items"})

	_, err := client.Execute(ctx, &hivepb.ExecuteRequest{
		Sql: `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)`,
	})
	require.NoError(t, err)

	resp, err := client.Execute(ctx, &hivepb.ExecuteRequest{
		Sql:  `INSERT INTO items (name) VALUES (?)`,
		Args: []*hivepb.Value{{Kind: &hivepb.Value_TextValue{TextValue: "alpha"}}},
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, resp.RowsAffected)
	require.EqualValues(t, 1, resp.LastInsertId)
}

func TestServer_Execute_Returning(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := newTestServer(t, []string{"items"})

	_, err := client.Execute(ctx, &hivepb.ExecuteRequest{
		Sql: `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)`,
	})
	require.NoError(t, err)

	resp, err := client.Execute(ctx, &hivepb.ExecuteRequest{
		Sql:        `INSERT INTO items (name) VALUES (?) RETURNING id, name`,
		Args:       []*hivepb.Value{{Kind: &hivepb.Value_TextValue{TextValue: "beta"}}},
		ReturnRows: true,
	})
	require.NoError(t, err)
	require.Len(t, resp.ReturnedRows, 1)
	require.Len(t, resp.ReturnedRows[0].Values, 2)
	require.EqualValues(t, 1, resp.ReturnedRows[0].Values[0].GetIntValue())
	require.Equal(t, "beta", resp.ReturnedRows[0].Values[1].GetTextValue())
}

func TestServer_Query_Stream(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := newTestServer(t, []string{"nums"})

	_, err := client.Execute(ctx, &hivepb.ExecuteRequest{
		Sql: `CREATE TABLE nums (n INTEGER)`,
	})
	require.NoError(t, err)
	for _, v := range []int64{10, 20, 30} {
		_, err = client.Execute(ctx, &hivepb.ExecuteRequest{
			Sql:  `INSERT INTO nums VALUES (?)`,
			Args: []*hivepb.Value{{Kind: &hivepb.Value_IntValue{IntValue: v}}},
		})
		require.NoError(t, err)
	}

	stream, err := client.Query(ctx, &hivepb.QueryRequest{
		Sql: `SELECT n FROM nums ORDER BY n`,
	})
	require.NoError(t, err)

	first, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, first.GetMeta())
	require.Len(t, first.GetMeta().Columns, 1)
	require.Equal(t, "n", first.GetMeta().Columns[0].Name)

	var nums []int64
	for {
		msg, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		require.NoError(t, recvErr)
		nums = append(nums, msg.GetRow().Values[0].GetIntValue())
	}
	require.Equal(t, []int64{10, 20, 30}, nums)
}

func TestServer_ListTables(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := newTestServer(t, []string{"users", "orders"})

	resp, err := client.ListTables(ctx, &hivepb.ListTablesRequest{})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"users", "orders"}, resp.Tables)
}

func TestServer_Transaction_CommitVisible(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := newTestServer(t, []string{"t"})

	_, err := client.Execute(ctx, &hivepb.ExecuteRequest{Sql: `CREATE TABLE t (v TEXT)`})
	require.NoError(t, err)

	txResp, err := client.BeginTx(ctx, &hivepb.BeginTxRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, txResp.TxId)

	_, err = client.Execute(ctx, &hivepb.ExecuteRequest{
		Sql:  `INSERT INTO t VALUES (?)`,
		Args: []*hivepb.Value{{Kind: &hivepb.Value_TextValue{TextValue: "hello"}}},
		TxId: txResp.TxId,
	})
	require.NoError(t, err)

	_, err = client.CommitTx(ctx, &hivepb.TxRequest{TxId: txResp.TxId})
	require.NoError(t, err)

	stream, err := client.Query(ctx, &hivepb.QueryRequest{Sql: `SELECT v FROM t`})
	require.NoError(t, err)
	_, err = stream.Recv() // meta
	require.NoError(t, err)
	row, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, "hello", row.GetRow().Values[0].GetTextValue())
}

func TestServer_Transaction_RollbackNotVisible(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := newTestServer(t, []string{"t"})

	_, err := client.Execute(ctx, &hivepb.ExecuteRequest{Sql: `CREATE TABLE t (v TEXT)`})
	require.NoError(t, err)

	txResp, err := client.BeginTx(ctx, &hivepb.BeginTxRequest{})
	require.NoError(t, err)

	_, err = client.Execute(ctx, &hivepb.ExecuteRequest{
		Sql:  `INSERT INTO t VALUES (?)`,
		Args: []*hivepb.Value{{Kind: &hivepb.Value_TextValue{TextValue: "gone"}}},
		TxId: txResp.TxId,
	})
	require.NoError(t, err)

	_, err = client.RollbackTx(ctx, &hivepb.TxRequest{TxId: txResp.TxId})
	require.NoError(t, err)

	stream, err := client.Query(ctx, &hivepb.QueryRequest{Sql: `SELECT COUNT(*) FROM t`})
	require.NoError(t, err)
	_, err = stream.Recv() // meta
	require.NoError(t, err)
	row, err := stream.Recv()
	require.NoError(t, err)
	require.EqualValues(t, 0, row.GetRow().Values[0].GetIntValue())
}

func TestServer_Snapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := newTestServer(t, []string{"snap"})

	_, err := client.Execute(ctx, &hivepb.ExecuteRequest{
		Sql: `CREATE TABLE snap (id INTEGER PRIMARY KEY, v TEXT)`,
	})
	require.NoError(t, err)
	_, err = client.Execute(ctx, &hivepb.ExecuteRequest{
		Sql:  `INSERT INTO snap VALUES (1, ?)`,
		Args: []*hivepb.Value{{Kind: &hivepb.Value_TextValue{TextValue: "data"}}},
	})
	require.NoError(t, err)

	stream, err := client.Snapshot(ctx, &hivepb.SnapshotRequest{})
	require.NoError(t, err)

	var buf []byte
	var totalSize int64
	first := true
	for {
		chunk, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		require.NoError(t, recvErr)
		if first {
			totalSize = chunk.TotalSize
			require.Positive(t, totalSize)
			first = false
		}
		buf = append(buf, chunk.Data...)
	}
	require.EqualValues(t, totalSize, len(buf))

	// Write received bytes to a temp file and open it as SQLite.
	snapPath := filepath.Join(t.TempDir(), "received.db")
	require.NoError(t, os.WriteFile(snapPath, buf, 0o600))

	snapDB, openErr := newSQLiteDB(t, snapPath)
	require.NoError(t, openErr)

	var v string
	require.NoError(t, snapDB.QueryRowContext(ctx, `SELECT v FROM snap WHERE id = 1`).Scan(&v))
	require.Equal(t, "data", v)
}
