package router_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"hive/gen/hivepb"
	"hive/internal/router"
)

// fakeMasterServer is an in-process HiveMaster gRPC server backed by a real master.DB.
// For routing tests we only need Execute and BeginTx/CommitTx/RollbackTx.
type fakeMasterServer struct {
	hivepb.UnimplementedHiveMasterServer

	execFn     func(req *hivepb.ExecuteRequest) (*hivepb.ExecuteResponse, error)
	beginFn    func() (*hivepb.BeginTxResponse, error)
	commitFn   func(txID string) error
	rollbackFn func(txID string) error
}

func (f *fakeMasterServer) Execute(_ context.Context, req *hivepb.ExecuteRequest) (*hivepb.ExecuteResponse, error) {
	if f.execFn != nil {
		return f.execFn(req)
	}
	return &hivepb.ExecuteResponse{RowsAffected: 1}, nil
}

func (f *fakeMasterServer) BeginTx(_ context.Context, _ *hivepb.BeginTxRequest) (*hivepb.BeginTxResponse, error) {
	if f.beginFn != nil {
		return f.beginFn()
	}
	return &hivepb.BeginTxResponse{TxId: "mtx-1"}, nil
}

func (f *fakeMasterServer) CommitTx(_ context.Context, req *hivepb.TxRequest) (*hivepb.TxResponse, error) {
	if f.commitFn != nil {
		if err := f.commitFn(req.TxId); err != nil {
			return nil, err
		}
	}
	return &hivepb.TxResponse{Ok: true}, nil
}

func (f *fakeMasterServer) RollbackTx(_ context.Context, req *hivepb.TxRequest) (*hivepb.TxResponse, error) {
	if f.rollbackFn != nil {
		if err := f.rollbackFn(req.TxId); err != nil {
			return nil, err
		}
	}
	return &hivepb.TxResponse{Ok: true}, nil
}

func (f *fakeMasterServer) Query(_ *hivepb.QueryRequest, stream hivepb.HiveMaster_QueryServer) error {
	return stream.Send(&hivepb.QueryResult{
		Payload: &hivepb.QueryResult_Meta{
			Meta: &hivepb.QueryMeta{Columns: []*hivepb.ColumnInfo{{Name: "id", Type: "INTEGER"}}},
		},
	})
}

// startFakeMaster starts a gRPC server for a fake master and registers it in meta.
// Returns the master ID and the fake server handle for assertions.
func startFakeMaster(t *testing.T, meta *router.MetaStore, masterID string, srv *fakeMasterServer) {
	t.Helper()

	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcSrv := grpc.NewServer()
	hivepb.RegisterHiveMasterServer(grpcSrv, srv)

	go func() {
		if serveErr := grpcSrv.Serve(lis); serveErr != nil && serveErr != grpc.ErrServerStopped {
			t.Logf("fake master serve: %v", serveErr)
		}
	}()
	t.Cleanup(func() { grpcSrv.GracefulStop() })

	err = meta.RegisterMaster(context.Background(), masterID, lis.Addr().String(), "")
	require.NoError(t, err)
}

// newTestRouter creates a Router backed by a real MetaStore and a gRPC server.
func newTestRouter(t *testing.T) (hivepb.HiveSQLClient, *router.MetaStore) {
	t.Helper()

	meta := newTestMeta(t)
	r := router.NewRouter(meta, meta, router.RouterConfig{}, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))

	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcSrv := grpc.NewServer()
	hivepb.RegisterHiveSQLServer(grpcSrv, r)

	go func() {
		if serveErr := grpcSrv.Serve(lis); serveErr != nil && serveErr != grpc.ErrServerStopped {
			t.Logf("router serve: %v", serveErr)
		}
	}()
	t.Cleanup(func() { grpcSrv.GracefulStop() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	return hivepb.NewHiveSQLClient(conn), meta
}

func TestRouter_Execute_DML_Routed(t *testing.T) {
	t.Parallel()

	client, meta := newTestRouter(t)
	ctx := context.Background()

	startFakeMaster(t, meta, "m1", &fakeMasterServer{})
	err := meta.RegisterTable(ctx, "users", "m1")
	require.NoError(t, err)

	resp, err := client.Execute(ctx, &hivepb.ExecuteRequest{
		Sql: "INSERT INTO users (name) VALUES ('alice')",
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, resp.RowsAffected)
}

func TestRouter_Execute_DML_UnknownTable(t *testing.T) {
	t.Parallel()

	client, _ := newTestRouter(t)

	_, err := client.Execute(context.Background(), &hivepb.ExecuteRequest{
		Sql: "INSERT INTO ghost (x) VALUES (1)",
	})
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestRouter_Execute_DDL_CreateTable(t *testing.T) {
	t.Parallel()

	client, meta := newTestRouter(t)
	ctx := context.Background()

	startFakeMaster(t, meta, "m1", &fakeMasterServer{})

	resp, err := client.Execute(ctx, &hivepb.ExecuteRequest{
		Sql: "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	masterID, err := meta.LookupMaster(ctx, "products")
	require.NoError(t, err)
	require.Equal(t, "m1", masterID)
}

func TestRouter_Execute_DDL_NoMasters(t *testing.T) {
	t.Parallel()

	client, _ := newTestRouter(t)

	_, err := client.Execute(context.Background(), &hivepb.ExecuteRequest{
		Sql: "CREATE TABLE products (id INTEGER PRIMARY KEY)",
	})
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestRouter_Query_Routed(t *testing.T) {
	t.Parallel()

	client, meta := newTestRouter(t)
	ctx := context.Background()

	startFakeMaster(t, meta, "m1", &fakeMasterServer{})
	err := meta.RegisterTable(ctx, "users", "m1")
	require.NoError(t, err)

	stream, err := client.Query(ctx, &hivepb.QueryRequest{Sql: "SELECT id FROM users"})
	require.NoError(t, err)

	msg, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, msg.GetMeta())

	_, err = stream.Recv()
	require.Equal(t, io.EOF, err)
}

func TestRouter_Transaction_SingleMaster(t *testing.T) {
	t.Parallel()

	var capturedTxID string
	fake := &fakeMasterServer{
		execFn: func(req *hivepb.ExecuteRequest) (*hivepb.ExecuteResponse, error) {
			capturedTxID = req.TxId
			return &hivepb.ExecuteResponse{RowsAffected: 1}, nil
		},
	}

	client, meta := newTestRouter(t)
	ctx := context.Background()

	startFakeMaster(t, meta, "m1", fake)
	err := meta.RegisterTable(ctx, "users", "m1")
	require.NoError(t, err)

	beginResp, err := client.BeginTx(ctx, &hivepb.BeginTxRequest{})
	require.NoError(t, err)
	txID := beginResp.TxId

	_, err = client.Execute(ctx, &hivepb.ExecuteRequest{
		Sql:  "INSERT INTO users (name) VALUES ('bob')",
		TxId: txID,
	})
	require.NoError(t, err)
	require.Equal(t, "mtx-1", capturedTxID)

	_, err = client.CommitTx(ctx, &hivepb.TxRequest{TxId: txID})
	require.NoError(t, err)
}

func TestRouter_Transaction_CrossShard_Rejected(t *testing.T) {
	t.Parallel()

	client, meta := newTestRouter(t)
	ctx := context.Background()

	startFakeMaster(t, meta, "m1", &fakeMasterServer{})
	startFakeMaster(t, meta, "m2", &fakeMasterServer{})
	err := meta.RegisterTable(ctx, "users", "m1")
	require.NoError(t, err)
	err = meta.RegisterTable(ctx, "orders", "m2")
	require.NoError(t, err)

	beginResp, err := client.BeginTx(ctx, &hivepb.BeginTxRequest{})
	require.NoError(t, err)
	txID := beginResp.TxId

	_, err = client.Execute(ctx, &hivepb.ExecuteRequest{
		Sql:  "INSERT INTO users (name) VALUES ('alice')",
		TxId: txID,
	})
	require.NoError(t, err)

	_, err = client.Execute(ctx, &hivepb.ExecuteRequest{
		Sql:  "INSERT INTO orders (user_id) VALUES (1)",
		TxId: txID,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestRouter_Transaction_UnknownTx(t *testing.T) {
	t.Parallel()

	client, _ := newTestRouter(t)

	_, err := client.CommitTx(context.Background(), &hivepb.TxRequest{TxId: "ghost-tx"})
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestRouter_Transaction_RollbackUnknown(t *testing.T) {
	t.Parallel()

	client, _ := newTestRouter(t)

	_, err := client.RollbackTx(context.Background(), &hivepb.TxRequest{TxId: "ghost-tx"})
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}
