package tests

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"hive/gen/hivepb"
	"hive/internal/master"
	"hive/internal/router"
	"hive/pkg/hivedriver"
)

const bufSize = 1 << 20 // 1 MiB

type stack struct {
	routerGRPCLis *bufconn.Listener
	routerHTTP    *httptest.Server
	meta          *router.MetaStore
	masters       []*masterNode
}

type masterNode struct {
	db     *master.DB
	id     string
	tables []string
}

func newStack(t *testing.T, masterDefs []struct {
	id     string
	tables []string
}) *stack {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	tmpDir := t.TempDir()

	meta, err := router.OpenMeta(ctx, filepath.Join(tmpDir, "meta.db"), discardLogger())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, meta.Close()) })

	routerSrv := router.NewRouter(meta, meta, router.RouterConfig{TxTimeout: 5 * time.Second}, discardLogger())
	registry := router.NewRegistry(meta, meta, router.RegistryConfig{
		DeadTimeout:       5 * time.Second,
		ReconcileInterval: 2 * time.Second,
	}, discardLogger())

	routerLis := bufconn.Listen(bufSize)
	grpcSrv := grpc.NewServer()
	hivepb.RegisterHiveSQLServer(grpcSrv, routerSrv)
	hivepb.RegisterHiveRegistryServer(grpcSrv, registry)
	go func() { _ = grpcSrv.Serve(routerLis) }()
	t.Cleanup(func() { grpcSrv.GracefulStop() })

	go registry.Run(ctx)

	var nodes []*masterNode
	for _, def := range masterDefs {
		dbPath := filepath.Join(tmpDir, def.id+".db")
		snapDir := filepath.Join(tmpDir, def.id+"-snaps")
		require.NoError(t, os.MkdirAll(snapDir, 0o700))

		mdb, err := master.Open(ctx, dbPath, discardLogger())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, mdb.Close()) })

		masterGRPCLis := bufconn.Listen(bufSize)
		masterSrv := master.NewServer(mdb, def.tables, snapDir, discardLogger())
		masterGRPC := grpc.NewServer()
		hivepb.RegisterHiveMasterServer(masterGRPC, masterSrv)
		go func() { _ = masterGRPC.Serve(masterGRPCLis) }()
		t.Cleanup(func() { masterGRPC.GracefulStop() })

		masterHTTP := httptest.NewServer(master.NewHTTPServer(mdb, snapDir, discardLogger()).Handler())
		t.Cleanup(masterHTTP.Close)

		// Tables are pre-registered so the router knows the mapping without waiting for DDL.
		regCtx, regCancel := context.WithTimeout(ctx, 3*time.Second)
		defer regCancel()
		require.NoError(t, meta.RegisterMaster(regCtx, def.id, "bufconn-"+def.id, masterHTTP.URL[len("http://"):]))
		for _, tbl := range def.tables {
			require.NoError(t, meta.RegisterTable(regCtx, tbl, def.id))
		}

		masterConn, err := grpc.NewClient("passthrough:///bufconn-"+def.id,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return masterGRPCLis.DialContext(ctx)
			}),
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = masterConn.Close() })
		router.InjectMasterConn(routerSrv, def.id, hivepb.NewHiveMasterClient(masterConn))

		nodes = append(nodes, &masterNode{
			db:     mdb,
			id:     def.id,
			tables: def.tables,
		})
	}

	merger := router.NewMerger(meta, tmpDir, discardLogger())
	routerHTTP := httptest.NewServer(router.NewHTTPServer(merger, meta, discardLogger()).Handler())
	t.Cleanup(routerHTTP.Close)

	return &stack{
		routerGRPCLis: routerLis,
		routerHTTP:    routerHTTP,
		meta:          meta,
		masters:       nodes,
	}
}

func (s *stack) openDB(t *testing.T) (*sql.DB, *hivedriver.Connector) {
	t.Helper()
	localDB := filepath.Join(t.TempDir(), "local.db")
	httpHost := s.routerHTTP.URL[len("http://"):]
	// passthrough:/// suppresses DNS resolution; the ContextDialer handles the actual dial.
	dsn := fmt.Sprintf("passthrough:///bufconn?local_db=%s&http_addr=%s", localDB, httpHost)

	connector, err := hivedriver.NewConnectorWithDialer(dsn,
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return s.routerGRPCLis.DialContext(ctx)
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connector.Close()) })

	db := sql.OpenDB(connector)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db, connector
}

func (s *stack) sync(t *testing.T, connector *hivedriver.Connector) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, connector.Sync(ctx))
}

// execOnMaster executes SQL directly on the n-th master, bypassing the router.
// Use when the table is already registered in MetaStore and DDL must not re-register it.
func (s *stack) execOnMaster(t *testing.T, n int, query string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err := s.masters[n].db.Exec(ctx, query)
	require.NoError(t, err)
}

func TestE2E_CreateTable(t *testing.T) {
	t.Parallel()
	s := newStack(t, []struct {
		id     string
		tables []string
	}{
		{"m1", nil},
	})
	db, _ := s.openDB(t)
	ctx := context.Background()

	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, val TEXT)`)
	require.NoError(t, err)

	masterID, err := s.meta.LookupMaster(ctx, "items")
	require.NoError(t, err)
	require.Equal(t, "m1", masterID)
}

func TestE2E_InsertAndSelectLocal(t *testing.T) {
	t.Parallel()
	s := newStack(t, []struct {
		id     string
		tables []string
	}{
		{"m1", []string{"users"}},
	})
	db, connector := s.openDB(t)
	ctx := context.Background()

	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `INSERT INTO users (name) VALUES (?) RETURNING id, name`, "alice")
	require.NoError(t, err)

	var name string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT name FROM users WHERE id=1`).Scan(&name))
	require.Equal(t, "alice", name)

	s.sync(t, connector)
	require.NoError(t, db.QueryRowContext(ctx, `SELECT name FROM users WHERE id=1`).Scan(&name))
	require.Equal(t, "alice", name)
}

func TestE2E_WriteThroughVerify(t *testing.T) {
	t.Parallel()
	s := newStack(t, []struct {
		id     string
		tables []string
	}{
		{"m1", []string{"products"}},
	})
	db, _ := s.openDB(t)
	ctx := context.Background()

	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)

	for _, name := range []string{"apple", "banana", "cherry"} {
		_, err = db.ExecContext(ctx, `INSERT INTO products (name) VALUES (?) RETURNING id, name`, name)
		require.NoError(t, err)
	}

	rows, err := db.QueryContext(ctx, `SELECT name FROM products ORDER BY id`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	var names []string
	for rows.Next() {
		var n string
		require.NoError(t, rows.Scan(&n))
		names = append(names, n)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"apple", "banana", "cherry"}, names)
}

func TestE2E_Transaction_SingleMaster(t *testing.T) {
	t.Parallel()
	s := newStack(t, []struct {
		id     string
		tables []string
	}{
		{"m1", []string{"accounts"}},
	})
	db, connector := s.openDB(t)
	ctx := context.Background()

	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS accounts (id INTEGER PRIMARY KEY, balance INTEGER)`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `INSERT INTO accounts (balance) VALUES (100) RETURNING id, balance`)
	require.NoError(t, err)

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	_, err = tx.ExecContext(ctx, `INSERT INTO accounts (balance) VALUES (200) RETURNING id, balance`)
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	// Tx write-through is applied on Commit; Sync picks up any remaining gaps.
	s.sync(t, connector)

	var count int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT COUNT(*) FROM accounts`).Scan(&count))
	require.Equal(t, 2, count)
}

func TestE2E_Transaction_CrossShard_Error(t *testing.T) {
	t.Parallel()
	s := newStack(t, []struct {
		id     string
		tables []string
	}{
		{"m1", nil},
		{"m2", nil},
	})
	db, _ := s.openDB(t)
	ctx := context.Background()

	// Each DDL goes to the least-loaded master; with two empty masters the
	// two tables land on different masters (round-robin by table count).
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, total INTEGER)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS payments (id INTEGER PRIMARY KEY, amount INTEGER)`)
	require.NoError(t, err)

	ordersM, err := s.meta.LookupMaster(ctx, "orders")
	require.NoError(t, err)
	paymentsM, err := s.meta.LookupMaster(ctx, "payments")
	require.NoError(t, err)
	require.NotEqual(t, ordersM, paymentsM, "orders and payments must be on different masters for cross-shard test")

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	_, err = tx.ExecContext(ctx, `INSERT INTO orders (total) VALUES (50)`)
	require.NoError(t, err)

	_, err = tx.ExecContext(ctx, `INSERT INTO payments (amount) VALUES (50)`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cross-shard")

	require.NoError(t, tx.Rollback())
}

func TestE2E_Sync_SeesOtherWrites(t *testing.T) {
	t.Parallel()
	s := newStack(t, []struct {
		id     string
		tables []string
	}{
		{"m1", nil},
	})

	dbA, connA := s.openDB(t)
	dbB, connB := s.openDB(t)
	ctx := context.Background()

	_, err := dbA.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, msg TEXT)`)
	require.NoError(t, err)

	_, err = dbA.ExecContext(ctx, `INSERT INTO events (msg) VALUES (?) RETURNING id, msg`, "from-A")
	require.NoError(t, err)

	// Before sync node B has no local snapshot of events yet.
	var countB int
	_ = dbB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&countB)
	require.Equal(t, 0, countB)

	s.sync(t, connA)
	s.sync(t, connB)

	require.NoError(t, dbB.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&countB))
	require.Equal(t, 1, countB)

	var msg string
	require.NoError(t, dbB.QueryRowContext(ctx, `SELECT msg FROM events WHERE id=1`).Scan(&msg))
	require.Equal(t, "from-A", msg)

	_, err = dbB.ExecContext(ctx, `INSERT INTO events (msg) VALUES (?) RETURNING id, msg`, "from-B")
	require.NoError(t, err)

	s.sync(t, connA)

	var countA int
	require.NoError(t, dbA.QueryRowContext(ctx, `SELECT COUNT(*) FROM events`).Scan(&countA))
	require.Equal(t, 2, countA)
}

func TestE2E_DDL_RoutesToLeastTables(t *testing.T) {
	t.Parallel()
	s := newStack(t, []struct {
		id     string
		tables []string
	}{
		{"m1", []string{"t1", "t2"}},
		{"m2", nil},
	})
	s.execOnMaster(t, 0, `CREATE TABLE IF NOT EXISTS t1 (id INTEGER PRIMARY KEY)`)
	s.execOnMaster(t, 0, `CREATE TABLE IF NOT EXISTS t2 (id INTEGER PRIMARY KEY)`)

	db, _ := s.openDB(t)
	ctx := context.Background()

	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS new_table (id INTEGER PRIMARY KEY)`)
	require.NoError(t, err)

	masterID, err := s.meta.LookupMaster(ctx, "new_table")
	require.NoError(t, err)
	require.Equal(t, "m2", masterID)
}

func TestE2E_Snapshot_Merged(t *testing.T) {
	t.Parallel()
	s := newStack(t, []struct {
		id     string
		tables []string
	}{
		{"m1", nil},
		{"m2", nil},
	})
	db, connector := s.openDB(t)
	ctx := context.Background()

	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS alpha (id INTEGER PRIMARY KEY, v TEXT)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS beta (id INTEGER PRIMARY KEY, v TEXT)`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `INSERT INTO alpha (v) VALUES (?) RETURNING id, v`, "a1")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO beta (v) VALUES (?) RETURNING id, v`, "b1")
	require.NoError(t, err)

	s.sync(t, connector)

	var va, vb string
	require.NoError(t, db.QueryRowContext(ctx, `SELECT v FROM alpha WHERE id=1`).Scan(&va))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT v FROM beta WHERE id=1`).Scan(&vb))
	require.Equal(t, "a1", va)
	require.Equal(t, "b1", vb)
}

func TestE2E_MasterDraining(t *testing.T) {
	t.Parallel()
	s := newStack(t, []struct {
		id     string
		tables []string
	}{
		{"m1", nil},
	})
	ctx := context.Background()

	require.NoError(t, s.meta.SetStatus(ctx, "m1", router.MasterStatusDraining))

	alive, err := s.meta.AliveMasters(ctx)
	require.NoError(t, err)
	for _, m := range alive {
		require.NotEqual(t, router.MasterStatusDraining, m.Status)
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
