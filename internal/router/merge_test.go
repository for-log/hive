package router_test

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"hive/internal/master"
	"hive/internal/router"
)

func startMasterHTTP(t *testing.T, populate func(ctx context.Context, db *sql.DB)) *httptest.Server {
	t.Helper()

	dir := t.TempDir()
	db, err := master.Open(context.Background(), filepath.Join(dir, "master.db"), slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	if populate != nil {
		rawDB, err := sql.Open("sqlite", filepath.Join(dir, "master.db")+"?_pragma=journal_mode(WAL)")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, rawDB.Close()) })
		populate(context.Background(), rawDB)
	}

	snapDir := t.TempDir()
	httpSrv := master.NewHTTPServer(db, snapDir, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	ts := httptest.NewServer(httpSrv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func newTestMerger(t *testing.T, meta *router.MetaStore) *router.Merger {
	t.Helper()
	return router.NewMerger(meta, t.TempDir(), slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
}

func openMergedDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func TestMerger_Merge_NoMasters(t *testing.T) {
	t.Parallel()

	meta := newTestMeta(t)
	merger := newTestMerger(t, meta)

	_, err := merger.Merge(context.Background())
	require.Error(t, err)
}

func TestMerger_Merge_SingleMaster(t *testing.T) {
	t.Parallel()

	meta := newTestMeta(t)
	ctx := context.Background()

	ts := startMasterHTTP(t, func(ctx context.Context, db *sql.DB) {
		_, err := db.ExecContext(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `INSERT INTO users VALUES (1, 'alice'), (2, 'bob')`)
		require.NoError(t, err)
	})

	addr := ts.URL[len("http://"):]
	require.NoError(t, meta.RegisterMaster(ctx, "m1", addr, addr))

	merger := newTestMerger(t, meta)
	outPath, err := merger.Merge(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if removeErr := os.Remove(outPath); removeErr != nil && !os.IsNotExist(removeErr) {
			t.Logf("remove merged db: %v", removeErr)
		}
	})

	db := openMergedDB(t, outPath)
	var count int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&count))
	require.Equal(t, 2, count)
}

func TestMerger_Merge_TwoMasters_Dedup(t *testing.T) {
	t.Parallel()

	meta := newTestMeta(t)
	ctx := context.Background()

	ts1 := startMasterHTTP(t, func(ctx context.Context, db *sql.DB) {
		_, err := db.ExecContext(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `INSERT INTO users VALUES (1, 'alice'), (2, 'bob')`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `INSERT INTO orders VALUES (10, 1)`)
		require.NoError(t, err)
	})

	ts2 := startMasterHTTP(t, func(ctx context.Context, db *sql.DB) {
		_, err := db.ExecContext(ctx, `CREATE TABLE products (id INTEGER PRIMARY KEY, title TEXT)`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `INSERT INTO products VALUES (100, 'widget')`)
		require.NoError(t, err)
		// id=1 is a duplicate of master-1; id=3 is new
		_, err = db.ExecContext(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `INSERT INTO users VALUES (1, 'alice-dup'), (3, 'charlie')`)
		require.NoError(t, err)
	})

	addr1 := ts1.URL[len("http://"):]
	addr2 := ts2.URL[len("http://"):]
	require.NoError(t, meta.RegisterMaster(ctx, "m1", addr1, addr1))
	require.NoError(t, meta.RegisterMaster(ctx, "m2", addr2, addr2))

	merger := newTestMerger(t, meta)
	outPath, err := merger.Merge(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if removeErr := os.Remove(outPath); removeErr != nil && !os.IsNotExist(removeErr) {
			t.Logf("remove merged db: %v", removeErr)
		}
	})

	db := openMergedDB(t, outPath)

	tests := []struct {
		name  string
		query string
		want  int
	}{
		{"users: 3 unique rows (id=1 deduped)", "SELECT COUNT(*) FROM users", 3},
		{"orders from m1", "SELECT COUNT(*) FROM orders", 1},
		{"products from m2", "SELECT COUNT(*) FROM products", 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var count int
			require.NoError(t, db.QueryRowContext(ctx, tc.query).Scan(&count))
			require.Equal(t, tc.want, count)
		})
	}
}

func TestMerger_Merge_EmptyMaster(t *testing.T) {
	t.Parallel()

	meta := newTestMeta(t)
	ctx := context.Background()

	ts := startMasterHTTP(t, nil)
	addr := ts.URL[len("http://"):]
	require.NoError(t, meta.RegisterMaster(ctx, "m1", addr, addr))

	merger := newTestMerger(t, meta)
	outPath, err := merger.Merge(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if removeErr := os.Remove(outPath); removeErr != nil && !os.IsNotExist(removeErr) {
			t.Logf("remove merged db: %v", removeErr)
		}
	})

	db := openMergedDB(t, outPath)
	var count int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='table'").Scan(&count))
	require.Equal(t, 0, count)
}

func TestMerger_Merge_MasterUnreachable(t *testing.T) {
	t.Parallel()

	meta := newTestMeta(t)
	ctx := context.Background()

	require.NoError(t, meta.RegisterMaster(ctx, "dead", "127.0.0.1:19999", "127.0.0.1:19999"))

	merger := newTestMerger(t, meta)
	_, err := merger.Merge(ctx)
	require.Error(t, err)
}

func TestMerger_Merge_BadHTTPStatus(t *testing.T) {
	t.Parallel()

	meta := newTestMeta(t)
	ctx := context.Background()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	t.Cleanup(ts.Close)

	addr := ts.URL[len("http://"):]
	require.NoError(t, meta.RegisterMaster(ctx, "m1", addr, addr))

	merger := newTestMerger(t, meta)
	_, err := merger.Merge(ctx)
	require.Error(t, err)
}
