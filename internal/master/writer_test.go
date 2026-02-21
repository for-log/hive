package master_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"hive/internal/master"

	_ "modernc.org/sqlite"
)

func TestDB_ExecAndQuery(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	ctx := context.Background()

	_, _, err := db.Exec(ctx, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`)
	require.NoError(t, err)

	ra, lid, err := db.Exec(ctx, `INSERT INTO items (name) VALUES (?)`, "alpha")
	require.NoError(t, err)
	require.EqualValues(t, 1, ra)
	require.EqualValues(t, 1, lid)

	ra, _, err = db.Exec(ctx, `INSERT INTO items (name) VALUES (?)`, "beta")
	require.NoError(t, err)
	require.EqualValues(t, 1, ra)

	rows, err := db.Query(ctx, `SELECT id, name FROM items ORDER BY id`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	type row struct {
		name string
		id   int
	}
	var got []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.id, &r.name))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []row{{"alpha", 1}, {"beta", 2}}, got)
}

func TestDB_QueryRow(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	ctx := context.Background()

	_, _, err := db.Exec(ctx, `CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)`)
	require.NoError(t, err)
	_, _, err = db.Exec(ctx, `INSERT INTO kv VALUES (?, ?)`, "hello", "world")
	require.NoError(t, err)

	var v string
	require.NoError(t, db.QueryRow(ctx, `SELECT v FROM kv WHERE k = ?`, "hello").Scan(&v))
	require.Equal(t, "world", v)
}

func TestDB_ExecCancelledContext(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, _, err := db.Exec(ctx, `CREATE TABLE x (id INTEGER)`)
	require.Error(t, err)
}

func TestOpen_EmptyPath(t *testing.T) {
	t.Parallel()
	_, err := master.Open(context.Background(), "", testLogger(t))
	require.Error(t, err)
}

func TestDB_MultipleWrites(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	ctx := context.Background()

	_, _, err := db.Exec(ctx, `CREATE TABLE nums (n INTEGER)`)
	require.NoError(t, err)

	const count = 50
	for i := range count {
		_, _, err = db.Exec(ctx, `INSERT INTO nums VALUES (?)`, i)
		require.NoError(t, err)
	}

	row := db.QueryRow(ctx, `SELECT COUNT(*) FROM nums`)
	var n int
	require.NoError(t, row.Scan(&n))
	require.Equal(t, count, n)
}

func TestDB_Transaction_CommitVisible(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	ctx := context.Background()

	_, _, err := db.Exec(ctx, `CREATE TABLE t (v TEXT)`)
	require.NoError(t, err)

	require.NoError(t, db.BeginTx(ctx))
	_, _, err = db.Exec(ctx, `INSERT INTO t VALUES (?)`, "hello")
	require.NoError(t, err)
	require.NoError(t, db.CommitTx(ctx))

	row := db.QueryRow(ctx, `SELECT v FROM t`)
	var v string
	require.NoError(t, row.Scan(&v))
	require.Equal(t, "hello", v)
}

func TestDB_Transaction_RollbackNotVisible(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	ctx := context.Background()

	_, _, err := db.Exec(ctx, `CREATE TABLE t (v TEXT)`)
	require.NoError(t, err)

	require.NoError(t, db.BeginTx(ctx))
	_, _, err = db.Exec(ctx, `INSERT INTO t VALUES (?)`, "should-not-appear")
	require.NoError(t, err)
	require.NoError(t, db.RollbackTx(ctx))

	row := db.QueryRow(ctx, `SELECT COUNT(*) FROM t`)
	var n int
	require.NoError(t, row.Scan(&n))
	require.Equal(t, 0, n)
}

func TestDB_Transaction_DoubleBeginError(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	ctx := context.Background()

	require.NoError(t, db.BeginTx(ctx))
	require.Error(t, db.BeginTx(ctx))
	require.NoError(t, db.RollbackTx(ctx))
}

func TestDB_Transaction_CommitWithoutBeginError(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	require.Error(t, db.CommitTx(context.Background()))
}

func TestDB_Transaction_RollbackWithoutBeginError(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	require.Error(t, db.RollbackTx(context.Background()))
}

func TestDB_ExecReturning(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	ctx := context.Background()

	_, _, err := db.Exec(ctx, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)

	rows, cols, err := db.ExecReturning(ctx,
		`INSERT INTO items (name) VALUES (?) RETURNING id, name`, "gamma")
	require.NoError(t, err)
	require.Equal(t, []string{"id", "name"}, cols)
	require.Len(t, rows, 1)
	require.Len(t, rows[0], 2)
	require.EqualValues(t, 1, rows[0][0])
	require.Equal(t, "gamma", rows[0][1])
}

func TestDB_ExecReturning_MultipleRows(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	ctx := context.Background()

	_, _, err := db.Exec(ctx, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)
	_, _, err = db.Exec(ctx, `INSERT INTO items (name) VALUES (?), (?), (?)`, "a", "b", "c")
	require.NoError(t, err)

	rows, cols, err := db.ExecReturning(ctx, `UPDATE items SET name = name || '!' RETURNING id, name`)
	require.NoError(t, err)
	require.Equal(t, []string{"id", "name"}, cols)
	require.Len(t, rows, 3)
}

func TestDB_ExecReturning_InsideTx(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	ctx := context.Background()

	_, _, err := db.Exec(ctx, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)

	require.NoError(t, db.BeginTx(ctx))
	rows, cols, err := db.ExecReturning(ctx,
		`INSERT INTO items (name) VALUES (?) RETURNING id`, "delta")
	require.NoError(t, err)
	require.Equal(t, []string{"id"}, cols)
	require.Len(t, rows, 1)
	require.NoError(t, db.CommitTx(ctx))

	var count int
	require.NoError(t, db.QueryRow(ctx, `SELECT COUNT(*) FROM items`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestDB_CreateSnapshot(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	ctx := context.Background()

	_, _, err := db.Exec(ctx, `CREATE TABLE snap (id INTEGER PRIMARY KEY, v TEXT)`)
	require.NoError(t, err)
	_, _, err = db.Exec(ctx, `INSERT INTO snap VALUES (1, 'hello'), (2, 'world')`)
	require.NoError(t, err)

	destPath := filepath.Join(t.TempDir(), "snapshot.db")
	require.NoError(t, db.CreateSnapshot(ctx, destPath))

	snapDB, err := newSQLiteDB(t, destPath)
	require.NoError(t, err)

	rows, err := snapDB.QueryContext(ctx, `SELECT id, v FROM snap ORDER BY id`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	type row struct {
		v  string
		id int
	}
	var got []row
	for rows.Next() {
		var r row
		require.NoError(t, rows.Scan(&r.id, &r.v))
		got = append(got, r)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []row{{"hello", 1}, {"world", 2}}, got)
}

func TestDB_CreateSnapshot_Idempotent(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	ctx := context.Background()

	_, _, err := db.Exec(ctx, `CREATE TABLE t (v TEXT)`)
	require.NoError(t, err)
	_, _, err = db.Exec(ctx, `INSERT INTO t VALUES (?)`, "x")
	require.NoError(t, err)

	destPath := filepath.Join(t.TempDir(), "snap.db")

	require.NoError(t, db.CreateSnapshot(ctx, destPath))

	_, _, err = db.Exec(ctx, `INSERT INTO t VALUES (?)`, "y")
	require.NoError(t, err)

	require.NoError(t, db.CreateSnapshot(ctx, destPath))

	snapDB, err := newSQLiteDB(t, destPath)
	require.NoError(t, err)

	var n int
	require.NoError(t, snapDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM t`).Scan(&n))
	require.Equal(t, 2, n)
}

func TestDB_CreateSnapshot_EmptyPath(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	require.Error(t, db.CreateSnapshot(context.Background(), ""))
}
