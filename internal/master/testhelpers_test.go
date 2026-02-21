package master_test

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"hive/internal/master"
)

func openFileForWrite(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
}

func testLogger(_ *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func newTestDB(t *testing.T) *master.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := master.Open(context.Background(), path, testLogger(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func newSQLiteDB(t *testing.T, path string) (*sql.DB, error) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db, nil
}
