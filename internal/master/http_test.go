package master_test

import (
	"context"
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"hive/internal/master"
)

func newTestHTTPServer(t *testing.T) (*master.HTTPServer, *master.DB) {
	t.Helper()
	db := newTestDB(t)
	snapDir := t.TempDir()
	srv := master.NewHTTPServer(db, snapDir, testLogger(t))
	return srv, db
}

func TestHTTPServer_GetDB_ReturnsSnapshot(t *testing.T) {
	t.Parallel()
	srv, db := newTestHTTPServer(t)
	ctx := context.Background()

	_, _, err := db.Exec(ctx, `CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)
	_, _, err = db.Exec(ctx, `INSERT INTO items VALUES (1, 'hello')`)
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/db", nil)
	require.NoError(t, err)
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))
	require.NotEmpty(t, resp.Header.Get("Content-Length"))

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Positive(t, len(data))

	// Write received bytes to a temp file and verify contents via SQLite.
	snapPath := t.TempDir() + "/received.db"
	require.NoError(t, writeFile(t, snapPath, data))

	snapDB, err := sql.Open("sqlite", snapPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, snapDB.Close()) }()

	var name string
	require.NoError(t, snapDB.QueryRowContext(ctx, `SELECT name FROM items WHERE id = 1`).Scan(&name))
	require.Equal(t, "hello", name)
}

func TestHTTPServer_GetDB_ContentLength_MatchesBody(t *testing.T) {
	t.Parallel()
	srv, db := newTestHTTPServer(t)
	ctx := context.Background()

	_, _, err := db.Exec(ctx, `CREATE TABLE t (v TEXT)`)
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/db", nil)
	require.NoError(t, err)
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, resp.ContentLength, int64(len(body)))
}

func TestHTTPServer_UnknownPath_Returns404(t *testing.T) {
	t.Parallel()
	srv, _ := newTestHTTPServer(t)
	ctx := context.Background()

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/unknown", nil)
	require.NoError(t, err)
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestHTTPServer_PostDB_Returns405(t *testing.T) {
	t.Parallel()
	srv, _ := newTestHTTPServer(t)
	ctx := context.Background()

	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/db", nil)
	require.NoError(t, err)
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()

	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}

func TestHTTPServer_ListenAndServe_ShutdownOnCancel(t *testing.T) {
	t.Parallel()
	srv, _ := newTestHTTPServer(t)

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe(ctx, "127.0.0.1:0")
	}()

	// Give the server a moment to start.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("ListenAndServe did not return after ctx cancel")
	}
}

// writeFile writes data to path; helper to avoid importing os in test body.
func writeFile(t *testing.T, path string, data []byte) error {
	t.Helper()
	f, err := openFileForWrite(path)
	if err != nil {
		return err
	}
	defer func() { require.NoError(t, f.Close()) }()
	_, err = f.Write(data)
	return err
}
