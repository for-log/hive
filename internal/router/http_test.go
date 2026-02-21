package router_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"hive/internal/router"
)

func newTestHTTPServer(t *testing.T, meta *router.MetaStore) *httptest.Server {
	t.Helper()
	merger := router.NewMerger(meta, t.TempDir(), slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	srv := router.NewHTTPServer(merger, meta, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func TestRouterHTTP_Masters_Empty(t *testing.T) {
	t.Parallel()

	meta := newTestMeta(t)
	ts := newTestHTTPServer(t, meta)

	resp, err := http.Get(ts.URL + "/masters") //nolint:noctx
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var masters []map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&masters))
	require.Empty(t, masters)
}

func TestRouterHTTP_Masters_WithMasters(t *testing.T) {
	t.Parallel()

	meta := newTestMeta(t)
	ctx := context.Background()

	require.NoError(t, meta.RegisterMaster(ctx, "m1", ":9001", ":8081"))
	require.NoError(t, meta.RegisterMaster(ctx, "m2", ":9002", ":8082"))

	ts := newTestHTTPServer(t, meta)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/masters", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var masters []map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&masters))
	require.Len(t, masters, 2)

	ids := make([]string, 0, 2)
	for _, m := range masters {
		id, ok := m["id"].(string)
		require.True(t, ok, "id field must be a string")
		ids = append(ids, id)
	}
	require.ElementsMatch(t, []string{"m1", "m2"}, ids)
}

func TestRouterHTTP_Merged_NoMasters(t *testing.T) {
	t.Parallel()

	meta := newTestMeta(t)
	ts := newTestHTTPServer(t, meta)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, ts.URL+"/db/merged", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()

	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestRouterHTTP_Merged_SingleMaster(t *testing.T) {
	t.Parallel()

	meta := newTestMeta(t)
	ctx := context.Background()

	ts1 := startMasterHTTP(t, func(ctx context.Context, db *sql.DB) {
		_, err := db.ExecContext(ctx, `CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `INSERT INTO items VALUES (1, 'x')`)
		require.NoError(t, err)
	})
	addr := ts1.URL[len("http://"):]
	require.NoError(t, meta.RegisterMaster(ctx, "m1", addr, addr))

	ts := newTestHTTPServer(t, meta)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/db/merged", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/octet-stream", resp.Header.Get("Content-Type"))

	tmpFile, err := os.CreateTemp(t.TempDir(), "merged-*.db")
	require.NoError(t, err)
	defer func() { require.NoError(t, tmpFile.Close()) }()

	_, err = io.Copy(tmpFile, resp.Body)
	require.NoError(t, err)

	db, err := sql.Open("sqlite", tmpFile.Name())
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	var count int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM items").Scan(&count))
	require.Equal(t, 1, count)
}
