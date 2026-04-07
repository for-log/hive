package hrana_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hive_v2/orchestrator/internal/config"
	"github.com/hive_v2/orchestrator/internal/hrana"
	"github.com/hive_v2/orchestrator/internal/router"
	gosql "github.com/hive_v2/orchestrator/internal/sql"
	"github.com/hive_v2/orchestrator/internal/stream"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// fakeMasterDoer records pipeline calls and returns a canned response.
// It also satisfies CursorDoer (unused in unit tests).
type fakePipelineDoer struct {
	resp *hrana.PipelineResponse
	err  error
}

func (f *fakePipelineDoer) Pipeline(_ context.Context, _ *hrana.PipelineRequest) (*hrana.PipelineResponse, error) {
	return f.resp, f.err
}

func (f *fakePipelineDoer) Cursor(_ context.Context, _ *hrana.CursorRequest) (*http.Response, error) {
	return nil, fmt.Errorf("cursor not implemented in fake")
}

// fakePool always returns the same doer regardless of master index.
type fakePool struct{ doer hrana.MasterDoer }

func (p *fakePool) ClientFor(_ int) hrana.MasterDoer { return p.doer }

// fakeDump returns a canned dump string.
type fakeDump struct{ dump string }

func (d *fakeDump) Dump(_ context.Context) (string, error) { return d.dump, nil }

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newTestServer(t *testing.T, doer hrana.MasterDoer) (*hrana.Server, *stream.Manager) {
	t.Helper()

	cfg := &config.Config{
		Masters: []config.MasterConfig{
			{URL: "http://master0"},
		},
		ReadPolicy:   config.ReadPolicyWriteMaster,
		StreamTTL:    5 * time.Second,
		MaxBodyBytes: 1 << 20,
	}

	rtr, err := router.New(cfg, gosql.TokenAnalyzer{})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	mgr := stream.NewManager(ctx, cfg.StreamTTL)

	srv := hrana.NewServer(hrana.ServerConfig{
		Pool:         &fakePool{doer: doer},
		Router:       rtr,
		StreamMgr:    mgr,
		DumpProvider: &fakeDump{dump: "-- dump\n"},
		MaxBodyBytes: cfg.MaxBodyBytes,
		Version:      "test-v0",
	})

	return srv, mgr
}

func postPipeline(t *testing.T, srv *hrana.Server, req hrana.PipelineRequest) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(req)
	require.NoError(t, err)

	r := httptest.NewRequest(http.MethodPost, "/v2/pipeline", bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mux := http.NewServeMux()
	srv.Register(mux)
	mux.ServeHTTP(w, r)
	return w
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestServer_Health(t *testing.T) {
	srv, _ := newTestServer(t, nil)
	mux := http.NewServeMux()
	srv.Register(mux)

	r := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestServer_Version(t *testing.T) {
	srv, _ := newTestServer(t, nil)
	mux := http.NewServeMux()
	srv.Register(mux)

	r := httptest.NewRequest(http.MethodGet, "/version", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "test-v0", w.Body.String())
}

func TestServer_Dump(t *testing.T) {
	srv, _ := newTestServer(t, nil)
	mux := http.NewServeMux()
	srv.Register(mux)

	r := httptest.NewRequest(http.MethodGet, "/dump", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "-- dump\n", w.Body.String())
}

func TestServer_Pipeline_NewStream(t *testing.T) {
	sql := "SELECT 1"
	doer := &fakePipelineDoer{
		resp: &hrana.PipelineResponse{
			Baton: "master-baton-1",
			Results: []hrana.StreamResult{
				hrana.OkResult(hrana.StreamResponse{Type: "execute"}),
			},
		},
	}
	srv, _ := newTestServer(t, doer)

	req := hrana.PipelineRequest{
		Requests: []hrana.StreamRequest{
			{Type: "execute", Stmt: &hrana.Stmt{SQL: &sql}},
		},
	}

	w := postPipeline(t, srv, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var resp hrana.PipelineResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.NotEmpty(t, resp.Baton, "baton must be returned for open stream")
	require.Len(t, resp.Results, 1)
	assert.Equal(t, "ok", resp.Results[0].Type)
}

func TestServer_Pipeline_BatonContinuity(t *testing.T) {
	sql := "SELECT 1"
	doer := &fakePipelineDoer{
		resp: &hrana.PipelineResponse{
			Baton: "master-baton-x",
			Results: []hrana.StreamResult{
				hrana.OkResult(hrana.StreamResponse{Type: "execute"}),
			},
		},
	}
	srv, _ := newTestServer(t, doer)

	// First request — no baton.
	req := hrana.PipelineRequest{
		Requests: []hrana.StreamRequest{
			{Type: "execute", Stmt: &hrana.Stmt{SQL: &sql}},
		},
	}
	w := postPipeline(t, srv, req)
	require.Equal(t, http.StatusOK, w.Code)

	var resp1 hrana.PipelineResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp1))
	baton1 := resp1.Baton
	require.NotEmpty(t, baton1)

	// Second request — use baton from first response.
	req.Baton = baton1
	w2 := postPipeline(t, srv, req)
	require.Equal(t, http.StatusOK, w2.Code)

	var resp2 hrana.PipelineResponse
	require.NoError(t, json.NewDecoder(w2.Body).Decode(&resp2))
	assert.NotEmpty(t, resp2.Baton)
	assert.NotEqual(t, baton1, resp2.Baton, "baton must rotate on each response")
}

func TestServer_Pipeline_UnknownBaton(t *testing.T) {
	srv, _ := newTestServer(t, nil)

	req := hrana.PipelineRequest{
		Baton:    "nonexistent-baton",
		Requests: []hrana.StreamRequest{{Type: "close"}},
	}
	w := postPipeline(t, srv, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestServer_Pipeline_CloseRequest(t *testing.T) {
	sql := "SELECT 1"
	doer := &fakePipelineDoer{
		resp: &hrana.PipelineResponse{
			Baton: "m-baton",
			Results: []hrana.StreamResult{
				hrana.OkResult(hrana.StreamResponse{Type: "execute"}),
			},
		},
	}
	srv, mgr := newTestServer(t, doer)

	// Open stream.
	req := hrana.PipelineRequest{
		Requests: []hrana.StreamRequest{
			{Type: "execute", Stmt: &hrana.Stmt{SQL: &sql}},
		},
	}
	w := postPipeline(t, srv, req)
	require.Equal(t, http.StatusOK, w.Code)
	var resp1 hrana.PipelineResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp1))
	assert.Equal(t, 1, mgr.Size())

	// Close stream.
	closeReq := hrana.PipelineRequest{
		Baton:    resp1.Baton,
		Requests: []hrana.StreamRequest{{Type: "close"}},
	}
	w2 := postPipeline(t, srv, closeReq)
	require.Equal(t, http.StatusOK, w2.Code)

	var resp2 hrana.PipelineResponse
	require.NoError(t, json.NewDecoder(w2.Body).Decode(&resp2))
	assert.Empty(t, resp2.Baton, "closed stream must not return a baton")
	assert.Equal(t, 0, mgr.Size())
}

func TestServer_Pipeline_StoreSql(t *testing.T) {
	sql := "SELECT 42"
	doer := &fakePipelineDoer{
		resp: &hrana.PipelineResponse{
			Baton: "m-baton",
			Results: []hrana.StreamResult{
				hrana.OkResult(hrana.StreamResponse{Type: "execute"}),
			},
		},
	}
	srv, _ := newTestServer(t, doer)

	sqlID := int32(1)
	req := hrana.PipelineRequest{
		Requests: []hrana.StreamRequest{
			{Type: "store_sql", SQLId: &sqlID, SQL: &sql},
		},
	}
	w := postPipeline(t, srv, req)
	require.Equal(t, http.StatusOK, w.Code)

	var resp hrana.PipelineResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	require.Len(t, resp.Results, 1)
	assert.Equal(t, "ok", resp.Results[0].Type)
}

func TestServer_Pipeline_TransactionUnpin(t *testing.T) {
	doer := &fakePipelineDoer{
		resp: &hrana.PipelineResponse{
			Baton: "m-baton",
			Results: []hrana.StreamResult{
				hrana.OkResult(hrana.StreamResponse{Type: "execute"}),
			},
		},
	}
	srv, mgr := newTestServer(t, doer)

	// Open stream.
	beginSQL := "BEGIN"
	w := postPipeline(t, srv, hrana.PipelineRequest{
		Requests: []hrana.StreamRequest{
			{Type: "execute", Stmt: &hrana.Stmt{SQL: &beginSQL}},
		},
	})
	require.Equal(t, http.StatusOK, w.Code)
	var resp1 hrana.PipelineResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp1))
	baton := resp1.Baton
	require.NotEmpty(t, baton)

	// Verify stream is pinned (get_autocommit returns false).
	w2 := postPipeline(t, srv, hrana.PipelineRequest{
		Baton:    baton,
		Requests: []hrana.StreamRequest{{Type: "get_autocommit"}},
	})
	require.Equal(t, http.StatusOK, w2.Code)
	var resp2 hrana.PipelineResponse
	require.NoError(t, json.NewDecoder(w2.Body).Decode(&resp2))
	baton = resp2.Baton
	require.Len(t, resp2.Results, 1)
	require.Equal(t, "ok", resp2.Results[0].Type)
	require.NotNil(t, resp2.Results[0].Response.IsAutocommit)
	assert.False(t, *resp2.Results[0].Response.IsAutocommit, "stream should be pinned after BEGIN")

	// COMMIT — should unpin.
	commitSQL := "COMMIT"
	w3 := postPipeline(t, srv, hrana.PipelineRequest{
		Baton:    baton,
		Requests: []hrana.StreamRequest{
			{Type: "execute", Stmt: &hrana.Stmt{SQL: &commitSQL}},
		},
	})
	require.Equal(t, http.StatusOK, w3.Code)
	var resp3 hrana.PipelineResponse
	require.NoError(t, json.NewDecoder(w3.Body).Decode(&resp3))
	baton = resp3.Baton

	// Verify stream is unpinned (get_autocommit returns true).
	w4 := postPipeline(t, srv, hrana.PipelineRequest{
		Baton:    baton,
		Requests: []hrana.StreamRequest{{Type: "get_autocommit"}},
	})
	require.Equal(t, http.StatusOK, w4.Code)
	var resp4 hrana.PipelineResponse
	require.NoError(t, json.NewDecoder(w4.Body).Decode(&resp4))
	require.Len(t, resp4.Results, 1)
	require.NotNil(t, resp4.Results[0].Response.IsAutocommit)
	assert.True(t, *resp4.Results[0].Response.IsAutocommit, "stream should be unpinned after COMMIT")

	_ = mgr
}

func TestServer_Pipeline_BadJSON(t *testing.T) {
	srv, _ := newTestServer(t, nil)

	r := httptest.NewRequest(http.MethodPost, "/v2/pipeline", strings.NewReader("{bad json"))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	mux := http.NewServeMux()
	srv.Register(mux)
	mux.ServeHTTP(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}
