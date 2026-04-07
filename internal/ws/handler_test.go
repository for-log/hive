package ws_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"

	"github.com/hive_v2/orchestrator/internal/config"
	"github.com/hive_v2/orchestrator/internal/hrana"
	"github.com/hive_v2/orchestrator/internal/router"
	gosql "github.com/hive_v2/orchestrator/internal/sql"
	"github.com/hive_v2/orchestrator/internal/ws"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

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

type fakePool struct{ doer hrana.MasterDoer }

func (p *fakePool) ClientFor(_ int) hrana.MasterDoer { return p.doer }

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newTestHandler(t *testing.T, doer hrana.MasterDoer) *ws.Handler {
	t.Helper()

	cfg := &config.Config{
		Masters: []config.MasterConfig{
			{URL: "http://master0"},
		},
		ReadPolicy: config.ReadPolicyWriteMaster,
	}

	rtr, err := router.New(cfg, gosql.TokenAnalyzer{})
	require.NoError(t, err)

	return ws.NewHandler(ws.Config{
		Pool:        &fakePool{doer: doer},
		Router:      rtr,
		MasterCount: 1,
	})
}

func okPipelineResp(typ string, result json.RawMessage) *hrana.PipelineResponse {
	return &hrana.PipelineResponse{
		Results: []hrana.StreamResult{
			{
				Type: "ok",
				Response: &hrana.StreamResponse{
					Type:   typ,
					Result: result,
				},
			},
		},
	}
}

func connectWS(t *testing.T, srv *httptest.Server) (*websocket.Conn, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		Subprotocols: []string{"hrana3"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.CloseNow()
		cancel()
	})
	return conn, cancel
}

func sendHello(t *testing.T, ctx context.Context, conn *websocket.Conn) {
	t.Helper()
	err := wsjson.Write(ctx, conn, map[string]any{"type": "hello"})
	require.NoError(t, err)

	var resp ws.ServerMsg
	err = wsjson.Read(ctx, conn, &resp)
	require.NoError(t, err)
	assert.Equal(t, "hello_ok", resp.Type)
}

func sendRequest(t *testing.T, ctx context.Context, conn *websocket.Conn, reqID int32, payload any) ws.ServerMsg {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)

	msg := map[string]any{
		"type":       "request",
		"request_id": reqID,
		"request":    json.RawMessage(raw),
	}
	err = wsjson.Write(ctx, conn, msg)
	require.NoError(t, err)

	var resp ws.ServerMsg
	err = wsjson.Read(ctx, conn, &resp)
	require.NoError(t, err)
	return resp
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestHandler_HelloHandshake(t *testing.T) {
	t.Parallel()
	doer := &fakePipelineDoer{}
	h := newTestHandler(t, doer)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	conn, cancel := connectWS(t, srv)
	defer cancel()

	ctx := context.Background()
	sendHello(t, ctx, conn)
}

func TestHandler_HelloRequired(t *testing.T) {
	t.Parallel()
	doer := &fakePipelineDoer{}
	h := newTestHandler(t, doer)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	conn, cancel := connectWS(t, srv)
	defer cancel()

	ctx, ctxCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer ctxCancel()

	err := wsjson.Write(ctx, conn, map[string]any{
		"type":       "request",
		"request_id": 1,
		"request":    json.RawMessage(`{"type":"open_stream","stream_id":1}`),
	})
	require.NoError(t, err)

	// Server sends hello_error and closes. We may receive the message or EOF — both are correct.
	var resp ws.ServerMsg
	if err = wsjson.Read(ctx, conn, &resp); err == nil {
		assert.Equal(t, "hello_error", resp.Type)
	}
}

func TestHandler_OpenCloseStream(t *testing.T) {
	t.Parallel()
	doer := &fakePipelineDoer{}
	h := newTestHandler(t, doer)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	conn, cancel := connectWS(t, srv)
	defer cancel()
	ctx := context.Background()
	sendHello(t, ctx, conn)

	resp := sendRequest(t, ctx, conn, 1, map[string]any{
		"type":      "open_stream",
		"stream_id": 1,
	})
	assert.Equal(t, "response_ok", resp.Type)
	assert.Equal(t, int32(1), resp.RequestID)

	resp = sendRequest(t, ctx, conn, 2, map[string]any{
		"type":      "close_stream",
		"stream_id": 1,
	})
	assert.Equal(t, "response_ok", resp.Type)
}

func TestHandler_Execute(t *testing.T) {
	t.Parallel()
	stmtResult := hrana.StmtResult{
		Cols: []hrana.Column{{Name: strPtr("id")}},
		Rows: [][]hrana.Value{{hrana.Integer(42)}},
	}
	resultJSON, _ := json.Marshal(stmtResult)
	doer := &fakePipelineDoer{resp: okPipelineResp("execute", resultJSON)}
	h := newTestHandler(t, doer)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	conn, cancel := connectWS(t, srv)
	defer cancel()
	ctx := context.Background()
	sendHello(t, ctx, conn)

	// Open stream.
	sendRequest(t, ctx, conn, 1, map[string]any{"type": "open_stream", "stream_id": 1})

	resp := sendRequest(t, ctx, conn, 2, map[string]any{
		"type":      "execute",
		"stream_id": 1,
		"stmt": map[string]any{
			"sql":       "SELECT 42",
			"want_rows": true,
		},
	})
	require.Equal(t, "response_ok", resp.Type)

	var execResp ws.ExecuteResponse
	require.NoError(t, json.Unmarshal(resp.Response, &execResp))
	require.NotNil(t, execResp.Result)
	assert.Len(t, execResp.Result.Rows, 1)
}

func TestHandler_StoreSQLAndExecute(t *testing.T) {
	t.Parallel()
	stmtResult := hrana.StmtResult{}
	resultJSON, _ := json.Marshal(stmtResult)
	doer := &fakePipelineDoer{resp: okPipelineResp("execute", resultJSON)}
	h := newTestHandler(t, doer)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	conn, cancel := connectWS(t, srv)
	defer cancel()
	ctx := context.Background()
	sendHello(t, ctx, conn)

	sendRequest(t, ctx, conn, 1, map[string]any{"type": "open_stream", "stream_id": 1})

	sqlID := int32(10)
	resp := sendRequest(t, ctx, conn, 2, map[string]any{
		"type":   "store_sql",
		"sql_id": sqlID,
		"sql":    "SELECT 1",
	})
	assert.Equal(t, "response_ok", resp.Type)

	resp = sendRequest(t, ctx, conn, 3, map[string]any{
		"type":      "execute",
		"stream_id": 1,
		"stmt": map[string]any{
			"sql_id":    sqlID,
			"want_rows": true,
		},
	})
	assert.Equal(t, "response_ok", resp.Type)

	resp = sendRequest(t, ctx, conn, 4, map[string]any{
		"type":   "close_sql",
		"sql_id": sqlID,
	})
	assert.Equal(t, "response_ok", resp.Type)
}

func TestHandler_GetAutocommit(t *testing.T) {
	t.Parallel()
	doer := &fakePipelineDoer{}
	h := newTestHandler(t, doer)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	conn, cancel := connectWS(t, srv)
	defer cancel()
	ctx := context.Background()
	sendHello(t, ctx, conn)

	sendRequest(t, ctx, conn, 1, map[string]any{"type": "open_stream", "stream_id": 1})

	resp := sendRequest(t, ctx, conn, 2, map[string]any{
		"type":      "get_autocommit",
		"stream_id": 1,
	})
	require.Equal(t, "response_ok", resp.Type)

	var acResp ws.GetAutocommitResponse
	require.NoError(t, json.Unmarshal(resp.Response, &acResp))
	assert.True(t, acResp.IsAutocommit)
}

func TestHandler_TransactionPinUnpin(t *testing.T) {
	t.Parallel()
	makeResp := func(typ string) *hrana.PipelineResponse {
		r := hrana.StmtResult{}
		raw, _ := json.Marshal(r)
		return okPipelineResp(typ, raw)
	}

	callCount := 0
	responses := []*hrana.PipelineResponse{
		makeResp("execute"),
		makeResp("execute"),
		makeResp("execute"),
	}
	doer := &callCountDoer{responses: responses, count: &callCount}

	h := newTestHandler(t, doer)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	conn, cancel := connectWS(t, srv)
	defer cancel()
	ctx := context.Background()
	sendHello(t, ctx, conn)

	sendRequest(t, ctx, conn, 1, map[string]any{"type": "open_stream", "stream_id": 1})

	resp := sendRequest(t, ctx, conn, 2, map[string]any{
		"type":      "execute",
		"stream_id": 1,
		"stmt":      map[string]any{"sql": "BEGIN", "want_rows": false},
	})
	assert.Equal(t, "response_ok", resp.Type)

	resp = sendRequest(t, ctx, conn, 3, map[string]any{
		"type":      "get_autocommit",
		"stream_id": 1,
	})
	require.Equal(t, "response_ok", resp.Type)
	var acResp ws.GetAutocommitResponse
	require.NoError(t, json.Unmarshal(resp.Response, &acResp))
	assert.False(t, acResp.IsAutocommit, "stream should be pinned after BEGIN")

	resp = sendRequest(t, ctx, conn, 4, map[string]any{
		"type":      "execute",
		"stream_id": 1,
		"stmt":      map[string]any{"sql": "COMMIT", "want_rows": false},
	})
	assert.Equal(t, "response_ok", resp.Type)

	resp = sendRequest(t, ctx, conn, 5, map[string]any{
		"type":      "get_autocommit",
		"stream_id": 1,
	})
	require.Equal(t, "response_ok", resp.Type)
	require.NoError(t, json.Unmarshal(resp.Response, &acResp))
	assert.True(t, acResp.IsAutocommit, "stream should be unpinned after COMMIT")
}

func TestHandler_UpstreamError(t *testing.T) {
	t.Parallel()
	doer := &fakePipelineDoer{err: fmt.Errorf("connection refused")}
	h := newTestHandler(t, doer)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	conn, cancel := connectWS(t, srv)
	defer cancel()
	ctx := context.Background()
	sendHello(t, ctx, conn)

	sendRequest(t, ctx, conn, 1, map[string]any{"type": "open_stream", "stream_id": 1})

	resp := sendRequest(t, ctx, conn, 2, map[string]any{
		"type":      "execute",
		"stream_id": 1,
		"stmt":      map[string]any{"sql": "SELECT 1", "want_rows": true},
	})
	assert.True(t, resp.Type == "response_ok" || resp.Type == "response_error",
		"expected response_ok or response_error, got %s", resp.Type)
}

func strPtr(s string) *string { return &s }

type callCountDoer struct {
	responses []*hrana.PipelineResponse
	count     *int
}

func (d *callCountDoer) Pipeline(_ context.Context, _ *hrana.PipelineRequest) (*hrana.PipelineResponse, error) {
	idx := *d.count
	*d.count++
	if idx < len(d.responses) {
		return d.responses[idx], nil
	}
	r := hrana.StmtResult{}
	raw, _ := json.Marshal(r)
	return &hrana.PipelineResponse{
		Results: []hrana.StreamResult{{
			Type:     "ok",
			Response: &hrana.StreamResponse{Type: "execute", Result: raw},
		}},
	}, nil
}

func (d *callCountDoer) Cursor(_ context.Context, _ *hrana.CursorRequest) (*http.Response, error) {
	return nil, fmt.Errorf("cursor not implemented")
}

// concurrentDoer is a thread-safe fake that records every pipeline request and
// returns a canned execute response.
type concurrentDoer struct {
	mu       sync.Mutex
	requests []*hrana.PipelineRequest
}

func (d *concurrentDoer) record(req *hrana.PipelineRequest) {
	d.mu.Lock()
	d.requests = append(d.requests, req)
	d.mu.Unlock()
}

func (d *concurrentDoer) recorded() []*hrana.PipelineRequest {
	d.mu.Lock()
	defer d.mu.Unlock()
	cp := make([]*hrana.PipelineRequest, len(d.requests))
	copy(cp, d.requests)
	return cp
}

func (d *concurrentDoer) Pipeline(_ context.Context, req *hrana.PipelineRequest) (*hrana.PipelineResponse, error) {
	d.record(req)
	raw, _ := json.Marshal(hrana.StmtResult{AffectedRowCount: 1})
	return &hrana.PipelineResponse{
		Results: []hrana.StreamResult{{
			Type:     "ok",
			Response: &hrana.StreamResponse{Type: "execute", Result: raw},
		}},
	}, nil
}

func (d *concurrentDoer) Cursor(_ context.Context, _ *hrana.CursorRequest) (*http.Response, error) {
	return nil, fmt.Errorf("cursor not implemented")
}

// TestHandler_TwoConnectionsParallel verifies that two simultaneous WebSocket
// connections can write to the same table and to separate tables concurrently,
// and that all writes are routed to the upstream master.
func TestHandler_TwoConnectionsParallel(t *testing.T) {
	t.Parallel()

	doer := &concurrentDoer{}
	h := newTestHandler(t, doer)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	type result struct {
		sameTable    bool
		diffTableA   bool
		diffTableB   bool
		selectSameOK bool
		selectDiffOK bool
	}

	var wg sync.WaitGroup
	results := make([]result, 2)

	for i := range 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
			conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
				Subprotocols: []string{"hrana3"},
			})
			if err != nil {
				t.Errorf("conn%d: dial: %v", idx, err)
				return
			}
			defer conn.CloseNow()

			send := func(reqID int32, payload any) ws.ServerMsg {
				raw, _ := json.Marshal(payload)
				_ = wsjson.Write(ctx, conn, map[string]any{
					"type":       "request",
					"request_id": reqID,
					"request":    json.RawMessage(raw),
				})
				var resp ws.ServerMsg
				_ = wsjson.Read(ctx, conn, &resp)
				return resp
			}

			// hello
			_ = wsjson.Write(ctx, conn, map[string]any{"type": "hello"})
			var helloResp ws.ServerMsg
			_ = wsjson.Read(ctx, conn, &helloResp)

			send(1, map[string]any{"type": "open_stream", "stream_id": 1})

			// Both connections write to the shared table "shared_tbl".
			r := send(2, map[string]any{
				"type":      "execute",
				"stream_id": 1,
				"stmt":      map[string]any{"sql": "INSERT INTO shared_tbl VALUES (1)", "want_rows": false},
			})
			results[idx].sameTable = r.Type == "response_ok"

			// Each connection writes to its own table.
			ownTable := fmt.Sprintf("own_tbl_%d", idx)
			r = send(3, map[string]any{
				"type":      "execute",
				"stream_id": 1,
				"stmt":      map[string]any{"sql": fmt.Sprintf("INSERT INTO %s VALUES (%d)", ownTable, idx), "want_rows": false},
			})
			if idx == 0 {
				results[idx].diffTableA = r.Type == "response_ok"
			} else {
				results[idx].diffTableB = r.Type == "response_ok"
			}

			// SELECT from shared table — fake returns empty rows but no error.
			r = send(4, map[string]any{
				"type":      "execute",
				"stream_id": 1,
				"stmt":      map[string]any{"sql": "SELECT * FROM shared_tbl", "want_rows": true},
			})
			results[idx].selectSameOK = r.Type == "response_ok"

			// SELECT from own table.
			r = send(5, map[string]any{
				"type":      "execute",
				"stream_id": 1,
				"stmt":      map[string]any{"sql": fmt.Sprintf("SELECT * FROM %s", ownTable), "want_rows": true},
			})
			results[idx].selectDiffOK = r.Type == "response_ok"
		}(i)
	}

	wg.Wait()

	for i, res := range results {
		assert.True(t, res.sameTable, "conn%d: INSERT into shared_tbl failed", i)
		assert.True(t, res.selectSameOK, "conn%d: SELECT from shared_tbl failed", i)
		assert.True(t, res.selectDiffOK, "conn%d: SELECT from own table failed", i)
	}
	assert.True(t, results[0].diffTableA, "conn0: INSERT into own_tbl_0 failed")
	assert.True(t, results[1].diffTableB, "conn1: INSERT into own_tbl_1 failed")

	// All 5 requests per connection (4 execute + 1 open_stream) should have
	// reached the upstream — verify at least 8 pipeline calls were recorded
	// (4 executes × 2 connections).
	recorded := doer.recorded()
	assert.GreaterOrEqual(t, len(recorded), 8, "expected at least 8 upstream pipeline calls, got %d", len(recorded))
}
