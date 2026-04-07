package hrana_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hive_v2/orchestrator/internal/hrana"
)

func newTestClient(t *testing.T, handler http.Handler) *hrana.Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return hrana.NewClient(hrana.ClientConfig{BaseURL: srv.URL})
}

func TestClient_Pipeline_Success(t *testing.T) {
	t.Parallel()

	wantResp := hrana.PipelineResponse{
		Results: []hrana.StreamResult{
			hrana.OkResult(hrana.StreamResponse{Type: "close"}),
		},
	}

	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/v2/pipeline", r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(wantResp)
	}))

	req := &hrana.PipelineRequest{Requests: []hrana.StreamRequest{hrana.CloseRequest()}}
	resp, err := client.Pipeline(context.Background(), req)
	require.NoError(t, err)
	require.Len(t, resp.Results, 1)
	assert.Equal(t, "ok", resp.Results[0].Type)
}

func TestClient_Pipeline_AuthHeader(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer secret", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(hrana.PipelineResponse{})
	}))
	t.Cleanup(srv.Close)

	client := hrana.NewClient(hrana.ClientConfig{BaseURL: srv.URL, Token: "secret"})
	_, err := client.Pipeline(context.Background(), &hrana.PipelineRequest{})
	require.NoError(t, err)
}

func TestClient_Pipeline_HTTPError(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(hrana.Error{Message: "unauthorized"})
	}))

	_, err := client.Pipeline(context.Background(), &hrana.PipelineRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "401")
	assert.Contains(t, err.Error(), "unauthorized")
}

func TestClient_Pipeline_ContextCancelled(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// never responds
		<-r.Context().Done()
	}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.Pipeline(ctx, &hrana.PipelineRequest{})
	require.Error(t, err)
}

func TestClient_Health_OK(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/health", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))

	require.NoError(t, client.Health(context.Background()))
}

func TestClient_Health_Error(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))

	err := client.Health(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "503")
}

func TestClient_Dump_Success(t *testing.T) {
	t.Parallel()

	const dumpBody = "BEGIN TRANSACTION;\nCREATE TABLE t (id INTEGER);\nCOMMIT;\n"

	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/dump", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(dumpBody))
	}))

	got, err := client.Dump(context.Background())
	require.NoError(t, err)
	assert.Equal(t, dumpBody, got)
}

func TestClient_Version_Success(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/version", r.URL.Path)
		_, _ = w.Write([]byte("sqld 0.21.9"))
	}))

	ver, err := client.Version(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "sqld 0.21.9", ver)
}

func TestClient_Describe_Success(t *testing.T) {
	t.Parallel()

	descResult := hrana.DescribeResult{IsReadOnly: true}
	rawResult, _ := json.Marshal(descResult)

	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := hrana.PipelineResponse{
			Results: []hrana.StreamResult{
				hrana.OkResult(hrana.StreamResponse{
					Type:   "describe",
					Result: rawResult,
				}),
				hrana.OkResult(hrana.StreamResponse{Type: "close"}),
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))

	res, err := client.Describe(context.Background(), "SELECT 1")
	require.NoError(t, err)
	assert.True(t, res.IsReadOnly)
}

func TestClient_Describe_ServerError(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := hrana.PipelineResponse{
			Results: []hrana.StreamResult{
				hrana.ErrResult(hrana.NewError("syntax error")),
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))

	_, err := client.Describe(context.Background(), "BAD SQL")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "syntax error")
}
