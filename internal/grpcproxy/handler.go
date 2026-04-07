package grpcproxy

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"golang.org/x/net/http2"

	"github.com/hive_v2/orchestrator/internal/router"
)

type Logger interface {
	Info(msg string)
	Error(msg string, err error)
}

type WriteReplicator interface {
	EnqueueGRPC(body []byte, path string, originMaster int)
}

type Config struct {
	Router     *router.Router
	Replicator WriteReplicator
	Logger     Logger
	Targets    []Target // one per master, indexed by master index
}

type Target struct {
	URL   string
	Proxy *httputil.ReverseProxy
}

func NewTarget(rawURL string) (Target, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return Target{}, err
	}
	transport := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
	}
	proxy := &httputil.ReverseProxy{
		Transport: transport,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(u)
			pr.Out.Host = u.Host
		},
	}
	return Target{URL: rawURL, Proxy: proxy}, nil
}

// Handler intercepts gRPC proxy.Proxy/* requests, extracts SQL text for
// routing, then forwards the raw gRPC body to the correct master.
type Handler struct {
	cfg Config
}

func NewHandler(cfg Config) *Handler {
	return &Handler{cfg: cfg}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	if path == "/proxy.Proxy/Execute" {
		h.handleExecute(w, r)
		return
	}

	// StreamExec, Describe, Disconnect — route to master[0] as default.
	if strings.HasPrefix(path, "/proxy.Proxy/") {
		h.logInfo("grpc proxy %s %s -> master[0] %s (default)", r.Method, path, h.cfg.Targets[0].URL)
		h.cfg.Targets[0].Proxy.ServeHTTP(w, r)
		return
	}
}

func (h *Handler) handleExecute(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		h.logError("grpc proxy: read body", err)
		http.Error(w, "read body", http.StatusBadGateway)
		return
	}

	queries := h.extractQueries(body)

	masterIdx := 0
	hasWrite := false
	stmtSummary := make([]string, len(queries))
	for i, q := range queries {
		stmtSummary[i] = truncate(q.SQL, 60)
		if !h.cfg.Router.IsReadOnly(q.SQL) {
			hasWrite = true
		}
	}
	if len(queries) > 0 {
		master, routeErr := h.cfg.Router.RouteQuery(queries[0].SQL, nil)
		if routeErr == nil {
			masterIdx = master.Index
		}
	}

	target := h.cfg.Targets[masterIdx]
	h.logInfo("grpc proxy Execute -> master[%d] %s stmts=%d sql=%s",
		masterIdx, target.URL, len(queries), truncate(joinStmts(stmtSummary), 120))

	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	target.Proxy.ServeHTTP(w, r)

	if h.cfg.Replicator != nil && hasWrite {
		h.cfg.Replicator.EnqueueGRPC(body, "/proxy.Proxy/Execute", masterIdx)
	}
}

// extractQueries decodes just enough of the gRPC body to extract SQL text
// for routing decisions. Parameters are NOT extracted — the raw body is
// forwarded to masters for both execution and replication.
func (h *Handler) extractQueries(body []byte) []QueryStmt {
	if len(body) < 5 {
		return nil
	}
	payload := body[5:] // skip 5-byte gRPC frame header
	req, err := DecodeProgramReq(payload)
	if err != nil {
		h.logError("grpc proxy: decode ProgramReq", err)
		return nil
	}
	return req.Queries
}

func (h *Handler) logInfo(format string, args ...any) {
	if h.cfg.Logger != nil {
		h.cfg.Logger.Info(fmt.Sprintf(format, args...))
	}
}

func (h *Handler) logError(msg string, err error) {
	if h.cfg.Logger != nil {
		h.cfg.Logger.Error(msg, err)
	}
}

func joinStmts(stmts []string) string {
	if len(stmts) == 0 {
		return "-"
	}
	if len(stmts) == 1 {
		return stmts[0]
	}
	return strings.Join(stmts, "; ")
}

func truncate(s string, maxLen int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
