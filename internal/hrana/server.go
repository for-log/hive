package hrana

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/hive_v2/orchestrator/internal/router"
	"github.com/hive_v2/orchestrator/internal/stream"
)

// MasterClientPool provides a Hrana client for a given master index.
// Defined here (consumer side) so the server package owns the abstraction.
type MasterClientPool interface {
	ClientFor(masterIndex int) MasterDoer
}

// MasterDoer combines all upstream operations a master client must support.
type MasterDoer interface {
	PipelineDoer
	CursorDoer
}

// PipelineDoer executes a single pipeline request against a master.
type PipelineDoer interface {
	Pipeline(ctx context.Context, req *PipelineRequest) (*PipelineResponse, error)
}

// CursorDoer opens a /v3/cursor streaming response from a master.
type CursorDoer interface {
	Cursor(ctx context.Context, req *CursorRequest) (*http.Response, error)
}

// DumpProvider returns the aggregated SQL dump of all masters.
type DumpProvider interface {
	Dump(ctx context.Context) (string, error)
}

// WriteReplicator fans out a write statement to all non-origin masters.
type WriteReplicator interface {
	Enqueue(sql string, originMaster int)
}

// Logger is a minimal logging interface so the server stays decoupled from
// any specific logging library.
type Logger interface {
	Info(msg string)
	Error(msg string, err error)
}

// ServerConfig groups all dependencies for the HTTP server.
type ServerConfig struct {
	Pool         MasterClientPool
	Router       *router.Router
	StreamMgr    *stream.Manager
	DumpProvider DumpProvider
	Replicator   WriteReplicator // may be nil (replication disabled)
	MaxBodyBytes int64
	Logger       Logger
	Version      string
}

// Server is the HRANA-compatible HTTP handler.
// Register it with http.ServeMux via Register.
type Server struct {
	cfg  ServerConfig
	proc *Processor
}

func NewServer(cfg ServerConfig) *Server {
	return &Server{
		cfg: cfg,
		proc: NewProcessor(ProcessorConfig{
			Pool:       cfg.Pool,
			Router:     cfg.Router,
			Replicator: cfg.Replicator,
			Logger:     cfg.Logger,
		}),
	}
}

// Register mounts all endpoints onto mux.
func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v2/pipeline", s.handlePipeline)
	mux.HandleFunc("POST /v3/pipeline", s.handlePipeline)
	mux.HandleFunc("POST /v3/cursor", s.handleCursor)
	mux.HandleFunc("POST /v1/execute", s.handleV1Execute)
	mux.HandleFunc("POST /v1/batch", s.handleV1Batch)
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /version", s.handleVersion)
	mux.HandleFunc("GET /dump", s.handleDump)
}

func (s *Server) handlePipeline(w http.ResponseWriter, r *http.Request) {
	var req PipelineRequest
	if err := decodeJSON(r, s.cfg.MaxBodyBytes, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	var strm *stream.Stream
	var err error
	if req.Baton == "" {
		strm, err = s.cfg.StreamMgr.Create()
	} else {
		strm, err = s.cfg.StreamMgr.Get(req.Baton)
	}
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}

	results, closed, err := s.executeRequests(r.Context(), strm, req.Requests)
	if err != nil {
		if s.cfg.Logger != nil {
			s.cfg.Logger.Error("pipeline execution error", err)
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	resp := PipelineResponse{Results: results}

	if closed {
		s.cfg.StreamMgr.Close(strm)
		// Closed stream: no baton returned (spec §close).
	} else {
		newBaton, rotErr := s.cfg.StreamMgr.Rotate(strm)
		if rotErr != nil {
			writeError(w, http.StatusInternalServerError, rotErr.Error())
			return
		}
		resp.Baton = newBaton
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) executeRequests(ctx context.Context, strm *stream.Stream, requests []StreamRequest) ([]StreamResult, bool, error) {
	return s.proc.ExecuteRequests(ctx, strm, requests)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleVersion(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, s.cfg.Version)
}

func (s *Server) handleDump(w http.ResponseWriter, r *http.Request) {
	dump, err := s.cfg.DumpProvider.Dump(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, dump)
}

func (s *Server) handleV1Execute(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Stmt Stmt `json:"stmt"`
	}
	if err := decodeJSON(r, s.cfg.MaxBodyBytes, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	sql := ""
	if req.Stmt.SQL != nil {
		sql = *req.Stmt.SQL
	}
	master, err := s.cfg.Router.RouteQuery(sql, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if s.cfg.Logger != nil {
		s.cfg.Logger.Info(fmt.Sprintf("query %s table=%s -> master[%d] %s sql=%s",
			sqlOpLabel(sql), sqlTableLabel(sql), master.Index, master.URL, truncateSQL(sql, 80)))
	}
	upReq := &PipelineRequest{
		Requests: []StreamRequest{{Type: "execute", Stmt: &req.Stmt}, CloseRequest()},
	}
	upResp, err := s.cfg.Pool.ClientFor(master.Index).Pipeline(r.Context(), upReq)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	if len(upResp.Results) == 0 || upResp.Results[0].Type != "ok" {
		msg := "upstream error"
		if len(upResp.Results) > 0 && upResp.Results[0].Error != nil {
			msg = upResp.Results[0].Error.Message
		}
		writeError(w, http.StatusInternalServerError, msg)
		return
	}
	if upResp.Results[0].Response == nil {
		writeError(w, http.StatusInternalServerError, "upstream returned nil response")
		return
	}
	var result StmtResult
	if err := json.Unmarshal(upResp.Results[0].Response.Result, &result); err != nil {
		writeError(w, http.StatusInternalServerError, "decode result: "+err.Error())
		return
	}
	s.proc.EnqueueReplication(sql, master.Index)
	writeJSON(w, http.StatusOK, struct {
		Result *StmtResult `json:"result"`
	}{Result: &result})
}

func (s *Server) handleV1Batch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Batch Batch `json:"batch"`
	}
	if err := decodeJSON(r, s.cfg.MaxBodyBytes, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	sql := ""
	if len(req.Batch.Steps) > 0 && req.Batch.Steps[0].Stmt.SQL != nil {
		sql = *req.Batch.Steps[0].Stmt.SQL
	}
	master, err := s.cfg.Router.RouteQuery(sql, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if s.cfg.Logger != nil {
		s.cfg.Logger.Info(fmt.Sprintf("batch %s table=%s -> master[%d] %s steps=%d",
			sqlOpLabel(sql), sqlTableLabel(sql), master.Index, master.URL, len(req.Batch.Steps)))
	}
	upReq := &PipelineRequest{
		Requests: []StreamRequest{{Type: "batch", Batch: &req.Batch}, CloseRequest()},
	}
	upResp, err := s.cfg.Pool.ClientFor(master.Index).Pipeline(r.Context(), upReq)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	if len(upResp.Results) == 0 || upResp.Results[0].Type != "ok" {
		msg := "upstream error"
		if len(upResp.Results) > 0 && upResp.Results[0].Error != nil {
			msg = upResp.Results[0].Error.Message
		}
		writeError(w, http.StatusInternalServerError, msg)
		return
	}
	if upResp.Results[0].Response == nil {
		writeError(w, http.StatusInternalServerError, "upstream returned nil response")
		return
	}
	var result BatchResult
	if err := json.Unmarshal(upResp.Results[0].Response.Result, &result); err != nil {
		writeError(w, http.StatusInternalServerError, "decode result: "+err.Error())
		return
	}
	for _, step := range req.Batch.Steps {
		if step.Stmt.SQL != nil {
			s.proc.EnqueueReplication(*step.Stmt.SQL, master.Index)
		}
	}
	writeJSON(w, http.StatusOK, struct {
		Result *BatchResult `json:"result"`
	}{Result: &result})
}

// handleCursor proxies a /v3/cursor request to the appropriate master and
// streams the chunked newline-delimited JSON response back to the client.
// The first line of the upstream response is a CursorResponseHeader containing
// the upstream baton; we replace it with a rotated client-side baton so the
// client can continue using the same stream for subsequent pipeline requests.
func (s *Server) handleCursor(w http.ResponseWriter, r *http.Request) {
	var req CursorRequest
	if err := decodeJSON(r, s.cfg.MaxBodyBytes, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	var strm *stream.Stream
	var err error
	if req.Baton == "" {
		strm, err = s.cfg.StreamMgr.Create()
	} else {
		strm, err = s.cfg.StreamMgr.Get(req.Baton)
	}
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}

	sql := ""
	if len(req.Batch.Steps) > 0 && req.Batch.Steps[0].Stmt.SQL != nil {
		sql = *req.Batch.Steps[0].Stmt.SQL
	}
	master, err := s.cfg.Router.RouteQuery(sql, strm.PinnedMaster)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	upReq := &CursorRequest{
		Baton: strm.MasterBatons[master.Index],
		Batch: req.Batch,
	}
	upResp, err := s.cfg.Pool.ClientFor(master.Index).Cursor(r.Context(), upReq)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer upResp.Body.Close()

	// First line is the header — parse it to capture the upstream baton and
	// emit a rewritten header with the rotated client baton.
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)
	flusher, canFlush := w.(http.Flusher)

	scanner := bufio.NewScanner(upResp.Body)
	scanner.Buffer(make([]byte, 1<<20), 1<<20) // 1 MiB per line
	firstLine := true

	for scanner.Scan() {
		line := scanner.Bytes()

		if firstLine {
			firstLine = false
			var hdr CursorResponseHeader
			if jsonErr := json.Unmarshal(line, &hdr); jsonErr == nil {
				if hdr.Baton != nil {
					strm.MasterBatons[master.Index] = *hdr.Baton
				} else {
					strm.MasterBatons[master.Index] = ""
				}
			}
			newBaton, rotErr := s.cfg.StreamMgr.Rotate(strm)
			if rotErr != nil {
				if s.cfg.Logger != nil {
					s.cfg.Logger.Error("cursor: baton rotate failed", rotErr)
				}
				return
			}
			rewritten, _ := json.Marshal(CursorResponseHeader{Baton: &newBaton})
			_, _ = w.Write(rewritten)
			_, _ = w.Write([]byte("\n"))
			if canFlush {
				flusher.Flush()
			}
			continue
		}

		_, _ = w.Write(line)
		_, _ = w.Write([]byte("\n"))
		if canFlush {
			flusher.Flush()
		}
	}
	if err := scanner.Err(); err != nil && s.cfg.Logger != nil {
		s.cfg.Logger.Error("cursor: upstream read error", err)
	}
}

func decodeJSON(r *http.Request, maxBytes int64, dst any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, maxBytes))
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("decode request: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, NewError(msg))
}
