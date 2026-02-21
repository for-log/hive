package router

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"
)

const (
	routerHTTPReadWriteTimeout = 10 * time.Second
	routerHTTPStreamTimeout    = 5 * time.Minute // generous for large merged snapshots
	routerHTTPChunkSize        = 32 * 1024
)

type HTTPServer struct {
	merger *Merger
	mr     MasterRegistry
	log    *slog.Logger
}

func NewHTTPServer(merger *Merger, mr MasterRegistry, log *slog.Logger) *HTTPServer {
	return &HTTPServer{merger: merger, mr: mr, log: log}
}

func (s *HTTPServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /db/merged", s.handleMerged)
	mux.HandleFunc("GET /masters", s.handleMasters)
	return mux
}

// ListenAndServe blocks until ctx is cancelled or a fatal listen error occurs.
func (s *HTTPServer) ListenAndServe(ctx context.Context, addr string) error {
	srv := &http.Server{
		Addr:         addr,
		Handler:      s.Handler(),
		ReadTimeout:  routerHTTPReadWriteTimeout,
		WriteTimeout: routerHTTPStreamTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		if err := srv.Shutdown(context.Background()); err != nil {
			return fmt.Errorf("router/http: shutdown: %w", err)
		}
		return nil
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("router/http: listen: %w", err)
		}
		return nil
	}
}

func (s *HTTPServer) handleMerged(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	outPath, err := s.merger.Merge(ctx)
	if err != nil {
		s.log.Error("router/http: merge snapshots", "err", err)
		http.Error(w, "merge failed", http.StatusInternalServerError)
		return
	}
	defer func() {
		if removeErr := os.Remove(outPath); removeErr != nil && !os.IsNotExist(removeErr) {
			s.log.Error("router/http: remove merged snapshot", "path", outPath, "err", removeErr)
		}
	}()

	f, err := os.Open(outPath)
	if err != nil {
		s.log.Error("router/http: open merged snapshot", "err", err)
		http.Error(w, "open snapshot failed", http.StatusInternalServerError)
		return
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			s.log.Error("router/http: close merged snapshot", "err", closeErr)
		}
	}()

	info, err := f.Stat()
	if err != nil {
		s.log.Error("router/http: stat merged snapshot", "err", err)
		http.Error(w, "stat snapshot failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", `attachment; filename="merged.db"`)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", info.Size()))
	w.WriteHeader(http.StatusOK)

	buf := make([]byte, routerHTTPChunkSize)
	if _, err = io.CopyBuffer(w, f, buf); err != nil {
		s.log.Error("router/http: stream merged snapshot", "err", err) // likely client disconnect
	}
}

type masterInfoJSON struct {
	LastHeartbeat string `json:"last_heartbeat"`
	ID            string `json:"id"`
	GRPCAddr      string `json:"grpc_addr"`
	HTTPAddr      string `json:"http_addr"`
	Status        string `json:"status"`
}

func (s *HTTPServer) handleMasters(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	masters, err := s.mr.AliveMasters(ctx)
	if err != nil {
		s.log.Error("router/http: list masters", "err", err)
		http.Error(w, "list masters failed", http.StatusInternalServerError)
		return
	}

	out := make([]masterInfoJSON, len(masters))
	for i, m := range masters {
		out[i] = masterInfoJSON{
			ID:            m.ID,
			GRPCAddr:      m.GRPCAddr,
			HTTPAddr:      m.HTTPAddr,
			Status:        string(m.Status),
			LastHeartbeat: m.LastHeartbeat.UTC().Format(time.RFC3339),
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(out); err != nil {
		s.log.Error("router/http: encode masters", "err", err)
	}
}
