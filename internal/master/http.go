package master

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

const (
	maxRequestBodyBytes   = 1 << 20   // 1 MiB; guards against OOM on unexpected payloads
	httpSnapshotChunkSize = 32 * 1024 // 32 KiB
	httpReadWriteTimeout  = 10 * time.Second
)

type HTTPServer struct {
	db      *DB
	log     *slog.Logger
	snapDir string
	seq     atomic.Uint64
}

func NewHTTPServer(db *DB, snapDir string, log *slog.Logger) *HTTPServer {
	return &HTTPServer{db: db, snapDir: snapDir, log: log}
}

func (s *HTTPServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /db", s.handleSnapshot)
	return mux
}

func (s *HTTPServer) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	// GET has no body, but drain anyway as defence-in-depth against unexpected payloads.
	if _, err := io.Copy(io.Discard, io.LimitReader(r.Body, maxRequestBodyBytes)); err != nil {
		s.log.Error("master/http: drain request body", "err", err)
	}

	ctx := r.Context()
	seq := s.seq.Add(1)
	snapPath := filepath.Join(s.snapDir, fmt.Sprintf("http-snap-%d.db", seq))
	defer func() {
		if err := os.Remove(snapPath); err != nil && !os.IsNotExist(err) {
			s.log.Error("master/http: remove snapshot", "err", err, "path", snapPath)
		}
	}()

	if err := s.db.CreateSnapshot(ctx, snapPath); err != nil {
		s.log.Error("master/http: create snapshot", "err", err)
		http.Error(w, "snapshot failed", http.StatusInternalServerError)
		return
	}

	f, err := os.Open(snapPath)
	if err != nil {
		s.log.Error("master/http: open snapshot", "err", err)
		http.Error(w, "open snapshot failed", http.StatusInternalServerError)
		return
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			s.log.Error("master/http: close snapshot file", "err", closeErr)
		}
	}()

	info, err := f.Stat()
	if err != nil {
		s.log.Error("master/http: stat snapshot", "err", err)
		http.Error(w, "stat snapshot failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", `attachment; filename="snapshot.db"`)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", info.Size()))
	w.WriteHeader(http.StatusOK)

	buf := make([]byte, httpSnapshotChunkSize)
	if _, err = io.CopyBuffer(w, f, buf); err != nil {
		s.log.Error("master/http: stream snapshot", "err", err) // likely client disconnect
	}
}

// ListenAndServe blocks until ctx is cancelled or a fatal listen error occurs.
func (s *HTTPServer) ListenAndServe(ctx context.Context, addr string) error {
	srv := &http.Server{
		Addr:         addr,
		Handler:      s.Handler(),
		ReadTimeout:  httpReadWriteTimeout,
		WriteTimeout: 5 * httpReadWriteTimeout, // generous for large snapshots
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
			return fmt.Errorf("master/http: shutdown: %w", err)
		}
		return nil
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("master/http: listen: %w", err)
		}
		return nil
	}
}
