package sync

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	_ "modernc.org/sqlite"
)

const maxPushBodyBytes = 4 << 20

type Handler struct {
	dumper     DumpProvider
	logger     Logger
	generation atomic.Uint32
	maxFrameNo atomic.Uint32
}

type DumpProvider interface {
	Dump(ctx context.Context) (string, error)
}

type Logger interface {
	Info(msg string)
	Error(msg string, err error)
}

func New(d DumpProvider, l Logger) *Handler {
	h := &Handler{dumper: d, logger: l}
	h.generation.Store(1)
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	segs := pathSegments(r.URL.Path)
	if len(segs) == 0 {
		http.NotFound(w, r)
		return
	}
	switch segs[0] {
	case "info":
		if len(segs) != 1 {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		h.handleInfo(w, r)
	case "export":
		if len(segs) != 2 {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		h.handleExport(w, r, segs[1])
	case "sync":
		if len(segs) < 4 {
			http.NotFound(w, r)
			return
		}
		switch r.Method {
		case http.MethodGet:
			h.handlePull(w, r)
		case http.MethodPost:
			h.handlePush(w, r, segs)
		default:
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		}
	default:
		http.NotFound(w, r)
	}
}

func pathSegments(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return nil
	}
	return strings.Split(path, "/")
}

func (h *Handler) handleInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(struct {
		CurrentGeneration uint32 `json:"current_generation"`
	}{CurrentGeneration: h.generation.Load()}); err != nil {
		h.logError("sync info: encode", err)
	}
}

func (h *Handler) handleExport(w http.ResponseWriter, r *http.Request, genStr string) {
	if _, err := strconv.ParseUint(genStr, 10, 32); err != nil {
		http.NotFound(w, r)
		return
	}
	ctx := r.Context()
	sqlDump, err := h.dumper.Dump(ctx)
	if err != nil {
		h.logError("sync export: dump", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	db, err := h.openMemoryDBFromDump(ctx, sqlDump)
	if err != nil {
		h.logError("sync export: build memory db", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	defer func() { _ = db.Close() }()
	outPath, err := h.vacuumExportToPath(ctx, db)
	if err != nil {
		h.logError("sync export: materialize file", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	defer func() { _ = os.Remove(outPath) }()

	f, err := os.Open(outPath)
	if err != nil {
		h.logError("sync export: open result", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	defer func() { _ = f.Close() }()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, f); err != nil {
		h.logError("sync export: stream", err)
	}
}

func (h *Handler) openMemoryDBFromDump(ctx context.Context, sqlDump string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, fmt.Errorf("open memory db: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	for _, stmt := range splitStatements(sqlDump) {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}
		upper := strings.ToUpper(stmt)
		if upper == "BEGIN TRANSACTION" || upper == "COMMIT" || upper == "BEGIN" {
			continue
		}
		if strings.HasPrefix(upper, "INSERT INTO") {
			stmt = "INSERT OR IGNORE" + stmt[len("INSERT"):]
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			prefix := stmt
			if len(prefix) > 80 {
				prefix = prefix[:80]
			}
			h.logError("sync export: exec: "+prefix, err)
			continue
		}
	}
	return db, nil
}

func (h *Handler) vacuumExportToPath(ctx context.Context, db *sql.DB) (outPath string, err error) {
	outFile, err := os.CreateTemp("", "hive-sync-export-*.db")
	if err != nil {
		return "", fmt.Errorf("create temp: %w", err)
	}
	outPath = outFile.Name()
	if err := outFile.Close(); err != nil {
		_ = os.Remove(outPath)
		return "", fmt.Errorf("close temp: %w", err)
	}
	if err := os.Remove(outPath); err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("remove before vacuum: %w", err)
	}
	escaped := strings.ReplaceAll(outPath, "'", "''")
	if _, err := db.ExecContext(ctx, "VACUUM INTO '"+escaped+"'"); err != nil {
		_ = os.Remove(outPath)
		return "", fmt.Errorf("vacuum into: %w", err)
	}
	return outPath, nil
}

func (h *Handler) handlePull(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	if err := json.NewEncoder(w).Encode(struct {
		Generation uint32 `json:"generation"`
	}{Generation: h.generation.Load()}); err != nil {
		h.logError("sync pull: encode", err)
	}
}

func (h *Handler) handlePush(w http.ResponseWriter, r *http.Request, segs []string) {
	if _, err := parseUint32Seg(segs[1]); err != nil {
		http.NotFound(w, r)
		return
	}
	if _, err := parseUint32Seg(segs[2]); err != nil {
		http.NotFound(w, r)
		return
	}
	frameEnd, err := parseUint32Seg(segs[3])
	if err != nil {
		http.NotFound(w, r)
		return
	}
	n, err := io.Copy(io.Discard, io.LimitReader(r.Body, maxPushBodyBytes+1))
	if err != nil {
		h.logError("sync push: read body", fmt.Errorf("discard body: %w", err))
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	if n > maxPushBodyBytes {
		http.Error(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
		return
	}
	var maxFrame uint32
	if frameEnd > 0 {
		maxFrame = frameEnd - 1
	}
	h.maxFrameNo.Store(maxFrame)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(struct {
		Status     string  `json:"status"`
		Generation uint32  `json:"generation"`
		MaxFrameNo uint32  `json:"max_frame_no"`
		Baton      *string `json:"baton"`
	}{
		Status:     "ok",
		Generation: h.generation.Load(),
		MaxFrameNo: maxFrame,
		Baton:      nil,
	}); err != nil {
		h.logError("sync push: encode", err)
	}
}

func splitStatements(dump string) []string {
	stmts := make([]string, 0, strings.Count(dump, ";")+1)
	var buf strings.Builder
	inQuote := false
	for i := 0; i < len(dump); i++ {
		ch := dump[i]
		if ch == '\'' {
			if inQuote && i+1 < len(dump) && dump[i+1] == '\'' {
				buf.WriteByte(ch)
				buf.WriteByte(ch)
				i++
				continue
			}
			inQuote = !inQuote
		}
		if ch == ';' && !inQuote {
			s := strings.TrimSpace(buf.String())
			if s != "" {
				stmts = append(stmts, s)
			}
			buf.Reset()
			continue
		}
		buf.WriteByte(ch)
	}
	if s := strings.TrimSpace(buf.String()); s != "" {
		stmts = append(stmts, s)
	}
	return stmts
}

func parseUint32Seg(s string) (uint32, error) {
	u, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(u), nil
}

func (h *Handler) logError(msg string, err error) {
	if h.logger != nil {
		h.logger.Error(msg, err)
	}
}
