package txlog

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type EntryType uint8

const (
	EntryBegin EntryType = iota + 1
	EntryStatement
	EntryCommitStart
	EntryMasterDone
	EntryComplete
	EntryRollback
)

// Entry is one NDJSON line in the transaction WAL.
type Entry struct {
	TxID      string    `json:"tx_id"`
	Timestamp int64     `json:"ts"`
	Type      EntryType `json:"type"`
	SQL       string    `json:"sql,omitempty"`
	MasterIdx *int      `json:"master,omitempty"`
}

// WAL is an append-only JSON-lines transaction log.
type WAL struct {
	mu   sync.Mutex
	path string
	file *os.File
	enc  *json.Encoder
}

// Open creates or opens the WAL file at path.
func Open(path string) (*WAL, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("txlog: mkdir: %w", err)
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("txlog: open %q: %w", path, err)
	}
	w := &WAL{
		path: path,
		file: f,
		enc:  json.NewEncoder(f),
	}
	w.enc.SetEscapeHTML(false)
	return w, nil
}

// Append writes one JSON object plus newline. Fsync runs only for EntryCommitStart.
func (w *WAL) Append(e Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return errors.New("txlog: closed")
	}
	if err := w.enc.Encode(e); err != nil {
		return fmt.Errorf("txlog: encode: %w", err)
	}
	if e.Type == EntryCommitStart {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("txlog: fsync: %w", err)
		}
	}
	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func (w *WAL) Path() string {
	return w.path
}

// ReadAll decodes all newline-delimited JSON entries from path.
func ReadAll(path string) ([]Entry, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("txlog: read %q: %w", path, err)
	}
	var out []Entry
	sc := bufio.NewScanner(bytes.NewReader(b))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var e Entry
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, fmt.Errorf("txlog: decode line: %w", err)
		}
		out = append(out, e)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("txlog: scan: %w", err)
	}
	return out, nil
}

// Compact rewrites the WAL, dropping all records belonging to transactions
// that reached EntryComplete or EntryRollback.
func (w *WAL) Compact() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entries, err := ReadAll(w.path)
	if err != nil {
		return err
	}
	closed := txIDsClosed(entries)
	var kept []Entry
	for _, e := range entries {
		if closed[e.TxID] {
			continue
		}
		kept = append(kept, e)
	}
	if len(kept) == len(entries) {
		return nil
	}

	dir := filepath.Dir(w.path)
	tmp, err := os.CreateTemp(dir, ".txwal-*")
	if err != nil {
		return fmt.Errorf("txlog: tempfile: %w", err)
	}
	tmpPath := tmp.Name()
	enc := json.NewEncoder(tmp)
	enc.SetEscapeHTML(false)
	for _, e := range kept {
		if err := enc.Encode(e); err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
			return fmt.Errorf("txlog: compact encode: %w", err)
		}
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("txlog: compact sync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("txlog: compact close temp: %w", err)
	}

	if err := os.Rename(tmpPath, w.path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("txlog: compact rename: %w", err)
	}

	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("txlog: compact reopen: %w", err)
	}
	if w.file != nil {
		_ = w.file.Close()
	}
	w.file = f
	w.enc = json.NewEncoder(f)
	w.enc.SetEscapeHTML(false)
	return nil
}

func txIDsClosed(entries []Entry) map[string]bool {
	closed := make(map[string]bool)
	for _, e := range entries {
		if e.TxID == "" {
			continue
		}
		switch e.Type {
		case EntryComplete, EntryRollback:
			closed[e.TxID] = true
		}
	}
	return closed
}
