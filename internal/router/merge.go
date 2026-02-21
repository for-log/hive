package router

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	_ "modernc.org/sqlite" // register "sqlite" driver
)

const (
	mergeHTTPTimeout      = 30 * time.Second
	mergeMaxSnapshotBytes = 1 << 30 // 1 GiB per master snapshot
	mergeChunkSize        = 32 * 1024
)

type SnapshotProvider interface {
	HTTPAddr() string
}

type httpSnapshotProvider struct {
	client *http.Client
	addr   string
}

func (p *httpSnapshotProvider) HTTPAddr() string { return p.addr }

func (p *httpSnapshotProvider) fetch(ctx context.Context) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.addr+"/db", nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http get: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		if closeErr := resp.Body.Close(); closeErr != nil {
			return nil, fmt.Errorf("unexpected status %d (body close: %w)", resp.StatusCode, closeErr)
		}
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return resp.Body, nil
}

// Merger downloads snapshots from all alive masters and merges them into a
// single SQLite file using ATTACH + INSERT OR IGNORE.
type Merger struct {
	mr      MasterRegistry
	log     *slog.Logger
	client  *http.Client
	snapDir string
	seq     atomic.Uint64
}

func NewMerger(mr MasterRegistry, snapDir string, log *slog.Logger) *Merger {
	return &Merger{
		mr:      mr,
		snapDir: snapDir,
		log:     log,
		client: &http.Client{
			Timeout: mergeHTTPTimeout,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 4,
			},
		},
	}
}

// Merge downloads a snapshot from every alive master, merges all data into a
// fresh SQLite file, and returns its path. The caller is responsible for
// deleting the file when done.
func (m *Merger) Merge(ctx context.Context) (string, error) {
	masters, err := m.mr.AliveMasters(ctx)
	if err != nil {
		return "", fmt.Errorf("list masters: %w", err)
	}
	if len(masters) == 0 {
		return "", fmt.Errorf("no alive masters")
	}

	seq := m.seq.Add(1)
	snapPaths := make([]string, len(masters))
	for i, master := range masters {
		snapPaths[i] = filepath.Join(m.snapDir, fmt.Sprintf("merge-%d-master-%s.db", seq, master.ID))
	}

	eg, egCtx := errgroup.WithContext(ctx)
	for i, master := range masters {
		i, master := i, master
		eg.Go(func() error {
			p := &httpSnapshotProvider{addr: "http://" + master.HTTPAddr, client: m.client}
			if err := m.downloadSnapshot(egCtx, p, snapPaths[i]); err != nil {
				return fmt.Errorf("download snapshot from %q: %w", master.ID, err)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		m.cleanupFiles(snapPaths)
		return "", err
	}

	outPath := filepath.Join(m.snapDir, fmt.Sprintf("merged-%d.db", seq))
	if err := m.mergeSnapshots(ctx, snapPaths, outPath); err != nil {
		m.cleanupFiles(snapPaths)
		if removeErr := os.Remove(outPath); removeErr != nil && !os.IsNotExist(removeErr) {
			m.log.Error("merger: remove partial merged db", "path", outPath, "err", removeErr)
		}
		return "", fmt.Errorf("merge snapshots: %w", err)
	}

	m.cleanupFiles(snapPaths)
	return outPath, nil
}

func (m *Merger) downloadSnapshot(ctx context.Context, p *httpSnapshotProvider, destPath string) error {
	body, err := p.fetch(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := body.Close(); closeErr != nil {
			m.log.Error("merger: close snapshot body", "err", closeErr)
		}
	}()

	f, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			m.log.Error("merger: close snapshot file", "err", closeErr)
		}
	}()

	buf := make([]byte, mergeChunkSize)
	if _, err := io.CopyBuffer(f, io.LimitReader(body, mergeMaxSnapshotBytes), buf); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}
	return nil
}

func (m *Merger) mergeSnapshots(ctx context.Context, snapPaths []string, outPath string) error {
	dsn := outPath + "?_pragma=journal_mode(WAL)&_pragma=foreign_keys(0)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("open merged db: %w", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			m.log.Error("merger: close merged db", "err", closeErr)
		}
	}()

	for i, snapPath := range snapPaths {
		alias := fmt.Sprintf("src%d", i)
		if err := m.attachAndCopy(ctx, db, snapPath, alias); err != nil {
			return fmt.Errorf("attach %q: %w", snapPath, err)
		}
	}
	return nil
}

func (m *Merger) attachAndCopy(ctx context.Context, db *sql.DB, snapPath, alias string) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf("ATTACH DATABASE %q AS %s", snapPath, alias)); err != nil {
		return fmt.Errorf("attach: %w", err)
	}
	defer func() {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("DETACH DATABASE %s", alias)); err != nil {
			m.log.Error("merger: detach database", "alias", alias, "err", err)
		}
	}()

	tables, err := m.listTables(ctx, db, alias)
	if err != nil {
		return fmt.Errorf("list tables in %s: %w", alias, err)
	}

	for _, table := range tables {
		if err := m.copyTable(ctx, db, alias, table); err != nil {
			return fmt.Errorf("copy table %q from %s: %w", table, alias, err)
		}
	}
	return nil
}

func (m *Merger) listTables(ctx context.Context, db *sql.DB, alias string) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		fmt.Sprintf("SELECT name FROM %s.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%%'", alias),
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			m.log.Error("merger: close rows", "err", closeErr)
		}
	}()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

func (m *Merger) copyTable(ctx context.Context, db *sql.DB, alias, table string) error {
	var createSQL string
	err := db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT sql FROM %s.sqlite_master WHERE type='table' AND name=?", alias),
		table,
	).Scan(&createSQL)
	if err != nil {
		return fmt.Errorf("get schema: %w", err)
	}

	// Rewrite "CREATE TABLE" → "CREATE TABLE IF NOT EXISTS" so re-running merge is safe.
	createIfNotExists := rewriteCreateIfNotExists(createSQL)
	if _, err := db.ExecContext(ctx, createIfNotExists); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	insertSQL := fmt.Sprintf("INSERT OR IGNORE INTO main.%q SELECT * FROM %s.%q", table, alias, table)
	if _, err := db.ExecContext(ctx, insertSQL); err != nil {
		return fmt.Errorf("insert rows: %w", err)
	}
	return nil
}

func (m *Merger) cleanupFiles(paths []string) {
	for _, p := range paths {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			m.log.Error("merger: remove temp snapshot", "path", p, "err", err)
		}
	}
}

func rewriteCreateIfNotExists(sql string) string {
	const (
		plain    = "CREATE TABLE "
		ifNotEx  = "CREATE TABLE IF NOT EXISTS "
		plainLen = len(plain)
	)
	if len(sql) >= plainLen && sql[:plainLen] == plain {
		return ifNotEx + sql[plainLen:]
	}
	return sql
}
