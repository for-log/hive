package hivedriver

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"
)

const (
	syncMaxBodyBytes = 1 << 30 // 1 GiB
	syncChunkSize    = 32 * 1024
	syncHTTPTimeout  = 5 * time.Minute
)

// Syncer downloads the merged snapshot from the router and replaces localDB
// contents via ATTACH + DELETE + INSERT (full table replacement per sync).
type Syncer struct {
	localDB    *sql.DB
	log        *slog.Logger
	client     *http.Client
	routerHTTP string
}

func newSyncer(localDB *sql.DB, routerHTTP string, log *slog.Logger) *Syncer {
	return &Syncer{
		localDB:    localDB,
		routerHTTP: routerHTTP,
		log:        log,
		client:     &http.Client{Timeout: syncHTTPTimeout},
	}
}

func (s *Syncer) Sync(ctx context.Context) error {
	tmpFile, err := os.CreateTemp("", "hive-sync-*.db")
	if err != nil {
		return fmt.Errorf("sync: create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer func() {
		if removeErr := os.Remove(tmpPath); removeErr != nil && !os.IsNotExist(removeErr) {
			s.log.Error("sync: remove temp file", "path", tmpPath, "err", removeErr)
		}
	}()

	if downloadErr := s.download(ctx, tmpFile); downloadErr != nil {
		if closeErr := tmpFile.Close(); closeErr != nil {
			return fmt.Errorf("sync: download: %v (also close: %w)", downloadErr, closeErr)
		}
		return fmt.Errorf("sync: download: %w", downloadErr)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("sync: close temp file: %w", err)
	}

	return s.applySnapshot(ctx, tmpPath)
}

func (s *Syncer) download(ctx context.Context, dst *os.File) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.routerHTTP+"/db/merged", nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("http get: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close body: %w", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	buf := make([]byte, syncChunkSize)
	if _, err := io.CopyBuffer(dst, io.LimitReader(resp.Body, syncMaxBodyBytes), buf); err != nil {
		return fmt.Errorf("write snapshot: %w", err)
	}
	return nil
}

// applySnapshot runs entirely on a single *sql.Conn so that ATTACH is visible
// to all subsequent queries and the transaction in the same session.
func (s *Syncer) applySnapshot(ctx context.Context, snapPath string) (err error) {
	conn, err := s.localDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire conn: %w", err)
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("release conn: %w", closeErr)
		}
	}()

	return s.applySnapshotOnConn(ctx, conn, snapPath)
}

func (s *Syncer) applySnapshotOnConn(ctx context.Context, conn *sql.Conn, snapPath string) (err error) {
	const alias = "snap"

	if _, attachErr := conn.ExecContext(ctx, fmt.Sprintf("ATTACH DATABASE %q AS %s", snapPath, alias)); attachErr != nil {
		return fmt.Errorf("attach: %w", attachErr)
	}
	defer func() {
		if _, detachErr := conn.ExecContext(ctx, fmt.Sprintf("DETACH DATABASE %s", alias)); detachErr != nil {
			s.log.Error("sync: detach", "alias", alias, "err", detachErr)
		}
	}()

	rows, err := conn.QueryContext(ctx,
		fmt.Sprintf("SELECT name, sql FROM %s.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%%'", alias),
	)
	if err != nil {
		return fmt.Errorf("list tables: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close rows: %w", closeErr)
		}
	}()

	type tableEntry struct {
		name      string
		createSQL string
	}
	var tables []tableEntry
	for rows.Next() {
		var e tableEntry
		if scanErr := rows.Scan(&e.name, &e.createSQL); scanErr != nil {
			return fmt.Errorf("scan table: %w", scanErr)
		}
		tables = append(tables, e)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return fmt.Errorf("rows error: %w", rowsErr)
	}
	// Close rows before beginning the transaction to avoid holding a read lock.
	if closeErr := rows.Close(); closeErr != nil && err == nil {
		return fmt.Errorf("close rows before tx: %w", closeErr)
	}

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				s.log.Error("sync: rollback", "err", rbErr)
			}
		}
	}()

	for _, tbl := range tables {
		createIfNotExists := rewriteCreateIfNotExists(tbl.createSQL)
		if _, err = tx.ExecContext(ctx, createIfNotExists); err != nil {
			return fmt.Errorf("create table %q: %w", tbl.name, err)
		}
		if _, err = tx.ExecContext(ctx,
			fmt.Sprintf("DELETE FROM main.%q", tbl.name),
		); err != nil {
			return fmt.Errorf("clear table %q: %w", tbl.name, err)
		}
		if _, err = tx.ExecContext(ctx,
			fmt.Sprintf("INSERT INTO main.%q SELECT * FROM %s.%q", tbl.name, alias, tbl.name),
		); err != nil {
			return fmt.Errorf("copy table %q: %w", tbl.name, err)
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// rewriteCreateIfNotExists is duplicated from router/merge.go to avoid
// a cross-package dependency between pkg/ and internal/.
func rewriteCreateIfNotExists(sqlStr string) string {
	const (
		plain    = "CREATE TABLE "
		ifNotEx  = "CREATE TABLE IF NOT EXISTS "
		plainLen = len(plain)
	)
	if len(sqlStr) >= plainLen && sqlStr[:plainLen] == plain {
		return ifNotEx + sqlStr[plainLen:]
	}
	return sqlStr
}
