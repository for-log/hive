// Example demonstrating two independent local nodes sharing data via the router.
//
// Node A: writes two rows, syncs, reads — sees its own writes immediately.
// Node B: reads before sync (empty), syncs, reads — now sees A's writes.
//
// Prerequisites: router on localhost:9000 (HTTP :8080), masters registered.
//
// Run:
//
//	go run ./examples/app/main.go
//
// Docker:
//
//	docker compose -f docker-compose.example.yaml up --build
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"hive/pkg/hivedriver"
)

const (
	syncDelay = 2 * time.Second
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	grpcAddr := envOr("HIVE_GRPC_ADDR", "localhost:9000")
	httpAddr := envOr("HIVE_HTTP_ADDR", "localhost:8080")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := run(ctx, grpcAddr, httpAddr, log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, grpcAddr, httpAddr string, log *slog.Logger) error {
	nodeA, dbA, err := openNode(grpcAddr, httpAddr, "/tmp/hive-node-a.db", log.With("node", "A"))
	if err != nil {
		return fmt.Errorf("open node A: %w", err)
	}
	defer closeNode(nodeA, dbA, log.With("node", "A"))

	nodeB, dbB, err := openNode(grpcAddr, httpAddr, "/tmp/hive-node-b.db", log.With("node", "B"))
	if err != nil {
		return fmt.Errorf("open node B: %w", err)
	}
	defer closeNode(nodeB, dbB, log.With("node", "B"))

	if _, err := dbA.ExecContext(ctx,
		`CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY, author TEXT NOT NULL, msg TEXT NOT NULL)`,
	); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	// aReady signals that A has written and synced; B waits for it before syncing.
	aReady := make(chan struct{})

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error { return runNodeA(gCtx, dbA, nodeA, log.With("node", "A"), aReady) })
	g.Go(func() error { return runNodeB(gCtx, dbB, nodeB, log.With("node", "B"), aReady) })

	return g.Wait()
}

// runNodeA writes two events, syncs, then reads.
func runNodeA(ctx context.Context, db *sql.DB, node *hivedriver.Connector, log *slog.Logger, ready chan<- struct{}) error {
	log.Info("--- writing events ---")
	for _, msg := range []string{"hello from A", "second event from A"} {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO events (author, msg) VALUES (?, ?)`, "node-A", msg,
		); err != nil {
			return fmt.Errorf("insert: %w", err)
		}
		log.Info("wrote", "msg", msg)
	}

	log.Info("--- syncing local snapshot ---")
	if err := syncNode(node); err != nil {
		return fmt.Errorf("sync: %w", err)
	}

	close(ready) // signal B that writes are committed and snapshot is available

	log.Info("--- reading local snapshot (should see own writes) ---")
	return queryEvents(ctx, db, log)
}

// runNodeB reads before sync (expects empty), waits for A, syncs, reads again.
func runNodeB(ctx context.Context, db *sql.DB, node *hivedriver.Connector, log *slog.Logger, ready <-chan struct{}) error {
	log.Info("--- reading before sync (expect: empty — table not yet in local snapshot) ---")
	if err := queryEvents(ctx, db, log); err != nil && !isNoSuchTable(err) {
		return err
	}

	log.Info("--- waiting for node A to write and sync ---")
	select {
	case <-ready:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Small delay so the router's HTTP snapshot reflects A's committed data.
	time.Sleep(syncDelay)

	log.Info("--- syncing local snapshot ---")
	if err := syncNode(node); err != nil {
		return fmt.Errorf("sync: %w", err)
	}

	log.Info("--- reading after sync (expect: A's events) ---")
	if err := queryEvents(ctx, db, log); err != nil {
		return err
	}

	log.Info("--- writing own event ---")
	if _, err := db.ExecContext(ctx,
		`INSERT INTO events (author, msg) VALUES (?, ?)`, "node-B", "hello from B",
	); err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	if err := syncNode(node); err != nil {
		return fmt.Errorf("sync after B write: %w", err)
	}

	log.Info("--- reading after B's write + sync (expect: A's + B's events) ---")
	return queryEvents(ctx, db, log)
}

func queryEvents(ctx context.Context, db *sql.DB, log *slog.Logger) error {
	rows, err := db.QueryContext(ctx, `SELECT id, author, msg FROM events ORDER BY id`)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			log.Error("close rows", "err", closeErr)
		}
	}()

	count := 0
	for rows.Next() {
		var id int64
		var author, msg string
		if err := rows.Scan(&id, &author, &msg); err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		log.Info("  event", "id", id, "author", author, "msg", msg)
		count++
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows: %w", err)
	}
	if count == 0 {
		log.Info("  (no rows)")
	}
	return nil
}

func openNode(grpcAddr, httpAddr, localDB string, log *slog.Logger) (*hivedriver.Connector, *sql.DB, error) {
	dsn := fmt.Sprintf("%s?local_db=%s&http_addr=%s&log_level=info", grpcAddr, localDB, httpAddr)
	connector, err := hivedriver.NewConnector(dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("new connector: %w", err)
	}
	db := sql.OpenDB(connector)
	log.Info("connected", "grpc", grpcAddr, "http", httpAddr, "local_db", localDB)
	return connector, db, nil
}

// syncNode uses an independent context so that errgroup cancellation
// (triggered by the other goroutine finishing) does not abort the HTTP download.
func syncNode(node *hivedriver.Connector) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return node.Sync(ctx)
}

func closeNode(node *hivedriver.Connector, db *sql.DB, log *slog.Logger) {
	if err := db.Close(); err != nil {
		log.Error("close db", "err", err)
	}
	if err := node.Close(); err != nil {
		log.Error("close connector", "err", err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func isNoSuchTable(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no such table")
}
