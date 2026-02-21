package router

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	_ "modernc.org/sqlite" // register "sqlite" driver
)

type MasterStatus string

const (
	MasterStatusAlive    MasterStatus = "alive"
	MasterStatusDraining MasterStatus = "draining"
	MasterStatusDead     MasterStatus = "dead"
)

type MasterInfo struct {
	LastHeartbeat time.Time
	ID            string
	GRPCAddr      string
	HTTPAddr      string
	Status        MasterStatus
}

// MetaStore is a SQLite-backed store for the router's persistent state:
//   - table_mapping: which master owns each table
//   - masters: registered master instances and their health state
//
// MetaStore implements both TableResolver and MasterRegistry interfaces
// (defined in registry.go where they are consumed).
type MetaStore struct {
	db  *sql.DB
	log *slog.Logger
}

func OpenMeta(ctx context.Context, path string, log *slog.Logger) (*MetaStore, error) {
	if path == "" {
		return nil, fmt.Errorf("router/meta: path must not be empty")
	}

	dsn := path + "?_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("router/meta: open db: %w", err)
	}
	// Meta DB is low-traffic; a small pool is sufficient.
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)

	if err = db.PingContext(ctx); err != nil {
		closeErr := db.Close()
		return nil, fmt.Errorf("router/meta: ping db: %w (close: %v)", err, closeErr)
	}

	m := &MetaStore{db: db, log: log}
	if err = m.migrate(ctx); err != nil {
		closeErr := db.Close()
		return nil, fmt.Errorf("router/meta: migrate: %w (close: %v)", err, closeErr)
	}
	return m, nil
}

func (m *MetaStore) Close() error {
	if err := m.db.Close(); err != nil {
		return fmt.Errorf("router/meta: close: %w", err)
	}
	return nil
}

func (m *MetaStore) migrate(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS table_mapping (
    table_name TEXT PRIMARY KEY,
    master_id  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS masters (
    id             TEXT PRIMARY KEY,
    grpc_addr      TEXT NOT NULL,
    http_addr      TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'alive',
    last_heartbeat INTEGER NOT NULL DEFAULT 0
);`

	if _, err := m.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}
	return nil
}

// LookupMaster wraps the returned error with sql.ErrNoRows when the table has no mapping.
func (m *MetaStore) LookupMaster(ctx context.Context, tableName string) (string, error) {
	var masterID string
	err := m.db.QueryRowContext(ctx,
		`SELECT master_id FROM table_mapping WHERE table_name = ?`, tableName,
	).Scan(&masterID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("router/meta: table %q not mapped: %w", tableName, err)
	}
	if err != nil {
		return "", fmt.Errorf("router/meta: lookup master for %q: %w", tableName, err)
	}
	return masterID, nil
}

// RegisterTable returns an error if tableName is already owned by a different master.
func (m *MetaStore) RegisterTable(ctx context.Context, tableName, masterID string) error {
	_, err := m.db.ExecContext(ctx,
		`INSERT INTO table_mapping (table_name, master_id) VALUES (?, ?)
         ON CONFLICT(table_name) DO UPDATE SET master_id = excluded.master_id
         WHERE master_id = excluded.master_id`,
		tableName, masterID,
	)
	if err != nil {
		return fmt.Errorf("router/meta: register table %q → %q: %w", tableName, masterID, err)
	}

	// Verify the mapping is now correct (handles conflict with different master).
	owner, err := m.LookupMaster(ctx, tableName)
	if err != nil {
		return fmt.Errorf("router/meta: verify table %q: %w", tableName, err)
	}
	if owner != masterID {
		return fmt.Errorf("router/meta: table %q already owned by master %q", tableName, owner)
	}
	return nil
}

func (m *MetaStore) RemoveTable(ctx context.Context, tableName string) error {
	if _, err := m.db.ExecContext(ctx,
		`DELETE FROM table_mapping WHERE table_name = ?`, tableName,
	); err != nil {
		return fmt.Errorf("router/meta: remove table %q: %w", tableName, err)
	}
	return nil
}

func (m *MetaStore) RegisterMaster(ctx context.Context, id, grpcAddr, httpAddr string) error {
	_, err := m.db.ExecContext(ctx,
		`INSERT INTO masters (id, grpc_addr, http_addr, status, last_heartbeat)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(id) DO UPDATE SET
             grpc_addr      = excluded.grpc_addr,
             http_addr      = excluded.http_addr,
             status         = excluded.status,
             last_heartbeat = excluded.last_heartbeat`,
		id, grpcAddr, httpAddr, MasterStatusAlive, time.Now().UnixMilli(),
	)
	if err != nil {
		return fmt.Errorf("router/meta: register master %q: %w", id, err)
	}
	return nil
}

// UpdateHeartbeat only touches alive masters; draining/dead masters are not reset.
func (m *MetaStore) UpdateHeartbeat(ctx context.Context, masterID string) error {
	res, err := m.db.ExecContext(ctx,
		`UPDATE masters SET last_heartbeat = ? WHERE id = ? AND status = ?`,
		time.Now().UnixMilli(), masterID, MasterStatusAlive,
	)
	if err != nil {
		return fmt.Errorf("router/meta: update heartbeat %q: %w", masterID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("router/meta: rows affected: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("router/meta: master %q not found", masterID)
	}
	return nil
}

func (m *MetaStore) SetStatus(ctx context.Context, masterID string, status MasterStatus) error {
	res, err := m.db.ExecContext(ctx,
		`UPDATE masters SET status = ? WHERE id = ?`, status, masterID,
	)
	if err != nil {
		return fmt.Errorf("router/meta: set status %q → %q: %w", masterID, status, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("router/meta: rows affected: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("router/meta: master %q not found", masterID)
	}
	return nil
}

func (m *MetaStore) AliveMasters(ctx context.Context) ([]MasterInfo, error) {
	rows, err := m.db.QueryContext(ctx,
		`SELECT id, grpc_addr, http_addr, status, last_heartbeat
         FROM masters WHERE status = ?`, MasterStatusAlive,
	)
	if err != nil {
		return nil, fmt.Errorf("router/meta: alive masters: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			m.log.Error("router/meta: close rows", "err", closeErr)
		}
	}()

	var result []MasterInfo
	for rows.Next() {
		var mi MasterInfo
		var tsMs int64
		var statusStr string
		if err = rows.Scan(&mi.ID, &mi.GRPCAddr, &mi.HTTPAddr, &statusStr, &tsMs); err != nil {
			return nil, fmt.Errorf("router/meta: scan master: %w", err)
		}
		mi.Status = MasterStatus(statusStr)
		mi.LastHeartbeat = time.UnixMilli(tsMs)
		result = append(result, mi)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("router/meta: rows error: %w", err)
	}
	return result, nil
}

// MarkDeadMasters returns IDs of masters newly transitioned; caller should remove their table mappings.
func (m *MetaStore) MarkDeadMasters(ctx context.Context, timeout time.Duration) ([]string, error) {
	cutoff := time.Now().Add(-timeout).UnixMilli()

	rows, err := m.db.QueryContext(ctx,
		`UPDATE masters SET status = ?
         WHERE status = ? AND last_heartbeat < ?
         RETURNING id`,
		MasterStatusDead, MasterStatusAlive, cutoff,
	)
	if err != nil {
		return nil, fmt.Errorf("router/meta: mark dead masters: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			m.log.Error("router/meta: close rows", "err", closeErr)
		}
	}()

	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("router/meta: scan dead master id: %w", err)
		}
		ids = append(ids, id)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("router/meta: rows error: %w", err)
	}
	return ids, nil
}

func (m *MetaStore) TablesByMaster(ctx context.Context, masterID string) ([]string, error) {
	rows, err := m.db.QueryContext(ctx,
		`SELECT table_name FROM table_mapping WHERE master_id = ?`, masterID,
	)
	if err != nil {
		return nil, fmt.Errorf("router/meta: tables by master %q: %w", masterID, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			m.log.Error("router/meta: close rows", "err", closeErr)
		}
	}()

	var tables []string
	for rows.Next() {
		var t string
		if err = rows.Scan(&t); err != nil {
			return nil, fmt.Errorf("router/meta: scan table name: %w", err)
		}
		tables = append(tables, t)
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("router/meta: rows error: %w", err)
	}
	return tables, nil
}
