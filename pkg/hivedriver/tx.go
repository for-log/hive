package hivedriver

import (
	"context"
	"fmt"

	"hive/gen/hivepb"
)

// Tx buffers write-through ops and applies them to localDB on Commit.
type Tx struct {
	conn *Conn
	txID string
	ops  []writeThroughOp
}

func newTx(conn *Conn, txID string) *Tx {
	return &Tx{conn: conn, txID: txID}
}

func (t *Tx) Commit() error {
	ctx := context.Background()
	t.conn.clearTx()

	_, err := t.conn.remote.CommitTx(ctx, &hivepb.TxRequest{TxId: t.txID})
	if err != nil {
		return fmt.Errorf("hivedriver: commit tx: %w", err)
	}

	if err := applyWriteThrough(ctx, t.conn.localDB, t.ops); err != nil {
		// Write-through failure is non-fatal: the remote commit succeeded.
		// The local snapshot will be corrected on the next Sync.
		t.conn.log.Error("hivedriver: write-through on commit", "err", err)
	}
	return nil
}

func (t *Tx) Rollback() error {
	ctx := context.Background()
	t.conn.clearTx()
	if _, err := t.conn.remote.RollbackTx(ctx, &hivepb.TxRequest{TxId: t.txID}); err != nil {
		return fmt.Errorf("hivedriver: rollback tx: %w", err)
	}
	return nil
}

type execResult struct {
	rowsAffected int64
	lastInsertID int64
}

func (r *execResult) LastInsertId() (int64, error) { return r.lastInsertID, nil }
func (r *execResult) RowsAffected() (int64, error) { return r.rowsAffected, nil }
