package hivedriver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"hive/gen/hivepb"
	"hive/internal/sqlparse"
)

type writeThroughOp struct {
	meta      *hivepb.QueryMeta
	tableName string
	rawSQL    string
	rows      []*hivepb.QueryRow
	queryType sqlparse.QueryType
	ddlAction sqlparse.DDLAction
}

func applyWriteThrough(ctx context.Context, localDB *sql.DB, ops []writeThroughOp) (err error) {
	if len(ops) == 0 {
		return nil
	}

	tx, err := localDB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("writethrough: begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				err = fmt.Errorf("%w (rollback: %v)", err, rbErr)
			}
		}
	}()

	for _, op := range ops {
		if err = applyOp(ctx, tx, op); err != nil {
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("writethrough: commit: %w", err)
	}
	return nil
}

func applyOp(ctx context.Context, tx *sql.Tx, op writeThroughOp) error {
	if op.queryType == sqlparse.QueryDDL {
		if _, err := tx.ExecContext(ctx, op.rawSQL); err != nil {
			return fmt.Errorf("writethrough: ddl %q: %w", op.rawSQL, err)
		}
		return nil
	}

	if op.meta == nil || len(op.rows) == 0 {
		return nil
	}

	switch op.ddlAction { //nolint:exhaustive
	default:
		return applyUpsert(ctx, tx, op)
	}
}

func applyUpsert(ctx context.Context, tx *sql.Tx, op writeThroughOp) (err error) {
	cols := make([]string, len(op.meta.Columns))
	for i, c := range op.meta.Columns {
		cols[i] = c.Name
	}

	placeholders := make([]string, len(cols))
	for i := range cols {
		placeholders[i] = "?"
	}

	query := fmt.Sprintf(
		"INSERT OR REPLACE INTO %q (%s) VALUES (%s)",
		op.tableName,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	)

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("writethrough: prepare upsert: %w", err)
	}
	defer func() {
		if closeErr := stmt.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("writethrough: close stmt: %w", closeErr)
		}
	}()

	for _, row := range op.rows {
		args := make([]any, len(row.Values))
		for i, v := range row.Values {
			args[i] = protoToGoValue(v)
		}
		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			return fmt.Errorf("writethrough: upsert row: %w", err)
		}
	}
	return nil
}
