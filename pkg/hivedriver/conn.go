package hivedriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log/slog"

	"hive/gen/hivepb"
	"hive/internal/sqlparse"
)

// Conn implements driver.Conn with read-local / write-remote split.
// database/sql calls ExecContext/QueryContext on the Conn even inside a
// transaction, so activeTxID must be kept here rather than only on Tx.
type Conn struct {
	remote      hivepb.HiveSQLClient
	localDB     *sql.DB
	log         *slog.Logger
	activeTxID  string
	activeTxOps *[]writeThroughOp // points to Tx.ops; nil outside a transaction
}

func newConn(remote hivepb.HiveSQLClient, localDB *sql.DB, log *slog.Logger) *Conn {
	return &Conn{remote: remote, localDB: localDB, log: log}
}

func (c *Conn) Prepare(_ string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (c *Conn) Close() error { return nil }

func (c *Conn) Begin() (driver.Tx, error) {
	return nil, driver.ErrSkip
}

func (c *Conn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	resp, err := c.remote.BeginTx(ctx, &hivepb.BeginTxRequest{})
	if err != nil {
		return nil, fmt.Errorf("hivedriver: begin tx: %w", err)
	}
	tx := newTx(c, resp.TxId)
	c.activeTxID = resp.TxId
	c.activeTxOps = &tx.ops
	return tx, nil
}

func (c *Conn) clearTx() {
	c.activeTxID = ""
	c.activeTxOps = nil
}

func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	pq, err := sqlparse.Parse(query)
	if err != nil {
		return nil, driver.ErrBadConn
	}

	protoArgs, err := namedValuesToProto(args)
	if err != nil {
		return nil, err
	}

	resp, err := c.remote.Execute(ctx, &hivepb.ExecuteRequest{
		Sql:        query,
		Args:       protoArgs,
		ReturnRows: true,
		TxId:       c.activeTxID,
	})
	if err != nil {
		return nil, fmt.Errorf("hivedriver: exec: %w", err)
	}

	if op, ok := buildWriteThroughOp(pq, query, resp); ok {
		if c.activeTxOps != nil {
			*c.activeTxOps = append(*c.activeTxOps, op)
		} else {
			if err := applyWriteThrough(ctx, c.localDB, []writeThroughOp{op}); err != nil {
				c.log.Error("hivedriver: write-through", "err", err)
			}
		}
	}

	return &execResult{
		rowsAffected: resp.RowsAffected,
		lastInsertID: resp.LastInsertId,
	}, nil
}

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	pq, err := sqlparse.Parse(query)
	if err != nil {
		return nil, driver.ErrBadConn
	}

	if pq.Type == sqlparse.QueryDML && pq.HasReturning {
		return c.execReturningQuery(ctx, query, args, pq)
	}
	return c.queryLocal(ctx, query, args)
}

func (c *Conn) execReturningQuery(ctx context.Context, query string, args []driver.NamedValue, pq sqlparse.ParsedQuery) (driver.Rows, error) {
	protoArgs, err := namedValuesToProto(args)
	if err != nil {
		return nil, err
	}

	resp, err := c.remote.Execute(ctx, &hivepb.ExecuteRequest{
		Sql:        query,
		Args:       protoArgs,
		ReturnRows: true,
		TxId:       c.activeTxID,
	})
	if err != nil {
		return nil, fmt.Errorf("hivedriver: exec returning: %w", err)
	}

	if op, ok := buildWriteThroughOp(pq, query, resp); ok {
		if c.activeTxOps != nil {
			*c.activeTxOps = append(*c.activeTxOps, op)
		} else {
			if err := applyWriteThrough(ctx, c.localDB, []writeThroughOp{op}); err != nil {
				c.log.Error("hivedriver: write-through returning", "err", err)
			}
		}
	}

	if resp.ReturnedMeta == nil {
		return newStaticRows(&hivepb.QueryMeta{}, nil), nil
	}
	return newStaticRows(resp.ReturnedMeta, resp.ReturnedRows), nil
}

func (c *Conn) queryLocal(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	sqlArgs := make([]any, len(args))
	for i, a := range args {
		sqlArgs[i] = a.Value
	}
	rows, err := c.localDB.QueryContext(ctx, query, sqlArgs...)
	if err != nil {
		return nil, fmt.Errorf("hivedriver: local query: %w", err)
	}
	return collectLocalRows(rows)
}

func collectLocalRows(rows *sql.Rows) (_ driver.Rows, err error) {
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("hivedriver: close rows: %w", closeErr)
		}
	}()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("hivedriver: columns: %w", err)
	}

	var result [][]driver.Value
	for rows.Next() {
		dest := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range dest {
			ptrs[i] = &dest[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("hivedriver: scan: %w", err)
		}
		row := make([]driver.Value, len(cols))
		for i, v := range dest {
			row[i] = v
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("hivedriver: rows: %w", err)
	}
	return newLocalRows(cols, result), nil
}

// buildWriteThroughOp constructs a writeThroughOp when local mirroring is needed.
// DDL is always mirrored (no rows required). DML is mirrored only when rows were returned.
func buildWriteThroughOp(pq sqlparse.ParsedQuery, rawSQL string, resp *hivepb.ExecuteResponse) (writeThroughOp, bool) {
	tableName := ""
	if len(pq.Tables) > 0 {
		tableName = pq.Tables[0]
	}
	if pq.Type == sqlparse.QueryDDL {
		return writeThroughOp{
			queryType: pq.Type,
			ddlAction: pq.DDLAction,
			tableName: tableName,
			rawSQL:    rawSQL,
		}, true
	}
	if resp.ReturnedMeta != nil && len(resp.ReturnedRows) > 0 {
		return writeThroughOp{
			meta:      resp.ReturnedMeta,
			rows:      resp.ReturnedRows,
			tableName: tableName,
			queryType: pq.Type,
			ddlAction: pq.DDLAction,
			rawSQL:    rawSQL,
		}, true
	}
	return writeThroughOp{}, false
}
