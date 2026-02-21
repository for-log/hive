// Package master implements the write-side SQLite engine for a hive-master node.
//
// Concurrency model:
//   - writeDB is owned exclusively by the writerLoop goroutine; no other code
//     may call writeDB methods directly.
//   - readDB is a *sql.DB opened with query_only(1) PRAGMA; any goroutine may
//     use it concurrently.
//   - All write requests are sent through writeCh (buffered) and the caller
//     blocks on the per-request result channel.
//   - Transactions: at most one active *sql.Tx lives inside writerLoop.
//     BeginTx/CommitTx/RollbackTx are also routed through writeCh so that
//     the transaction object never escapes the writer goroutine.
package master

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"runtime"

	_ "modernc.org/sqlite" // register "sqlite" driver
)

const (
	writeDSNTmpl = "%s?_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)"
	readDSNTmpl  = "%s?_pragma=query_only(1)&_pragma=busy_timeout(5000)"

	// Small buffer avoids head-of-line blocking on burst writes without consuming excessive memory.
	writeChanBuf = 64
)

type cmdKind uint8

const (
	cmdExec       cmdKind = iota
	cmdExecReturn         // rows scanned inside the writer goroutine
	cmdBeginTx
	cmdCommitTx
	cmdRollbackTx
	cmdSnapshot // VACUUM INTO destPath
)

// result must be a buffered channel of size 1 so the writer never blocks on send.
type writeRequest struct {
	ctx    context.Context //nolint:containedctx // intentional: carries per-request deadline
	result chan<- writeResult
	sql    string
	args   []any
	kind   cmdKind
}

type writeResult struct {
	err          error
	rows         [][]any
	cols         []string
	rowsAffected int64
	lastInsertID int64
}

type DB struct {
	readDB  *sql.DB
	writeDB *sql.DB
	log     *slog.Logger
	writeCh chan writeRequest
	path    string
}

// Open starts the background writer goroutine. Cancelling ctx stops the loop
// but does not release file handles — call Close for that.
func Open(ctx context.Context, path string, log *slog.Logger) (*DB, error) {
	if path == "" {
		return nil, fmt.Errorf("master/writer: path must not be empty")
	}

	writeDSN := fmt.Sprintf(writeDSNTmpl, path)
	writeDB, err := sql.Open("sqlite", writeDSN)
	if err != nil {
		return nil, fmt.Errorf("master/writer: open write db: %w", err)
	}
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)

	if err = writeDB.PingContext(ctx); err != nil {
		closeErr := writeDB.Close()
		return nil, fmt.Errorf("master/writer: ping write db: %w (close: %v)", err, closeErr)
	}

	readDSN := fmt.Sprintf(readDSNTmpl, path)
	readDB, err := sql.Open("sqlite", readDSN)
	if err != nil {
		closeErr := writeDB.Close()
		return nil, fmt.Errorf("master/writer: open read db: %w (close: %v)", err, closeErr)
	}
	maxReaders := runtime.GOMAXPROCS(0)
	readDB.SetMaxOpenConns(maxReaders)
	readDB.SetMaxIdleConns(maxReaders)

	if err = readDB.PingContext(ctx); err != nil {
		readCloseErr := readDB.Close()
		writeCloseErr := writeDB.Close()
		return nil, fmt.Errorf("master/writer: ping read db: %w (close: %v, %v)", err, readCloseErr, writeCloseErr)
	}

	db := &DB{
		path:    path,
		log:     log,
		readDB:  readDB,
		writeDB: writeDB,
		writeCh: make(chan writeRequest, writeChanBuf),
	}

	go db.writerLoop(ctx)

	return db, nil
}

// Close does not wait for in-flight writes; drain result channels first.
func (db *DB) Close() error {
	readErr := db.readDB.Close()
	writeErr := db.writeDB.Close()
	if readErr != nil {
		return fmt.Errorf("master/writer: close read db: %w", readErr)
	}
	if writeErr != nil {
		return fmt.Errorf("master/writer: close write db: %w", writeErr)
	}
	return nil
}

func (db *DB) Exec(ctx context.Context, query string, args ...any) (rowsAffected, lastInsertID int64, err error) {
	res, err := db.send(ctx, cmdExec, query, args)
	if err != nil {
		return 0, 0, err
	}
	return res.rowsAffected, res.lastInsertID, nil
}

// ExecReturning scans rows inside the writer goroutine so *sql.Rows never
// escapes to another goroutine. Returns (rows, columnNames, error).
func (db *DB) ExecReturning(ctx context.Context, query string, args ...any) ([][]any, []string, error) {
	res, err := db.send(ctx, cmdExecReturn, query, args)
	if err != nil {
		return nil, nil, err
	}
	return res.rows, res.cols, nil
}

// BeginTx returns an error if a transaction is already active.
func (db *DB) BeginTx(ctx context.Context) error {
	_, err := db.send(ctx, cmdBeginTx, "", nil)
	return err
}

// CommitTx returns an error if no transaction is active.
func (db *DB) CommitTx(ctx context.Context) error {
	_, err := db.send(ctx, cmdCommitTx, "", nil)
	return err
}

// RollbackTx returns an error if no transaction is active.
func (db *DB) RollbackTx(ctx context.Context) error {
	_, err := db.send(ctx, cmdRollbackTx, "", nil)
	return err
}

// CreateSnapshot uses VACUUM INTO, routed through the writer goroutine so it
// is never concurrent with an active transaction.
func (db *DB) CreateSnapshot(ctx context.Context, destPath string) error {
	if destPath == "" {
		return fmt.Errorf("master/writer: snapshot dest path must not be empty")
	}
	_, err := db.send(ctx, cmdSnapshot, destPath, nil)
	return err
}

func (db *DB) send(ctx context.Context, kind cmdKind, query string, args []any) (writeResult, error) {
	resultCh := make(chan writeResult, 1)
	req := writeRequest{
		ctx:    ctx,
		kind:   kind,
		sql:    query,
		args:   args,
		result: resultCh,
	}

	select {
	case db.writeCh <- req:
	case <-ctx.Done():
		return writeResult{}, fmt.Errorf("master/writer: send request: %w", ctx.Err())
	}

	select {
	case res := <-resultCh:
		return res, res.err
	case <-ctx.Done():
		return writeResult{}, fmt.Errorf("master/writer: wait result: %w", ctx.Err())
	}
}

func (db *DB) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	rows, err := db.readDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("master/writer: query: %w", err)
	}
	return rows, nil
}

func (db *DB) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return db.readDB.QueryRowContext(ctx, query, args...)
}

func (db *DB) writerLoop(ctx context.Context) {
	var activeTx *sql.Tx
	for {
		select {
		case <-ctx.Done():
			if activeTx != nil {
				if err := activeTx.Rollback(); err != nil {
					db.log.Error("master/writer: rollback on shutdown", "err", err)
				}
			}
			return
		case req := <-db.writeCh:
			var res writeResult
			switch req.kind {
			case cmdBeginTx:
				res = db.handleBegin(req.ctx, &activeTx)
			case cmdCommitTx:
				res = db.handleCommit(&activeTx)
			case cmdRollbackTx:
				res = db.handleRollback(&activeTx)
			case cmdExec:
				res = db.handleExec(req.ctx, activeTx, req.sql, req.args)
			case cmdExecReturn:
				res = db.handleExecReturn(req.ctx, activeTx, req.sql, req.args)
			case cmdSnapshot:
				res = db.handleSnapshot(req.ctx, req.sql)
			}
			req.result <- res
		}
	}
}

// handleBegin starts a new transaction; called only from writerLoop.
// The transaction is opened with context.Background() so that it is not
// cancelled when the initiating RPC request context expires. The caller
// (router or server) is responsible for explicit Commit/Rollback and for
// enforcing transaction timeouts at a higher level.
func (db *DB) handleBegin(_ context.Context, activeTx **sql.Tx) writeResult {
	if *activeTx != nil {
		return writeResult{err: fmt.Errorf("master/writer: transaction already active")}
	}
	tx, err := db.writeDB.BeginTx(context.Background(), nil)
	if err != nil {
		return writeResult{err: fmt.Errorf("master/writer: begin tx: %w", err)}
	}
	*activeTx = tx
	return writeResult{}
}

func (db *DB) handleCommit(activeTx **sql.Tx) writeResult {
	if *activeTx == nil {
		return writeResult{err: fmt.Errorf("master/writer: no active transaction")}
	}
	err := (*activeTx).Commit()
	*activeTx = nil
	if err != nil {
		return writeResult{err: fmt.Errorf("master/writer: commit tx: %w", err)}
	}
	return writeResult{}
}

func (db *DB) handleRollback(activeTx **sql.Tx) writeResult {
	if *activeTx == nil {
		return writeResult{err: fmt.Errorf("master/writer: no active transaction")}
	}
	err := (*activeTx).Rollback()
	*activeTx = nil
	if err != nil {
		return writeResult{err: fmt.Errorf("master/writer: rollback tx: %w", err)}
	}
	return writeResult{}
}

func (db *DB) handleExec(ctx context.Context, activeTx *sql.Tx, query string, args []any) writeResult {
	var sqlResult sql.Result
	var err error
	if activeTx != nil {
		sqlResult, err = activeTx.ExecContext(ctx, query, args...)
	} else {
		sqlResult, err = db.writeDB.ExecContext(ctx, query, args...)
	}
	if err != nil {
		return writeResult{err: fmt.Errorf("exec: %w", err)}
	}
	ra, err := sqlResult.RowsAffected()
	if err != nil {
		return writeResult{err: fmt.Errorf("rows affected: %w", err)}
	}
	lid, err := sqlResult.LastInsertId()
	if err != nil {
		return writeResult{err: fmt.Errorf("last insert id: %w", err)}
	}
	return writeResult{rowsAffected: ra, lastInsertID: lid}
}

func (db *DB) handleExecReturn(ctx context.Context, activeTx *sql.Tx, query string, args []any) writeResult {
	var rows *sql.Rows
	var err error
	if activeTx != nil {
		rows, err = activeTx.QueryContext(ctx, query, args...)
	} else {
		rows, err = db.writeDB.QueryContext(ctx, query, args...)
	}
	if err != nil {
		return writeResult{err: fmt.Errorf("exec returning: %w", err)}
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			db.log.Error("master/writer: close returning rows", "err", closeErr)
		}
	}()

	cols, err := rows.Columns()
	if err != nil {
		return writeResult{err: fmt.Errorf("exec returning columns: %w", err)}
	}

	var result [][]any
	for rows.Next() {
		dest := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range dest {
			ptrs[i] = &dest[i]
		}
		if err = rows.Scan(ptrs...); err != nil {
			return writeResult{err: fmt.Errorf("exec returning scan: %w", err)}
		}
		result = append(result, dest)
	}
	if err = rows.Err(); err != nil {
		return writeResult{err: fmt.Errorf("exec returning rows: %w", err)}
	}
	return writeResult{rows: result, cols: cols}
}

func (db *DB) handleSnapshot(ctx context.Context, destPath string) writeResult {
	// VACUUM INTO fails if the file already exists.
	if err := os.Remove(destPath); err != nil && !os.IsNotExist(err) {
		return writeResult{err: fmt.Errorf("remove old snapshot: %w", err)}
	}
	if _, err := db.writeDB.ExecContext(ctx, "VACUUM INTO ?", destPath); err != nil {
		return writeResult{err: fmt.Errorf("vacuum into: %w", err)}
	}
	return writeResult{}
}
