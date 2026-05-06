package hrana

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/hive_v2/orchestrator/internal/router"
	gosql "github.com/hive_v2/orchestrator/internal/sql"
	"github.com/hive_v2/orchestrator/internal/stream"
	"github.com/hive_v2/orchestrator/internal/transaction"
	"github.com/hive_v2/orchestrator/internal/txlog"
)

// ProcessorConfig groups the dependencies needed to process Hrana requests.
type ProcessorConfig struct {
	Pool          MasterClientPool
	Router        *router.Router
	Replicator    WriteReplicator
	TxLog         *txlog.WAL // optional transaction WAL
	Logger        Logger
	CommitTimeout time.Duration
}

// Processor routes and executes Hrana stream requests against the master pool.
// Callers must serialise access per stream.Stream; the Processor itself is stateless.
type Processor struct {
	cfg ProcessorConfig
}

func NewProcessor(cfg ProcessorConfig) *Processor {
	if cfg.CommitTimeout == 0 {
		cfg.CommitTimeout = 30 * time.Second
	}
	return &Processor{cfg: cfg}
}

// ExecuteRequests processes each request in order and returns results, a closed
// flag (true when a "close" request was seen), and any fatal routing error.
func (p *Processor) ExecuteRequests(ctx context.Context, strm *stream.Stream, requests []StreamRequest) ([]StreamResult, bool, error) {
	results := make([]StreamResult, 0, len(requests))
	closed := false

	for _, req := range requests {
		if req.Type == "close" {
			results = append(results, OkResult(StreamResponse{Type: "close"}))
			closed = true
			continue
		}

		if req.Type == "store_sql" {
			results = append(results, p.HandleStoreSql(strm, req))
			continue
		}

		if req.Type == "close_sql" {
			results = append(results, p.HandleCloseSql(strm, req))
			continue
		}

		if req.Type == "get_autocommit" {
			isAutocommit := !strm.InTransaction()
			results = append(results, OkResult(StreamResponse{
				Type:         "get_autocommit",
				IsAutocommit: &isAutocommit,
			}))
			continue
		}

		result, err := p.ProxyRequest(ctx, strm, req)
		if err != nil {
			return nil, false, err
		}
		results = append(results, result)
	}

	return results, closed, nil
}

// ProxyRequest routes a single StreamRequest to the correct master, updates
// transaction pin state on success, and enqueues replication for writes.
func (p *Processor) ProxyRequest(ctx context.Context, strm *stream.Stream, req StreamRequest) (StreamResult, error) {
	sql := p.ResolveSQL(strm, req)
	info := gosql.Analyze(sql)

	isExecLike := req.Type == "execute" || req.Type == "batch" || req.Type == "sequence"

	inBufferedTxn := strm.TxBuffer != nil && strm.TxBuffer.Active
	if p.cfg.TxLog != nil && inBufferedTxn && isExecLike && info.IsTxEnd && info.Type != gosql.QueryTypeTxRollback && !p.lazyCrossTxActive(strm) {
		p.appendTxLog(strm, txlog.EntryCommitStart, sql, nil)
	}

	if isExecLike && p.cfg.Router.CrossMasterEnabled() && info.IsTxBegin {
		return p.handleCrossMasterBegin(ctx, strm, req, sql)
	}

	if isExecLike && p.lazyCrossTxActive(strm) && info.IsTxEnd {
		if info.Type == gosql.QueryTypeTxRollback {
			return p.handleCrossMasterRollback(ctx, strm, req, sql)
		}
		return p.handleCrossMasterCommit(ctx, strm, req, sql)
	}

	routePerStmt := p.lazyCrossTxActive(strm)
	master, err := p.cfg.Router.RouteQuery(sql, strm.PinnedMaster, routePerStmt)
	if err != nil {
		return ErrResult(NewError(err.Error())), nil
	}

	upReqEntry := req
	if req.Stmt != nil && req.Stmt.SQLId != nil && sql != "" {
		upReqEntry.Stmt = &Stmt{
			SQL:       &sql,
			Args:      req.Stmt.Args,
			NamedArgs: req.Stmt.NamedArgs,
			WantRows:  req.Stmt.WantRows,
		}
	}
	if req.Batch != nil {
		expandedSteps := make([]BatchStep, len(req.Batch.Steps))
		copy(expandedSteps, req.Batch.Steps)
		for i, step := range expandedSteps {
			if step.Stmt.SQLId != nil && step.Stmt.SQL == nil {
				resolved := strm.SQLStore.Load(*step.Stmt.SQLId)
				expandedSteps[i].Stmt = Stmt{
					SQL:       &resolved,
					Args:      step.Stmt.Args,
					NamedArgs: step.Stmt.NamedArgs,
					WantRows:  step.Stmt.WantRows,
				}
			}
		}
		expandedBatch := *req.Batch
		expandedBatch.Steps = expandedSteps
		upReqEntry.Batch = &expandedBatch
	}

	clientIdx := 0
	prependBegin := isExecLike && p.lazyCrossTxActive(strm) && !info.IsReadOnly && !strm.TxBuffer.MasterHasOpenTx(master.Index)
	inTx := p.lazyCrossTxActive(strm) || strm.PinnedMaster != nil
	keepOpen := inTx || (isExecLike && info.IsTxBegin)

	var requests []StreamRequest
	if prependBegin {
		beginSQL := strm.TxBuffer.BeginSQL
		if beginSQL == "" {
			beginSQL = "BEGIN"
		}
		requests = []StreamRequest{
			ExecuteRequest(beginSQL, false),
			upReqEntry,
		}
		clientIdx = 1
	} else if keepOpen {
		requests = []StreamRequest{upReqEntry}
	} else {
		requests = []StreamRequest{upReqEntry, CloseRequest()}
	}

	upReq := &PipelineRequest{
		Baton:    strm.MasterBatons[master.Index],
		Requests: requests,
	}

	if p.cfg.Logger != nil {
		p.cfg.Logger.Info(fmt.Sprintf("query %s table=%s -> master[%d] %s sql=%s",
			sqlOpLabel(sql), sqlTableLabel(sql), master.Index, master.URL, truncateSQL(sql, 80)))
	}

	client := p.cfg.Pool.ClientFor(master.Index)
	upResp, err := client.Pipeline(ctx, upReq)
	if err != nil {
		if p.cfg.Logger != nil {
			p.cfg.Logger.Error(fmt.Sprintf("upstream[%d] pipeline error (type=%s sql=%q baton=%q)", master.Index, req.Type, sql, upReq.Baton), err)
		}
		return ErrResult(NewError(fmt.Sprintf("upstream error: %s", err))), nil
	}

	if len(upResp.Results) == 0 {
		return ErrResult(NewError("upstream returned empty results")), nil
	}
	if clientIdx >= len(upResp.Results) {
		return ErrResult(NewError("upstream returned unexpected result count")), nil
	}
	for _, res := range upResp.Results {
		if res.Type != "ok" {
			msg := "upstream error"
			if res.Error != nil {
				msg = res.Error.Message
			}
			return ErrResult(NewError(msg)), nil
		}
	}

	strm.MasterBatons[master.Index] = upResp.Baton

	if prependBegin {
		strm.TxBuffer.MarkMasterTxOpen(master.Index)
	}

	result := upResp.Results[clientIdx]

	UpdateTxState(strm, sql, master)

	if result.Type == "ok" && isExecLike && info.IsTxBegin && p.cfg.Replicator != nil {
		if strm.TxBuffer == nil || !strm.TxBuffer.Active {
			p.initReplicationTxBuffer(strm, sql, master.Index)
		}
	}

	writeRepl := result.Type == "ok" && (req.Type == "execute" || req.Type == "batch" || req.Type == "sequence")

	if writeRepl {
		if strm.TxBuffer != nil && strm.TxBuffer.Active && info.IsTxEnd {
			if info.Type == gosql.QueryTypeTxRollback {
				p.appendTxLog(strm, txlog.EntryRollback, sql, nil)
				strm.TxBuffer.Reset()
				strm.TxWALID = ""
			} else {
				m := master.Index
				p.appendTxLog(strm, txlog.EntryMasterDone, "", &m)
				p.flushTxReplication(strm)
				p.appendTxLog(strm, txlog.EntryComplete, "", nil)
				strm.TxBuffer.Reset()
				strm.TxWALID = ""
			}
		} else {
			inBufferedTxn := strm.TxBuffer != nil && strm.TxBuffer.Active
			if inBufferedTxn && !info.IsReadOnly && !info.IsTxEnd {
				p.bufferTxWrites(strm, req, sql, master.Index)
			} else if !inBufferedTxn && !info.IsTxEnd {
				p.replicateWriteStmts(strm, req, sql, master.Index)
			}
		}
	}

	return result, nil
}

func (p *Processor) lazyCrossTxActive(strm *stream.Stream) bool {
	return p.cfg.Router.CrossMasterEnabled() && strm.LazyCrossTxActive()
}

func (p *Processor) handleCrossMasterBegin(ctx context.Context, strm *stream.Stream, req StreamRequest, sql string) (StreamResult, error) {
	if strm.TxBuffer != nil && strm.TxBuffer.Active {
		idxs := strm.TxBuffer.OpenMasterIndices()
		if len(idxs) == 0 {
			return ErrResult(NewError("transaction already active")), nil
		}
		master := idxs[0]
		upReq := &PipelineRequest{
			Baton:    strm.MasterBatons[master],
			Requests: []StreamRequest{req},
		}
		upResp, err := p.cfg.Pool.ClientFor(master).Pipeline(ctx, upReq)
		if err != nil {
			return ErrResult(NewError(fmt.Sprintf("upstream error: %s", err))), nil
		}
		if len(upResp.Results) > 0 {
			strm.MasterBatons[master] = upResp.Baton
			return upResp.Results[0], nil
		}
		return ErrResult(NewError("upstream returned empty results")), nil
	}
	if strm.TxBuffer == nil {
		strm.TxBuffer = transaction.NewTxBuffer()
	} else {
		strm.TxBuffer.Reset()
	}
	strm.TxBuffer.Active = true
	strm.TxBuffer.BeginSQL = sql

	master, err := p.cfg.Router.RouteQuery(sql, nil, false)
	if err != nil {
		strm.TxBuffer.Reset()
		strm.TxWALID = ""
		return ErrResult(NewError(err.Error())), nil
	}

	upReq := &PipelineRequest{
		Baton:    strm.MasterBatons[master.Index],
		Requests: []StreamRequest{req},
	}
	client := p.cfg.Pool.ClientFor(master.Index)
	upResp, err := client.Pipeline(ctx, upReq)
	if err != nil {
		strm.TxBuffer.Reset()
		strm.TxWALID = ""
		return ErrResult(NewError(fmt.Sprintf("upstream error: %s", err))), nil
	}
	if len(upResp.Results) == 0 || upResp.Results[0].Type != "ok" {
		strm.TxBuffer.Reset()
		strm.TxWALID = ""
		msg := "upstream error"
		if len(upResp.Results) > 0 && upResp.Results[0].Error != nil {
			msg = upResp.Results[0].Error.Message
		}
		return ErrResult(NewError(msg)), nil
	}

	strm.MasterBatons[master.Index] = upResp.Baton
	strm.TxBuffer.MarkMasterTxOpen(master.Index)

	if p.cfg.TxLog != nil {
		strm.TxWALID = strm.ClientBaton
		mi := master.Index
		p.appendTxLog(strm, txlog.EntryBegin, sql, &mi)
	}

	return upResp.Results[0], nil
}

func (p *Processor) handleCrossMasterCommit(ctx context.Context, strm *stream.Stream, req StreamRequest, endSQL string) (StreamResult, error) {
	if strm.TxBuffer == nil || !strm.TxBuffer.Active {
		return ErrResult(NewError("no active transaction")), nil
	}
	idxs := strm.TxBuffer.OpenMasterIndices()
	if len(idxs) == 0 {
		strm.TxBuffer.Reset()
		strm.TxWALID = ""
		return p.emptyExecuteOK(), nil
	}
	cctx, cancel := context.WithTimeout(ctx, p.cfg.CommitTimeout)
	defer cancel()

	p.appendTxLog(strm, txlog.EntryCommitStart, endSQL, nil)

	firstIdx := idxs[0]
	upReq := &PipelineRequest{
		Baton:    strm.MasterBatons[firstIdx],
		Requests: []StreamRequest{req, CloseRequest()},
	}
	firstResp, err := p.cfg.Pool.ClientFor(firstIdx).Pipeline(cctx, upReq)
	if err != nil {
		return ErrResult(NewError(fmt.Sprintf("commit master[%d]: %s", firstIdx, err))), nil
	}
	if len(firstResp.Results) == 0 || firstResp.Results[0].Type != "ok" {
		msg := "upstream error"
		if len(firstResp.Results) > 0 && firstResp.Results[0].Error != nil {
			msg = firstResp.Results[0].Error.Message
		}
		return ErrResult(NewError(fmt.Sprintf("commit master[%d]: %s", firstIdx, msg))), nil
	}
	strm.MasterBatons[firstIdx] = firstResp.Baton
	clientResult := firstResp.Results[0]

	m0 := firstIdx
	p.appendTxLog(strm, txlog.EntryMasterDone, "", &m0)

	if len(idxs) > 1 {
		_, err = commitAllMasters(cctx, p.cfg.Pool, strm.MasterBatons, idxs[1:], endSQL, func(idx int) {
			ix := idx
			p.appendTxLog(strm, txlog.EntryMasterDone, "", &ix)
		})
		if err != nil {
			strm.TxBuffer.Reset()
			strm.TxWALID = ""
			return ErrResult(NewError(err.Error())), nil
		}
	}

	p.flushTxReplication(strm)
	p.appendTxLog(strm, txlog.EntryComplete, "", nil)
	strm.TxBuffer.Reset()
	strm.TxWALID = ""
	return clientResult, nil
}

func (p *Processor) handleCrossMasterRollback(ctx context.Context, strm *stream.Stream, req StreamRequest, endSQL string) (StreamResult, error) {
	if strm.TxBuffer == nil || !strm.TxBuffer.Active {
		return ErrResult(NewError("no active transaction")), nil
	}
	idxs := strm.TxBuffer.OpenMasterIndices()
	if len(idxs) == 0 {
		strm.TxBuffer.Reset()
		strm.TxWALID = ""
		return p.emptyExecuteOK(), nil
	}
	cctx, cancel := context.WithTimeout(ctx, p.cfg.CommitTimeout)
	defer cancel()

	firstIdx := idxs[0]
	upReq := &PipelineRequest{
		Baton:    strm.MasterBatons[firstIdx],
		Requests: []StreamRequest{req, CloseRequest()},
	}
	firstResp, err := p.cfg.Pool.ClientFor(firstIdx).Pipeline(cctx, upReq)
	if err != nil {
		return ErrResult(NewError(fmt.Sprintf("rollback master[%d]: %s", firstIdx, err))), nil
	}
	if len(firstResp.Results) == 0 || firstResp.Results[0].Type != "ok" {
		msg := "upstream error"
		if len(firstResp.Results) > 0 && firstResp.Results[0].Error != nil {
			msg = firstResp.Results[0].Error.Message
		}
		return ErrResult(NewError(fmt.Sprintf("rollback master[%d]: %s", firstIdx, msg))), nil
	}
	strm.MasterBatons[firstIdx] = firstResp.Baton
	clientResult := firstResp.Results[0]

	if len(idxs) > 1 {
		_, err = rollbackAllMasters(cctx, p.cfg.Pool, strm.MasterBatons, idxs[1:])
		if err != nil {
			strm.TxBuffer.Reset()
			strm.TxWALID = ""
			return ErrResult(NewError(err.Error())), nil
		}
	}

	p.appendTxLog(strm, txlog.EntryRollback, endSQL, nil)
	strm.TxBuffer.Reset()
	strm.TxWALID = ""
	return clientResult, nil
}

func (p *Processor) emptyExecuteOK() StreamResult {
	raw, _ := EmptyStmtResultJSON()
	return OkResult(StreamResponse{Type: "execute", Result: raw})
}

// OpenCursorUpstream opens a /v3/cursor stream on the routed master.
// The caller is responsible for closing resp.Body.
func (p *Processor) OpenCursorUpstream(ctx context.Context, strm *stream.Stream, batch Batch) (*router.Master, *http.Response, error) {
	sql := ""
	if len(batch.Steps) > 0 && batch.Steps[0].Stmt.SQL != nil {
		sql = *batch.Steps[0].Stmt.SQL
	}
	master, err := p.cfg.Router.RouteQuery(sql, strm.PinnedMaster, strm.LazyCrossTxActive())
	if err != nil {
		return nil, nil, err
	}

	upReq := &CursorRequest{
		Baton: strm.MasterBatons[master.Index],
		Batch: batch,
	}
	resp, err := p.cfg.Pool.ClientFor(master.Index).Cursor(ctx, upReq)
	if err != nil {
		return nil, nil, fmt.Errorf("upstream cursor: %w", err)
	}
	return master, resp, nil
}

func (p *Processor) ResolveSQL(strm *stream.Stream, req StreamRequest) string {
	if req.Stmt != nil {
		if req.Stmt.SQL != nil {
			return *req.Stmt.SQL
		}
		if req.Stmt.SQLId != nil {
			return strm.SQLStore.Load(*req.Stmt.SQLId)
		}
	}
	if req.SQL != nil {
		return *req.SQL
	}
	if req.SQLId != nil {
		return strm.SQLStore.Load(*req.SQLId)
	}
	if req.Batch != nil && len(req.Batch.Steps) > 0 {
		step := req.Batch.Steps[0]
		if step.Stmt.SQL != nil {
			return *step.Stmt.SQL
		}
		if step.Stmt.SQLId != nil {
			return strm.SQLStore.Load(*step.Stmt.SQLId)
		}
	}
	return ""
}

func (p *Processor) HandleStoreSql(strm *stream.Stream, req StreamRequest) StreamResult {
	if req.SQLId == nil || req.SQL == nil {
		return ErrResult(NewError("store_sql: missing sql_id or sql"))
	}
	strm.SQLStore.Store(*req.SQLId, *req.SQL)
	return OkResult(StreamResponse{Type: "store_sql"})
}

func (p *Processor) HandleCloseSql(strm *stream.Stream, req StreamRequest) StreamResult {
	if req.SQLId == nil {
		return ErrResult(NewError("close_sql: missing sql_id"))
	}
	strm.SQLStore.Delete(*req.SQLId)
	return OkResult(StreamResponse{Type: "close_sql"})
}

func (p *Processor) EnqueueReplication(sql string, masterIdx int) {
	if p.cfg.Replicator != nil && !p.cfg.Router.IsReadOnly(sql) {
		p.cfg.Replicator.Enqueue(sql, masterIdx)
	}
}

func (p *Processor) initReplicationTxBuffer(strm *stream.Stream, beginSQL string, masterIdx int) {
	if strm.TxBuffer == nil {
		strm.TxBuffer = transaction.NewTxBuffer()
	} else {
		strm.TxBuffer.Reset()
	}
	strm.TxBuffer.Active = true
	strm.TxBuffer.BeginSQL = beginSQL
	strm.TxBuffer.MarkMasterTxOpen(masterIdx)
	if p.cfg.TxLog != nil {
		strm.TxWALID = strm.ClientBaton
		mi := masterIdx
		p.appendTxLog(strm, txlog.EntryBegin, beginSQL, &mi)
	}
}

func (p *Processor) flushTxReplication(strm *stream.Stream) {
	if p.cfg.Replicator == nil || strm.TxBuffer == nil || !strm.TxBuffer.Active {
		return
	}
	begin := strm.TxBuffer.BeginSQL
	if begin == "" {
		begin = "BEGIN"
	}
	for _, origin := range strm.TxBuffer.AffectedMasterIndices() {
		stmts := strm.TxBuffer.StmtsForMaster(origin)
		if len(stmts) == 0 {
			continue
		}
		sqls := make([]string, 0, len(stmts)+2)
		sqls = append(sqls, begin)
		for _, s := range stmts {
			sqls = append(sqls, s.SQL)
		}
		sqls = append(sqls, "COMMIT")
		p.cfg.Replicator.EnqueueTxn(sqls, origin)
	}
}

func (p *Processor) bufferTxWrites(strm *stream.Stream, req StreamRequest, sql string, masterIdx int) {
	switch req.Type {
	case "execute", "sequence":
		inf := gosql.Analyze(sql)
		if inf.IsReadOnly || inf.IsTxEnd {
			return
		}
		strm.TxBuffer.Add(transaction.BufferedStmt{SQL: sql, MasterIdx: masterIdx})
		m := masterIdx
		p.appendTxLog(strm, txlog.EntryStatement, sql, &m)
	case "batch":
		if req.Batch == nil {
			return
		}
		for _, step := range req.Batch.Steps {
			stepSQL := p.resolveStmtSQL(strm, step.Stmt)
			if stepSQL == "" {
				continue
			}
			inf := gosql.Analyze(stepSQL)
			if inf.IsReadOnly || inf.IsTxEnd {
				continue
			}
			strm.TxBuffer.Add(transaction.BufferedStmt{SQL: stepSQL, MasterIdx: masterIdx})
			m := masterIdx
			p.appendTxLog(strm, txlog.EntryStatement, stepSQL, &m)
		}
	}
}

func (p *Processor) replicateWriteStmts(strm *stream.Stream, req StreamRequest, sql string, masterIdx int) {
	switch req.Type {
	case "execute", "sequence":
		if p.cfg.Router.IsReadOnly(sql) {
			return
		}
		p.EnqueueReplication(sql, masterIdx)
	case "batch":
		if req.Batch == nil {
			return
		}
		for _, step := range req.Batch.Steps {
			stepSQL := p.resolveStmtSQL(strm, step.Stmt)
			if stepSQL == "" {
				continue
			}
			if p.cfg.Router.IsReadOnly(stepSQL) {
				continue
			}
			p.EnqueueReplication(stepSQL, masterIdx)
		}
	}
}

func (p *Processor) resolveStmtSQL(strm *stream.Stream, st Stmt) string {
	if st.SQL != nil {
		return *st.SQL
	}
	if st.SQLId != nil && strm != nil {
		return strm.SQLStore.Load(*st.SQLId)
	}
	return ""
}

// UpdateTxState must be called after a successful upstream response.
func UpdateTxState(strm *stream.Stream, sql string, master *router.Master) {
	switch sql {
	case "BEGIN", "BEGIN DEFERRED", "BEGIN IMMEDIATE", "BEGIN EXCLUSIVE":
		strm.PinnedMaster = master
	case "COMMIT", "END", "ROLLBACK":
		strm.PinnedMaster = nil
	}
}

// ParseCursorEntries streams NDJSON lines from an upstream /v3/cursor response
// into ch. The first line (header baton) is sent to headerBaton; ch is closed
// when the body is exhausted or ctx is cancelled.
func ParseCursorEntries(ctx context.Context, resp *http.Response, headerBaton chan<- *string, ch chan<- CursorChunk) {
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()
	defer close(ch)

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)

	firstLine := true
	batch := make([]json.RawMessage, 0, 64)

	flush := func(done bool, err error) bool {
		select {
		case <-ctx.Done():
			return false
		case ch <- CursorChunk{RawEntries: batch, Done: done, Err: err}:
			batch = nil
			return true
		}
	}

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		if firstLine {
			firstLine = false
			var hdr CursorResponseHeader
			if jsonErr := json.Unmarshal(line, &hdr); jsonErr == nil {
				select {
				case <-ctx.Done():
					return
				case headerBaton <- hdr.Baton:
				}
			} else {
				select {
				case <-ctx.Done():
					return
				case headerBaton <- nil:
				}
			}
			continue
		}

		cp := make([]byte, len(line))
		copy(cp, line)
		batch = append(batch, cp)

		if len(batch) >= 64 {
			if !flush(false, nil) {
				return
			}
		}
	}

	if err := scanner.Err(); err != nil {
		flush(true, err)
		return
	}
	flush(true, nil)
}

// CursorChunk is a batch of raw JSON cursor entry lines from an upstream /v3/cursor stream.
type CursorChunk struct {
	RawEntries []json.RawMessage
	Done       bool
	Err        error
}

func (p *Processor) appendTxLog(strm *stream.Stream, typ txlog.EntryType, sql string, masterIdx *int) {
	if p.cfg.TxLog == nil || strm.TxWALID == "" {
		return
	}
	e := txlog.Entry{
		TxID:      strm.TxWALID,
		Timestamp: time.Now().UnixMilli(),
		Type:      typ,
		SQL:       sql,
		MasterIdx: masterIdx,
	}
	if err := p.cfg.TxLog.Append(e); err != nil && p.cfg.Logger != nil {
		p.cfg.Logger.Error("txlog: append", err)
	}
}

var queryTypeLabels = map[gosql.QueryType]string{
	gosql.QueryTypeSelect:      "READ",
	gosql.QueryTypeInsert:      "INSERT",
	gosql.QueryTypeUpdate:      "UPDATE",
	gosql.QueryTypeDelete:      "DELETE",
	gosql.QueryTypeCreateTable: "DDL:CREATE",
	gosql.QueryTypeAlterTable:  "DDL:ALTER",
	gosql.QueryTypeDropTable:   "DDL:DROP",
	gosql.QueryTypeCreateIndex: "DDL:INDEX",
	gosql.QueryTypeDropIndex:   "DDL:DROP_IDX",
	gosql.QueryTypeTxBegin:     "TX:BEGIN",
	gosql.QueryTypeTxCommit:    "TX:COMMIT",
	gosql.QueryTypeTxRollback:  "TX:ROLLBACK",
	gosql.QueryTypePragma:      "PRAGMA",
	gosql.QueryTypeOther:       "OTHER",
}

func sqlOpLabel(sql string) string {
	info := gosql.Analyze(sql)
	if label, ok := queryTypeLabels[info.Type]; ok {
		return label
	}
	return "UNKNOWN"
}

func sqlTableLabel(sql string) string {
	info := gosql.Analyze(sql)
	if len(info.Tables) > 0 {
		return info.Tables[0]
	}
	return "-"
}

func truncateSQL(sql string, maxLen int) string {
	s := strings.Join(strings.Fields(sql), " ")
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
