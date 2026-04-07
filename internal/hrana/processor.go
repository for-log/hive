package hrana

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/hive_v2/orchestrator/internal/router"
	gosql "github.com/hive_v2/orchestrator/internal/sql"
	"github.com/hive_v2/orchestrator/internal/stream"
)

// ProcessorConfig groups the dependencies needed to process Hrana requests.
type ProcessorConfig struct {
	Pool       MasterClientPool
	Router     *router.Router
	Replicator WriteReplicator
	Logger     Logger
}

// Processor routes and executes Hrana stream requests against the master pool.
// Callers must serialise access per stream.Stream; the Processor itself is stateless.
type Processor struct {
	cfg ProcessorConfig
}

func NewProcessor(cfg ProcessorConfig) *Processor {
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
			isAutocommit := strm.PinnedMaster == nil
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

	master, err := p.cfg.Router.RouteQuery(sql, strm.PinnedMaster)
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
				resolved := strm.SQLStore[*step.Stmt.SQLId]
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

	upReq := &PipelineRequest{
		Baton:    strm.MasterBatons[master.Index],
		Requests: []StreamRequest{upReqEntry, CloseRequest()},
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

	strm.MasterBatons[master.Index] = upResp.Baton

	if len(upResp.Results) == 0 {
		return ErrResult(NewError("upstream returned empty results")), nil
	}

	UpdateTxState(strm, sql, master)

	if upResp.Results[0].Type == "ok" && (req.Type == "execute" || req.Type == "batch") {
		if req.Type == "batch" && req.Batch != nil {
			for _, step := range req.Batch.Steps {
				if step.Stmt.SQL != nil {
					p.EnqueueReplication(*step.Stmt.SQL, master.Index)
				}
			}
		} else {
			p.EnqueueReplication(sql, master.Index)
		}
	}

	return upResp.Results[0], nil
}

// OpenCursorUpstream opens a /v3/cursor stream on the routed master.
// The caller is responsible for closing resp.Body.
func (p *Processor) OpenCursorUpstream(ctx context.Context, strm *stream.Stream, batch Batch) (*router.Master, *http.Response, error) {
	sql := ""
	if len(batch.Steps) > 0 && batch.Steps[0].Stmt.SQL != nil {
		sql = *batch.Steps[0].Stmt.SQL
	}
	master, err := p.cfg.Router.RouteQuery(sql, strm.PinnedMaster)
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
			return strm.SQLStore[*req.Stmt.SQLId]
		}
	}
	if req.SQL != nil {
		return *req.SQL
	}
	if req.SQLId != nil {
		return strm.SQLStore[*req.SQLId]
	}
	if req.Batch != nil && len(req.Batch.Steps) > 0 {
		step := req.Batch.Steps[0]
		if step.Stmt.SQL != nil {
			return *step.Stmt.SQL
		}
		if step.Stmt.SQLId != nil {
			return strm.SQLStore[*step.Stmt.SQLId]
		}
	}
	return ""
}

func (p *Processor) HandleStoreSql(strm *stream.Stream, req StreamRequest) StreamResult {
	if req.SQLId == nil || req.SQL == nil {
		return ErrResult(NewError("store_sql: missing sql_id or sql"))
	}
	strm.SQLStore[*req.SQLId] = *req.SQL
	return OkResult(StreamResponse{Type: "store_sql"})
}

func (p *Processor) HandleCloseSql(strm *stream.Stream, req StreamRequest) StreamResult {
	if req.SQLId == nil {
		return ErrResult(NewError("close_sql: missing sql_id"))
	}
	delete(strm.SQLStore, *req.SQLId)
	return OkResult(StreamResponse{Type: "close_sql"})
}

func (p *Processor) EnqueueReplication(sql string, masterIdx int) {
	if p.cfg.Replicator != nil && !p.cfg.Router.IsReadOnly(sql) {
		p.cfg.Replicator.Enqueue(sql, masterIdx)
	}
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
	defer resp.Body.Close()
	defer close(ch)

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)

	firstLine := true
	var batch []json.RawMessage

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
