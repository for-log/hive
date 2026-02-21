package router

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"hive/gen/hivepb"
	"hive/internal/sqlparse"
)

// errNotReady is the gRPC status message returned when no masters are alive.
// The hivedriver recognises this exact string and retries with backoff.
const errNotReady = "database_not_ready: no alive masters"

// MasterExecutor is the consumer-side interface for a single master node.
type MasterExecutor interface {
	Execute(ctx context.Context, req *hivepb.ExecuteRequest, opts ...grpc.CallOption) (*hivepb.ExecuteResponse, error)
	Query(ctx context.Context, req *hivepb.QueryRequest, opts ...grpc.CallOption) (hivepb.HiveMaster_QueryClient, error)
	BeginTx(ctx context.Context, req *hivepb.BeginTxRequest, opts ...grpc.CallOption) (*hivepb.BeginTxResponse, error)
	CommitTx(ctx context.Context, req *hivepb.TxRequest, opts ...grpc.CallOption) (*hivepb.TxResponse, error)
	RollbackTx(ctx context.Context, req *hivepb.TxRequest, opts ...grpc.CallOption) (*hivepb.TxResponse, error)
}

// pendingTx holds the state of a lazy transaction before the first query
// determines which master it targets.
type pendingTx struct {
	masterID string // empty until the first query resolves it
	masterTx string // tx_id returned by the master's BeginTx
}

type RouterConfig struct {
	TxTimeout time.Duration // max lifetime of an open transaction; 0 means no timeout
}

func (c *RouterConfig) applyDefaults() {
	if c.TxTimeout <= 0 {
		c.TxTimeout = 30 * time.Second
	}
}

type Router struct {
	hivepb.UnimplementedHiveSQLServer
	tr      TableResolver
	mr      MasterRegistry
	log     *slog.Logger
	conns   map[string]MasterExecutor
	txs     map[string]*pendingTx
	cfg     RouterConfig
	txSeq   atomic.Uint64
	connsMu sync.RWMutex
	txsMu   sync.Mutex
}

func NewRouter(tr TableResolver, mr MasterRegistry, cfg RouterConfig, log *slog.Logger) *Router {
	cfg.applyDefaults()
	return &Router{
		tr:    tr,
		mr:    mr,
		log:   log,
		cfg:   cfg,
		conns: make(map[string]MasterExecutor),
		txs:   make(map[string]*pendingTx),
	}
}

func (r *Router) Execute(ctx context.Context, req *hivepb.ExecuteRequest) (*hivepb.ExecuteResponse, error) {
	pq, err := sqlparse.Parse(req.Sql)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "parse sql: %v", err)
	}

	if pq.Type == sqlparse.QueryDDL {
		return r.executeDDL(ctx, req, pq)
	}
	return r.executeDML(ctx, req, pq)
}

func (r *Router) Query(req *hivepb.QueryRequest, stream hivepb.HiveSQL_QueryServer) error {
	ctx := stream.Context()

	pq, err := sqlparse.Parse(req.Sql)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "parse sql: %v", err)
	}

	if len(pq.Tables) == 0 {
		return status.Error(codes.InvalidArgument, "query references no tables")
	}

	masterID, err := r.resolveMaster(ctx, pq.Tables[0])
	if err != nil {
		return err
	}

	var masterTxID string
	if req.TxId != "" {
		masterID, masterTxID, err = r.txMaster(ctx, req.TxId, masterID)
		if err != nil {
			return err
		}
	}

	exec, err := r.masterConn(ctx, masterID)
	if err != nil {
		return err
	}

	upstream, err := exec.Query(ctx, &hivepb.QueryRequest{
		Sql:  req.Sql,
		Args: req.Args,
		TxId: masterTxID,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "master query: %v", err)
	}

	for {
		msg, recvErr := upstream.Recv()
		if recvErr == io.EOF {
			return nil
		}
		if recvErr != nil {
			return status.Errorf(codes.Internal, "recv from master: %v", recvErr)
		}
		if sendErr := stream.Send(msg); sendErr != nil {
			return status.Errorf(codes.Internal, "send to client: %v", sendErr)
		}
	}
}

// BeginTx creates a lazy transaction; the master is not contacted until the first query.
func (r *Router) BeginTx(ctx context.Context, _ *hivepb.BeginTxRequest) (*hivepb.BeginTxResponse, error) {
	txID := fmt.Sprintf("rtx-%d", r.txSeq.Add(1))

	r.txsMu.Lock()
	r.txs[txID] = &pendingTx{}
	r.txsMu.Unlock()

	go func() {
		select {
		case <-ctx.Done():
		case <-time.After(r.cfg.TxTimeout):
			r.log.Warn("router: tx timeout, rolling back", "tx_id", txID)
			rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := r.rollbackTx(rollbackCtx, txID); err != nil {
				r.log.Error("router: timeout rollback failed", "tx_id", txID, "err", err)
			}
		}
	}()

	return &hivepb.BeginTxResponse{TxId: txID}, nil
}

func (r *Router) CommitTx(ctx context.Context, req *hivepb.TxRequest) (*hivepb.TxResponse, error) {
	r.txsMu.Lock()
	tx, ok := r.txs[req.TxId]
	if ok {
		delete(r.txs, req.TxId)
	}
	r.txsMu.Unlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "tx %q not found", req.TxId)
	}

	// tx.masterID is empty when BeginTx was called but no query followed.
	if tx.masterID == "" {
		return &hivepb.TxResponse{Ok: true}, nil
	}

	exec, err := r.masterConn(ctx, tx.masterID)
	if err != nil {
		return nil, err
	}

	if _, err := exec.CommitTx(ctx, &hivepb.TxRequest{TxId: tx.masterTx}); err != nil {
		return nil, status.Errorf(codes.Internal, "commit on master: %v", err)
	}
	return &hivepb.TxResponse{Ok: true}, nil
}

func (r *Router) RollbackTx(ctx context.Context, req *hivepb.TxRequest) (*hivepb.TxResponse, error) {
	if err := r.rollbackTx(ctx, req.TxId); err != nil {
		return nil, err
	}
	return &hivepb.TxResponse{Ok: true}, nil
}

func (r *Router) rollbackTx(ctx context.Context, txID string) error {
	r.txsMu.Lock()
	tx, ok := r.txs[txID]
	if ok {
		delete(r.txs, txID)
	}
	r.txsMu.Unlock()

	if !ok {
		return status.Errorf(codes.NotFound, "tx %q not found", txID)
	}
	if tx.masterID == "" {
		return nil
	}

	exec, err := r.masterConn(ctx, tx.masterID)
	if err != nil {
		return err
	}

	if _, err := exec.RollbackTx(ctx, &hivepb.TxRequest{TxId: tx.masterTx}); err != nil {
		return status.Errorf(codes.Internal, "rollback on master: %v", err)
	}
	return nil
}

func (r *Router) executeDDL(ctx context.Context, req *hivepb.ExecuteRequest, pq sqlparse.ParsedQuery) (*hivepb.ExecuteResponse, error) {
	if len(pq.Tables) == 0 {
		return nil, status.Error(codes.InvalidArgument, "DDL references no tables")
	}
	tableName := pq.Tables[0]

	var masterID string
	var err error

	if pq.DDLAction == sqlparse.DDLCreate {
		masterID, err = r.pickMasterForDDL(ctx)
		if err != nil {
			return nil, err
		}
		if err = r.tr.RegisterTable(ctx, tableName, masterID); err != nil {
			if !pq.IfNotExists {
				return nil, status.Errorf(codes.AlreadyExists, "register table: %v", err)
			}
			// Table already registered — forward to the owning master so the
			// master can also apply IF NOT EXISTS semantics on its own SQLite.
			masterID, err = r.resolveMaster(ctx, tableName)
			if err != nil {
				return nil, err
			}
		}
	} else {
		masterID, err = r.resolveMaster(ctx, tableName)
		if err != nil {
			return nil, err
		}
	}

	exec, err := r.masterConn(ctx, masterID)
	if err != nil {
		return nil, err
	}

	resp, err := exec.Execute(ctx, &hivepb.ExecuteRequest{Sql: req.Sql, Args: req.Args})
	if err != nil {
		if pq.DDLAction == sqlparse.DDLCreate {
			if removeErr := r.tr.RemoveTable(ctx, tableName); removeErr != nil {
				r.log.Error("router: remove table after DDL failure", "table", tableName, "err", removeErr)
			}
		}
		return nil, status.Errorf(codes.Internal, "ddl on master: %v", err)
	}

	if pq.DDLAction == sqlparse.DDLDrop {
		if removeErr := r.tr.RemoveTable(ctx, tableName); removeErr != nil {
			r.log.Error("router: remove table after DROP", "table", tableName, "err", removeErr)
		}
	}

	return resp, nil
}

func (r *Router) executeDML(ctx context.Context, req *hivepb.ExecuteRequest, pq sqlparse.ParsedQuery) (*hivepb.ExecuteResponse, error) {
	if len(pq.Tables) == 0 {
		return nil, status.Error(codes.InvalidArgument, "DML references no tables")
	}

	masterID, err := r.resolveMaster(ctx, pq.Tables[0])
	if err != nil {
		return nil, err
	}

	var masterTxID string
	if req.TxId != "" {
		masterID, masterTxID, err = r.txMaster(ctx, req.TxId, masterID)
		if err != nil {
			return nil, err
		}
	}

	exec, err := r.masterConn(ctx, masterID)
	if err != nil {
		return nil, err
	}

	resp, err := exec.Execute(ctx, &hivepb.ExecuteRequest{
		Sql:        req.Sql,
		Args:       req.Args,
		ReturnRows: req.ReturnRows,
		TxId:       masterTxID,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "execute on master: %v", err)
	}
	return resp, nil
}

func (r *Router) resolveMaster(ctx context.Context, tableName string) (string, error) {
	masterID, err := r.tr.LookupMaster(ctx, tableName)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", status.Errorf(codes.NotFound, "table %q not routed", tableName)
		}
		return "", status.Errorf(codes.Internal, "lookup master: %v", err)
	}
	return masterID, nil
}

// txMaster binds txID to masterID on first use (opening BeginTx on the master),
// or verifies the existing binding. Returns masterID and the master-side tx_id.
// Returns FailedPrecondition if the query targets a different master than the bound one.
func (r *Router) txMaster(ctx context.Context, txID, masterID string) (string, string, error) {
	r.txsMu.Lock()
	tx, ok := r.txs[txID]
	r.txsMu.Unlock()

	if !ok {
		return "", "", status.Errorf(codes.NotFound, "tx %q not found", txID)
	}

	if tx.masterID == "" {
		exec, err := r.masterConn(ctx, masterID)
		if err != nil {
			return "", "", err
		}
		resp, err := exec.BeginTx(ctx, &hivepb.BeginTxRequest{})
		if err != nil {
			return "", "", status.Errorf(codes.Internal, "begin tx on master: %v", err)
		}
		r.txsMu.Lock()
		tx.masterID = masterID
		tx.masterTx = resp.TxId
		r.txsMu.Unlock()
		return masterID, resp.TxId, nil
	}

	if tx.masterID != masterID {
		return "", "", status.Errorf(codes.FailedPrecondition,
			"cross-shard transaction: tx bound to master %q, query targets %q",
			tx.masterID, masterID)
	}
	return tx.masterID, tx.masterTx, nil
}

func (r *Router) pickMasterForDDL(ctx context.Context) (string, error) {
	masters, err := r.mr.AliveMasters(ctx)
	if err != nil {
		return "", status.Errorf(codes.Internal, "list masters: %v", err)
	}
	if len(masters) == 0 {
		return "", status.Error(codes.Unavailable, errNotReady)
	}

	best := masters[0].ID
	bestCount := -1
	for _, m := range masters {
		tables, err := r.tr.TablesByMaster(ctx, m.ID)
		if err != nil {
			r.log.Warn("router: count tables for master", "master_id", m.ID, "err", err)
			continue
		}
		if bestCount == -1 || len(tables) < bestCount {
			bestCount = len(tables)
			best = m.ID
		}
	}
	return best, nil
}

// InjectMasterConn pre-populates the connection cache for a master (test helper).
func InjectMasterConn(r *Router, masterID string, exec MasterExecutor) {
	r.connsMu.Lock()
	r.conns[masterID] = exec
	r.connsMu.Unlock()
}

func (r *Router) masterConn(ctx context.Context, masterID string) (MasterExecutor, error) {
	r.connsMu.RLock()
	exec, ok := r.conns[masterID]
	r.connsMu.RUnlock()
	if ok {
		return exec, nil
	}

	masters, err := r.mr.AliveMasters(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list masters: %v", err)
	}
	if len(masters) == 0 {
		return nil, status.Error(codes.Unavailable, errNotReady)
	}

	var grpcAddr string
	for _, m := range masters {
		if m.ID == masterID {
			grpcAddr = m.GRPCAddr
			break
		}
	}
	if grpcAddr == "" {
		return nil, status.Errorf(codes.Unavailable, "master %q not found or not alive", masterID)
	}

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "dial master %q: %v", masterID, err)
	}

	client := hivepb.NewHiveMasterClient(conn)

	r.connsMu.Lock()
	r.conns[masterID] = client
	r.connsMu.Unlock()

	return client, nil
}
