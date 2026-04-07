package replication

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hive_v2/orchestrator/internal/config"
	"github.com/hive_v2/orchestrator/internal/hrana"
)

type Task struct {
	SQL          string
	GRPCBody     []byte // raw gRPC frame to forward (mutually exclusive with SQL)
	GRPCPath     string // e.g. "/proxy.Proxy/Execute"
	OriginMaster int
}

// MasterExecutor executes a single SQL statement on a master via Hrana.
type MasterExecutor interface {
	Pipeline(ctx context.Context, req *hrana.PipelineRequest) (*hrana.PipelineResponse, error)
}

// ExecutorPool provides a Hrana executor for a given master index.
type ExecutorPool interface {
	ClientFor(masterIndex int) MasterExecutor
}

// GRPCForwarder sends a raw gRPC request body to a specific master.
type GRPCForwarder interface {
	ForwardGRPC(ctx context.Context, masterIdx int, path string, body []byte) error
}

type Logger interface {
	Info(msg string)
	Error(msg string, err error)
}

// TargetNamer resolves a replication target index to a human-readable name.
// When nil, the replicator falls back to "master[N]".
type TargetNamer interface {
	TargetName(index int) string
}

// Replicator fans out write statements to all masters except the origin.
// Supports two modes: raw gRPC body forwarding (preferred for gRPC clients)
// and Hrana SQL replay (for Hrana HTTP/WS clients).
// Uses a bounded channel as a work queue with a fixed pool of workers.
// Dropped tasks (queue full) are counted but not retried.
type Replicator struct {
	pool         ExecutorPool
	grpc         GRPCForwarder // may be nil if only Hrana replication is used
	masterCount  int
	queue        chan Task
	retryMax     int
	retryBackoff time.Duration
	dropped      atomic.Uint64
	logger       Logger
	namer        TargetNamer
}

// New starts worker goroutines that stop when ctx is cancelled.
func New(ctx context.Context, cfg config.ReplicationConfig, masterCount int, pool ExecutorPool, logger Logger) *Replicator {
	r := &Replicator{
		pool:         pool,
		masterCount:  masterCount,
		queue:        make(chan Task, cfg.QueueCapacity),
		retryMax:     cfg.RetryMax,
		retryBackoff: cfg.RetryBackoff,
		logger:       logger,
	}
	for range cfg.Workers {
		go r.worker(ctx)
	}
	return r
}

// SetTargetNamer sets an optional namer for human-readable log output.
func (r *Replicator) SetTargetNamer(n TargetNamer) {
	r.namer = n
}

// SetGRPCForwarder configures raw gRPC forwarding for replication.
func (r *Replicator) SetGRPCForwarder(f GRPCForwarder) {
	r.grpc = f
}

// Enqueue adds a Hrana SQL replication task.
func (r *Replicator) Enqueue(sql string, originMaster int) {
	r.enqueue(Task{SQL: sql, OriginMaster: originMaster}, truncate(sql, 60))
}

// EnqueueGRPC adds a raw gRPC body forwarding task.
// The same bytes that the origin master received are sent to all other masters.
func (r *Replicator) EnqueueGRPC(body []byte, path string, originMaster int) {
	cp := make([]byte, len(body))
	copy(cp, body)
	r.enqueue(Task{GRPCBody: cp, GRPCPath: path, OriginMaster: originMaster}, fmt.Sprintf("gRPC %s (%d bytes)", path, len(body)))
}

func (r *Replicator) enqueue(task Task, label string) {
	select {
	case r.queue <- task:
	default:
		r.dropped.Add(1)
		if r.logger != nil {
			r.logger.Error(fmt.Sprintf("replication: queue full, dropped %s", label), nil)
		}
	}
}

// Dropped returns the number of tasks dropped due to a full queue.
func (r *Replicator) Dropped() uint64 {
	return r.dropped.Load()
}

// DrainAndStop processes remaining queued tasks up to the given timeout.
// Should be called during graceful shutdown after the HTTP server has stopped
// accepting new requests.
func (r *Replicator) DrainAndStop(timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		select {
		case task := <-r.queue:
			r.replicate(ctx, task)
		case <-ctx.Done():
			remaining := len(r.queue)
			if remaining > 0 && r.logger != nil {
				r.logger.Error(fmt.Sprintf("replication: %d tasks dropped on shutdown", remaining), nil)
			}
			return
		default:
			return
		}
	}
}

func (r *Replicator) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-r.queue:
			r.replicate(ctx, task)
		}
	}
}

func (r *Replicator) replicate(ctx context.Context, task Task) {
	for i := range r.masterCount {
		if i == task.OriginMaster {
			continue
		}
		if len(task.GRPCBody) > 0 {
			r.replicateGRPC(ctx, task.GRPCBody, task.GRPCPath, i)
		} else {
			r.replicateSQL(ctx, task.SQL, i)
		}
	}
}

func (r *Replicator) replicateGRPC(ctx context.Context, body []byte, path string, masterIdx int) {
	name := r.targetName(masterIdx)
	if r.grpc == nil {
		if r.logger != nil {
			r.logger.Error(fmt.Sprintf("replication: no gRPC forwarder for %s", name), nil)
		}
		return
	}

	backoff := r.retryBackoff
	for attempt := range r.retryMax + 1 {
		err := r.grpc.ForwardGRPC(ctx, masterIdx, path, body)
		if err == nil {
			if r.logger != nil {
				r.logger.Info(fmt.Sprintf("replicated gRPC -> %s %s (%d bytes)", name, path, len(body)))
			}
			return
		}
		if attempt == r.retryMax {
			if r.logger != nil {
				r.logger.Error(
					fmt.Sprintf("replication: %s gRPC failed after %d attempts %s", name, r.retryMax+1, path),
					err,
				)
			}
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			backoff *= 2
		}
	}
}

func (r *Replicator) replicateSQL(ctx context.Context, sql string, masterIdx int) {
	name := r.targetName(masterIdx)
	executor := r.pool.ClientFor(masterIdx)
	stmt := &hrana.Stmt{SQL: &sql, WantRows: false}
	req := &hrana.PipelineRequest{
		Requests: []hrana.StreamRequest{
			{Type: "execute", Stmt: stmt},
			hrana.CloseRequest(),
		},
	}

	backoff := r.retryBackoff
	for attempt := range r.retryMax + 1 {
		resp, err := executor.Pipeline(ctx, req)
		if err == nil {
			err = checkPipelineErrors(resp)
		}
		if err == nil {
			if r.logger != nil {
				r.logger.Info(fmt.Sprintf("replicated SQL -> %s sql=%s", name, truncate(sql, 60)))
			}
			return
		}
		if attempt == r.retryMax {
			if r.logger != nil {
				r.logger.Error(
					fmt.Sprintf("replication: %s SQL failed after %d attempts sql=%s", name, r.retryMax+1, truncate(sql, 60)),
					err,
				)
			}
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			backoff *= 2
		}
	}
}

// checkPipelineErrors inspects a PipelineResponse for per-statement errors
// (e.g. FOREIGN KEY constraint violations) that the upstream master returns
// inside a successful HTTP response.
func checkPipelineErrors(resp *hrana.PipelineResponse) error {
	if resp == nil {
		return nil
	}
	for _, r := range resp.Results {
		if r.Type == "error" && r.Error != nil {
			return fmt.Errorf("hrana stmt error [%s]: %s", r.Error.Code, r.Error.Message)
		}
	}
	return nil
}

func (r *Replicator) targetName(idx int) string {
	if r.namer != nil {
		return r.namer.TargetName(idx)
	}
	return fmt.Sprintf("master[%d]", idx)
}

func truncate(s string, maxLen int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
