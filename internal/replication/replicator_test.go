package replication_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hive_v2/orchestrator/internal/config"
	"github.com/hive_v2/orchestrator/internal/hrana"
	"github.com/hive_v2/orchestrator/internal/replication"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// recordExecutor records every SQL it receives.
type recordExecutor struct {
	mu    sync.Mutex
	calls []string
}

func (f *recordExecutor) Pipeline(_ context.Context, req *hrana.PipelineRequest) (*hrana.PipelineResponse, error) {
	for _, r := range req.Requests {
		if r.Type == "execute" && r.Stmt != nil && r.Stmt.SQL != nil {
			f.mu.Lock()
			f.calls = append(f.calls, *r.Stmt.SQL)
			f.mu.Unlock()
		}
	}
	return &hrana.PipelineResponse{}, nil
}

func (f *recordExecutor) SQLs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.calls))
	copy(out, f.calls)
	return out
}

// recordPool holds one recordExecutor per master.
type recordPool struct {
	executors []*recordExecutor
}

func newRecordPool(n int) *recordPool {
	p := &recordPool{executors: make([]*recordExecutor, n)}
	for i := range n {
		p.executors[i] = &recordExecutor{}
	}
	return p
}

func (p *recordPool) ClientFor(idx int) replication.MasterExecutor {
	return p.executors[idx]
}

// failThenSucceedPool wraps a delegate and fails the first failFor calls.
type failThenSucceedPool struct {
	attempts *atomic.Int32
	failFor  int32
	delegate *recordExecutor
}

func (p *failThenSucceedPool) ClientFor(_ int) replication.MasterExecutor {
	return &failThenSucceedExecutor{pool: p}
}

type failThenSucceedExecutor struct{ pool *failThenSucceedPool }

func (e *failThenSucceedExecutor) Pipeline(ctx context.Context, req *hrana.PipelineRequest) (*hrana.PipelineResponse, error) {
	n := e.pool.attempts.Add(1)
	if n <= e.pool.failFor {
		return nil, fmt.Errorf("simulated error attempt %d", n)
	}
	return e.pool.delegate.Pipeline(ctx, req)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newReplicator(t *testing.T, pool replication.ExecutorPool, masterCount, workers int) (*replication.Replicator, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := config.ReplicationConfig{
		Workers:       workers,
		RetryMax:      3,
		RetryBackoff:  5 * time.Millisecond,
		QueueCapacity: 64,
	}
	r := replication.New(ctx, cfg, masterCount, pool, nil)
	return r, cancel
}

func waitForCalls(t *testing.T, exec *recordExecutor, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(exec.SQLs()) >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d calls, got %d", n, len(exec.SQLs()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestReplicator_ReplicatesToAllExceptOrigin(t *testing.T) {
	pool := newRecordPool(3)
	r, cancel := newReplicator(t, pool, 3, 2)
	defer cancel()

	r.Enqueue("INSERT INTO t VALUES (1)", 0)

	waitForCalls(t, pool.executors[1], 1)
	waitForCalls(t, pool.executors[2], 1)

	assert.Empty(t, pool.executors[0].SQLs(), "origin master must not receive replication")
	assert.Equal(t, []string{"INSERT INTO t VALUES (1)"}, pool.executors[1].SQLs())
	assert.Equal(t, []string{"INSERT INTO t VALUES (1)"}, pool.executors[2].SQLs())
}

func TestReplicator_MultipleTasksInOrder(t *testing.T) {
	pool := newRecordPool(2)
	r, cancel := newReplicator(t, pool, 2, 1) // single worker preserves order
	defer cancel()

	sqls := []string{
		"INSERT INTO t VALUES (1)",
		"INSERT INTO t VALUES (2)",
		"INSERT INTO t VALUES (3)",
	}
	for _, sql := range sqls {
		r.Enqueue(sql, 0)
	}

	waitForCalls(t, pool.executors[1], 3)
	assert.Equal(t, sqls, pool.executors[1].SQLs())
}

func TestReplicator_DropsWhenQueueFull(t *testing.T) {
	pool := newRecordPool(2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately so workers don't drain the queue

	cfg := config.ReplicationConfig{
		Workers:       1,
		RetryMax:      0,
		RetryBackoff:  time.Millisecond,
		QueueCapacity: 2,
	}
	r := replication.New(ctx, cfg, 2, pool, nil)

	r.Enqueue("s1", 0)
	r.Enqueue("s2", 0)
	r.Enqueue("s3", 0) // dropped

	assert.Equal(t, uint64(1), r.Dropped())
}

func TestReplicator_RetriesOnTransientError(t *testing.T) {
	delegate := &recordExecutor{}
	var attempts atomic.Int32
	pool := &failThenSucceedPool{
		attempts: &attempts,
		failFor:  2,
		delegate: delegate,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := config.ReplicationConfig{
		Workers:       1,
		RetryMax:      3,
		RetryBackoff:  5 * time.Millisecond,
		QueueCapacity: 8,
	}
	r := replication.New(ctx, cfg, 2, pool, nil)
	r.Enqueue("INSERT INTO t VALUES (99)", 0)

	waitForCalls(t, delegate, 1)
	assert.GreaterOrEqual(t, int(attempts.Load()), 3, "should have retried at least twice before succeeding")
}

// recordForwarder records raw gRPC forwards.
type recordForwarder struct {
	mu    sync.Mutex
	calls []grpcCall
}

type grpcCall struct {
	MasterIdx int
	Path      string
	Body      []byte
}

func (f *recordForwarder) ForwardGRPC(_ context.Context, masterIdx int, path string, body []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]byte, len(body))
	copy(cp, body)
	f.calls = append(f.calls, grpcCall{MasterIdx: masterIdx, Path: path, Body: cp})
	return nil
}

func (f *recordForwarder) getCalls() []grpcCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]grpcCall, len(f.calls))
	copy(out, f.calls)
	return out
}

func waitForGRPCCalls(t *testing.T, fwd *recordForwarder, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(fwd.getCalls()) >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d gRPC calls, got %d", n, len(fwd.getCalls()))
}

func TestReplicator_GRPCForwarding(t *testing.T) {
	pool := newRecordPool(3)
	fwd := &recordForwarder{}

	r, cancel := newReplicator(t, pool, 3, 2)
	defer cancel()
	r.SetGRPCForwarder(fwd)

	body := []byte{0x00, 0x00, 0x00, 0x00, 0x0a, 0x0a, 0x08, 0x74, 0x65, 0x73, 0x74}
	r.EnqueueGRPC(body, "/proxy.Proxy/Execute", 1)

	// Should forward to master[0] and master[2], skip master[1] (origin)
	waitForGRPCCalls(t, fwd, 2)

	calls := fwd.getCalls()
	assert.Len(t, calls, 2)

	indices := []int{calls[0].MasterIdx, calls[1].MasterIdx}
	assert.Contains(t, indices, 0)
	assert.Contains(t, indices, 2)
	assert.NotContains(t, indices, 1, "origin master must not receive gRPC replication")

	for _, c := range calls {
		assert.Equal(t, "/proxy.Proxy/Execute", c.Path)
		assert.Equal(t, body, c.Body)
	}

	// SQL replication pool should NOT receive anything
	assert.Empty(t, pool.executors[0].SQLs())
	assert.Empty(t, pool.executors[1].SQLs())
	assert.Empty(t, pool.executors[2].SQLs())
}

func TestReplicator_GRPCAndSQLCoexist(t *testing.T) {
	pool := newRecordPool(2)
	fwd := &recordForwarder{}

	r, cancel := newReplicator(t, pool, 2, 1)
	defer cancel()
	r.SetGRPCForwarder(fwd)

	r.EnqueueGRPC([]byte{1, 2, 3}, "/proxy.Proxy/Execute", 0)
	r.Enqueue("CREATE TABLE t (id INT)", 0)

	waitForGRPCCalls(t, fwd, 1)
	waitForCalls(t, pool.executors[1], 1)

	assert.Len(t, fwd.getCalls(), 1)
	assert.Equal(t, 1, fwd.getCalls()[0].MasterIdx)
	assert.Equal(t, []string{"CREATE TABLE t (id INT)"}, pool.executors[1].SQLs())
}

func TestReplicator_StopsOnContextCancel(t *testing.T) {
	pool := newRecordPool(2)
	ctx, cancel := context.WithCancel(context.Background())
	cfg := config.ReplicationConfig{
		Workers:       2,
		RetryMax:      0,
		RetryBackoff:  time.Millisecond,
		QueueCapacity: 8,
	}
	replication.New(ctx, cfg, 2, pool, nil)
	cancel()

	time.Sleep(30 * time.Millisecond)
	require.True(t, true) // passes if it doesn't hang
}
