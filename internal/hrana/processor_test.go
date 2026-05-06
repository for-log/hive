package hrana

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hive_v2/orchestrator/internal/config"
	"github.com/hive_v2/orchestrator/internal/router"
	gosql "github.com/hive_v2/orchestrator/internal/sql"
	"github.com/hive_v2/orchestrator/internal/stream"
)

type testFlexDoer struct {
	calls int
}

func (f *testFlexDoer) Pipeline(_ context.Context, req *PipelineRequest) (*PipelineResponse, error) {
	f.calls++
	n := len(req.Requests)
	res := make([]StreamResult, n)
	for i := 0; i < n; i++ {
		typ := req.Requests[i].Type
		if typ == "execute" {
			raw, _ := EmptyStmtResultJSON()
			res[i] = OkResult(StreamResponse{Type: "execute", Result: raw})
		} else {
			res[i] = OkResult(StreamResponse{Type: typ})
		}
	}
	return &PipelineResponse{Baton: req.Baton + "-n", Results: res}, nil
}

func (f *testFlexDoer) Cursor(_ context.Context, _ *CursorRequest) (*http.Response, error) {
	return nil, fmt.Errorf("cursor not used in test")
}

type testIdxPool struct {
	byIdx map[int]MasterDoer
}

func (p *testIdxPool) ClientFor(i int) MasterDoer { return p.byIdx[i] }

type fakeRepl struct {
	enqueueCalls []string
	txnCalls     [][]string
}

func (f *fakeRepl) Enqueue(sql string, _ int) {
	f.enqueueCalls = append(f.enqueueCalls, sql)
}

func (f *fakeRepl) EnqueueTxn(sqls []string, _ int) {
	cp := make([]string, len(sqls))
	copy(cp, sqls)
	f.txnCalls = append(f.txnCalls, cp)
}

func testRouterSingleTable(t *testing.T) *router.Router {
	t.Helper()
	cfg := &config.Config{
		Masters:          []config.MasterConfig{{URL: "http://m0"}},
		TableAssignments: map[string]int{"t": 0},
		ReadPolicy:       config.ReadPolicyWriteMaster,
	}
	rtr, err := router.New(cfg, gosql.TokenAnalyzer{})
	require.NoError(t, err)
	return rtr
}

func TestCommitRollbackAllMasters(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	a, b := &testFlexDoer{}, &testFlexDoer{}
	pool := &testIdxPool{byIdx: map[int]MasterDoer{0: a, 1: b}}
	batons := map[int]string{0: "x", 1: "y"}

	_, err := commitAllMasters(ctx, pool, batons, []int{1, 0}, "COMMIT", nil)
	require.NoError(t, err)
	assert.Equal(t, "x-n", batons[0])
	assert.Equal(t, "y-n", batons[1])

	batons[0], batons[1] = "p", "q"
	pool2 := &testIdxPool{byIdx: map[int]MasterDoer{0: &testFlexDoer{}, 1: &testFlexDoer{}}}
	_, err = rollbackAllMasters(ctx, pool2, batons, []int{0, 1})
	require.NoError(t, err)
}

func TestProcessor_ReplicationAutocommitImmediate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repl := &fakeRepl{}
	p := NewProcessor(ProcessorConfig{
		Pool:       &testIdxPool{byIdx: map[int]MasterDoer{0: &testFlexDoer{}}},
		Router:     testRouterSingleTable(t),
		Replicator: repl,
	})
	strm := &stream.Stream{
		MasterBatons: map[int]string{0: "b0"},
		SQLStore:     stream.NewSQLCache(),
	}
	sql := "INSERT INTO t VALUES (1)"
	res, err := p.ProxyRequest(ctx, strm, ExecuteRequest(sql, false))
	require.NoError(t, err)
	require.Equal(t, "ok", res.Type)
	require.Equal(t, []string{sql}, repl.enqueueCalls)
	require.Empty(t, repl.txnCalls)
}

func TestProcessor_ReplicationTxBufferedUntilCommit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repl := &fakeRepl{}
	p := NewProcessor(ProcessorConfig{
		Pool:       &testIdxPool{byIdx: map[int]MasterDoer{0: &testFlexDoer{}}},
		Router:     testRouterSingleTable(t),
		Replicator: repl,
	})
	strm := &stream.Stream{
		MasterBatons: map[int]string{0: "b0"},
		SQLStore:     stream.NewSQLCache(),
	}
	begin := "BEGIN"
	ins := "INSERT INTO t VALUES (1)"
	commit := "COMMIT"
	res, err := p.ProxyRequest(ctx, strm, ExecuteRequest(begin, false))
	require.NoError(t, err)
	require.Equal(t, "ok", res.Type)
	require.Empty(t, repl.enqueueCalls)
	require.Empty(t, repl.txnCalls)

	res, err = p.ProxyRequest(ctx, strm, ExecuteRequest(ins, false))
	require.NoError(t, err)
	require.Equal(t, "ok", res.Type)
	require.Empty(t, repl.enqueueCalls)
	require.Empty(t, repl.txnCalls)

	res, err = p.ProxyRequest(ctx, strm, ExecuteRequest(commit, false))
	require.NoError(t, err)
	require.Equal(t, "ok", res.Type)
	require.Empty(t, repl.enqueueCalls)
	require.Len(t, repl.txnCalls, 1)
	assert.Equal(t, []string{begin, ins, "COMMIT"}, repl.txnCalls[0])
}

func TestProcessor_DuplicateBeginPreservesBufferedReplication(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repl := &fakeRepl{}
	p := NewProcessor(ProcessorConfig{
		Pool:       &testIdxPool{byIdx: map[int]MasterDoer{0: &testFlexDoer{}}},
		Router:     testRouterSingleTable(t),
		Replicator: repl,
	})
	strm := &stream.Stream{
		MasterBatons: map[int]string{0: "b0"},
		SQLStore:     stream.NewSQLCache(),
	}
	ins1 := "INSERT INTO t VALUES (1)"
	ins2 := "INSERT INTO t VALUES (2)"

	res, err := p.ProxyRequest(ctx, strm, ExecuteRequest("BEGIN", false))
	require.NoError(t, err)
	require.Equal(t, "ok", res.Type)
	require.NotNil(t, strm.TxBuffer)
	require.True(t, strm.TxBuffer.Active)
	require.Empty(t, repl.txnCalls)

	res, err = p.ProxyRequest(ctx, strm, ExecuteRequest(ins1, false))
	require.NoError(t, err)
	require.Equal(t, "ok", res.Type)
	require.Len(t, strm.TxBuffer.Stmts, 1)

	// Second BEGIN (e.g. go-libsql sending BEGIN after BEGIN;) must not reset the replication buffer.
	res, err = p.ProxyRequest(ctx, strm, ExecuteRequest("BEGIN;", false))
	require.NoError(t, err)
	require.Equal(t, "ok", res.Type)
	require.Len(t, strm.TxBuffer.Stmts, 1, "duplicate BEGIN must not clear buffered writes")

	res, err = p.ProxyRequest(ctx, strm, ExecuteRequest(ins2, false))
	require.NoError(t, err)
	require.Equal(t, "ok", res.Type)
	require.Len(t, strm.TxBuffer.Stmts, 2)

	res, err = p.ProxyRequest(ctx, strm, ExecuteRequest("COMMIT", false))
	require.NoError(t, err)
	require.Equal(t, "ok", res.Type)
	require.Len(t, repl.txnCalls, 1)
	assert.Equal(t, []string{"BEGIN", ins1, ins2, "COMMIT"}, repl.txnCalls[0])
}

func TestProcessor_ReplicationTxRollbackNoReplicate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repl := &fakeRepl{}
	p := NewProcessor(ProcessorConfig{
		Pool:       &testIdxPool{byIdx: map[int]MasterDoer{0: &testFlexDoer{}}},
		Router:     testRouterSingleTable(t),
		Replicator: repl,
	})
	strm := &stream.Stream{
		MasterBatons: map[int]string{0: "b0"},
		SQLStore:     stream.NewSQLCache(),
	}
	res, err := p.ProxyRequest(ctx, strm, ExecuteRequest("BEGIN", false))
	require.NoError(t, err)
	require.Equal(t, "ok", res.Type)
	res, err = p.ProxyRequest(ctx, strm, ExecuteRequest("INSERT INTO t VALUES (1)", false))
	require.NoError(t, err)
	require.Equal(t, "ok", res.Type)
	res, err = p.ProxyRequest(ctx, strm, ExecuteRequest("ROLLBACK", false))
	require.NoError(t, err)
	require.Equal(t, "ok", res.Type)
	require.Empty(t, repl.enqueueCalls)
	require.Empty(t, repl.txnCalls)
}
