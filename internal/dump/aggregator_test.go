package dump_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hive_v2/orchestrator/internal/dump"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

type fakeDumper struct {
	body string
	err  error
}

func (f *fakeDumper) Dump(_ context.Context) (string, error) {
	return f.body, f.err
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestAggregator_MergesTwoDumps(t *testing.T) {
	agg := dump.New([]dump.MasterDumper{
		&fakeDumper{body: "CREATE TABLE users (id INTEGER);\nINSERT INTO users VALUES (1);"},
		&fakeDumper{body: "CREATE TABLE posts (id INTEGER);\nINSERT INTO posts VALUES (42);"},
	})

	result, err := agg.Dump(context.Background())
	require.NoError(t, err)

	assert.Contains(t, result, "CREATE TABLE users")
	assert.Contains(t, result, "CREATE TABLE posts")
	assert.Contains(t, result, "INSERT INTO users VALUES (1)")
	assert.Contains(t, result, "INSERT INTO posts VALUES (42)")
	assert.Contains(t, result, "PRAGMA foreign_keys=OFF;")
	assert.Contains(t, result, "PRAGMA foreign_keys=ON;")
}

func TestAggregator_SingleMaster(t *testing.T) {
	body := "CREATE TABLE t (x TEXT);\nINSERT INTO t VALUES ('hello');"
	agg := dump.New([]dump.MasterDumper{
		&fakeDumper{body: body},
	})

	result, err := agg.Dump(context.Background())
	require.NoError(t, err)
	assert.Contains(t, result, "CREATE TABLE t")
	assert.Contains(t, result, "hello")
}

func TestAggregator_EmptyDump(t *testing.T) {
	agg := dump.New([]dump.MasterDumper{
		&fakeDumper{body: ""},
		&fakeDumper{body: ""},
	})

	result, err := agg.Dump(context.Background())
	require.NoError(t, err)
	assert.Contains(t, result, "PRAGMA foreign_keys=OFF;")
	assert.Contains(t, result, "PRAGMA foreign_keys=ON;")
}

func TestAggregator_ErrorFromOneMaster(t *testing.T) {
	agg := dump.New([]dump.MasterDumper{
		&fakeDumper{body: "CREATE TABLE t (x TEXT);"},
		&fakeDumper{err: fmt.Errorf("connection refused")},
	})

	_, err := agg.Dump(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "master[1]")
	assert.Contains(t, err.Error(), "connection refused")
}

func TestAggregator_MasterOrderPreserved(t *testing.T) {
	agg := dump.New([]dump.MasterDumper{
		&fakeDumper{body: "-- from master 0"},
		&fakeDumper{body: "-- from master 1"},
		&fakeDumper{body: "-- from master 2"},
	})

	result, err := agg.Dump(context.Background())
	require.NoError(t, err)

	idx0 := strings.Index(result, "-- from master 0")
	idx1 := strings.Index(result, "-- from master 1")
	idx2 := strings.Index(result, "-- from master 2")
	assert.Less(t, idx0, idx1, "master 0 should appear before master 1")
	assert.Less(t, idx1, idx2, "master 1 should appear before master 2")
}

func TestAggregator_StripsPragmaVariants(t *testing.T) {
	// Dump body with PRAGMA lines in various formats (with newlines, spaces, mixed case).
	body := "PRAGMA foreign_keys=OFF;\nCREATE TABLE t (x TEXT);\nPRAGMA foreign_keys=ON;\n"
	agg := dump.New([]dump.MasterDumper{
		&fakeDumper{body: body},
	})

	result, err := agg.Dump(context.Background())
	require.NoError(t, err)

	// The outer pair should be present exactly once.
	assert.Equal(t, 1, strings.Count(result, "PRAGMA foreign_keys=OFF;"), "should have exactly one PRAGMA OFF")
	assert.Equal(t, 1, strings.Count(result, "PRAGMA foreign_keys=ON;"), "should have exactly one PRAGMA ON")
	assert.Contains(t, result, "CREATE TABLE t")
}

func TestAggregator_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	agg := dump.New([]dump.MasterDumper{
		&fakeDumper{err: context.Canceled},
	})

	_, err := agg.Dump(ctx)
	require.Error(t, err)
}
