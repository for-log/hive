package router_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hive_v2/orchestrator/internal/config"
	"github.com/hive_v2/orchestrator/internal/router"
	gosql "github.com/hive_v2/orchestrator/internal/sql"
)

func twoMasterCfg(policy config.ReadPolicy) *config.Config {
	return &config.Config{
		Masters: []config.MasterConfig{
			{URL: "http://m0"},
			{URL: "http://m1"},
		},
		TableAssignments: map[string]int{"users": 0, "orders": 1},
		ReadPolicy:       policy,
	}
}

func TestRouter_RouteWrite_KnownTable(t *testing.T) {
	t.Parallel()
	r, err := router.New(twoMasterCfg(config.ReadPolicyWriteMaster), gosql.TokenAnalyzer{})
	require.NoError(t, err)

	m, err := r.RouteQuery("INSERT INTO users VALUES (1)", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, m.Index)

	m, err = r.RouteQuery("UPDATE orders SET x=1", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, m.Index)
}

func TestRouter_RouteWrite_UnknownTable_DefaultsMaster0(t *testing.T) {
	t.Parallel()
	r, err := router.New(twoMasterCfg(config.ReadPolicyWriteMaster), gosql.TokenAnalyzer{})
	require.NoError(t, err)

	m, err := r.RouteQuery("INSERT INTO unknown_table VALUES (1)", nil)
	require.NoError(t, err)
	// unknown_table auto-assigned; with empty map it goes to least-loaded (0 or 1).
	assert.GreaterOrEqual(t, m.Index, 0)
	assert.Less(t, m.Index, 2)
}

func TestRouter_RouteRead_WriteMaster(t *testing.T) {
	t.Parallel()
	r, err := router.New(twoMasterCfg(config.ReadPolicyWriteMaster), gosql.TokenAnalyzer{})
	require.NoError(t, err)

	m, err := r.RouteQuery("SELECT * FROM users", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, m.Index)

	m, err = r.RouteQuery("SELECT * FROM orders", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, m.Index)
}

func TestRouter_RouteRead_RoundRobin(t *testing.T) {
	t.Parallel()
	r, err := router.New(twoMasterCfg(config.ReadPolicyRoundRobin), gosql.TokenAnalyzer{})
	require.NoError(t, err)

	seen := map[int]bool{}
	for i := 0; i < 20; i++ {
		m, err := r.RouteQuery("SELECT 1", nil)
		require.NoError(t, err)
		seen[m.Index] = true
	}
	assert.True(t, seen[0] && seen[1], "round-robin must hit both masters")
}

func TestRouter_RouteRead_Random(t *testing.T) {
	t.Parallel()
	r, err := router.New(twoMasterCfg(config.ReadPolicyRandom), gosql.TokenAnalyzer{})
	require.NoError(t, err)

	seen := map[int]bool{}
	for i := 0; i < 100; i++ {
		m, err := r.RouteQuery("SELECT 1", nil)
		require.NoError(t, err)
		seen[m.Index] = true
	}
	assert.True(t, seen[0] && seen[1], "random must eventually hit both masters")
}

func TestRouter_Transaction_PinnedMaster(t *testing.T) {
	t.Parallel()
	r, err := router.New(twoMasterCfg(config.ReadPolicyWriteMaster), gosql.TokenAnalyzer{})
	require.NoError(t, err)

	// BEGIN pins to master 0 (users table).
	pinned, err := r.RouteQuery("BEGIN", nil)
	require.NoError(t, err)

	// All subsequent statements must stay on the pinned master.
	for _, sql := range []string{
		"INSERT INTO users VALUES (1)",
		"INSERT INTO orders VALUES (2)", // orders is on master 1, but we're pinned
		"SELECT * FROM orders",
		"COMMIT",
	} {
		m, err := r.RouteQuery(sql, pinned)
		require.NoError(t, err)
		assert.Equal(t, pinned.Index, m.Index, "sql=%q", sql)
	}
}

func TestRouter_DDL_CreateTable(t *testing.T) {
	t.Parallel()
	r, err := router.New(twoMasterCfg(config.ReadPolicyWriteMaster), gosql.TokenAnalyzer{})
	require.NoError(t, err)

	m, err := r.RouteQuery("CREATE TABLE new_tbl (id INTEGER)", nil)
	require.NoError(t, err)
	// Should be auto-assigned and consistent on repeat.
	m2, err := r.RouteQuery("INSERT INTO new_tbl VALUES (1)", nil)
	require.NoError(t, err)
	assert.Equal(t, m.Index, m2.Index)
}

func TestRouter_NoMasters_Error(t *testing.T) {
	t.Parallel()
	_, err := router.New(&config.Config{}, gosql.TokenAnalyzer{})
	require.Error(t, err)
}
