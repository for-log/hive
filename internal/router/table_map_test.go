package router_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hive_v2/orchestrator/internal/router"
)

func TestTableMap_InitialAssignments(t *testing.T) {
	t.Parallel()
	tm, err := router.NewTableMap(3, map[string]int{"users": 0, "orders": 1})
	require.NoError(t, err)
	assert.Equal(t, 0, tm.MasterFor("users"))
	assert.Equal(t, 1, tm.MasterFor("orders"))
}

func TestTableMap_AutoAssign_LeastLoaded(t *testing.T) {
	t.Parallel()
	// master 0 has 2 tables, master 1 has 1, master 2 has 0 → new table → master 2
	tm, err := router.NewTableMap(3, map[string]int{
		"a": 0, "b": 0, "c": 1,
	})
	require.NoError(t, err)
	assert.Equal(t, 2, tm.MasterFor("new_table"))
}

func TestTableMap_AutoAssign_Stable(t *testing.T) {
	t.Parallel()
	tm, err := router.NewTableMap(2, nil)
	require.NoError(t, err)
	first := tm.MasterFor("tbl")
	// Subsequent calls must return the same master.
	for i := 0; i < 10; i++ {
		assert.Equal(t, first, tm.MasterFor("tbl"))
	}
}

func TestTableMap_Assign_Override(t *testing.T) {
	t.Parallel()
	tm, err := router.NewTableMap(2, map[string]int{"users": 0})
	require.NoError(t, err)
	require.NoError(t, tm.Assign("users", 1))
	assert.Equal(t, 1, tm.MasterFor("users"))
}

func TestTableMap_Assign_OutOfRange(t *testing.T) {
	t.Parallel()
	tm, err := router.NewTableMap(2, nil)
	require.NoError(t, err)
	assert.Error(t, tm.Assign("t", 5))
	assert.Error(t, tm.Assign("t", -1))
}

func TestTableMap_InvalidInit(t *testing.T) {
	t.Parallel()
	_, err := router.NewTableMap(0, nil)
	require.Error(t, err)

	_, err = router.NewTableMap(2, map[string]int{"t": 5})
	require.Error(t, err)
}

func TestTableMap_Snapshot(t *testing.T) {
	t.Parallel()
	tm, err := router.NewTableMap(2, map[string]int{"a": 0, "b": 1})
	require.NoError(t, err)
	snap := tm.Snapshot()
	assert.Equal(t, map[string]int{"a": 0, "b": 1}, snap)
	// Mutating the snapshot must not affect the map.
	snap["a"] = 99
	assert.Equal(t, 0, tm.MasterFor("a"))
}

func TestTableMap_ConcurrentAutoAssign(t *testing.T) {
	t.Parallel()
	tm, err := router.NewTableMap(3, nil)
	require.NoError(t, err)

	var wg sync.WaitGroup
	results := make([]int, 50)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = tm.MasterFor("shared_table")
		}(i)
	}
	wg.Wait()

	first := results[0]
	for _, v := range results {
		assert.Equal(t, first, v, "all goroutines must see the same assignment")
	}
}
