package router

import (
	"fmt"
	"sync"
)

type TableMap struct {
	mu          sync.RWMutex
	assignments map[string]int // table name -> master index
	masterCount int
}

func NewTableMap(masterCount int, initial map[string]int) (*TableMap, error) {
	if masterCount <= 0 {
		return nil, fmt.Errorf("router: masterCount must be > 0")
	}
	assignments := make(map[string]int, len(initial))
	for table, idx := range initial {
		if idx < 0 || idx >= masterCount {
			return nil, fmt.Errorf("router: table %q assigned to master %d, but only %d masters", table, idx, masterCount)
		}
		assignments[table] = idx
	}
	return &TableMap{
		assignments: assignments,
		masterCount: masterCount,
	}, nil
}

// If the table is not yet assigned, it is auto-assigned to the master
// with the fewest tables (ties broken by round-robin).
func (m *TableMap) MasterFor(table string) int {
	m.mu.RLock()
	idx, ok := m.assignments[table]
	m.mu.RUnlock()
	if ok {
		return idx
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// Re-check under write lock (another goroutine may have assigned it).
	if idx, ok = m.assignments[table]; ok {
		return idx
	}
	idx = m.leastLoadedMaster()
	m.assignments[table] = idx
	return idx
}

// Assign explicitly sets the master for a table, overwriting any existing assignment.
func (m *TableMap) Assign(table string, masterIdx int) error {
	if masterIdx < 0 || masterIdx >= m.masterCount {
		return fmt.Errorf("router: master index %d out of range [0, %d)", masterIdx, m.masterCount)
	}
	m.mu.Lock()
	m.assignments[table] = masterIdx
	m.mu.Unlock()
	return nil
}

func (m *TableMap) Snapshot() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]int, len(m.assignments))
	for k, v := range m.assignments {
		out[k] = v
	}
	return out
}

// leastLoadedMaster must be called with m.mu write-locked.
func (m *TableMap) leastLoadedMaster() int {
	counts := make([]int, m.masterCount)
	for _, idx := range m.assignments {
		counts[idx]++
	}
	best := 0
	for i := 1; i < m.masterCount; i++ {
		if counts[i] < counts[best] {
			best = i
		}
	}
	return best
}
