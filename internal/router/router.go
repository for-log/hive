package router

import (
	"fmt"
	"math/rand/v2"
	"sync/atomic"

	"github.com/hive_v2/orchestrator/internal/config"
	gosql "github.com/hive_v2/orchestrator/internal/sql"
)

// Master holds the index and URL of a single libSQL master.
type Master struct {
	Index int
	URL   string
}

// TransactionCoordinator is the seam for transaction boundary routing (BEGIN/COMMIT/ROLLBACK).
type TransactionCoordinator interface {
	// MasterForTx returns the master that should handle a transaction.
	// pinned is non-nil when the stream is already mid-transaction.
	MasterForTx(tables []string, pinned *Master) (*Master, error)
}

// Router decides which master handles a given query.
// Construct with New; do not copy after first use.
type Router struct {
	masters            []Master
	tableMap           *TableMap
	readPolicy         config.ReadPolicy
	analyzer           gosql.Analyzer
	rrCounter          atomic.Uint64 // round-robin state for reads
	txCoord            TransactionCoordinator
	crossMasterEnabled bool
}

// New creates a Router from the provided config.
func New(cfg *config.Config, analyzer gosql.Analyzer) (*Router, error) {
	if len(cfg.Masters) == 0 {
		return nil, fmt.Errorf("router: no masters configured")
	}

	masters := make([]Master, len(cfg.Masters))
	for i, m := range cfg.Masters {
		masters[i] = Master{Index: i, URL: m.URL}
	}

	tm, err := NewTableMap(len(masters), cfg.TableAssignments)
	if err != nil {
		return nil, err
	}

	r := &Router{
		masters:            masters,
		tableMap:           tm,
		readPolicy:         cfg.ReadPolicy,
		analyzer:           analyzer,
		crossMasterEnabled: cfg.Transaction.CrossMasterEnabled,
	}
	r.txCoord = &singleMasterTxCoordinator{router: r}
	return r, nil
}

// CrossMasterEnabled reports whether per-statement routing is allowed mid-transaction.
func (r *Router) CrossMasterEnabled() bool {
	return r.crossMasterEnabled
}

// RouteQuery returns the master that should handle the given SQL statement.
// For writes, it uses the table map. For reads, it applies the read policy.
// For transactions, it delegates to the TransactionCoordinator.
//
// routePerStatement, when true with CrossMasterEnabled, ignores pinning so each
// statement is routed by its tables (lazy cross-master transactions).
//
// pinned is the master the current stream is already bound to (mid-transaction).
func (r *Router) RouteQuery(sql string, pinned *Master, routePerStatement bool) (*Master, error) {
	info := r.analyzer.Analyze(sql)

	if info.IsTxBegin || info.IsTxEnd {
		return r.txCoord.MasterForTx(info.Tables, pinned)
	}

	if pinned != nil && !(routePerStatement && r.crossMasterEnabled) {
		return pinned, nil
	}

	if info.IsReadOnly {
		return r.routeRead(info.Tables), nil
	}

	return r.routeWrite(info.Tables)
}

// RouteWrite returns the master that owns the first recognised table.
// DDL (CREATE TABLE) also goes through here so the table gets assigned.
func (r *Router) RouteWrite(tables []string) (*Master, error) {
	return r.routeWrite(tables)
}

// RouteRead returns the master to use for a read query.
func (r *Router) RouteRead(tables []string) *Master {
	return r.routeRead(tables)
}

func (r *Router) TableMap() *TableMap {
	return r.tableMap
}

func (r *Router) routeWrite(tables []string) (*Master, error) {
	if len(tables) == 0 {
		// No table info — send to master 0 as a safe default.
		return &r.masters[0], nil
	}
	idx := r.tableMap.MasterFor(tables[0])
	return &r.masters[idx], nil
}

func (r *Router) routeRead(tables []string) *Master {
	switch r.readPolicy {
	case config.ReadPolicyWriteMaster:
		if len(tables) > 0 {
			idx := r.tableMap.MasterFor(tables[0])
			return &r.masters[idx]
		}
		return &r.masters[0]

	case config.ReadPolicyRoundRobin:
		n := uint64(len(r.masters))
		idx := (r.rrCounter.Add(1) - 1) % n
		return &r.masters[idx]

	case config.ReadPolicyRandom:
		return &r.masters[rand.IntN(len(r.masters))]

	default:
		return &r.masters[0]
	}
}

// IsReadOnly returns true when sql is a read-only statement (SELECT, PRAGMA, etc.).
func (r *Router) IsReadOnly(sql string) bool {
	return r.analyzer.Analyze(sql).IsReadOnly
}

// singleMasterTxCoordinator routes the entire transaction to one master.
// If the stream is already pinned (mid-tx), that master is reused.
type singleMasterTxCoordinator struct {
	router *Router
}

func (c *singleMasterTxCoordinator) MasterForTx(tables []string, pinned *Master) (*Master, error) {
	if pinned != nil {
		return pinned, nil
	}
	if len(tables) == 0 {
		return &c.router.masters[0], nil
	}
	idx := c.router.tableMap.MasterFor(tables[0])
	return &c.router.masters[idx], nil
}
