package transaction

import "sort"

// BufferedStmt records a statement for bookkeeping (lazy-BEGIN executes writes immediately).
type BufferedStmt struct {
	SQL       string
	MasterIdx int
}

// TxBuffer holds orchestrator-side transaction state for cross-master lazy BEGIN.
type TxBuffer struct {
	Stmts             []BufferedStmt
	AffectedMasters   map[int]bool
	Active            bool
	BeginSQL          string
	MastersWithOpenTx map[int]bool
}

func NewTxBuffer() *TxBuffer {
	return &TxBuffer{
		AffectedMasters:   make(map[int]bool),
		MastersWithOpenTx: make(map[int]bool),
	}
}

func (b *TxBuffer) Add(stmt BufferedStmt) {
	b.Stmts = append(b.Stmts, stmt)
	b.AffectedMasters[stmt.MasterIdx] = true
}

func (b *TxBuffer) StmtsForMaster(idx int) []BufferedStmt {
	var out []BufferedStmt
	for _, s := range b.Stmts {
		if s.MasterIdx == idx {
			out = append(out, s)
		}
	}
	return out
}

func (b *TxBuffer) IsCrossMaster() bool {
	return len(b.AffectedMasters) > 1
}

func (b *TxBuffer) AffectedMasterIndices() []int {
	if len(b.AffectedMasters) == 0 {
		return nil
	}
	idxs := make([]int, 0, len(b.AffectedMasters))
	for i := range b.AffectedMasters {
		idxs = append(idxs, i)
	}
	sort.Ints(idxs)
	return idxs
}

func (b *TxBuffer) Reset() {
	b.Stmts = b.Stmts[:0]
	clear(b.AffectedMasters)
	clear(b.MastersWithOpenTx)
	b.Active = false
	b.BeginSQL = ""
}

func (b *TxBuffer) MarkMasterTxOpen(idx int) {
	b.MastersWithOpenTx[idx] = true
}

func (b *TxBuffer) MasterHasOpenTx(idx int) bool {
	return b.MastersWithOpenTx[idx]
}

func (b *TxBuffer) OpenMasterIndices() []int {
	if len(b.MastersWithOpenTx) == 0 {
		return nil
	}
	idxs := make([]int, 0, len(b.MastersWithOpenTx))
	for i := range b.MastersWithOpenTx {
		idxs = append(idxs, i)
	}
	sort.Ints(idxs)
	return idxs
}
