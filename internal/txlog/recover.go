package txlog

import "sort"

// PendingTx describes a transaction that still needs orchestrator action after a crash.
type PendingTx struct {
	TxID         string
	State        EntryType // Last WAL entry type for this tx (for debugging)
	Statements   []Entry   // Entries with Type == EntryStatement
	DoneMasters  map[int]bool
	AllMasters   map[int]bool
	commitSeen   bool
	completeSeen bool
	rollbackSeen bool
}

func (p PendingTx) NeedsCommit() bool {
	if !p.commitSeen || p.completeSeen || p.rollbackSeen {
		return false
	}
	for m := range p.AllMasters {
		if !p.DoneMasters[m] {
			return true
		}
	}
	return false
}

func (p PendingTx) MastersPendingCommit() []int {
	if !p.NeedsCommit() {
		return nil
	}
	var out []int
	for m := range p.AllMasters {
		if !p.DoneMasters[m] {
			out = append(out, m)
		}
	}
	sort.Ints(out)
	return out
}

func (p PendingTx) NeedsRollback() bool {
	if p.completeSeen || p.rollbackSeen || p.commitSeen {
		return false
	}
	return len(p.AllMasters) > 0 || len(p.Statements) > 0 || p.State == EntryBegin
}

func (p PendingTx) MastersForRollback() []int {
	var out []int
	for m := range p.AllMasters {
		out = append(out, m)
	}
	sort.Ints(out)
	return out
}

func Recover(entries []Entry) []PendingTx {
	byTx := make(map[string]*aggTx)

	for _, e := range entries {
		if e.TxID == "" {
			continue
		}
		a := byTx[e.TxID]
		if a == nil {
			a = &aggTx{
				TxID:        e.TxID,
				allMasters:  make(map[int]bool),
				doneMasters: make(map[int]bool),
			}
			byTx[e.TxID] = a
		}
		a.apply(e)
	}

	var out []PendingTx
	for _, a := range byTx {
		if a.completeSeen || a.rollbackSeen {
			continue
		}
		pt := PendingTx{
			TxID:         a.TxID,
			State:        a.lastType,
			Statements:   cloneEntries(a.statements),
			DoneMasters:  copyBoolMap(a.doneMasters),
			AllMasters:   copyBoolMap(a.allMasters),
			commitSeen:   a.commitSeen,
			completeSeen: a.completeSeen,
			rollbackSeen: a.rollbackSeen,
		}
		if pt.NeedsCommit() || pt.NeedsRollback() {
			out = append(out, pt)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TxID < out[j].TxID })
	return out
}

type aggTx struct {
	TxID         string
	lastType     EntryType
	statements   []Entry
	allMasters   map[int]bool
	doneMasters  map[int]bool
	commitSeen   bool
	completeSeen bool
	rollbackSeen bool
}

func (a *aggTx) apply(e Entry) {
	a.lastType = e.Type
	switch e.Type {
	case EntryBegin:
		addMasterPtr(a.allMasters, e.MasterIdx)
	case EntryStatement:
		a.statements = append(a.statements, e)
		addMasterPtr(a.allMasters, e.MasterIdx)
	case EntryCommitStart:
		a.commitSeen = true
	case EntryMasterDone:
		addMasterPtr(a.allMasters, e.MasterIdx)
		if e.MasterIdx != nil {
			a.doneMasters[*e.MasterIdx] = true
		}
	case EntryComplete:
		a.completeSeen = true
	case EntryRollback:
		a.rollbackSeen = true
	}
}

func addMasterPtr(m map[int]bool, p *int) {
	if p == nil {
		return
	}
	m[*p] = true
}

func copyBoolMap(in map[int]bool) map[int]bool {
	out := make(map[int]bool, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneEntries(in []Entry) []Entry {
	out := make([]Entry, len(in))
	copy(out, in)
	return out
}
