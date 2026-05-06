package txlog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func ptrI(i int) *int { return &i }

func TestRecoverPartialCommit(t *testing.T) {
	t.Parallel()
	e := []Entry{
		{TxID: "t1", Type: EntryBegin, MasterIdx: ptrI(0)},
		{TxID: "t1", Type: EntryStatement, SQL: "INSERT INTO a VALUES (1)", MasterIdx: ptrI(0)},
		{TxID: "t1", Type: EntryStatement, SQL: "INSERT INTO b VALUES (1)", MasterIdx: ptrI(1)},
		{TxID: "t1", Type: EntryCommitStart, SQL: "COMMIT"},
		{TxID: "t1", Type: EntryMasterDone, MasterIdx: ptrI(0)},
	}
	p := Recover(e)
	require.Len(t, p, 1)
	require.True(t, p[0].NeedsCommit())
	require.False(t, p[0].NeedsRollback())
	require.Equal(t, []int{1}, p[0].MastersPendingCommit())
}

func TestRecoverAbandonedTransaction(t *testing.T) {
	t.Parallel()
	e := []Entry{
		{TxID: "t2", Type: EntryBegin, MasterIdx: ptrI(0)},
		{TxID: "t2", Type: EntryStatement, SQL: "INSERT INTO t VALUES (1)", MasterIdx: ptrI(0)},
	}
	p := Recover(e)
	require.Len(t, p, 1)
	require.False(t, p[0].NeedsCommit())
	require.True(t, p[0].NeedsRollback())
	require.Equal(t, []int{0}, p[0].MastersForRollback())
}

func TestRecoverIgnoresFinished(t *testing.T) {
	t.Parallel()
	e := []Entry{
		{TxID: "t3", Type: EntryBegin, MasterIdx: ptrI(0)},
		{TxID: "t3", Type: EntryRollback},
	}
	require.Empty(t, Recover(e))

	e2 := []Entry{
		{TxID: "t4", Type: EntryBegin, MasterIdx: ptrI(0)},
		{TxID: "t4", Type: EntryCommitStart, SQL: "COMMIT"},
		{TxID: "t4", Type: EntryMasterDone, MasterIdx: ptrI(0)},
		{TxID: "t4", Type: EntryComplete},
	}
	require.Empty(t, Recover(e2))
}

func TestRecoverAllMastersDoneWithoutCompleteNotPending(t *testing.T) {
	t.Parallel()
	e := []Entry{
		{TxID: "t5", Type: EntryBegin, MasterIdx: ptrI(0)},
		{TxID: "t5", Type: EntryCommitStart, SQL: "COMMIT"},
		{TxID: "t5", Type: EntryMasterDone, MasterIdx: ptrI(0)},
	}
	require.Empty(t, Recover(e))
}
