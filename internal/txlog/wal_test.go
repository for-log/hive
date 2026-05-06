package txlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenAppendReadAllClose(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "tx.wal")
	w, err := Open(path)
	require.NoError(t, err)
	m0 := 0
	require.NoError(t, w.Append(Entry{TxID: "a", Type: EntryBegin, SQL: "BEGIN", MasterIdx: &m0}))
	require.NoError(t, w.Append(Entry{TxID: "a", Type: EntryStatement, SQL: "INSERT INTO t VALUES (1)", MasterIdx: &m0}))
	require.NoError(t, w.Append(Entry{TxID: "a", Type: EntryCommitStart, SQL: "COMMIT"}))
	require.NoError(t, w.Close())

	entries, err := ReadAll(path)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	require.Equal(t, "a", entries[0].TxID)
	require.Equal(t, EntryBegin, entries[0].Type)
	require.NotNil(t, entries[0].MasterIdx)
	require.Equal(t, 0, *entries[0].MasterIdx)
	require.Equal(t, EntryCommitStart, entries[2].Type)
}

func TestReadAllMissingFile(t *testing.T) {
	t.Parallel()
	entries, err := ReadAll(filepath.Join(t.TempDir(), "none"))
	require.NoError(t, err)
	require.Nil(t, entries)
}

func TestCompactRemovesCompletedTransactions(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "tx.wal")
	w, err := Open(path)
	require.NoError(t, err)
	m0 := 0
	require.NoError(t, w.Append(Entry{TxID: "done", Type: EntryBegin, MasterIdx: &m0}))
	require.NoError(t, w.Append(Entry{TxID: "done", Type: EntryComplete}))
	require.NoError(t, w.Append(Entry{TxID: "open", Type: EntryBegin, MasterIdx: &m0}))
	require.NoError(t, w.Append(Entry{TxID: "open", Type: EntryStatement, SQL: "INSERT INTO t VALUES (1)", MasterIdx: &m0}))
	require.NoError(t, w.Compact())

	b, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(b), "open")
	require.NotContains(t, string(b), "done")

	require.NoError(t, w.Close())
}

func TestCompactNoopWhenNothingToDrop(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "tx.wal")
	w, err := Open(path)
	require.NoError(t, err)
	m0 := 0
	require.NoError(t, w.Append(Entry{TxID: "x", Type: EntryBegin, MasterIdx: &m0}))
	st := mustStat(t, path)
	size := st.Size()
	require.NoError(t, w.Compact())
	st2 := mustStat(t, path)
	require.Equal(t, size, st2.Size())
	require.NoError(t, w.Close())
}

func mustStat(t *testing.T, path string) os.FileInfo {
	t.Helper()
	fi, err := os.Stat(path)
	require.NoError(t, err)
	return fi
}
