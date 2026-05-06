package transaction_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hive_v2/orchestrator/internal/transaction"
)

func TestTxBuffer_IsCrossMaster(t *testing.T) {
	t.Parallel()
	b := transaction.NewTxBuffer()
	b.Add(transaction.BufferedStmt{SQL: "SELECT 1", MasterIdx: 0})
	assert.False(t, b.IsCrossMaster())
	b.Add(transaction.BufferedStmt{SQL: "SELECT 1", MasterIdx: 1})
	assert.True(t, b.IsCrossMaster())
}
