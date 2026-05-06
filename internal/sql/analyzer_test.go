package sql_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	gosql "github.com/hive_v2/orchestrator/internal/sql"
)

func TestAnalyze(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sql        string
		wantType   gosql.QueryType
		wantTables []string
		wantRO     bool
		wantBegin  bool
		wantEnd    bool
		wantDDL    bool
	}{
		// SELECT
		{
			name:       "simple select",
			sql:        "SELECT * FROM users",
			wantType:   gosql.QueryTypeSelect,
			wantTables: []string{"users"},
			wantRO:     true,
		},
		{
			name:       "select uppercase",
			sql:        "SELECT id, name FROM orders WHERE id = 1",
			wantType:   gosql.QueryTypeSelect,
			wantTables: []string{"orders"},
			wantRO:     true,
		},
		{
			name:     "select no from",
			sql:      "SELECT 1",
			wantType: gosql.QueryTypeSelect,
			wantRO:   true,
		},
		{
			name:     "explain",
			sql:      "EXPLAIN SELECT 1",
			wantType: gosql.QueryTypeSelect,
			wantRO:   true,
		},
		{
			name:     "with cte",
			sql:      "WITH cte AS (SELECT 1) SELECT * FROM cte",
			wantType: gosql.QueryTypeSelect,
			wantRO:   true,
		},

		// INSERT
		{
			name:       "insert",
			sql:        "INSERT INTO users (name) VALUES ('alice')",
			wantType:   gosql.QueryTypeInsert,
			wantTables: []string{"users"},
		},
		{
			name:       "insert or replace",
			sql:        "INSERT OR REPLACE INTO orders VALUES (1, 2)",
			wantType:   gosql.QueryTypeInsert,
			wantTables: []string{"orders"},
		},
		{
			name:       "replace into",
			sql:        "REPLACE INTO users VALUES (1, 'bob')",
			wantType:   gosql.QueryTypeInsert,
			wantTables: []string{"users"},
		},
		{
			name:       "insert quoted table",
			sql:        `INSERT INTO "my table" VALUES (1)`,
			wantType:   gosql.QueryTypeInsert,
			wantTables: []string{"my table"},
		},
		{
			name:       "insert backtick table",
			sql:        "INSERT INTO `orders` VALUES (1)",
			wantType:   gosql.QueryTypeInsert,
			wantTables: []string{"orders"},
		},
		{
			// "main" is SQLite's default schema alias — main.users == users.
			name:       "insert main schema stripped",
			sql:        "INSERT INTO main.users VALUES (1)",
			wantType:   gosql.QueryTypeInsert,
			wantTables: []string{"users"},
		},
		{
			// Non-default schema (ATTACH DATABASE … AS other) is preserved so
			// the router can distinguish other.users from main.users.
			name:       "insert non-default schema preserved",
			sql:        "INSERT INTO other.users VALUES (1)",
			wantType:   gosql.QueryTypeInsert,
			wantTables: []string{"other.users"},
		},

		// UPDATE
		{
			name:       "update",
			sql:        "UPDATE users SET name = 'bob' WHERE id = 1",
			wantType:   gosql.QueryTypeUpdate,
			wantTables: []string{"users"},
		},
		{
			name:       "update or ignore",
			sql:        "UPDATE OR IGNORE orders SET status = 1",
			wantType:   gosql.QueryTypeUpdate,
			wantTables: []string{"orders"},
		},

		// DELETE
		{
			name:       "delete",
			sql:        "DELETE FROM users WHERE id = 1",
			wantType:   gosql.QueryTypeDelete,
			wantTables: []string{"users"},
		},

		// CREATE TABLE
		{
			name:       "create table",
			sql:        "CREATE TABLE users (id INTEGER PRIMARY KEY)",
			wantType:   gosql.QueryTypeCreateTable,
			wantTables: []string{"users"},
			wantDDL:    true,
		},
		{
			name:       "create temp table",
			sql:        "CREATE TEMP TABLE tmp (id INTEGER)",
			wantType:   gosql.QueryTypeCreateTable,
			wantTables: []string{"tmp"},
			wantDDL:    true,
		},
		{
			name:       "create table if not exists",
			sql:        "CREATE TABLE IF NOT EXISTS orders (id INTEGER)",
			wantType:   gosql.QueryTypeCreateTable,
			wantTables: []string{"orders"},
			wantDDL:    true,
		},

		// ALTER TABLE
		{
			name:       "alter table",
			sql:        "ALTER TABLE users ADD COLUMN email TEXT",
			wantType:   gosql.QueryTypeAlterTable,
			wantTables: []string{"users"},
			wantDDL:    true,
		},

		// DROP TABLE
		{
			name:       "drop table",
			sql:        "DROP TABLE users",
			wantType:   gosql.QueryTypeDropTable,
			wantTables: []string{"users"},
			wantDDL:    true,
		},
		{
			name:       "drop table if exists",
			sql:        "DROP TABLE IF EXISTS orders",
			wantType:   gosql.QueryTypeDropTable,
			wantTables: []string{"orders"},
			wantDDL:    true,
		},

		// CREATE INDEX
		{
			name:       "create index",
			sql:        "CREATE INDEX idx_users_name ON users (name)",
			wantType:   gosql.QueryTypeCreateIndex,
			wantTables: []string{"users"},
			wantDDL:    true,
		},
		{
			name:       "create unique index",
			sql:        "CREATE UNIQUE INDEX idx ON orders (ref)",
			wantType:   gosql.QueryTypeCreateIndex,
			wantTables: []string{"orders"},
			wantDDL:    true,
		},

		// DROP INDEX
		{
			name:     "drop index",
			sql:      "DROP INDEX idx_users_name",
			wantType: gosql.QueryTypeDropIndex,
			wantDDL:  true,
		},

		// Transactions
		{
			name:      "begin",
			sql:       "BEGIN",
			wantType:  gosql.QueryTypeTxBegin,
			wantBegin: true,
			wantRO:    true,
		},
		{
			name:      "begin deferred",
			sql:       "BEGIN DEFERRED",
			wantType:  gosql.QueryTypeTxBegin,
			wantBegin: true,
			wantRO:    true,
		},
		{
			name:      "begin immediate",
			sql:       "BEGIN IMMEDIATE TRANSACTION",
			wantType:  gosql.QueryTypeTxBegin,
			wantBegin: true,
			wantRO:    true,
		},
		{
			name:     "commit",
			sql:      "COMMIT",
			wantType: gosql.QueryTypeTxCommit,
			wantEnd:  true,
		},
		{
			name:     "end",
			sql:      "END",
			wantType: gosql.QueryTypeTxCommit,
			wantEnd:  true,
		},
		{
			name:     "rollback",
			sql:      "ROLLBACK",
			wantType: gosql.QueryTypeTxRollback,
			wantEnd:  true,
		},

		// PRAGMA
		{
			name:     "pragma read",
			sql:      "PRAGMA journal_mode",
			wantType: gosql.QueryTypePragma,
			wantRO:   true,
		},
		{
			name:     "pragma write",
			sql:      "PRAGMA journal_mode = WAL",
			wantType: gosql.QueryTypePragma,
			wantRO:   false,
		},

		// Comments stripped
		{
			name:     "line comment",
			sql:      "-- get all users\nSELECT * FROM users",
			wantType: gosql.QueryTypeSelect,
			wantRO:   true,
		},
		{
			name:       "block comment",
			sql:        "/* insert */ INSERT INTO users VALUES (1)",
			wantType:   gosql.QueryTypeInsert,
			wantTables: []string{"users"},
		},

		// Unclosed block comment — must not panic, treated as unknown
		{
			name:     "unclosed block comment",
			sql:      "/* this comment is never closed SELECT * FROM t",
			wantType: gosql.QueryTypeUnknown,
		},

		// Empty / unknown
		{
			name:     "empty",
			sql:      "",
			wantType: gosql.QueryTypeUnknown,
		},
		{
			name:     "vacuum",
			sql:      "VACUUM",
			wantType: gosql.QueryTypeOther,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := gosql.Analyze(tc.sql)
			assert.Equal(t, tc.wantType, got.Type, "Type")
			assert.Equal(t, tc.wantRO, got.IsReadOnly, "IsReadOnly")
			assert.Equal(t, tc.wantBegin, got.IsTxBegin, "IsTxBegin")
			assert.Equal(t, tc.wantEnd, got.IsTxEnd, "IsTxEnd")
			assert.Equal(t, tc.wantDDL, got.IsDDL, "IsDDL")
			if tc.wantTables != nil {
				assert.Equal(t, tc.wantTables, got.Tables, "Tables")
			}
		})
	}
}
