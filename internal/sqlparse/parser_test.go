package sqlparse_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"hive/internal/sqlparse"
)

func TestParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		sql           string
		wantTables    []string
		wantType      sqlparse.QueryType
		wantDDL       sqlparse.DDLAction
		wantReturning bool
		wantErr       bool
	}{
		{
			name:       "simple select",
			sql:        "SELECT id, name FROM users",
			wantType:   sqlparse.QueryDML,
			wantTables: []string{"users"},
		},
		{
			name:       "select with where",
			sql:        "SELECT * FROM orders WHERE id = 1",
			wantType:   sqlparse.QueryDML,
			wantTables: []string{"orders"},
		},
		{
			name:       "select join two tables",
			sql:        "SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id",
			wantType:   sqlparse.QueryDML,
			wantTables: []string{"users", "orders"},
		},
		{
			name:       "select with alias",
			sql:        "SELECT * FROM users AS u",
			wantType:   sqlparse.QueryDML,
			wantTables: []string{"users"},
		},
		{
			name:       "simple insert",
			sql:        "INSERT INTO users (name) VALUES ('Alice')",
			wantType:   sqlparse.QueryDML,
			wantTables: []string{"users"},
		},
		{
			name:          "insert with returning",
			sql:           "INSERT INTO users (name) VALUES ('Bob') RETURNING *",
			wantType:      sqlparse.QueryDML,
			wantTables:    []string{"users"},
			wantReturning: true,
		},
		{
			name:       "simple update",
			sql:        "UPDATE users SET name = 'Alice' WHERE id = 1",
			wantType:   sqlparse.QueryDML,
			wantTables: []string{"users"},
		},
		{
			name:          "update with returning",
			sql:           "UPDATE users SET name = 'Alice' WHERE id = 1 RETURNING id, name",
			wantType:      sqlparse.QueryDML,
			wantTables:    []string{"users"},
			wantReturning: true,
		},
		{
			name:       "simple delete",
			sql:        "DELETE FROM orders WHERE id = 5",
			wantType:   sqlparse.QueryDML,
			wantTables: []string{"orders"},
		},
		{
			name:          "delete with returning",
			sql:           "DELETE FROM orders WHERE id = 5 RETURNING id",
			wantType:      sqlparse.QueryDML,
			wantTables:    []string{"orders"},
			wantReturning: true,
		},
		{
			name:       "create table",
			sql:        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
			wantType:   sqlparse.QueryDDL,
			wantDDL:    sqlparse.DDLCreate,
			wantTables: []string{"users"},
		},
		{
			name:       "drop table",
			sql:        "DROP TABLE orders",
			wantType:   sqlparse.QueryDDL,
			wantDDL:    sqlparse.DDLDrop,
			wantTables: []string{"orders"},
		},
		{
			name:       "alter table add column",
			sql:        "ALTER TABLE users ADD COLUMN email TEXT",
			wantType:   sqlparse.QueryDDL,
			wantDDL:    sqlparse.DDLAlter,
			wantTables: []string{"users"},
		},
		{
			name:       "table name is lower-cased",
			sql:        "SELECT * FROM USERS",
			wantType:   sqlparse.QueryDML,
			wantTables: []string{"users"},
		},
		{
			name:          "returning keyword lowercase",
			sql:           "INSERT INTO users (name) VALUES ('x') returning id",
			wantType:      sqlparse.QueryDML,
			wantTables:    []string{"users"},
			wantReturning: true,
		},
		{
			name:    "invalid sql",
			sql:     "THIS IS NOT SQL !!!",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pq, err := sqlparse.Parse(tc.sql)

			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.wantType, pq.Type, "QueryType mismatch")
			require.Equal(t, tc.wantTables, pq.Tables, "Tables mismatch")
			require.Equal(t, tc.wantReturning, pq.HasReturning, "HasReturning mismatch")

			if tc.wantType == sqlparse.QueryDDL {
				require.Equal(t, tc.wantDDL, pq.DDLAction, "DDLAction mismatch")
			}
		})
	}
}
