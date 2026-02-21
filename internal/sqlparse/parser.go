// Package sqlparse wraps vitess-sqlparser to extract routing metadata from SQL.
//
// The router and the driver both use this package to decide:
//   - whether a statement is DDL or DML
//   - which tables are referenced (for shard lookup)
//   - whether a RETURNING clause is present (INSERT/UPDATE/DELETE … RETURNING)
package sqlparse

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
)

type QueryType int

const (
	QueryDML QueryType = iota // SELECT, INSERT, UPDATE, DELETE
	QueryDDL                  // CREATE/DROP/ALTER/RENAME TABLE
)

type DDLAction int

const (
	DDLCreate DDLAction = iota
	DDLDrop
	DDLAlter
	DDLRename
)

type ParsedQuery struct {
	Tables       []string // lower-cased; DDL has one element (two for RENAME)
	Type         QueryType
	DDLAction    DDLAction // meaningful only when Type == QueryDDL
	HasReturning bool      // detected via regex; vitess-sqlparser does not model RETURNING
}

var returningRE = regexp.MustCompile(`(?i)\s+RETURNING\b.*$`)

func Parse(sql string) (ParsedQuery, error) {
	hasReturning := returningRE.MatchString(sql)

	// Strip RETURNING clause before handing to vitess-sqlparser, which does
	// not support it and would return a syntax error.
	parseable := sql
	if hasReturning {
		parseable = returningRE.ReplaceAllString(sql, "")
	}

	stmt, err := sqlparser.Parse(parseable)
	if err != nil {
		return ParsedQuery{}, fmt.Errorf("parse sql: %w", err)
	}

	pq := ParsedQuery{
		HasReturning: hasReturning,
	}

	switch s := stmt.(type) {
	case *sqlparser.CreateTable:
		pq.Type = QueryDDL
		pq.DDLAction = DDLCreate
		pq.Tables = []string{tableName(s.NewName)}

	case *sqlparser.DDL:
		pq.Type = QueryDDL
		pq.DDLAction = ddlAction(s.Action)
		pq.Tables = ddlTables(s)

	case *sqlparser.Select:
		pq.Type = QueryDML
		pq.Tables = tableExprs(s.From)

	case *sqlparser.Insert:
		pq.Type = QueryDML
		pq.Tables = []string{tableName(s.Table)}

	case *sqlparser.Update:
		pq.Type = QueryDML
		pq.Tables = tableExprs(s.TableExprs)

	case *sqlparser.Delete:
		pq.Type = QueryDML
		pq.Tables = tableExprs(s.TableExprs)

	case *sqlparser.Union:
		pq.Type = QueryDML
		pq.Tables = unionTables(s)

	default:
		// SHOW, SET, BEGIN, COMMIT, etc. — treat as DML with no tables.
		pq.Type = QueryDML
	}

	return pq, nil
}

func ddlAction(action string) DDLAction {
	switch action {
	case sqlparser.DropStr:
		return DDLDrop
	case sqlparser.AlterStr:
		return DDLAlter
	case sqlparser.RenameStr:
		return DDLRename
	default:
		return DDLCreate
	}
}

// ddlTables includes both old and new names for RENAME.
func ddlTables(d *sqlparser.DDL) []string {
	name := tableName(d.Table)
	if d.Action == sqlparser.RenameStr {
		newName := tableName(d.NewName)
		if newName != "" && newName != name {
			return []string{name, newName}
		}
	}
	return []string{name}
}

func tableExprs(exprs sqlparser.TableExprs) []string {
	names := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		names = append(names, tableExpr(expr)...)
	}
	return names
}

func tableExpr(expr sqlparser.TableExpr) []string {
	switch e := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		if tn, ok := e.Expr.(sqlparser.TableName); ok {
			if name := tableName(tn); name != "" {
				return []string{name}
			}
		}
		return nil

	case *sqlparser.JoinTableExpr:
		left := tableExpr(e.LeftExpr)
		right := tableExpr(e.RightExpr)
		result := make([]string, 0, len(left)+len(right))
		result = append(result, left...)
		result = append(result, right...)
		return result

	case *sqlparser.ParenTableExpr:
		return tableExprs(e.Exprs)

	default:
		return nil
	}
}

func unionTables(u *sqlparser.Union) []string {
	var names []string
	if sel, ok := u.Left.(*sqlparser.Select); ok {
		names = append(names, tableExprs(sel.From)...)
	}
	if sel, ok := u.Right.(*sqlparser.Select); ok {
		names = append(names, tableExprs(sel.From)...)
	}
	return names
}

func tableName(tn sqlparser.TableName) string {
	return strings.ToLower(tn.Name.String())
}
