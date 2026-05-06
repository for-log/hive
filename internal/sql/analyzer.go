package sql

import (
	"strings"
	"unicode"
)

type QueryType int

const (
	QueryTypeUnknown     QueryType = iota
	QueryTypeSelect                // SELECT / EXPLAIN
	QueryTypeInsert                // INSERT / REPLACE
	QueryTypeUpdate                // UPDATE
	QueryTypeDelete                // DELETE
	QueryTypeCreateTable           // CREATE TABLE
	QueryTypeAlterTable            // ALTER TABLE
	QueryTypeDropTable             // DROP TABLE
	QueryTypeCreateIndex           // CREATE INDEX / CREATE UNIQUE INDEX
	QueryTypeDropIndex             // DROP INDEX
	QueryTypeTxBegin               // BEGIN / BEGIN DEFERRED / BEGIN IMMEDIATE / BEGIN EXCLUSIVE
	QueryTypeTxCommit              // COMMIT / END
	QueryTypeTxRollback            // ROLLBACK
	QueryTypePragma                // PRAGMA
	QueryTypeOther                 // VACUUM, ATTACH, DETACH, …
)

type QueryInfo struct {
	Type       QueryType
	Tables     []string
	IsReadOnly bool
	IsTxBegin  bool
	IsTxEnd    bool // COMMIT or ROLLBACK
	IsDDL      bool // CREATE / ALTER / DROP
}

// Analyzer extracts routing metadata from a single SQL statement.
// The interface is intentionally narrow so alternative implementations
// (e.g. a full AST-based parser) can be swapped in without touching callers.
type Analyzer interface {
	Analyze(sql string) QueryInfo
}

// TokenAnalyzer is the default lightweight implementation based on tokenization.
// It covers the common SQLite DML/DDL patterns needed for routing without
// pulling in an external parser dependency.
type TokenAnalyzer struct{}

func (TokenAnalyzer) Analyze(sql string) QueryInfo {
	return Analyze(sql)
}

// Analyze parses sql (a single SQLite statement) and returns routing metadata.
// It uses a lightweight token-based approach; it does not validate SQL syntax.
// Prefer injecting an Analyzer interface over calling this directly.
func Analyze(sql string) QueryInfo {
	tokens := tokenize(sql)
	if len(tokens) == 0 {
		return QueryInfo{Type: QueryTypeUnknown}
	}

	first := upper(tokens[0])

	switch first {
	case "SELECT", "EXPLAIN":
		return QueryInfo{
			Type:       QueryTypeSelect,
			Tables:     extractSelectTables(tokens),
			IsReadOnly: true,
		}

	case "WITH":
		// CTE: tables inside the CTE body are not routing-relevant;
		// we look for the final SELECT's FROM clause after the closing paren.
		return QueryInfo{Type: QueryTypeSelect, IsReadOnly: true}

	case "INSERT", "REPLACE":
		return QueryInfo{
			Type:   QueryTypeInsert,
			Tables: extractInsertTable(tokens),
		}

	case "UPDATE":
		return QueryInfo{
			Type:   QueryTypeUpdate,
			Tables: extractUpdateTable(tokens),
		}

	case "DELETE":
		return QueryInfo{
			Type:   QueryTypeDelete,
			Tables: extractDeleteTable(tokens),
		}

	case "CREATE":
		return analyzeCreate(tokens)

	case "ALTER":
		return analyzeAlter(tokens)

	case "DROP":
		return analyzeDrop(tokens)

	case "BEGIN":
		return QueryInfo{Type: QueryTypeTxBegin, IsTxBegin: true, IsReadOnly: true}

	case "COMMIT", "END":
		return QueryInfo{Type: QueryTypeTxCommit, IsTxEnd: true}

	case "ROLLBACK":
		return QueryInfo{Type: QueryTypeTxRollback, IsTxEnd: true}

	case "PRAGMA":
		return analyzePragma(tokens)

	default:
		return QueryInfo{Type: QueryTypeOther}
	}
}

func analyzeCreate(tokens []string) QueryInfo {
	// CREATE [TEMP|TEMPORARY] TABLE [IF NOT EXISTS] <name>
	// CREATE [UNIQUE] INDEX [IF NOT EXISTS] <name> ON <table>
	idx := 1
	skipWords(&idx, tokens, "TEMP", "TEMPORARY", "UNIQUE")

	if idx >= len(tokens) {
		return QueryInfo{Type: QueryTypeOther, IsDDL: true}
	}

	switch upper(tokens[idx]) {
	case "TABLE":
		idx++
		skipWords(&idx, tokens, "IF", "NOT", "EXISTS")
		return QueryInfo{
			Type:   QueryTypeCreateTable,
			Tables: tableNameAt(tokens, idx),
			IsDDL:  true,
		}
	case "INDEX":
		idx++
		skipWords(&idx, tokens, "IF", "NOT", "EXISTS")
		// index name is at idx, then ON <table>
		onIdx := findToken(tokens, "ON", idx)
		if onIdx >= 0 {
			return QueryInfo{
				Type:   QueryTypeCreateIndex,
				Tables: tableNameAt(tokens, onIdx+1),
				IsDDL:  true,
			}
		}
		return QueryInfo{Type: QueryTypeCreateIndex, IsDDL: true}
	}
	return QueryInfo{Type: QueryTypeOther, IsDDL: true}
}

func analyzeAlter(tokens []string) QueryInfo {
	// ALTER TABLE <name> ...
	if len(tokens) >= 3 && upper(tokens[1]) == "TABLE" {
		return QueryInfo{
			Type:   QueryTypeAlterTable,
			Tables: tableNameAt(tokens, 2),
			IsDDL:  true,
		}
	}
	return QueryInfo{Type: QueryTypeOther, IsDDL: true}
}

func analyzeDrop(tokens []string) QueryInfo {
	if len(tokens) < 2 {
		return QueryInfo{Type: QueryTypeOther, IsDDL: true}
	}
	switch upper(tokens[1]) {
	case "TABLE":
		// DROP TABLE [IF EXISTS] <name>
		idx := 2
		skipWords(&idx, tokens, "IF", "EXISTS")
		return QueryInfo{
			Type:   QueryTypeDropTable,
			Tables: tableNameAt(tokens, idx),
			IsDDL:  true,
		}
	case "INDEX":
		return QueryInfo{Type: QueryTypeDropIndex, IsDDL: true}
	}
	return QueryInfo{Type: QueryTypeOther, IsDDL: true}
}

func analyzePragma(tokens []string) QueryInfo {
	// PRAGMA schema_version; -- read-only
	// PRAGMA journal_mode = WAL; -- write
	readonly := true
	for _, t := range tokens[1:] {
		if t == "=" {
			readonly = false
			break
		}
	}
	return QueryInfo{Type: QueryTypePragma, IsReadOnly: readonly}
}

// extractSelectTables returns the first table from a SELECT … FROM clause.
// JOIN targets are ignored — for write_master routing only the primary table matters.
func extractSelectTables(tokens []string) []string {
	idx := findToken(tokens, "FROM", 0)
	if idx < 0 {
		return nil
	}
	return tableNameAt(tokens, idx+1)
}

// extractInsertTable handles:
//
//	INSERT [OR REPLACE|OR IGNORE|…] INTO <table>
//	REPLACE INTO <table>
func extractInsertTable(tokens []string) []string {
	idx := findToken(tokens, "INTO", 0)
	if idx < 0 {
		return nil
	}
	return tableNameAt(tokens, idx+1)
}

func extractUpdateTable(tokens []string) []string {
	// UPDATE [OR REPLACE|…] <table>
	idx := 1
	skipWords(&idx, tokens, "OR", "REPLACE", "IGNORE", "ABORT", "FAIL", "ROLLBACK")
	return tableNameAt(tokens, idx)
}

func extractDeleteTable(tokens []string) []string {
	// DELETE FROM <table>
	idx := findToken(tokens, "FROM", 0)
	if idx < 0 {
		return nil
	}
	return tableNameAt(tokens, idx+1)
}

// tableNameAt returns the canonical table name at position idx.
//
// SQLite allows schema-qualified names (schema.table) where schema is an
// attached database alias. "main" is the default schema and is equivalent to
// no schema at all, so "main.users" normalises to "users". Any other schema
// prefix is preserved as "schema.table" so the router can distinguish
// "other.users" from "users" / "main.users".
func tableNameAt(tokens []string, idx int) []string {
	if idx >= len(tokens) {
		return nil
	}
	name := unquote(tokens[idx])
	if dot := strings.LastIndex(name, "."); dot >= 0 {
		schema := strings.ToLower(name[:dot])
		table := strings.ToLower(name[dot+1:])
		if schema == "main" || schema == "" {
			name = table
		} else {
			name = schema + "." + table
		}
	} else {
		name = strings.ToLower(name)
	}
	if name == "" {
		return nil
	}
	return []string{name}
}

// skipWords advances idx past any of the given keywords (case-insensitive).
func skipWords(idx *int, tokens []string, words ...string) {
	for *idx < len(tokens) {
		up := upper(tokens[*idx])
		found := false
		for _, w := range words {
			if up == w {
				found = true
				break
			}
		}
		if !found {
			break
		}
		*idx++
	}
}

// findToken returns the index of the first occurrence of keyword (case-insensitive)
// at or after start, or -1.
func findToken(tokens []string, keyword string, start int) int {
	for i := start; i < len(tokens); i++ {
		if upper(tokens[i]) == keyword {
			return i
		}
	}
	return -1
}

func upper(s string) string { return strings.ToUpper(s) }

// unquote strips surrounding backticks, double-quotes, or square brackets.
func unquote(s string) string {
	if len(s) < 2 {
		return s
	}
	switch {
	case s[0] == '`' && s[len(s)-1] == '`':
		return s[1 : len(s)-1]
	case s[0] == '"' && s[len(s)-1] == '"':
		return s[1 : len(s)-1]
	case s[0] == '[' && s[len(s)-1] == ']':
		return s[1 : len(s)-1]
	}
	return s
}

// tokenize splits sql into tokens: words, operators (=), and punctuation.
// It strips SQL line comments (--) and block comments (/* */), and ignores
// string literals (their content is not relevant for routing).
func tokenize(sql string) []string {
	var tokens []string
	r := []rune(strings.TrimSpace(sql))
	i := 0
	for i < len(r) {
		// Skip whitespace
		if unicode.IsSpace(r[i]) {
			i++
			continue
		}

		// Line comment
		if i+1 < len(r) && r[i] == '-' && r[i+1] == '-' {
			for i < len(r) && r[i] != '\n' {
				i++
			}
			continue
		}

		// Block comment
		if i+1 < len(r) && r[i] == '/' && r[i+1] == '*' {
			i += 2
			for i+1 < len(r) && (r[i] != '*' || r[i+1] != '/') {
				i++
			}
			// Advance past closing */ only if it is present; otherwise we
			// reached end-of-input inside an unclosed comment.
			if i+1 < len(r) {
				i += 2
			} else {
				i = len(r)
			}
			continue
		}

		// String literal — skip content, keep nothing
		if r[i] == '\'' {
			i++
			for i < len(r) {
				if r[i] == '\'' {
					i++
					// escaped quote ''
					if i < len(r) && r[i] == '\'' {
						i++
						continue
					}
					break
				}
				i++
			}
			continue
		}

		// Quoted identifier — keep as single token including quotes
		if r[i] == '"' || r[i] == '`' || r[i] == '[' {
			close := map[rune]rune{'"': '"', '`': '`', '[': ']'}[r[i]]
			start := i
			i++
			for i < len(r) && r[i] != close {
				i++
			}
			i++ // consume closing delimiter
			tokens = append(tokens, string(r[start:i]))
			continue
		}

		// Equals sign (needed for PRAGMA detection)
		if r[i] == '=' {
			tokens = append(tokens, "=")
			i++
			continue
		}

		// Semicolon / parenthesis / comma — stop token accumulation
		if r[i] == ';' || r[i] == '(' || r[i] == ')' || r[i] == ',' {
			i++
			continue
		}

		// Word / identifier / number
		start := i
		for i < len(r) && !unicode.IsSpace(r[i]) && r[i] != ',' && r[i] != '(' && r[i] != ')' && r[i] != ';' && r[i] != '=' {
			i++
		}
		if i > start {
			tokens = append(tokens, string(r[start:i]))
		}
	}
	return tokens
}
