package hrana

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
)

type PipelineRequest struct {
	Baton    string          `json:"baton,omitempty"`
	Requests []StreamRequest `json:"requests"`
}

type PipelineResponse struct {
	Baton   string         `json:"baton,omitempty"`
	BaseURL string         `json:"base_url,omitempty"`
	Results []StreamResult `json:"results"`
}

// StreamRequest carries exactly one payload field matching Type.
type StreamRequest struct {
	Type  string  `json:"type"`
	Stmt  *Stmt   `json:"stmt,omitempty"`
	Batch *Batch  `json:"batch,omitempty"`
	SQL   *string `json:"sql,omitempty"`
	SQLId *int32  `json:"sql_id,omitempty"`
}

type StreamResult struct {
	Type     string          `json:"type"`
	Response *StreamResponse `json:"response,omitempty"`
	Error    *Error          `json:"error,omitempty"`
}

// StreamResponse keeps Result as raw JSON so callers unmarshal into the
// concrete type they need (StmtResult, BatchResult, DescribeResult, …).
type StreamResponse struct {
	Type         string          `json:"type"`
	Result       json.RawMessage `json:"result,omitempty"`
	IsAutocommit *bool           `json:"is_autocommit,omitempty"` // v3 get_autocommit response
}

func (r *StreamResponse) ExecuteResult() (*StmtResult, error) {
	if r.Type != "execute" {
		return nil, fmt.Errorf("hrana: expected response type \"execute\", got %q", r.Type)
	}
	var res StmtResult
	if err := json.Unmarshal(r.Result, &res); err != nil {
		return nil, fmt.Errorf("hrana: unmarshal StmtResult: %w", err)
	}
	return &res, nil
}

func (r *StreamResponse) BatchResultValue() (*BatchResult, error) {
	if r.Type != "batch" {
		return nil, fmt.Errorf("hrana: expected response type \"batch\", got %q", r.Type)
	}
	var res BatchResult
	if err := json.Unmarshal(r.Result, &res); err != nil {
		return nil, fmt.Errorf("hrana: unmarshal BatchResult: %w", err)
	}
	return &res, nil
}

func (r *StreamResponse) DescribeResultValue() (*DescribeResult, error) {
	if r.Type != "describe" {
		return nil, fmt.Errorf("hrana: expected response type \"describe\", got %q", r.Type)
	}
	var res DescribeResult
	if err := json.Unmarshal(r.Result, &res); err != nil {
		return nil, fmt.Errorf("hrana: unmarshal DescribeResult: %w", err)
	}
	return &res, nil
}

type Stmt struct {
	SQL       *string    `json:"sql,omitempty"`
	SQLId     *int32     `json:"sql_id,omitempty"`
	Args      []Value    `json:"args,omitempty"`
	NamedArgs []NamedArg `json:"named_args,omitempty"`
	WantRows  bool       `json:"want_rows"`
}

type NamedArg struct {
	Name  string `json:"name"`
	Value Value  `json:"value"`
}

func NewStmt(sql string, wantRows bool) *Stmt {
	return &Stmt{SQL: &sql, WantRows: wantRows}
}

type StmtResult struct {
	Cols             []Column        `json:"cols"`
	Rows             [][]Value       `json:"rows"`
	AffectedRowCount uint64          `json:"affected_row_count"`
	LastInsertRowID  *string         `json:"last_insert_rowid"`
	ReplicationIndex json.RawMessage `json:"replication_index,omitempty"`
	RowsRead         uint64          `json:"rows_read,omitempty"`
	RowsWritten      uint64          `json:"rows_written,omitempty"`
	QueryDurationMs  float64         `json:"query_duration_ms,omitempty"`
}

type Column struct {
	Name     *string `json:"name"`
	DeclType *string `json:"decltype"`
}

// CursorRequest is the body of POST /v3/cursor.
type CursorRequest struct {
	Baton string `json:"baton,omitempty"`
	Batch Batch  `json:"batch"`
}

// CursorResponseHeader is the first JSON line of a /v3/cursor chunked response.
type CursorResponseHeader struct {
	Baton   *string `json:"baton"`
	BaseURL *string `json:"base_url"`
}

type Batch struct {
	Steps            []BatchStep     `json:"steps"`
	ReplicationIndex json.RawMessage `json:"replication_index,omitempty"`
}

type BatchStep struct {
	Stmt      Stmt       `json:"stmt"`
	Condition *BatchCond `json:"condition,omitempty"`
}

type BatchCond struct {
	Type  string      `json:"type"`
	Step  *int32      `json:"step,omitempty"`
	Cond  *BatchCond  `json:"cond,omitempty"`
	Conds []BatchCond `json:"conds,omitempty"`
}

type BatchResult struct {
	StepResults      []*StmtResult   `json:"step_results"`
	StepErrors       []*Error        `json:"step_errors"`
	ReplicationIndex json.RawMessage `json:"replication_index,omitempty"`
}

type DescribeResult struct {
	Params     []DescribeParam `json:"params"`
	Cols       []DescribeCol   `json:"cols"`
	IsExplain  bool            `json:"is_explain"`
	IsReadOnly bool            `json:"is_readonly"`
}

type DescribeParam struct {
	Name *string `json:"name"`
}

type DescribeCol struct {
	Name     string  `json:"name"`
	DeclType *string `json:"decltype"`
}

type Error struct {
	Message string `json:"message"`
	Code    string `json:"code"` // must always be a non-null string for libsql-experimental
}

func (e *Error) Error() string { return e.Message }

type ValueType string

const (
	ValueTypeNull    ValueType = "null"
	ValueTypeInteger ValueType = "integer"
	ValueTypeFloat   ValueType = "float"
	ValueTypeText    ValueType = "text"
	ValueTypeBlob    ValueType = "blob"
)

// Value represents a SQLite value. integer is encoded as a JSON string to
// preserve full int64 precision (some JSON parsers treat all numbers as float64).
type Value struct {
	typ     ValueType
	intVal  int64
	fltVal  float64
	strVal  string
	blobVal []byte
}

func Null() Value           { return Value{typ: ValueTypeNull} }
func Integer(v int64) Value { return Value{typ: ValueTypeInteger, intVal: v} }
func Float(v float64) Value { return Value{typ: ValueTypeFloat, fltVal: v} }
func Text(v string) Value   { return Value{typ: ValueTypeText, strVal: v} }
func Blob(v []byte) Value   { return Value{typ: ValueTypeBlob, blobVal: v} }

func (v Value) Type() ValueType  { return v.typ }
func (v Value) Int64() int64     { return v.intVal }
func (v Value) Float64() float64 { return v.fltVal }
func (v Value) String() string   { return v.strVal }
func (v Value) Bytes() []byte    { return v.blobVal }

func (v Value) MarshalJSON() ([]byte, error) {
	switch v.typ {
	case ValueTypeNull:
		return []byte(`{"type":"null"}`), nil
	case ValueTypeInteger:
		return json.Marshal(struct {
			Type  string `json:"type"`
			Value string `json:"value"`
		}{Type: "integer", Value: strconv.FormatInt(v.intVal, 10)})
	case ValueTypeFloat:
		return json.Marshal(struct {
			Type  string  `json:"type"`
			Value float64 `json:"value"`
		}{Type: "float", Value: v.fltVal})
	case ValueTypeText:
		return json.Marshal(struct {
			Type  string `json:"type"`
			Value string `json:"value"`
		}{Type: "text", Value: v.strVal})
	case ValueTypeBlob:
		return json.Marshal(struct {
			Type   string `json:"type"`
			Base64 string `json:"base64"`
		}{Type: "blob", Base64: base64.StdEncoding.EncodeToString(v.blobVal)})
	default:
		return nil, fmt.Errorf("hrana: unknown value type %q", v.typ)
	}
}

func (v *Value) UnmarshalJSON(data []byte) error {
	var raw struct {
		Type   string          `json:"type"`
		Value  json.RawMessage `json:"value"`
		Base64 *string         `json:"base64"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("hrana: unmarshal Value: %w", err)
	}

	switch ValueType(raw.Type) {
	case ValueTypeNull:
		v.typ = ValueTypeNull
	case ValueTypeInteger:
		var s string
		if err := json.Unmarshal(raw.Value, &s); err != nil {
			return fmt.Errorf("hrana: integer value must be a string: %w", err)
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return fmt.Errorf("hrana: parse integer %q: %w", s, err)
		}
		v.typ = ValueTypeInteger
		v.intVal = n
	case ValueTypeFloat:
		var f float64
		if err := json.Unmarshal(raw.Value, &f); err != nil {
			return fmt.Errorf("hrana: float value must be a number: %w", err)
		}
		v.typ = ValueTypeFloat
		v.fltVal = f
	case ValueTypeText:
		var s string
		if err := json.Unmarshal(raw.Value, &s); err != nil {
			return fmt.Errorf("hrana: text value must be a string: %w", err)
		}
		v.typ = ValueTypeText
		v.strVal = s
	case ValueTypeBlob:
		if raw.Base64 == nil {
			return fmt.Errorf("hrana: blob value missing \"base64\" field")
		}
		b, err := base64.StdEncoding.DecodeString(*raw.Base64)
		if err != nil {
			return fmt.Errorf("hrana: decode blob base64: %w", err)
		}
		v.typ = ValueTypeBlob
		v.blobVal = b
	default:
		return fmt.Errorf("hrana: unknown value type %q", raw.Type)
	}
	return nil
}

func ExecuteRequest(sql string, wantRows bool) StreamRequest {
	return StreamRequest{Type: "execute", Stmt: NewStmt(sql, wantRows)}
}

func ExecuteRequestWithArgs(sql string, args []Value, wantRows bool) StreamRequest {
	s := NewStmt(sql, wantRows)
	s.Args = args
	return StreamRequest{Type: "execute", Stmt: s}
}

func ExecuteRequestWithNamedArgs(sql string, named []NamedArg, wantRows bool) StreamRequest {
	s := NewStmt(sql, wantRows)
	s.NamedArgs = named
	return StreamRequest{Type: "execute", Stmt: s}
}

func CloseRequest() StreamRequest {
	return StreamRequest{Type: "close"}
}

func BatchRequest(b Batch) StreamRequest {
	return StreamRequest{Type: "batch", Batch: &b}
}

func DescribeRequest(sql string) StreamRequest {
	return StreamRequest{Type: "describe", SQL: &sql}
}

func StoreSQLRequest(sql string, id int32) StreamRequest {
	return StreamRequest{Type: "store_sql", SQL: &sql, SQLId: &id}
}

func CloseSQLRequest(id int32) StreamRequest {
	return StreamRequest{Type: "close_sql", SQLId: &id}
}

func GetAutocommitRequest() StreamRequest {
	return StreamRequest{Type: "get_autocommit"}
}

func SequenceRequest(sql string) StreamRequest {
	return StreamRequest{Type: "sequence", SQL: &sql}
}

func OkResult(resp StreamResponse) StreamResult {
	return StreamResult{Type: "ok", Response: &resp}
}

func ErrResult(err *Error) StreamResult {
	return StreamResult{Type: "error", Error: err}
}

func NewError(msg string) *Error {
	return &Error{Message: msg, Code: "INTERNAL_ERROR"}
}

func NewErrorWithCode(msg, code string) *Error {
	return &Error{Message: msg, Code: code}
}
