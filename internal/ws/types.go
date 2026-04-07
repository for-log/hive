// Package ws implements the Hrana v3 WebSocket transport for the orchestrator.
// The WebSocket protocol is defined in the Hrana 3 spec:
// https://github.com/tursodatabase/libsql/blob/main/docs/HRANA_3_SPEC.md
package ws

import (
	"encoding/json"
	"fmt"

	"github.com/hive_v2/orchestrator/internal/hrana"
)

// ClientMsg is the top-level WebSocket message from the client.
// Exactly one of Hello or Request will be set.
type ClientMsg struct {
	Type string `json:"type"`

	// hello
	JWT *string `json:"jwt,omitempty"`

	// request
	RequestID int32           `json:"request_id,omitempty"`
	Request   json.RawMessage `json:"request,omitempty"`
}

// ClientRequest is the payload inside a ClientMsg of type "request".
// The Type field determines which fields are populated.
type ClientRequest struct {
	Type string `json:"type"`

	// open_stream
	StreamID int32 `json:"stream_id,omitempty"`

	// close_stream
	// StreamID reused

	// execute
	// StreamID reused
	Stmt *hrana.Stmt `json:"stmt,omitempty"`

	// batch
	// StreamID reused
	Batch *hrana.Batch `json:"batch,omitempty"`

	// describe
	// StreamID reused
	SQL   *string `json:"sql,omitempty"`
	SQLId *int32  `json:"sql_id,omitempty"`

	// store_sql (connection-level, no stream_id)
	// SQL and SQLId reused

	// close_sql (connection-level, no stream_id)
	// SQLId reused

	// get_autocommit
	// StreamID reused

	// open_cursor
	// StreamID reused
	CursorID int32        `json:"cursor_id,omitempty"`
	// Batch reused for open_cursor payload

	// fetch_cursor
	// CursorID reused
	MaxCount int32 `json:"max_count,omitempty"`

	// close_cursor
	// CursorID reused

	// sequence
	// StreamID reused
	// SQL reused
}

// ServerMsg is the top-level WebSocket message from the server.
// Exactly one of the payload fields will be set, matching Type.
type ServerMsg struct {
	Type string `json:"type"`

	// hello_ok / hello_error
	Error *hrana.Error `json:"error,omitempty"`

	// response_ok / response_error
	RequestID int32           `json:"request_id,omitempty"`
	Response  json.RawMessage `json:"response,omitempty"`
}

type OpenStreamResponse struct{}

type CloseStreamResponse struct{}

type ExecuteResponse struct {
	Result *hrana.StmtResult `json:"result"`
}

type BatchResponse struct {
	Result *hrana.BatchResult `json:"result"`
}

type DescribeResponse struct {
	Result *hrana.DescribeResult `json:"result"`
}

type StoreSQLResponse struct{}

type CloseSQLResponse struct{}

type GetAutocommitResponse struct {
	IsAutocommit bool `json:"is_autocommit"`
}

type OpenCursorResponse struct{}

type FetchCursorResponse struct {
	Entries []CursorEntry `json:"entries"`
	Done    bool          `json:"done"`
}

type CloseCursorResponse struct{}

type SequenceResponse struct{}

// CursorEntry is a single entry in a cursor response stream.
// The Type field determines which fields are populated.
type CursorEntry struct {
	Type string `json:"type"`

	// step_begin
	Step      *int32              `json:"step,omitempty"`
	Cols      []hrana.Column      `json:"cols,omitempty"`

	// step_end
	Affected *uint64 `json:"affected_row_count,omitempty"`
	LastID   *string `json:"last_insert_rowid,omitempty"`

	// step_error
	Error *hrana.Error `json:"error,omitempty"`

	// row
	Row []hrana.Value `json:"row,omitempty"`
}

func helloOK() ServerMsg {
	return ServerMsg{Type: "hello_ok"}
}

func helloError(err *hrana.Error) ServerMsg {
	return ServerMsg{Type: "hello_error", Error: err}
}

func responseOK(reqID int32, payload any) (ServerMsg, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return ServerMsg{}, fmt.Errorf("ws: marshal response: %w", err)
	}
	return ServerMsg{Type: "response_ok", RequestID: reqID, Response: raw}, nil
}

func responseError(reqID int32, err *hrana.Error) ServerMsg {
	raw, _ := json.Marshal(err)
	return ServerMsg{Type: "response_error", RequestID: reqID, Response: raw}
}
