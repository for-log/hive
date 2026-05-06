package hrana_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hive_v2/orchestrator/internal/hrana"
)

func TestValue_MarshalUnmarshal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    hrana.Value
		wantJSON string
	}{
		{
			name:     "null",
			value:    hrana.Null(),
			wantJSON: `{"type":"null"}`,
		},
		{
			name:     "integer",
			value:    hrana.Integer(42),
			wantJSON: `{"type":"integer","value":"42"}`,
		},
		{
			name:     "negative integer",
			value:    hrana.Integer(-9223372036854775808),
			wantJSON: `{"type":"integer","value":"-9223372036854775808"}`,
		},
		{
			name:     "float",
			value:    hrana.Float(3.14),
			wantJSON: `{"type":"float","value":3.14}`,
		},
		{
			name:     "text",
			value:    hrana.Text("hello"),
			wantJSON: `{"type":"text","value":"hello"}`,
		},
		{
			name:     "blob",
			value:    hrana.Blob([]byte{0x01, 0x02, 0x03}),
			wantJSON: `{"type":"blob","base64":"AQID"}`,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Marshal
			got, err := json.Marshal(tc.value)
			require.NoError(t, err)
			assert.JSONEq(t, tc.wantJSON, string(got))

			// Unmarshal back
			var v hrana.Value
			require.NoError(t, json.Unmarshal(got, &v))
			assert.Equal(t, tc.value.Type(), v.Type())

			switch tc.value.Type() {
			case hrana.ValueTypeInteger:
				assert.Equal(t, tc.value.Int64(), v.Int64())
			case hrana.ValueTypeFloat:
				assert.InDelta(t, tc.value.Float64(), v.Float64(), 1e-9)
			case hrana.ValueTypeText:
				assert.Equal(t, tc.value.String(), v.String())
			case hrana.ValueTypeBlob:
				assert.Equal(t, tc.value.Bytes(), v.Bytes())
			}
		})
	}
}

func TestValue_UnmarshalErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{
			name:    "unknown type",
			input:   `{"type":"unknown"}`,
			wantErr: "unknown value type",
		},
		{
			name:    "integer not string",
			input:   `{"type":"integer","value":42}`,
			wantErr: "integer value must be a string",
		},
		{
			name:    "blob missing base64",
			input:   `{"type":"blob"}`,
			wantErr: `missing "base64" field`,
		},
		{
			name:    "invalid json",
			input:   `{bad json`,
			wantErr: "invalid character",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var v hrana.Value
			err := json.Unmarshal([]byte(tc.input), &v)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestStreamResponse_ExecuteResult(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(hrana.StmtResult{
		AffectedRowCount: 1,
	})
	require.NoError(t, err)

	resp := hrana.StreamResponse{Type: "execute", Result: raw}
	res, err := resp.ExecuteResult()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), res.AffectedRowCount)
}

func TestStreamResponse_ExecuteResult_WrongType(t *testing.T) {
	t.Parallel()
	resp := hrana.StreamResponse{Type: "batch"}
	_, err := resp.ExecuteResult()
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"execute"`)
}

func TestStreamResponse_BatchResultValue(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(hrana.BatchResult{
		StepResults: []*hrana.StmtResult{nil},
		StepErrors:  []*hrana.Error{nil},
	})
	require.NoError(t, err)

	resp := hrana.StreamResponse{Type: "batch", Result: raw}
	res, err := resp.BatchResultValue()
	require.NoError(t, err)
	assert.Len(t, res.StepResults, 1)
}

func TestConvenienceConstructors(t *testing.T) {
	t.Parallel()

	t.Run("ExecuteRequest", func(t *testing.T) {
		t.Parallel()
		r := hrana.ExecuteRequest("SELECT 1", true)
		assert.Equal(t, "execute", r.Type)
		require.NotNil(t, r.Stmt)
		assert.Equal(t, "SELECT 1", *r.Stmt.SQL)
		assert.True(t, r.Stmt.WantRows)
	})

	t.Run("CloseRequest", func(t *testing.T) {
		t.Parallel()
		r := hrana.CloseRequest()
		assert.Equal(t, "close", r.Type)
	})

	t.Run("StoreSQLRequest", func(t *testing.T) {
		t.Parallel()
		r := hrana.StoreSQLRequest("SELECT 1", 7)
		assert.Equal(t, "store_sql", r.Type)
		assert.Equal(t, "SELECT 1", *r.SQL)
		assert.Equal(t, int32(7), *r.SQLId)
	})

	t.Run("OkResult", func(t *testing.T) {
		t.Parallel()
		res := hrana.OkResult(hrana.StreamResponse{Type: "close"})
		assert.Equal(t, "ok", res.Type)
		require.NotNil(t, res.Response)
	})

	t.Run("ErrResult", func(t *testing.T) {
		t.Parallel()
		res := hrana.ErrResult(hrana.NewError("boom"))
		assert.Equal(t, "error", res.Type)
		require.NotNil(t, res.Error)
		assert.Equal(t, "boom", res.Error.Message)
	})
}

func TestPipelineRequest_JSON(t *testing.T) {
	t.Parallel()

	req := hrana.PipelineRequest{
		Baton: "tok123",
		Requests: []hrana.StreamRequest{
			hrana.ExecuteRequest("SELECT 1", true),
			hrana.CloseRequest(),
		},
	}

	data, err := json.Marshal(req)
	require.NoError(t, err)

	var got hrana.PipelineRequest
	require.NoError(t, json.Unmarshal(data, &got))

	assert.Equal(t, req.Baton, got.Baton)
	assert.Len(t, got.Requests, 2)
	assert.Equal(t, "execute", got.Requests[0].Type)
	assert.Equal(t, "close", got.Requests[1].Type)
}
