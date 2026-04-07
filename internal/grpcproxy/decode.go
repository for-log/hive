package grpcproxy

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// QueryStmt holds the SQL text extracted from a gRPC ProgramReq.
// Only SQL is decoded — parameters are opaque and forwarded as raw bytes.
type QueryStmt struct {
	SQL string
}

// ProgramReq holds the client ID and extracted SQL statements.
type ProgramReq struct {
	ClientID string
	Queries  []QueryStmt
}

// DecodeProgramReq extracts client_id and SQL statements from a ProgramReq
// protobuf message. Only SQL text is decoded for routing; parameters and
// other fields are skipped.
//
// ProgramReq layout (proxy.proto):
//
//	message ProgramReq { string client_id = 1; Program pgm = 2; }
//	message Program    { repeated Step steps = 1; }
//	message Step       { optional Cond cond = 1; Query query = 2; }
//	message Query      { string stmt = 1; ... }
func DecodeProgramReq(b []byte) (*ProgramReq, error) {
	var req ProgramReq
	data := b
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, fmt.Errorf("bad tag")
		}
		data = data[n:]

		switch {
		case num == 1 && typ == protowire.BytesType:
			v, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return nil, fmt.Errorf("bad client_id")
			}
			req.ClientID = string(v)
			data = data[n:]

		case num == 2 && typ == protowire.BytesType:
			v, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return nil, fmt.Errorf("bad pgm")
			}
			queries, err := decodeProgram(v)
			if err != nil {
				return nil, fmt.Errorf("program: %w", err)
			}
			req.Queries = queries
			data = data[n:]

		default:
			n := protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return nil, fmt.Errorf("bad field %d", num)
			}
			data = data[n:]
		}
	}
	return &req, nil
}

func decodeProgram(b []byte) ([]QueryStmt, error) {
	var queries []QueryStmt
	data := b
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, fmt.Errorf("bad tag")
		}
		data = data[n:]

		if num == 1 && typ == protowire.BytesType {
			v, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return nil, fmt.Errorf("bad step")
			}
			q, err := decodeStep(v)
			if err != nil {
				return nil, err
			}
			if q.SQL != "" {
				queries = append(queries, q)
			}
			data = data[n:]
		} else {
			n := protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return nil, fmt.Errorf("bad field %d", num)
			}
			data = data[n:]
		}
	}
	return queries, nil
}

func decodeStep(b []byte) (QueryStmt, error) {
	data := b
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return QueryStmt{}, fmt.Errorf("bad tag")
		}
		data = data[n:]

		if num == 2 && typ == protowire.BytesType {
			v, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return QueryStmt{}, fmt.Errorf("bad query")
			}
			return decodeQuery(v)
		}
		n = protowire.ConsumeFieldValue(num, typ, data)
		if n < 0 {
			return QueryStmt{}, fmt.Errorf("bad field %d", num)
		}
		data = data[n:]
	}
	return QueryStmt{}, nil
}

// decodeQuery extracts only the SQL text (field 1) from a Query message.
// All other fields (positional params, named params) are skipped.
func decodeQuery(b []byte) (QueryStmt, error) {
	var q QueryStmt
	data := b
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return q, fmt.Errorf("bad tag")
		}
		data = data[n:]

		if num == 1 && typ == protowire.BytesType {
			v, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return q, fmt.Errorf("bad stmt")
			}
			q.SQL = string(v)
			data = data[n:]
		} else {
			n := protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return q, fmt.Errorf("bad field %d", num)
			}
			data = data[n:]
		}
	}
	return q, nil
}
