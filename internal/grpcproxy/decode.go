package grpcproxy

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

type QueryStmt struct {
	SQL string
}

type ProgramReq struct {
	ClientID string
	Queries  []QueryStmt
}

func DecodeProgramReq(b []byte) (*ProgramReq, error) {
	var req ProgramReq
	data := b
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, fmt.Errorf("ProgramReq: bad tag")
		}
		data = data[n:]

		switch {
		case num == 1 && typ == protowire.BytesType:
			v, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return nil, fmt.Errorf("ProgramReq: bad client_id")
			}
			req.ClientID = string(v)
			data = data[n:]

		case num == 2 && typ == protowire.BytesType:
			v, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return nil, fmt.Errorf("ProgramReq: bad pgm")
			}
			queries, err := decodeProgram(v)
			if err != nil {
				return nil, fmt.Errorf("ProgramReq program: %w", err)
			}
			req.Queries = queries
			data = data[n:]

		default:
			n := protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return nil, fmt.Errorf("ProgramReq: bad field %d", num)
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
			return nil, fmt.Errorf("program: bad tag")
		}
		data = data[n:]

		if num == 1 && typ == protowire.BytesType {
			v, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return nil, fmt.Errorf("program: bad step")
			}
			q, err := decodeStep(v)
			if err != nil {
				return nil, fmt.Errorf("program step: %w", err)
			}
			if q.SQL != "" {
				queries = append(queries, q)
			}
			data = data[n:]
		} else {
			n := protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return nil, fmt.Errorf("program: bad field %d", num)
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
			return QueryStmt{}, fmt.Errorf("step: bad tag")
		}
		data = data[n:]

		if num == 2 && typ == protowire.BytesType {
			v, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return QueryStmt{}, fmt.Errorf("step: bad query")
			}
			return decodeQuery(v)
		}
		n = protowire.ConsumeFieldValue(num, typ, data)
		if n < 0 {
			return QueryStmt{}, fmt.Errorf("step: bad field %d", num)
		}
		data = data[n:]
	}
	return QueryStmt{}, nil
}

func decodeQuery(b []byte) (QueryStmt, error) {
	var q QueryStmt
	data := b
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return q, fmt.Errorf("query: bad tag")
		}
		data = data[n:]

		if num == 1 && typ == protowire.BytesType {
			v, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return q, fmt.Errorf("query: bad stmt")
			}
			q.SQL = string(v)
			data = data[n:]
		} else {
			n := protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return q, fmt.Errorf("query: bad field %d", num)
			}
			data = data[n:]
		}
	}
	return q, nil
}
