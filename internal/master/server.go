package master

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"hive/gen/hivepb"
)

const (
	snapshotChunkSize = 32 * 1024 // 32 KiB per streaming chunk
)

type Server struct {
	hivepb.UnimplementedHiveMasterServer
	db      *DB
	log     *slog.Logger
	snapDir string
	tables  []string
	snapSeq atomic.Uint64
}

func NewServer(db *DB, tables []string, snapDir string, log *slog.Logger) *Server {
	return &Server{
		db:      db,
		tables:  tables,
		snapDir: snapDir,
		log:     log,
	}
}

func (s *Server) Execute(ctx context.Context, req *hivepb.ExecuteRequest) (*hivepb.ExecuteResponse, error) {
	args, err := valuesToArgs(req.Args)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "convert args: %v", err)
	}

	if req.ReturnRows {
		rows, cols, execErr := s.db.ExecReturning(ctx, req.Sql, args...)
		if execErr != nil {
			s.log.Error("master: execute returning", "err", execErr, "sql", req.Sql)
			return nil, status.Errorf(codes.Internal, "execute returning: %v", execErr)
		}
		meta, pbRows := rowsToProto(cols, rows)
		return &hivepb.ExecuteResponse{
			ReturnedMeta: meta,
			ReturnedRows: pbRows,
		}, nil
	}

	ra, lid, execErr := s.db.Exec(ctx, req.Sql, args...)
	if execErr != nil {
		s.log.Error("master: execute", "err", execErr, "sql", req.Sql)
		return nil, status.Errorf(codes.Internal, "execute: %v", execErr)
	}
	return &hivepb.ExecuteResponse{
		RowsAffected: ra,
		LastInsertId: lid,
	}, nil
}

// Query sends QueryMeta as the first message so the receiver reconstructs the schema without a round-trip.
func (s *Server) Query(req *hivepb.QueryRequest, stream hivepb.HiveMaster_QueryServer) error {
	ctx := stream.Context()
	args, err := valuesToArgs(req.Args)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "convert args: %v", err)
	}

	rows, err := s.db.Query(ctx, req.Sql, args...)
	if err != nil {
		s.log.Error("master: query", "err", err, "sql", req.Sql)
		return status.Errorf(codes.Internal, "query: %v", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			s.log.Error("master: close query rows", "err", closeErr)
		}
	}()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return status.Errorf(codes.Internal, "column types: %v", err)
	}

	cols := make([]*hivepb.ColumnInfo, len(colTypes))
	for i, ct := range colTypes {
		cols[i] = &hivepb.ColumnInfo{Name: ct.Name(), Type: ct.DatabaseTypeName()}
	}
	if err = stream.Send(&hivepb.QueryResult{
		Payload: &hivepb.QueryResult_Meta{
			Meta: &hivepb.QueryMeta{Columns: cols},
		},
	}); err != nil {
		return status.Errorf(codes.Internal, "send meta: %v", err)
	}

	dest := make([]any, len(colTypes))
	ptrs := make([]any, len(colTypes))
	for i := range dest {
		ptrs[i] = &dest[i]
	}

	for rows.Next() {
		if err = rows.Scan(ptrs...); err != nil {
			return status.Errorf(codes.Internal, "scan row: %v", err)
		}
		vals := make([]*hivepb.Value, len(dest))
		for i, v := range dest {
			vals[i] = anyToValue(v)
		}
		if err = stream.Send(&hivepb.QueryResult{
			Payload: &hivepb.QueryResult_Row{
				Row: &hivepb.QueryRow{Values: vals},
			},
		}); err != nil {
			return status.Errorf(codes.Internal, "send row: %v", err)
		}
	}
	if err = rows.Err(); err != nil {
		return status.Errorf(codes.Internal, "rows error: %v", err)
	}
	return nil
}

func (s *Server) BeginTx(ctx context.Context, _ *hivepb.BeginTxRequest) (*hivepb.BeginTxResponse, error) {
	if err := s.db.BeginTx(ctx); err != nil {
		s.log.Error("master: begin tx", "err", err)
		return nil, status.Errorf(codes.Internal, "begin tx: %v", err)
	}
	// tx_id is a fixed sentinel on the master side; the router manages the
	// mapping between client tx_ids and master instances.
	return &hivepb.BeginTxResponse{TxId: "master-tx"}, nil
}

func (s *Server) CommitTx(ctx context.Context, _ *hivepb.TxRequest) (*hivepb.TxResponse, error) {
	if err := s.db.CommitTx(ctx); err != nil {
		s.log.Error("master: commit tx", "err", err)
		return nil, status.Errorf(codes.Internal, "commit tx: %v", err)
	}
	return &hivepb.TxResponse{Ok: true}, nil
}

func (s *Server) RollbackTx(ctx context.Context, _ *hivepb.TxRequest) (*hivepb.TxResponse, error) {
	if err := s.db.RollbackTx(ctx); err != nil {
		s.log.Error("master: rollback tx", "err", err)
		return nil, status.Errorf(codes.Internal, "rollback tx: %v", err)
	}
	return &hivepb.TxResponse{Ok: true}, nil
}

func (s *Server) ListTables(_ context.Context, _ *hivepb.ListTablesRequest) (*hivepb.ListTablesResponse, error) {
	return &hivepb.ListTablesResponse{Tables: s.tables}, nil
}

// Snapshot streams the file in chunks; the first chunk carries total_size so the receiver
// can track progress without buffering the whole file.
func (s *Server) Snapshot(_ *hivepb.SnapshotRequest, stream hivepb.HiveMaster_SnapshotServer) error {
	ctx := stream.Context()

	seq := s.snapSeq.Add(1)
	snapPath := filepath.Join(s.snapDir, fmt.Sprintf("snap-%d.db", seq))
	defer func() {
		if err := os.Remove(snapPath); err != nil && !os.IsNotExist(err) {
			s.log.Error("master: remove snapshot file", "err", err, "path", snapPath)
		}
	}()

	if err := s.db.CreateSnapshot(ctx, snapPath); err != nil {
		s.log.Error("master: create snapshot", "err", err)
		return status.Errorf(codes.Internal, "create snapshot: %v", err)
	}

	f, err := os.Open(snapPath)
	if err != nil {
		return status.Errorf(codes.Internal, "open snapshot: %v", err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			s.log.Error("master: close snapshot file", "err", closeErr)
		}
	}()

	info, err := f.Stat()
	if err != nil {
		return status.Errorf(codes.Internal, "stat snapshot: %v", err)
	}
	totalSize := info.Size()

	buf := make([]byte, snapshotChunkSize)
	first := true
	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			chunk := &hivepb.SnapshotChunk{Data: buf[:n]}
			if first {
				chunk.TotalSize = totalSize
				first = false
			}
			if sendErr := stream.Send(chunk); sendErr != nil {
				return status.Errorf(codes.Internal, "send chunk: %v", sendErr)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return status.Errorf(codes.Internal, "read snapshot: %v", readErr)
		}
	}
	return nil
}

// valuesToArgs uses []any because database/sql's Exec/Query accept variadic any
// to accommodate all SQLite column types at runtime.
func valuesToArgs(vals []*hivepb.Value) ([]any, error) {
	args := make([]any, len(vals))
	for i, v := range vals {
		a, err := valueToAny(v)
		if err != nil {
			return nil, fmt.Errorf("arg[%d]: %w", i, err)
		}
		args[i] = a
	}
	return args, nil
}

func valueToAny(v *hivepb.Value) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch k := v.Kind.(type) {
	case *hivepb.Value_NullValue:
		return nil, nil
	case *hivepb.Value_IntValue:
		return k.IntValue, nil
	case *hivepb.Value_RealValue:
		return k.RealValue, nil
	case *hivepb.Value_TextValue:
		return k.TextValue, nil
	case *hivepb.Value_BlobValue:
		return k.BlobValue, nil
	default:
		return nil, fmt.Errorf("unknown value kind %T", v.Kind)
	}
}

func anyToValue(v any) *hivepb.Value {
	if v == nil {
		return &hivepb.Value{Kind: &hivepb.Value_NullValue{NullValue: true}}
	}
	switch val := v.(type) {
	case int64:
		return &hivepb.Value{Kind: &hivepb.Value_IntValue{IntValue: val}}
	case float64:
		return &hivepb.Value{Kind: &hivepb.Value_RealValue{RealValue: val}}
	case string:
		return &hivepb.Value{Kind: &hivepb.Value_TextValue{TextValue: val}}
	case []byte:
		return &hivepb.Value{Kind: &hivepb.Value_BlobValue{BlobValue: val}}
	case bool:
		if val {
			return &hivepb.Value{Kind: &hivepb.Value_IntValue{IntValue: 1}}
		}
		return &hivepb.Value{Kind: &hivepb.Value_IntValue{IntValue: 0}}
	default:
		// Fallback: stringify unknown types.
		return &hivepb.Value{Kind: &hivepb.Value_TextValue{TextValue: fmt.Sprintf("%v", v)}}
	}
}

func rowsToProto(colNames []string, rows [][]any) (*hivepb.QueryMeta, []*hivepb.QueryRow) {
	if len(rows) == 0 {
		return &hivepb.QueryMeta{}, nil
	}
	cols := make([]*hivepb.ColumnInfo, len(colNames))
	for i, name := range colNames {
		cols[i] = &hivepb.ColumnInfo{Name: name}
	}
	meta := &hivepb.QueryMeta{Columns: cols}

	pbRows := make([]*hivepb.QueryRow, len(rows))
	for i, row := range rows {
		vals := make([]*hivepb.Value, len(row))
		for j, v := range row {
			vals[j] = anyToValue(v)
		}
		pbRows[i] = &hivepb.QueryRow{Values: vals}
	}
	return meta, pbRows
}
