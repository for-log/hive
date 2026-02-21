package hivedriver

import (
	"database/sql/driver"
	"io"

	"hive/gen/hivepb"
)

type LocalRows struct {
	cols []string
	rows [][]driver.Value
	pos  int
}

func newLocalRows(cols []string, rows [][]driver.Value) *LocalRows {
	return &LocalRows{cols: cols, rows: rows}
}

func (r *LocalRows) Columns() []string { return r.cols }

func (r *LocalRows) Close() error { return nil }

func (r *LocalRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.pos])
	r.pos++
	return nil
}

type staticRows struct {
	cols []string
	rows []*hivepb.QueryRow
	pos  int
}

func newStaticRows(meta *hivepb.QueryMeta, rows []*hivepb.QueryRow) *staticRows {
	cols := make([]string, len(meta.Columns))
	for i, c := range meta.Columns {
		cols[i] = c.Name
	}
	return &staticRows{cols: cols, rows: rows}
}

func (r *staticRows) Columns() []string { return r.cols }

func (r *staticRows) Close() error { return nil }

func (r *staticRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.rows) {
		return io.EOF
	}
	row := r.rows[r.pos]
	r.pos++
	for i, v := range row.Values {
		if i < len(dest) {
			dest[i] = protoToGoValue(v)
		}
	}
	return nil
}
