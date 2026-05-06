package ws

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/hive_v2/orchestrator/internal/hrana"
	"github.com/hive_v2/orchestrator/internal/stream"
)

// wsStream state is identified by stream_id (int32) rather than batons.
type wsStream struct {
	// mu serialises requests on this stream per Hrana spec §stream-ordering.
	mu   sync.Mutex
	strm *stream.Stream
}

type cursorState struct {
	cancel    context.CancelFunc
	ch        chan hrana.CursorChunk
	masterIdx int
}

type Connection struct {
	proc *hrana.Processor

	mu       sync.Mutex
	streams  map[int32]*wsStream
	sqlStore *stream.SQLCache
	cursors  map[int32]*cursorState
}

func newConnection(proc *hrana.Processor) *Connection {
	return &Connection{
		proc:     proc,
		streams:  make(map[int32]*wsStream),
		sqlStore: stream.NewSQLCache(),
		cursors:  make(map[int32]*cursorState),
	}
}

func (c *Connection) cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, cs := range c.cursors {
		cs.cancel()
	}
}

func (c *Connection) getOrCreateStream(streamID int32) *wsStream {
	c.mu.Lock()
	defer c.mu.Unlock()
	ws, ok := c.streams[streamID]
	if !ok {
		ws = &wsStream{
			strm: &stream.Stream{
				MasterBatons: make(map[int]string),
				SQLStore:     c.sqlStore, // shared at connection level
			},
		}
		c.streams[streamID] = ws
	}
	return ws
}

func (c *Connection) closeStream(streamID int32) {
	c.mu.Lock()
	delete(c.streams, streamID)
	c.mu.Unlock()
}

func (c *Connection) storeSQL(sqlID int32, sql string) {
	c.sqlStore.Store(sqlID, sql)
}

func (c *Connection) closeSQL(sqlID int32) {
	c.sqlStore.Delete(sqlID)
}

func (c *Connection) openCursor(ctx context.Context, cursorID int32, streamID int32, batch hrana.Batch) error {
	ws := c.getOrCreateStream(streamID)
	ws.mu.Lock()
	strm := ws.strm
	ws.mu.Unlock()

	cursorCtx, cancel := context.WithCancel(ctx)

	master, upResp, err := c.proc.OpenCursorUpstream(cursorCtx, strm, batch)
	if err != nil {
		cancel()
		return err
	}

	ch := make(chan hrana.CursorChunk, 16)
	headerBaton := make(chan *string, 1)

	go func() {
		hrana.ParseCursorEntries(cursorCtx, upResp, headerBaton, ch)
	}()

	go func() {
		if baton, ok := <-headerBaton; ok && baton != nil {
			ws.mu.Lock()
			strm.MasterBatons[master.Index] = *baton
			ws.mu.Unlock()
		}
	}()

	cs := &cursorState{
		cancel:    cancel,
		ch:        ch,
		masterIdx: master.Index,
	}

	c.mu.Lock()
	c.cursors[cursorID] = cs
	c.mu.Unlock()

	return nil
}

func (c *Connection) fetchCursor(cursorID int32, maxCount int32) ([]CursorEntry, bool, error) {
	c.mu.Lock()
	cs, ok := c.cursors[cursorID]
	c.mu.Unlock()
	if !ok {
		return nil, false, hrana.NewError("cursor not found")
	}

	entries := make([]CursorEntry, 0, maxCount)
	remaining := int(maxCount)

	for remaining > 0 {
		chunk, open := <-cs.ch
		if chunk.Err != nil {
			return entries, true, chunk.Err
		}
		for _, raw := range chunk.RawEntries {
			if remaining <= 0 {
				break
			}
			var e CursorEntry
			if err := json.Unmarshal(raw, &e); err == nil {
				entries = append(entries, e)
				remaining--
			}
		}
		if chunk.Done {
			c.mu.Lock()
			delete(c.cursors, cursorID)
			c.mu.Unlock()
			return entries, true, nil
		}
		if !open {
			c.mu.Lock()
			delete(c.cursors, cursorID)
			c.mu.Unlock()
			return entries, true, nil
		}
		if remaining <= 0 {
			break
		}
	}

	return entries, false, nil
}

func (c *Connection) closeCursor(cursorID int32) {
	c.mu.Lock()
	cs, ok := c.cursors[cursorID]
	if ok {
		delete(c.cursors, cursorID)
	}
	c.mu.Unlock()
	if ok {
		cs.cancel()
	}
}

func (c *Connection) buildStreamRequest(req ClientRequest) hrana.StreamRequest {
	sr := hrana.StreamRequest{Type: req.Type}

	switch req.Type {
	case "execute":
		if req.Stmt != nil {
			stmt := *req.Stmt
			if stmt.SQLId != nil && stmt.SQL == nil {
				sql := c.sqlStore.Load(*stmt.SQLId)
				stmt.SQL = &sql
				stmt.SQLId = nil
			}
			sr.Stmt = &stmt
		}
	case "batch":
		if req.Batch != nil {
			sr.Batch = req.Batch
		}
	case "describe":
		sr.SQL = req.SQL
		sr.SQLId = req.SQLId
	case "sequence":
		sr.SQL = req.SQL
	case "store_sql":
		sr.SQL = req.SQL
		sr.SQLId = req.SQLId
	case "close_sql":
		sr.SQLId = req.SQLId
	case "get_autocommit":
		// no payload
	}
	return sr
}
