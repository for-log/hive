package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/hive_v2/orchestrator/internal/hrana"
	"github.com/hive_v2/orchestrator/internal/router"
	"github.com/hive_v2/orchestrator/internal/txlog"
)

type Config struct {
	Pool          hrana.MasterClientPool
	Router        *router.Router
	Replicator    hrana.WriteReplicator
	TxLog         *txlog.WAL
	Logger        hrana.Logger
	MasterCount   int
	CommitTimeout time.Duration
}

type Handler struct {
	cfg  Config
	proc *hrana.Processor
}

func NewHandler(cfg Config) *Handler {
	return &Handler{
		cfg: cfg,
		proc: hrana.NewProcessor(hrana.ProcessorConfig{
			Pool:          cfg.Pool,
			Router:        cfg.Router,
			Replicator:    cfg.Replicator,
			TxLog:         cfg.TxLog,
			Logger:        cfg.Logger,
			CommitTimeout: cfg.CommitTimeout,
		}),
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols: []string{"hrana3"},
	})
	if err != nil {
		return
	}
	defer func() { _ = conn.CloseNow() }()

	if conn.Subprotocol() != "hrana3" {
		_ = conn.Close(websocket.StatusPolicyViolation, "unsupported subprotocol")
		return
	}

	h.serve(r.Context(), conn)
}

func (h *Handler) serve(ctx context.Context, conn *websocket.Conn) {
	c := newConnection(h.proc)
	defer c.cleanup()

	writeCh := make(chan ServerMsg, 64)

	writeCtx, cancelWrite := context.WithCancel(ctx)
	defer cancelWrite()
	go func() {
		for {
			select {
			case <-writeCtx.Done():
				return
			case msg, ok := <-writeCh:
				if !ok {
					return
				}
				if err := wsjson.Write(writeCtx, conn, msg); err != nil {
					if h.cfg.Logger != nil {
						h.cfg.Logger.Error("ws: write error", err)
					}
					cancelWrite()
					return
				}
			}
		}
	}()

	send := func(msg ServerMsg) {
		select {
		case writeCh <- msg:
		case <-writeCtx.Done():
		}
	}

	helloReceived := false

	for {
		var raw json.RawMessage
		if err := wsjson.Read(ctx, conn, &raw); err != nil {
			if h.cfg.Logger != nil && ctx.Err() == nil {
				h.cfg.Logger.Error("ws: read error", err)
			}
			return
		}

		var peek struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &peek); err != nil {
			send(helloError(hrana.NewError("invalid message: " + err.Error())))
			return
		}

		if peek.Type == "hello" {
			helloReceived = true
			send(helloOK())
			continue
		}

		if !helloReceived {
			send(helloError(hrana.NewError("expected hello message first")))
			return
		}

		if peek.Type == "request" {
			var msg ClientMsg
			if err := json.Unmarshal(raw, &msg); err != nil {
				send(helloError(hrana.NewError("invalid request: " + err.Error())))
				return
			}

			var req ClientRequest
			if err := json.Unmarshal(msg.Request, &req); err != nil {
				send(responseError(msg.RequestID, hrana.NewError("invalid request payload: "+err.Error())))
				continue
			}

			reqID := msg.RequestID
			go h.dispatchRequest(ctx, c, reqID, req, send)
			continue
		}

		// Unknown message types are silently ignored per Hrana spec §client-messages.
	}
}

func (h *Handler) dispatchRequest(ctx context.Context, c *Connection, reqID int32, req ClientRequest, send func(ServerMsg)) {
	switch req.Type {
	case "open_stream":
		c.getOrCreateStream(req.StreamID)
		msg, err := responseOK(reqID, OpenStreamResponse{})
		if err != nil {
			send(responseError(reqID, hrana.NewError(err.Error())))
			return
		}
		send(msg)

	case "close_stream":
		c.closeStream(req.StreamID)
		msg, err := responseOK(reqID, CloseStreamResponse{})
		if err != nil {
			send(responseError(reqID, hrana.NewError(err.Error())))
			return
		}
		send(msg)

	case "store_sql":
		if req.SQLId == nil || req.SQL == nil {
			send(responseError(reqID, hrana.NewError("store_sql: missing sql_id or sql")))
			return
		}
		c.storeSQL(*req.SQLId, *req.SQL)
		msg, err := responseOK(reqID, StoreSQLResponse{})
		if err != nil {
			send(responseError(reqID, hrana.NewError(err.Error())))
			return
		}
		send(msg)

	case "close_sql":
		if req.SQLId == nil {
			send(responseError(reqID, hrana.NewError("close_sql: missing sql_id")))
			return
		}
		c.closeSQL(*req.SQLId)
		msg, err := responseOK(reqID, CloseSQLResponse{})
		if err != nil {
			send(responseError(reqID, hrana.NewError(err.Error())))
			return
		}
		send(msg)

	case "open_cursor":
		if req.Batch == nil {
			send(responseError(reqID, hrana.NewError("open_cursor: missing batch")))
			return
		}
		if err := c.openCursor(ctx, req.CursorID, req.StreamID, *req.Batch); err != nil {
			send(responseError(reqID, hrana.NewError(fmt.Sprintf("open_cursor: %s", err))))
			return
		}
		msg, err := responseOK(reqID, OpenCursorResponse{})
		if err != nil {
			send(responseError(reqID, hrana.NewError(err.Error())))
			return
		}
		send(msg)

	case "fetch_cursor":
		maxCount := req.MaxCount
		if maxCount <= 0 {
			maxCount = 100
		}
		entries, done, err := c.fetchCursor(req.CursorID, maxCount)
		if err != nil {
			send(responseError(reqID, hrana.NewError(fmt.Sprintf("fetch_cursor: %s", err))))
			return
		}
		if entries == nil {
			entries = []CursorEntry{}
		}
		msg, mErr := responseOK(reqID, FetchCursorResponse{Entries: entries, Done: done})
		if mErr != nil {
			send(responseError(reqID, hrana.NewError(mErr.Error())))
			return
		}
		send(msg)

	case "close_cursor":
		c.closeCursor(req.CursorID)
		msg, err := responseOK(reqID, CloseCursorResponse{})
		if err != nil {
			send(responseError(reqID, hrana.NewError(err.Error())))
			return
		}
		send(msg)

	case "get_autocommit":
		ws := c.getOrCreateStream(req.StreamID)
		ws.mu.Lock()
		isAutocommit := !ws.strm.InTransaction()
		ws.mu.Unlock()
		msg, err := responseOK(reqID, GetAutocommitResponse{IsAutocommit: isAutocommit})
		if err != nil {
			send(responseError(reqID, hrana.NewError(err.Error())))
			return
		}
		send(msg)

	case "execute", "batch", "describe", "sequence":
		h.handleStreamRequest(ctx, c, reqID, req, send)

	default:
		send(responseError(reqID, hrana.NewError(fmt.Sprintf("unknown request type: %s", req.Type))))
	}
}

// handleStreamRequest acquires the per-stream mutex before proxying to enforce
// Hrana spec §stream-ordering: requests on the same stream are serial.
func (h *Handler) handleStreamRequest(ctx context.Context, c *Connection, reqID int32, req ClientRequest, send func(ServerMsg)) {
	ws := c.getOrCreateStream(req.StreamID)
	ws.mu.Lock()
	defer ws.mu.Unlock()

	sr := c.buildStreamRequest(req)
	result, err := h.proc.ProxyRequest(ctx, ws.strm, sr)
	if err != nil {
		send(responseError(reqID, hrana.NewError(err.Error())))
		return
	}

	if result.Type == "error" && result.Error != nil {
		send(responseError(reqID, result.Error))
		return
	}

	if result.Response == nil {
		send(responseError(reqID, hrana.NewError("upstream returned nil response")))
		return
	}

	var payload any
	switch req.Type {
	case "execute":
		var r hrana.StmtResult
		if err := json.Unmarshal(result.Response.Result, &r); err != nil {
			send(responseError(reqID, hrana.NewError("decode result: "+err.Error())))
			return
		}
		payload = ExecuteResponse{Result: &r}
	case "batch":
		var r hrana.BatchResult
		if err := json.Unmarshal(result.Response.Result, &r); err != nil {
			send(responseError(reqID, hrana.NewError("decode result: "+err.Error())))
			return
		}
		payload = BatchResponse{Result: &r}
	case "describe":
		var r hrana.DescribeResult
		if err := json.Unmarshal(result.Response.Result, &r); err != nil {
			send(responseError(reqID, hrana.NewError("decode result: "+err.Error())))
			return
		}
		payload = DescribeResponse{Result: &r}
	case "sequence":
		payload = SequenceResponse{}
	}

	msg, err := responseOK(reqID, payload)
	if err != nil {
		send(responseError(reqID, hrana.NewError(err.Error())))
		return
	}
	send(msg)
}
