package stream

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"github.com/hive_v2/orchestrator/internal/router"
	"github.com/hive_v2/orchestrator/internal/transaction"
)

// SQLCache is a concurrency-safe store for SQL texts registered via store_sql.
type SQLCache struct {
	mu sync.RWMutex
	m  map[int32]string
}

func NewSQLCache() *SQLCache {
	return &SQLCache{m: make(map[int32]string)}
}

func (c *SQLCache) Store(id int32, sql string) {
	c.mu.Lock()
	c.m[id] = sql
	c.mu.Unlock()
}

func (c *SQLCache) Load(id int32) string {
	c.mu.RLock()
	v := c.m[id]
	c.mu.RUnlock()
	return v
}

func (c *SQLCache) Delete(id int32) {
	c.mu.Lock()
	delete(c.m, id)
	c.mu.Unlock()
}

type Stream struct {
	// ClientBaton is the token returned to the client; it changes on every
	// pipeline response to force serial request ordering (per Hrana spec).
	ClientBaton string

	// MasterBatons stores the upstream baton per master index.
	// Each master maintains its own stateful stream; we must not mix batons.
	MasterBatons map[int]string

	// PinnedMaster is set when the stream enters a transaction (BEGIN).
	// All subsequent statements are routed here until COMMIT/ROLLBACK.
	PinnedMaster *router.Master

	SQLStore *SQLCache

	// TxWALID is the stream ClientBaton captured when the current txn started; used for tx WAL correlation.
	TxWALID string

	// TxBuffer is non-nil while cross-master lazy transactions may be active.
	TxBuffer *transaction.TxBuffer

	lastUsed time.Time
}

func (s *Stream) touch() { s.lastUsed = time.Now() }

func (s *Stream) InTransaction() bool {
	if s.PinnedMaster != nil {
		return true
	}
	if s.TxBuffer != nil && s.TxBuffer.Active {
		return true
	}
	return false
}

func (s *Stream) LazyCrossTxActive() bool {
	return s.TxBuffer != nil && s.TxBuffer.Active
}

// EvictFunc is called when an expired stream had an active transaction.
// Implementations should send ROLLBACK to upstream masters.
type EvictFunc func(s *Stream)

type Manager struct {
	mu      sync.Mutex
	streams map[string]*Stream
	ttl     time.Duration
	onEvict EvictFunc
}

// The reaper stops when ctx is cancelled.
// onEvict is called (if non-nil) for each expired stream that had an active transaction.
func NewManager(ctx context.Context, ttl time.Duration, onEvict EvictFunc) *Manager {
	m := &Manager{
		streams: make(map[string]*Stream),
		ttl:     ttl,
		onEvict: onEvict,
	}
	go m.reap(ctx)
	return m
}

func (m *Manager) Create() (*Stream, error) {
	baton, err := newBaton()
	if err != nil {
		return nil, err
	}
	s := &Stream{
		ClientBaton:  baton,
		MasterBatons: make(map[int]string),
		SQLStore:     NewSQLCache(),
		lastUsed:     time.Now(),
	}
	m.mu.Lock()
	m.streams[baton] = s
	m.mu.Unlock()
	return s, nil
}

// Get resolves a baton to a Stream. Returns an error if the baton is unknown
// or the stream has already been closed.
func (m *Manager) Get(baton string) (*Stream, error) {
	m.mu.Lock()
	s, ok := m.streams[baton]
	m.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("stream: unknown or expired baton")
	}
	return s, nil
}

// Must be called after every successful pipeline response (Hrana spec §baton).
func (m *Manager) Rotate(s *Stream) (string, error) {
	newBaton, err := newBaton()
	if err != nil {
		return "", err
	}
	m.mu.Lock()
	delete(m.streams, s.ClientBaton)
	s.ClientBaton = newBaton
	s.touch()
	m.streams[newBaton] = s
	m.mu.Unlock()
	return newBaton, nil
}

func (m *Manager) Close(s *Stream) {
	m.mu.Lock()
	delete(m.streams, s.ClientBaton)
	m.mu.Unlock()
}

func (m *Manager) Size() int {
	m.mu.Lock()
	n := len(m.streams)
	m.mu.Unlock()
	return n
}

// reap periodically removes streams that have been idle longer than the TTL.
func (m *Manager) reap(ctx context.Context) {
	ticker := time.NewTicker(m.ttl / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.evictExpired()
		}
	}
}

func (m *Manager) evictExpired() {
	deadline := time.Now().Add(-m.ttl)
	var expired []*Stream
	m.mu.Lock()
	for baton, s := range m.streams {
		if s.lastUsed.Before(deadline) {
			delete(m.streams, baton)
			expired = append(expired, s)
		}
	}
	m.mu.Unlock()

	for _, s := range expired {
		if m.onEvict != nil && s.InTransaction() {
			m.onEvict(s)
		}
	}
}

// newBaton generates a cryptographically random, URL-safe token.
// 24 bytes → 32 base64url characters; collision probability is negligible.
func newBaton() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("stream: generate baton: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
