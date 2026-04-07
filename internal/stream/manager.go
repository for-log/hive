package stream

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"github.com/hive_v2/orchestrator/internal/router"
)

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

	// SQLStore caches SQL texts registered via store_sql requests.
	// Keyed by the client-assigned sql_id.
	SQLStore map[int32]string

	lastUsed time.Time
}

func (s *Stream) touch() { s.lastUsed = time.Now() }

// Manager creates, resolves, and expires Streams.
// All methods are safe for concurrent use.
type Manager struct {
	mu      sync.Mutex
	streams map[string]*Stream
	ttl     time.Duration
}

// NewManager creates a Manager and starts a background TTL reaper.
// The reaper stops when ctx is cancelled.
func NewManager(ctx context.Context, ttl time.Duration) *Manager {
	m := &Manager{
		streams: make(map[string]*Stream),
		ttl:     ttl,
	}
	go m.reap(ctx)
	return m
}

// Create allocates a new Stream and returns its initial baton.
func (m *Manager) Create() (*Stream, error) {
	baton, err := newBaton()
	if err != nil {
		return nil, err
	}
	s := &Stream{
		ClientBaton:  baton,
		MasterBatons: make(map[int]string),
		SQLStore:     make(map[int32]string),
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

// Rotate issues a new client baton for the stream, replacing the old one.
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

// Close removes the stream from the manager, releasing all resources.
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
	m.mu.Lock()
	for baton, s := range m.streams {
		if s.lastUsed.Before(deadline) {
			delete(m.streams, baton)
		}
	}
	m.mu.Unlock()
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
