package stream_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hive_v2/orchestrator/internal/stream"
)

func newManager(t *testing.T, ttl time.Duration) *stream.Manager {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return stream.NewManager(ctx, ttl, nil)
}

func TestManager_CreateAndGet(t *testing.T) {
	t.Parallel()
	m := newManager(t, 10*time.Second)

	s, err := m.Create()
	require.NoError(t, err)
	assert.NotEmpty(t, s.ClientBaton)

	got, err := m.Get(s.ClientBaton)
	require.NoError(t, err)
	assert.Same(t, s, got)
}

func TestManager_Get_UnknownBaton(t *testing.T) {
	t.Parallel()
	m := newManager(t, 10*time.Second)

	_, err := m.Get("nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown or expired")
}

func TestManager_Rotate(t *testing.T) {
	t.Parallel()
	m := newManager(t, 10*time.Second)

	s, err := m.Create()
	require.NoError(t, err)
	oldBaton := s.ClientBaton

	newBaton, err := m.Rotate(s)
	require.NoError(t, err)
	assert.NotEqual(t, oldBaton, newBaton)
	assert.Equal(t, newBaton, s.ClientBaton)

	// Old baton must no longer resolve.
	_, err = m.Get(oldBaton)
	require.Error(t, err)

	// New baton must resolve to the same stream.
	got, err := m.Get(newBaton)
	require.NoError(t, err)
	assert.Same(t, s, got)
}

func TestManager_Close(t *testing.T) {
	t.Parallel()
	m := newManager(t, 10*time.Second)

	s, err := m.Create()
	require.NoError(t, err)
	baton := s.ClientBaton

	m.Close(s)
	assert.Equal(t, 0, m.Size())

	_, err = m.Get(baton)
	require.Error(t, err)
}

func TestManager_TTL_Eviction(t *testing.T) {
	t.Parallel()
	ttl := 50 * time.Millisecond
	m := newManager(t, ttl)

	s, err := m.Create()
	require.NoError(t, err)
	baton := s.ClientBaton

	// Wait for TTL + one reap cycle (ttl/2 interval).
	time.Sleep(ttl * 3)

	assert.Equal(t, 0, m.Size())
	_, err = m.Get(baton)
	require.Error(t, err)
}

func TestManager_Rotate_ResetsExpiry(t *testing.T) {
	t.Parallel()
	ttl := 100 * time.Millisecond
	m := newManager(t, ttl)

	s, err := m.Create()
	require.NoError(t, err)

	// Rotate just before TTL would expire.
	time.Sleep(ttl / 2)
	_, err = m.Rotate(s)
	require.NoError(t, err)

	// After another half-TTL the stream should still be alive.
	time.Sleep(ttl / 2)
	_, err = m.Get(s.ClientBaton)
	require.NoError(t, err)
}

func TestManager_Size(t *testing.T) {
	t.Parallel()
	m := newManager(t, 10*time.Second)

	assert.Equal(t, 0, m.Size())

	s1, _ := m.Create()
	s2, _ := m.Create()
	assert.Equal(t, 2, m.Size())

	m.Close(s1)
	assert.Equal(t, 1, m.Size())

	m.Close(s2)
	assert.Equal(t, 0, m.Size())
}

func TestManager_BatonUniqueness(t *testing.T) {
	t.Parallel()
	m := newManager(t, 10*time.Second)

	batons := make(map[string]struct{}, 100)
	for i := 0; i < 100; i++ {
		s, err := m.Create()
		require.NoError(t, err)
		_, dup := batons[s.ClientBaton]
		assert.False(t, dup, "duplicate baton generated")
		batons[s.ClientBaton] = struct{}{}
	}
}

func TestStream_SQLStore(t *testing.T) {
	t.Parallel()
	m := newManager(t, 10*time.Second)

	s, err := m.Create()
	require.NoError(t, err)

	s.SQLStore[1] = "SELECT * FROM users"
	s.SQLStore[2] = "INSERT INTO orders VALUES (?)"

	got, err := m.Get(s.ClientBaton)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM users", got.SQLStore[1])
	assert.Equal(t, "INSERT INTO orders VALUES (?)", got.SQLStore[2])
}
