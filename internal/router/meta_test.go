package router_test

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"hive/internal/router"
)

func newTestMeta(t *testing.T) *router.MetaStore {
	t.Helper()
	path := filepath.Join(t.TempDir(), "meta.db")
	m, err := router.OpenMeta(context.Background(), path, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, m.Close()) })
	return m
}

func TestMetaStore_TableResolver(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, m *router.MetaStore)
		run   func(t *testing.T, m *router.MetaStore)
		name  string
	}{
		{
			name: "register and lookup",
			run: func(t *testing.T, m *router.MetaStore) {
				t.Helper()
				ctx := context.Background()
				require.NoError(t, m.RegisterTable(ctx, "users", "master-1"))
				id, err := m.LookupMaster(ctx, "users")
				require.NoError(t, err)
				require.Equal(t, "master-1", id)
			},
		},
		{
			name: "lookup not found returns ErrNoRows",
			run: func(t *testing.T, m *router.MetaStore) {
				t.Helper()
				_, err := m.LookupMaster(context.Background(), "nonexistent")
				require.Error(t, err)
				require.True(t, errors.Is(err, sql.ErrNoRows))
			},
		},
		{
			name: "register same master is idempotent",
			run: func(t *testing.T, m *router.MetaStore) {
				t.Helper()
				ctx := context.Background()
				require.NoError(t, m.RegisterTable(ctx, "orders", "master-2"))
				require.NoError(t, m.RegisterTable(ctx, "orders", "master-2"))
			},
		},
		{
			name: "register conflict with different master returns error",
			run: func(t *testing.T, m *router.MetaStore) {
				t.Helper()
				ctx := context.Background()
				require.NoError(t, m.RegisterTable(ctx, "orders", "master-1"))
				err := m.RegisterTable(ctx, "orders", "master-2")
				require.Error(t, err)
				require.Contains(t, err.Error(), "already owned")
			},
		},
		{
			name: "remove table makes it unfindable",
			run: func(t *testing.T, m *router.MetaStore) {
				t.Helper()
				ctx := context.Background()
				require.NoError(t, m.RegisterTable(ctx, "items", "master-1"))
				require.NoError(t, m.RemoveTable(ctx, "items"))
				_, err := m.LookupMaster(ctx, "items")
				require.Error(t, err)
			},
		},
		{
			name: "remove nonexistent table is noop",
			run: func(t *testing.T, m *router.MetaStore) {
				t.Helper()
				require.NoError(t, m.RemoveTable(context.Background(), "nonexistent"))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := newTestMeta(t)
			if tc.setup != nil {
				tc.setup(t, m)
			}
			tc.run(t, m)
		})
	}
}

func TestMetaStore_MasterRegistry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, m *router.MetaStore)
		name string
	}{
		{
			name: "register master appears in AliveMasters",
			run: func(t *testing.T, m *router.MetaStore) {
				t.Helper()
				ctx := context.Background()
				require.NoError(t, m.RegisterMaster(ctx, "m1", ":9001", ":8081"))
				masters, err := m.AliveMasters(ctx)
				require.NoError(t, err)
				require.Len(t, masters, 1)
				require.Equal(t, "m1", masters[0].ID)
				require.Equal(t, ":9001", masters[0].GRPCAddr)
				require.Equal(t, router.MasterStatusAlive, masters[0].Status)
			},
		},
		{
			name: "register master upserts addresses",
			run: func(t *testing.T, m *router.MetaStore) {
				t.Helper()
				ctx := context.Background()
				require.NoError(t, m.RegisterMaster(ctx, "m1", ":9001", ":8081"))
				require.NoError(t, m.RegisterMaster(ctx, "m1", ":9002", ":8082"))
				masters, err := m.AliveMasters(ctx)
				require.NoError(t, err)
				require.Len(t, masters, 1)
				require.Equal(t, ":9002", masters[0].GRPCAddr)
			},
		},
		{
			name: "update heartbeat succeeds for known master",
			run: func(t *testing.T, m *router.MetaStore) {
				t.Helper()
				ctx := context.Background()
				require.NoError(t, m.RegisterMaster(ctx, "m1", ":9001", ":8081"))
				require.NoError(t, m.UpdateHeartbeat(ctx, "m1"))
			},
		},
		{
			name: "update heartbeat for unknown master returns error",
			run: func(t *testing.T, m *router.MetaStore) {
				t.Helper()
				require.Error(t, m.UpdateHeartbeat(context.Background(), "ghost"))
			},
		},
		{
			name: "set status draining hides master from AliveMasters",
			run: func(t *testing.T, m *router.MetaStore) {
				t.Helper()
				ctx := context.Background()
				require.NoError(t, m.RegisterMaster(ctx, "m1", ":9001", ":8081"))
				require.NoError(t, m.SetStatus(ctx, "m1", router.MasterStatusDraining))
				masters, err := m.AliveMasters(ctx)
				require.NoError(t, err)
				require.Empty(t, masters)
			},
		},
		{
			name: "set status for unknown master returns error",
			run: func(t *testing.T, m *router.MetaStore) {
				t.Helper()
				require.Error(t, m.SetStatus(context.Background(), "ghost", router.MasterStatusDead))
			},
		},
		{
			name: "mark dead masters by stale heartbeat",
			run: func(t *testing.T, m *router.MetaStore) {
				t.Helper()
				ctx := context.Background()
				require.NoError(t, m.RegisterMaster(ctx, "old", ":9001", ":8081"))

				// Sleep long enough so "old" is clearly older than the timeout,
				// then register "fresh" just before calling MarkDeadMasters.
				time.Sleep(100 * time.Millisecond)
				require.NoError(t, m.RegisterMaster(ctx, "fresh", ":9002", ":8082"))

				// timeout=50ms: "old" (100ms ago) is dead, "fresh" (just now) is alive.
				dead, err := m.MarkDeadMasters(ctx, 50*time.Millisecond)
				require.NoError(t, err)
				require.Contains(t, dead, "old")
				require.NotContains(t, dead, "fresh")
			},
		},
		{
			name: "tables by master returns correct subset",
			run: func(t *testing.T, m *router.MetaStore) {
				t.Helper()
				ctx := context.Background()
				require.NoError(t, m.RegisterTable(ctx, "users", "m1"))
				require.NoError(t, m.RegisterTable(ctx, "profiles", "m1"))
				require.NoError(t, m.RegisterTable(ctx, "orders", "m2"))

				tables, err := m.TablesByMaster(ctx, "m1")
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"users", "profiles"}, tables)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := newTestMeta(t)
			tc.run(t, m)
		})
	}
}

func TestOpenMeta_EmptyPath(t *testing.T) {
	t.Parallel()
	_, err := router.OpenMeta(context.Background(), "", slog.Default())
	require.Error(t, err)
}
