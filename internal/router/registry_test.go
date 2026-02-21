package router_test

import (
	"context"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"hive/gen/hivepb"
	"hive/internal/router"
)

func newTestRegistry(t *testing.T, cfg router.RegistryConfig) (hivepb.HiveRegistryClient, *router.Registry) {
	t.Helper()

	meta := newTestMeta(t)
	reg := router.NewRegistry(meta, meta, cfg, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))

	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcSrv := grpc.NewServer()
	hivepb.RegisterHiveRegistryServer(grpcSrv, reg)

	go func() {
		if serveErr := grpcSrv.Serve(lis); serveErr != nil && serveErr != grpc.ErrServerStopped {
			t.Logf("registry serve: %v", serveErr)
		}
	}()
	t.Cleanup(func() { grpcSrv.GracefulStop() })

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	return hivepb.NewHiveRegistryClient(conn), reg
}

func TestRegistry_Register(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req          *hivepb.RegisterRequest
		name         string
		wantConflict []string
		wantOK       bool
	}{
		{
			name: "fresh registration succeeds",
			req: &hivepb.RegisterRequest{
				MasterId: "m1", GrpcAddr: ":9001", HttpAddr: ":8081",
				Tables: []string{"users", "orders"},
			},
			wantOK: true,
		},
		{
			name:   "empty master_id rejected",
			req:    &hivepb.RegisterRequest{GrpcAddr: ":9001"},
			wantOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, _ := newTestRegistry(t, router.RegistryConfig{})

			resp, err := client.Register(context.Background(), tc.req)
			if !tc.wantOK && tc.req.MasterId == "" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantOK, resp.Ok)
		})
	}
}

func TestRegistry_Register_TableConflict(t *testing.T) {
	t.Parallel()
	client, _ := newTestRegistry(t, router.RegistryConfig{})
	ctx := context.Background()

	_, err := client.Register(ctx, &hivepb.RegisterRequest{
		MasterId: "m1", GrpcAddr: ":9001", HttpAddr: ":8081",
		Tables: []string{"users"},
	})
	require.NoError(t, err)

	resp, err := client.Register(ctx, &hivepb.RegisterRequest{
		MasterId: "m2", GrpcAddr: ":9002", HttpAddr: ":8082",
		Tables: []string{"users", "orders"},
	})
	require.NoError(t, err)
	require.False(t, resp.Ok)
	require.Contains(t, resp.Conflicts, "users")
	require.NotContains(t, resp.Conflicts, "orders")
}

func TestRegistry_Register_Idempotent(t *testing.T) {
	t.Parallel()
	client, _ := newTestRegistry(t, router.RegistryConfig{})
	ctx := context.Background()

	req := &hivepb.RegisterRequest{
		MasterId: "m1", GrpcAddr: ":9001", HttpAddr: ":8081",
		Tables: []string{"users", "orders"},
	}
	resp, err := client.Register(ctx, req)
	require.NoError(t, err)
	require.True(t, resp.Ok)

	resp, err = client.Register(ctx, req)
	require.NoError(t, err)
	require.True(t, resp.Ok)
}

func TestRegistry_Register_ReconcileRemovedTable(t *testing.T) {
	t.Parallel()
	client, _ := newTestRegistry(t, router.RegistryConfig{})
	ctx := context.Background()

	_, err := client.Register(ctx, &hivepb.RegisterRequest{
		MasterId: "m1", GrpcAddr: ":9001", HttpAddr: ":8081",
		Tables: []string{"users", "orders"},
	})
	require.NoError(t, err)

	_, err = client.Register(ctx, &hivepb.RegisterRequest{
		MasterId: "m1", GrpcAddr: ":9001", HttpAddr: ":8081",
		Tables: []string{"users"},
	})
	require.NoError(t, err)

	resp, err := client.Register(ctx, &hivepb.RegisterRequest{
		MasterId: "m2", GrpcAddr: ":9002", HttpAddr: ":8082",
		Tables: []string{"orders"},
	})
	require.NoError(t, err)
	require.True(t, resp.Ok)
}

func TestRegistry_Heartbeat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client hivepb.HiveRegistryClient)
		req     *hivepb.HeartbeatRequest
		name    string
		wantErr bool
	}{
		{
			name: "heartbeat for registered master succeeds",
			setup: func(t *testing.T, client hivepb.HiveRegistryClient) {
				t.Helper()
				_, err := client.Register(context.Background(), &hivepb.RegisterRequest{
					MasterId: "m1", GrpcAddr: ":9001", HttpAddr: ":8081",
				})
				require.NoError(t, err)
			},
			req:     &hivepb.HeartbeatRequest{MasterId: "m1"},
			wantErr: false,
		},
		{
			name:    "heartbeat for unknown master returns error",
			req:     &hivepb.HeartbeatRequest{MasterId: "ghost"},
			wantErr: true,
		},
		{
			name:    "empty master_id rejected",
			req:     &hivepb.HeartbeatRequest{},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, _ := newTestRegistry(t, router.RegistryConfig{})
			if tc.setup != nil {
				tc.setup(t, client)
			}
			_, err := client.Heartbeat(context.Background(), tc.req)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRegistry_Heartbeat_Draining(t *testing.T) {
	t.Parallel()
	client, _ := newTestRegistry(t, router.RegistryConfig{})
	ctx := context.Background()

	_, err := client.Register(ctx, &hivepb.RegisterRequest{
		MasterId: "m1", GrpcAddr: ":9001", HttpAddr: ":8081",
	})
	require.NoError(t, err)

	_, err = client.Heartbeat(ctx, &hivepb.HeartbeatRequest{MasterId: "m1", Draining: true})
	require.NoError(t, err)

	_, err = client.Heartbeat(ctx, &hivepb.HeartbeatRequest{MasterId: "m1"})
	require.Error(t, err)
}

func TestRegistry_Run_DeadDetection(t *testing.T) {
	t.Parallel()

	cfg := router.RegistryConfig{
		DeadTimeout:       20 * time.Millisecond,
		ReconcileInterval: 10 * time.Millisecond,
	}
	client, reg := newTestRegistry(t, cfg)
	ctx := context.Background()

	_, err := client.Register(ctx, &hivepb.RegisterRequest{
		MasterId: "stale", GrpcAddr: ":9001", HttpAddr: ":8081",
		Tables: []string{"users"},
	})
	require.NoError(t, err)

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go reg.Run(runCtx)

	time.Sleep(150 * time.Millisecond) // enough for DeadTimeout=20ms + ReconcileInterval=10ms to fire

	resp, err := client.Register(ctx, &hivepb.RegisterRequest{
		MasterId: "m2", GrpcAddr: ":9002", HttpAddr: ":8082",
		Tables: []string{"users"},
	})
	require.NoError(t, err)
	require.True(t, resp.Ok)
}
