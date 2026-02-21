package master_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"hive/gen/hivepb"
	"hive/internal/master"
)

type fakeRegistry struct {
	hivepb.UnimplementedHiveRegistryServer

	registerCalls  atomic.Int32
	heartbeatCalls atomic.Int32
	drainingCalls  atomic.Int32

	rejectNext bool // if true, next Register returns ok=false
}

func (f *fakeRegistry) Register(_ context.Context, req *hivepb.RegisterRequest) (*hivepb.RegisterResponse, error) {
	f.registerCalls.Add(1)
	if f.rejectNext {
		return &hivepb.RegisterResponse{Ok: false, Conflicts: req.Tables}, nil
	}
	return &hivepb.RegisterResponse{Ok: true}, nil
}

func (f *fakeRegistry) Heartbeat(_ context.Context, req *hivepb.HeartbeatRequest) (*hivepb.HeartbeatResponse, error) {
	f.heartbeatCalls.Add(1)
	if req.Draining {
		f.drainingCalls.Add(1)
	}
	return &hivepb.HeartbeatResponse{Ok: true}, nil
}

func newFakeRegistryServer(t *testing.T) (*fakeRegistry, func(context.Context, string) (net.Conn, error)) {
	t.Helper()
	fake := &fakeRegistry{}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	hivepb.RegisterHiveRegistryServer(srv, fake)

	go func() {
		if err := srv.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			t.Logf("fake registry serve: %v", err)
		}
	}()
	t.Cleanup(func() { srv.GracefulStop() })

	dial := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	return fake, dial
}

func newRegistrar(t *testing.T, dial func(context.Context, string) (net.Conn, error), interval time.Duration) *master.Registrar {
	t.Helper()
	cfg := master.RegistrarConfig{
		MasterID:          "test-master",
		GRPCAddr:          "localhost:9001",
		HTTPAddr:          "localhost:9002",
		RouterAddr:        "passthrough:///bufnet",
		Tables:            []string{"users", "orders"},
		HeartbeatInterval: interval,
	}
	opts := []grpc.DialOption{
		grpc.WithContextDialer(dial),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	reg, err := master.NewRegistrar(cfg, opts, testLogger(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reg.Close()) })
	return reg
}

func TestRegistrar_Register_OK(t *testing.T) {
	t.Parallel()
	fake, dial := newFakeRegistryServer(t)
	reg := newRegistrar(t, dial, 5*time.Second)

	require.NoError(t, reg.Register(context.Background()))
	require.EqualValues(t, 1, fake.registerCalls.Load())
}

func TestRegistrar_Register_Rejected(t *testing.T) {
	t.Parallel()
	fake, dial := newFakeRegistryServer(t)
	fake.rejectNext = true
	reg := newRegistrar(t, dial, 5*time.Second)

	err := reg.Register(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "conflicts")
}

func TestRegistrar_Heartbeat_SentPeriodically(t *testing.T) {
	t.Parallel()
	fake, dial := newFakeRegistryServer(t)
	reg := newRegistrar(t, dial, 50*time.Millisecond)

	require.NoError(t, reg.Register(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	go reg.Run(ctx)
	<-ctx.Done()

	// At least 2 heartbeats should have been sent in 300ms with 50ms interval.
	require.GreaterOrEqual(t, fake.heartbeatCalls.Load(), int32(2))
}

func TestRegistrar_DrainingHeartbeat_OnShutdown(t *testing.T) {
	t.Parallel()
	fake, dial := newFakeRegistryServer(t)
	reg := newRegistrar(t, dial, 10*time.Second) // long interval so only draining fires

	require.NoError(t, reg.Register(context.Background()))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		reg.Run(ctx)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}

	require.EqualValues(t, 1, fake.drainingCalls.Load())
}

func TestRegistrar_NewRegistrar_EmptyMasterID(t *testing.T) {
	t.Parallel()
	_, err := master.NewRegistrar(master.RegistrarConfig{
		RouterAddr: "localhost:9000",
	}, nil, testLogger(t))
	require.Error(t, err)
}

func TestRegistrar_NewRegistrar_EmptyRouterAddr(t *testing.T) {
	t.Parallel()
	_, err := master.NewRegistrar(master.RegistrarConfig{
		MasterID: "m1",
	}, nil, testLogger(t))
	require.Error(t, err)
}
