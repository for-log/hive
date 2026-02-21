package master

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"hive/gen/hivepb"
)

type RegistrarConfig struct {
	MasterID          string
	GRPCAddr          string // dialled by the router for DML/DDL
	HTTPAddr          string // used by the router for snapshot downloads
	RouterAddr        string
	Tables            []string
	HeartbeatInterval time.Duration
}

type Registrar struct {
	client hivepb.HiveRegistryClient
	conn   *grpc.ClientConn
	log    *slog.Logger
	cfg    RegistrarConfig
}

// NewRegistrar dials the router; opts extend the default insecure credentials
// (useful for injecting bufconn in tests).
func NewRegistrar(cfg RegistrarConfig, opts []grpc.DialOption, log *slog.Logger) (*Registrar, error) {
	if cfg.MasterID == "" {
		return nil, fmt.Errorf("master/register: master_id must not be empty")
	}
	if cfg.RouterAddr == "" {
		return nil, fmt.Errorf("master/register: router_addr must not be empty")
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 5 * time.Second
	}

	dialOpts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, opts...)

	conn, err := grpc.NewClient(cfg.RouterAddr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("master/register: dial router %q: %w", cfg.RouterAddr, err)
	}

	return &Registrar{
		cfg:    cfg,
		conn:   conn,
		client: hivepb.NewHiveRegistryClient(conn),
		log:    log,
	}, nil
}

func (r *Registrar) Close() error {
	if err := r.conn.Close(); err != nil {
		return fmt.Errorf("master/register: close conn: %w", err)
	}
	return nil
}

func (r *Registrar) Register(ctx context.Context) error {
	resp, err := r.client.Register(ctx, &hivepb.RegisterRequest{
		MasterId: r.cfg.MasterID,
		GrpcAddr: r.cfg.GRPCAddr,
		HttpAddr: r.cfg.HTTPAddr,
		Tables:   r.cfg.Tables,
	})
	if err != nil {
		return fmt.Errorf("master/register: register rpc: %w", err)
	}
	if !resp.Ok {
		return fmt.Errorf("master/register: router rejected registration, conflicts: %v", resp.Conflicts)
	}
	r.log.Info("master registered",
		"master_id", r.cfg.MasterID,
		"tables", r.cfg.Tables,
		"router", r.cfg.RouterAddr,
	)
	return nil
}

// Run sends a draining heartbeat on shutdown so the router reacts immediately
// instead of waiting for the dead-detection timeout.
func (r *Registrar) Run(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// ctx is already cancelled; use a fresh context with a bounded
			// timeout so the draining heartbeat doesn't block indefinitely.
			drainCtx, drainCancel := context.WithTimeout(context.Background(), r.cfg.HeartbeatInterval)
			r.sendHeartbeat(drainCtx, true)
			drainCancel()
			return
		case <-ticker.C:
			r.sendHeartbeat(ctx, false)
		}
	}
}

// sendHeartbeat logs errors; a missed beat is not fatal — the router applies its own timeout.
func (r *Registrar) sendHeartbeat(ctx context.Context, draining bool) {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.HeartbeatInterval/2)
	defer cancel()

	_, err := r.client.Heartbeat(ctx, &hivepb.HeartbeatRequest{
		MasterId: r.cfg.MasterID,
		Draining: draining,
	})
	if err != nil {
		r.log.Error("master: heartbeat failed",
			"master_id", r.cfg.MasterID,
			"draining", draining,
			"err", err,
		)
		return
	}
	if draining {
		r.log.Info("master: sent draining heartbeat", "master_id", r.cfg.MasterID)
	}
}
