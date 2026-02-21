package router

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"hive/gen/hivepb"
)

type TableResolver interface {
	LookupMaster(ctx context.Context, tableName string) (string, error)
	RegisterTable(ctx context.Context, tableName, masterID string) error
	RemoveTable(ctx context.Context, tableName string) error
	TablesByMaster(ctx context.Context, masterID string) ([]string, error)
}

type MasterRegistry interface {
	RegisterMaster(ctx context.Context, id, grpcAddr, httpAddr string) error
	UpdateHeartbeat(ctx context.Context, masterID string) error
	SetStatus(ctx context.Context, masterID string, status MasterStatus) error
	AliveMasters(ctx context.Context) ([]MasterInfo, error)
	MarkDeadMasters(ctx context.Context, timeout time.Duration) ([]string, error)
}

type RegistryConfig struct {
	DeadTimeout       time.Duration // how long a master can miss heartbeats before being marked dead
	ReconcileInterval time.Duration // how often the dead-detection loop runs
}

func (c *RegistryConfig) applyDefaults() {
	if c.DeadTimeout <= 0 {
		c.DeadTimeout = 30 * time.Second
	}
	if c.ReconcileInterval <= 0 {
		c.ReconcileInterval = 10 * time.Second
	}
}

type Registry struct {
	hivepb.UnimplementedHiveRegistryServer
	mr  MasterRegistry
	tr  TableResolver
	log *slog.Logger
	cfg RegistryConfig
}

func NewRegistry(tr TableResolver, mr MasterRegistry, cfg RegistryConfig, log *slog.Logger) *Registry {
	cfg.applyDefaults()
	return &Registry{tr: tr, mr: mr, log: log, cfg: cfg}
}

// Register returns conflicts for tables already owned by a different master.
func (r *Registry) Register(ctx context.Context, req *hivepb.RegisterRequest) (*hivepb.RegisterResponse, error) {
	if req.MasterId == "" {
		return nil, status.Error(codes.InvalidArgument, "master_id is required")
	}

	if err := r.mr.RegisterMaster(ctx, req.MasterId, req.GrpcAddr, req.HttpAddr); err != nil {
		r.log.Error("registry: register master", "master_id", req.MasterId, "err", err)
		return nil, status.Errorf(codes.Internal, "register master: %v", err)
	}

	var conflicts []string
	for _, table := range req.Tables {
		if err := r.tr.RegisterTable(ctx, table, req.MasterId); err != nil {
			r.log.Warn("registry: table conflict", "table", table, "master_id", req.MasterId, "err", err)
			conflicts = append(conflicts, table)
		}
	}

	// Non-fatal: stale mappings are cleaned on the next successful register.
	if err := r.reconcileTables(ctx, req.MasterId, req.Tables); err != nil {
		r.log.Error("registry: reconcile tables", "master_id", req.MasterId, "err", err)
	}

	if len(conflicts) > 0 {
		return &hivepb.RegisterResponse{Ok: false, Conflicts: conflicts}, nil
	}

	r.log.Info("registry: master registered",
		"master_id", req.MasterId,
		"tables", req.Tables,
		"grpc_addr", req.GrpcAddr,
	)
	return &hivepb.RegisterResponse{Ok: true}, nil
}

func (r *Registry) Heartbeat(ctx context.Context, req *hivepb.HeartbeatRequest) (*hivepb.HeartbeatResponse, error) {
	if req.MasterId == "" {
		return nil, status.Error(codes.InvalidArgument, "master_id is required")
	}

	if req.Draining {
		if err := r.mr.SetStatus(ctx, req.MasterId, MasterStatusDraining); err != nil {
			r.log.Error("registry: set draining", "master_id", req.MasterId, "err", err)
			return nil, status.Errorf(codes.Internal, "set draining: %v", err)
		}
		r.log.Info("registry: master draining", "master_id", req.MasterId)
		return &hivepb.HeartbeatResponse{Ok: true}, nil
	}

	if err := r.mr.UpdateHeartbeat(ctx, req.MasterId); err != nil {
		r.log.Error("registry: update heartbeat", "master_id", req.MasterId, "err", err)
		return nil, status.Errorf(codes.NotFound, "update heartbeat: %v", err)
	}
	return &hivepb.HeartbeatResponse{Ok: true}, nil
}

func (r *Registry) Run(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.ReconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.detectDead(ctx)
		}
	}
}

func (r *Registry) detectDead(ctx context.Context) {
	deadIDs, err := r.mr.MarkDeadMasters(ctx, r.cfg.DeadTimeout)
	if err != nil {
		r.log.Error("registry: mark dead masters", "err", err)
		return
	}
	for _, id := range deadIDs {
		r.log.Warn("registry: master declared dead", "master_id", id)
		r.removeTablesForMaster(ctx, id)
	}
}

func (r *Registry) removeTablesForMaster(ctx context.Context, masterID string) {
	tables, err := r.tr.TablesByMaster(ctx, masterID)
	if err != nil {
		r.log.Error("registry: list tables for dead master", "master_id", masterID, "err", err)
		return
	}
	for _, table := range tables {
		if err := r.tr.RemoveTable(ctx, table); err != nil {
			r.log.Error("registry: remove table for dead master", "table", table, "master_id", masterID, "err", err)
		}
	}
}

// reconcileTables removes mappings absent from declared; handles master restart with a reduced table list.
func (r *Registry) reconcileTables(ctx context.Context, masterID string, declared []string) error {
	existing, err := r.tr.TablesByMaster(ctx, masterID)
	if err != nil {
		return err
	}

	declaredSet := make(map[string]struct{}, len(declared))
	for _, t := range declared {
		declaredSet[t] = struct{}{}
	}

	for _, t := range existing {
		if _, ok := declaredSet[t]; !ok {
			if err := r.tr.RemoveTable(ctx, t); err != nil {
				r.log.Error("registry: remove stale table", "table", t, "master_id", masterID, "err", err)
			}
		}
	}
	return nil
}
