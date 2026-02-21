// Binary hive-router routes SQL queries across hive-master shards.
//
// It maintains a persistent table→master mapping in a local SQLite meta-store,
// exposes a gRPC HiveSQL endpoint for clients, a gRPC HiveRegistry endpoint
// for master registration, and an HTTP endpoint for merged snapshots.
//
// Usage:
//
//	hive-router -config ./configs/router.yaml
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"hive/gen/hivepb"
	"hive/internal/config"
	"hive/internal/router"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "hive-router: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfgPath := flag.String("config", "", "path to router YAML config (required)")
	flag.Parse()

	if *cfgPath == "" {
		return fmt.Errorf("-config flag is required")
	}

	cfg, err := config.LoadRouter(*cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	log := newLogger(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	meta, err := router.OpenMeta(ctx, cfg.MetaDBPath, log)
	if err != nil {
		return fmt.Errorf("open meta db: %w", err)
	}
	defer func() {
		if closeErr := meta.Close(); closeErr != nil {
			log.Error("close meta db", "err", closeErr)
		}
	}()

	routerSrv := router.NewRouter(meta, meta, router.RouterConfig{
		TxTimeout: cfg.TxTimeout,
	}, log)

	registry := router.NewRegistry(meta, meta, router.RegistryConfig{
		DeadTimeout:       cfg.HeartbeatTimeout,
		ReconcileInterval: cfg.HeartbeatTimeout / 3,
	}, log)

	merger := router.NewMerger(meta, cfg.SnapshotDir, log)
	httpSrv := router.NewHTTPServer(merger, meta, log)

	grpcSrv := grpc.NewServer()
	hivepb.RegisterHiveSQLServer(grpcSrv, routerSrv)
	hivepb.RegisterHiveRegistryServer(grpcSrv, registry)

	grpcLis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen grpc %q: %w", cfg.GRPCAddr, err)
	}

	log.Info("hive-router starting",
		"grpc", cfg.GRPCAddr,
		"http", cfg.HTTPAddr,
		"meta_db", cfg.MetaDBPath,
	)

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if serveErr := grpcSrv.Serve(grpcLis); serveErr != nil {
			return fmt.Errorf("grpc serve: %w", serveErr)
		}
		return nil
	})

	g.Go(func() error {
		if serveErr := httpSrv.ListenAndServe(gCtx, cfg.HTTPAddr); serveErr != nil {
			return fmt.Errorf("http serve: %w", serveErr)
		}
		return nil
	})

	g.Go(func() error {
		registry.Run(gCtx)
		return nil
	})

	g.Go(func() error {
		<-gCtx.Done()
		grpcSrv.GracefulStop()
		return nil
	})

	if err = g.Wait(); err != nil {
		return fmt.Errorf("serve: %w", err)
	}

	log.Info("hive-router stopped")
	return nil
}

// newLogger defaults to Info for unrecognised level strings.
func newLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: l}))
}
