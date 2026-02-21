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
	"hive/internal/master"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "hive-master: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfgPath := flag.String("config", "", "path to master YAML config (required)")
	flag.Parse()

	if *cfgPath == "" {
		return fmt.Errorf("-config flag is required")
	}

	cfg, err := config.LoadMaster(*cfgPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	log := newLogger(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	db, err := master.Open(ctx, cfg.DBPath, log)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			log.Error("close db", "err", closeErr)
		}
	}()

	tables := cfg.Tables

	grpcSrv := grpc.NewServer()
	masterSrv := master.NewServer(db, tables, cfg.SnapshotDir, log)
	hivepb.RegisterHiveMasterServer(grpcSrv, masterSrv)

	grpcLis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen grpc %q: %w", cfg.GRPCAddr, err)
	}

	httpSrv := master.NewHTTPServer(db, cfg.SnapshotDir, log)

	reg, err := master.NewRegistrar(master.RegistrarConfig{
		MasterID:          cfg.ID,
		GRPCAddr:          cfg.AdvertiseGRPCAddr,
		HTTPAddr:          cfg.AdvertiseHTTPAddr,
		RouterAddr:        cfg.RouterAddr,
		Tables:            tables,
		HeartbeatInterval: cfg.HeartbeatInterval,
	}, nil, log)
	if err != nil {
		return fmt.Errorf("create registrar: %w", err)
	}
	defer func() {
		if closeErr := reg.Close(); closeErr != nil {
			log.Error("close registrar", "err", closeErr)
		}
	}()

	if err = reg.Register(ctx); err != nil {
		return fmt.Errorf("register with router: %w", err)
	}

	log.Info("hive-master starting",
		"id", cfg.ID,
		"grpc", cfg.GRPCAddr,
		"http", cfg.HTTPAddr,
		"tables", tables,
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
		reg.Run(gCtx)
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

	log.Info("hive-master stopped", "id", cfg.ID)
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
