package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type DDLStrategy string

const (
	DDLStrategyLeastTables DDLStrategy = "least_tables"
	DDLStrategyRoundRobin  DDLStrategy = "round_robin"
	DDLStrategyExplicit    DDLStrategy = "explicit"
)

type RouterConfig struct {
	GRPCAddr           string        `yaml:"grpc_addr"`
	HTTPAddr           string        `yaml:"http_addr"`
	MetaDBPath         string        `yaml:"meta_db_path"`
	SnapshotDir        string        `yaml:"snapshot_dir"`
	DefaultDDLStrategy DDLStrategy   `yaml:"default_ddl_strategy"`
	LogLevel           string        `yaml:"log_level"`
	HeartbeatTimeout   time.Duration `yaml:"heartbeat_timeout"`
	TxTimeout          time.Duration `yaml:"tx_timeout"`
}

type MasterConfig struct {
	ID                string        `yaml:"id"`
	GRPCAddr          string        `yaml:"grpc_addr"`
	HTTPAddr          string        `yaml:"http_addr"`
	AdvertiseGRPCAddr string        `yaml:"advertise_grpc_addr"` // reported to router; defaults to GRPCAddr
	AdvertiseHTTPAddr string        `yaml:"advertise_http_addr"` // reported to router; defaults to HTTPAddr
	DBPath            string        `yaml:"db_path"`
	RouterAddr        string        `yaml:"router_addr"`
	SnapshotDir       string        `yaml:"snapshot_dir"`
	LogLevel          string        `yaml:"log_level"`
	Tables            []string      `yaml:"tables"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	TxTimeout         time.Duration `yaml:"tx_timeout"`
}

func LoadRouter(path string) (RouterConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return RouterConfig{}, fmt.Errorf("read router config %q: %w", path, err)
	}

	var cfg RouterConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return RouterConfig{}, fmt.Errorf("parse router config %q: %w", path, err)
	}

	if err := cfg.validate(); err != nil {
		return RouterConfig{}, fmt.Errorf("invalid router config %q: %w", path, err)
	}

	return cfg, nil
}

func LoadMaster(path string) (MasterConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return MasterConfig{}, fmt.Errorf("read master config %q: %w", path, err)
	}

	var cfg MasterConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return MasterConfig{}, fmt.Errorf("parse master config %q: %w", path, err)
	}

	if err := cfg.validate(); err != nil {
		return MasterConfig{}, fmt.Errorf("invalid master config %q: %w", path, err)
	}

	return cfg, nil
}

func (c *RouterConfig) validate() error {
	if c.GRPCAddr == "" {
		return fmt.Errorf("grpc_addr is required")
	}
	if c.HTTPAddr == "" {
		return fmt.Errorf("http_addr is required")
	}
	if c.MetaDBPath == "" {
		return fmt.Errorf("meta_db_path is required")
	}
	if c.HeartbeatTimeout <= 0 {
		c.HeartbeatTimeout = 30 * time.Second
	}
	if c.TxTimeout <= 0 {
		c.TxTimeout = 30 * time.Second
	}
	if c.DefaultDDLStrategy == "" {
		c.DefaultDDLStrategy = DDLStrategyLeastTables
	}
	if c.SnapshotDir == "" {
		c.SnapshotDir = os.TempDir()
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	return nil
}

func (c *MasterConfig) validate() error {
	if c.ID == "" {
		return fmt.Errorf("id is required")
	}
	if c.GRPCAddr == "" {
		return fmt.Errorf("grpc_addr is required")
	}
	if c.HTTPAddr == "" {
		return fmt.Errorf("http_addr is required")
	}
	if c.DBPath == "" {
		return fmt.Errorf("db_path is required")
	}
	if c.RouterAddr == "" {
		return fmt.Errorf("router_addr is required")
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 5 * time.Second
	}
	if c.TxTimeout <= 0 {
		c.TxTimeout = 30 * time.Second
	}
	if c.AdvertiseGRPCAddr == "" {
		c.AdvertiseGRPCAddr = c.GRPCAddr
	}
	if c.AdvertiseHTTPAddr == "" {
		c.AdvertiseHTTPAddr = c.HTTPAddr
	}
	if c.SnapshotDir == "" {
		c.SnapshotDir = os.TempDir()
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	return nil
}
