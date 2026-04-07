package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// ReadPolicy defines how reads are routed across masters.
type ReadPolicy string

const (
	ReadPolicyWriteMaster ReadPolicy = "write_master"
	ReadPolicyRoundRobin  ReadPolicy = "round_robin"
	ReadPolicyRandom      ReadPolicy = "random"
)

// MasterConfig holds connection settings for a single libSQL master.
type MasterConfig struct {
	URL   string `yaml:"url"`
	Token string `yaml:"token"`
}

// ReplicationConfig holds settings for the async replicator.
type ReplicationConfig struct {
	Workers       int           `yaml:"workers"`
	RetryMax      int           `yaml:"retry_max"`
	RetryBackoff  time.Duration `yaml:"retry_backoff"`
	QueueCapacity int           `yaml:"queue_capacity"`
}

// Config is the root configuration for the orchestrator.
type Config struct {
	ListenAddr       string            `yaml:"listen_addr"`
	Masters          []MasterConfig    `yaml:"masters"`
	TableAssignments map[string]int    `yaml:"table_assignments"`
	ReadPolicy       ReadPolicy        `yaml:"read_policy"`
	StreamTTL        time.Duration     `yaml:"stream_ttl"`
	MaxBodyBytes     int64             `yaml:"max_body_bytes"`
	Replication      ReplicationConfig `yaml:"replication"`
}

// defaults fills in zero-value fields with sensible defaults.
func (c *Config) defaults() {
	if c.ListenAddr == "" {
		c.ListenAddr = ":8080"
	}
	if c.ReadPolicy == "" {
		c.ReadPolicy = ReadPolicyWriteMaster
	}
	if c.StreamTTL == 0 {
		c.StreamTTL = 10 * time.Second
	}
	if c.MaxBodyBytes == 0 {
		c.MaxBodyBytes = 4 * 1024 * 1024 // 4 MiB
	}
	if c.Replication.Workers == 0 {
		c.Replication.Workers = 4
	}
	if c.Replication.RetryMax == 0 {
		c.Replication.RetryMax = 3
	}
	if c.Replication.RetryBackoff == 0 {
		c.Replication.RetryBackoff = time.Second
	}
	if c.Replication.QueueCapacity == 0 {
		c.Replication.QueueCapacity = 1024
	}
	if c.TableAssignments == nil {
		c.TableAssignments = make(map[string]int)
	}
}

// validate checks that required fields are present and values are in range.
func (c *Config) validate() error {
	if len(c.Masters) == 0 {
		return fmt.Errorf("config: at least one master must be configured")
	}
	for i, m := range c.Masters {
		if m.URL == "" {
			return fmt.Errorf("config: master[%d].url is required", i)
		}
	}
	for table, idx := range c.TableAssignments {
		if idx < 0 || idx >= len(c.Masters) {
			return fmt.Errorf("config: table_assignments[%q] references master index %d, but only %d masters configured", table, idx, len(c.Masters))
		}
	}
	switch c.ReadPolicy {
	case ReadPolicyWriteMaster, ReadPolicyRoundRobin, ReadPolicyRandom:
	default:
		return fmt.Errorf("config: unknown read_policy %q", c.ReadPolicy)
	}
	return nil
}

// Load reads a YAML config file from path, applies defaults and validates it.
func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("config: open %q: %w", path, err)
	}
	defer f.Close()

	var cfg Config
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("config: decode %q: %w", path, err)
	}

	cfg.defaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}
