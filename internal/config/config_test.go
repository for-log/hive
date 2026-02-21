package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"hive/internal/config"
)

func TestLoadRouter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check       func(t *testing.T, cfg config.RouterConfig)
		name        string
		yaml        string
		errContains string
		wantErr     bool
	}{
		{
			name: "valid full config",
			yaml: `
grpc_addr: ":9000"
http_addr: ":8080"
meta_db_path: "./data/meta.db"
heartbeat_timeout: "30s"
tx_timeout: "15s"
default_ddl_strategy: "round_robin"
log_level: "debug"
`,
			check: func(t *testing.T, cfg config.RouterConfig) {
				t.Helper()
				require.Equal(t, ":9000", cfg.GRPCAddr)
				require.Equal(t, ":8080", cfg.HTTPAddr)
				require.Equal(t, "./data/meta.db", cfg.MetaDBPath)
				require.Equal(t, 30*time.Second, cfg.HeartbeatTimeout)
				require.Equal(t, 15*time.Second, cfg.TxTimeout)
				require.Equal(t, config.DDLStrategyRoundRobin, cfg.DefaultDDLStrategy)
				require.Equal(t, "debug", cfg.LogLevel)
			},
		},
		{
			name: "defaults applied when optional fields absent",
			yaml: `
grpc_addr: ":9000"
http_addr: ":8080"
meta_db_path: "./meta.db"
`,
			check: func(t *testing.T, cfg config.RouterConfig) {
				t.Helper()
				require.Equal(t, 30*time.Second, cfg.HeartbeatTimeout)
				require.Equal(t, 30*time.Second, cfg.TxTimeout)
				require.Equal(t, config.DDLStrategyLeastTables, cfg.DefaultDDLStrategy)
				require.Equal(t, "info", cfg.LogLevel)
			},
		},
		{
			name:        "missing grpc_addr",
			yaml:        "http_addr: \":8080\"\nmeta_db_path: \"./meta.db\"\n",
			wantErr:     true,
			errContains: "grpc_addr is required",
		},
		{
			name:        "missing http_addr",
			yaml:        "grpc_addr: \":9000\"\nmeta_db_path: \"./meta.db\"\n",
			wantErr:     true,
			errContains: "http_addr is required",
		},
		{
			name:        "missing meta_db_path",
			yaml:        "grpc_addr: \":9000\"\nhttp_addr: \":8080\"\n",
			wantErr:     true,
			errContains: "meta_db_path is required",
		},
		{
			name:        "invalid yaml",
			yaml:        "grpc_addr: [invalid",
			wantErr:     true,
			errContains: "parse router config",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path := writeTemp(t, tc.yaml)
			cfg, err := config.LoadRouter(path)

			if tc.wantErr {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.errContains)
				return
			}

			require.NoError(t, err)
			tc.check(t, cfg)
		})
	}
}

func TestLoadRouter_FileNotFound(t *testing.T) {
	t.Parallel()

	_, err := config.LoadRouter("/nonexistent/path/router.yaml")
	require.Error(t, err)
	require.ErrorContains(t, err, "read router config")
}

func TestLoadMaster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check       func(t *testing.T, cfg config.MasterConfig)
		name        string
		yaml        string
		errContains string
		wantErr     bool
	}{
		{
			name: "valid full config",
			yaml: `
id: "master-1"
grpc_addr: ":9001"
http_addr: ":8081"
db_path: "./data/master-1.db"
router_addr: "localhost:9000"
heartbeat_interval: "10s"
tx_timeout: "20s"
snapshot_dir: "/tmp/snaps"
log_level: "warn"
`,
			check: func(t *testing.T, cfg config.MasterConfig) {
				t.Helper()
				require.Equal(t, "master-1", cfg.ID)
				require.Equal(t, ":9001", cfg.GRPCAddr)
				require.Equal(t, ":8081", cfg.HTTPAddr)
				require.Equal(t, "./data/master-1.db", cfg.DBPath)
				require.Equal(t, "localhost:9000", cfg.RouterAddr)
				require.Equal(t, 10*time.Second, cfg.HeartbeatInterval)
				require.Equal(t, 20*time.Second, cfg.TxTimeout)
				require.Equal(t, "/tmp/snaps", cfg.SnapshotDir)
				require.Equal(t, "warn", cfg.LogLevel)
			},
		},
		{
			name: "defaults applied when optional fields absent",
			yaml: `
id: "master-1"
grpc_addr: ":9001"
http_addr: ":8081"
db_path: "./master.db"
router_addr: "localhost:9000"
`,
			check: func(t *testing.T, cfg config.MasterConfig) {
				t.Helper()
				require.Equal(t, 5*time.Second, cfg.HeartbeatInterval)
				require.Equal(t, 30*time.Second, cfg.TxTimeout)
				require.NotEmpty(t, cfg.SnapshotDir)
				require.Equal(t, "info", cfg.LogLevel)
			},
		},
		{
			name:        "missing id",
			yaml:        "grpc_addr: \":9001\"\nhttp_addr: \":8081\"\ndb_path: \"./m.db\"\nrouter_addr: \"localhost:9000\"\n",
			wantErr:     true,
			errContains: "id is required",
		},
		{
			name:        "missing db_path",
			yaml:        "id: \"m1\"\ngrpc_addr: \":9001\"\nhttp_addr: \":8081\"\nrouter_addr: \"localhost:9000\"\n",
			wantErr:     true,
			errContains: "db_path is required",
		},
		{
			name:        "missing router_addr",
			yaml:        "id: \"m1\"\ngrpc_addr: \":9001\"\nhttp_addr: \":8081\"\ndb_path: \"./m.db\"\n",
			wantErr:     true,
			errContains: "router_addr is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path := writeTemp(t, tc.yaml)
			cfg, err := config.LoadMaster(path)

			if tc.wantErr {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.errContains)
				return
			}

			require.NoError(t, err)
			tc.check(t, cfg)
		})
	}
}

func TestLoadMaster_FileNotFound(t *testing.T) {
	t.Parallel()

	_, err := config.LoadMaster("/nonexistent/path/master.yaml")
	require.Error(t, err)
	require.ErrorContains(t, err, "read master config")
}

func writeTemp(t *testing.T, content string) string {
	t.Helper()

	f, err := os.CreateTemp(t.TempDir(), "*.yaml")
	require.NoError(t, err)

	_, err = f.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	return filepath.Clean(f.Name())
}
