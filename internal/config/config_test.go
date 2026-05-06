package config_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hive_v2/orchestrator/internal/config"
)

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "config-*.yaml")
	require.NoError(t, err)
	_, err = f.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

func TestLoad_Defaults(t *testing.T) {
	t.Parallel()
	path := writeTemp(t, `
masters:
  - url: "http://localhost:8081"
    token: "tok"
`)
	cfg, err := config.Load(path)
	require.NoError(t, err)

	assert.Equal(t, ":8080", cfg.ListenAddr)
	assert.Equal(t, config.ReadPolicyWriteMaster, cfg.ReadPolicy)
	assert.Equal(t, 10*time.Second, cfg.StreamTTL)
	assert.Equal(t, int64(4*1024*1024), cfg.MaxBodyBytes)
	assert.Equal(t, 4, cfg.Replication.Workers)
	assert.Equal(t, 3, cfg.Replication.RetryMax)
	assert.Equal(t, time.Second, cfg.Replication.RetryBackoff)
	assert.Equal(t, 1024, cfg.Replication.QueueCapacity)
	assert.Equal(t, 30*time.Second, cfg.Transaction.CommitTimeout)
}

func TestLoad_ExplicitValues(t *testing.T) {
	t.Parallel()
	path := writeTemp(t, `
listen_addr: ":9090"
masters:
  - url: "http://m1:8081"
    token: "t1"
  - url: "http://m2:8082"
    token: "t2"
table_assignments:
  users: 0
  orders: 1
read_policy: "round_robin"
stream_ttl: "30s"
replication:
  workers: 8
  retry_max: 5
  retry_backoff: "2s"
  queue_capacity: 512
transaction:
  cross_master_enabled: true
  commit_timeout: "45s"
`)
	cfg, err := config.Load(path)
	require.NoError(t, err)

	assert.Equal(t, ":9090", cfg.ListenAddr)
	assert.Len(t, cfg.Masters, 2)
	assert.Equal(t, "http://m1:8081", cfg.Masters[0].URL)
	assert.Equal(t, config.ReadPolicyRoundRobin, cfg.ReadPolicy)
	assert.Equal(t, 30*time.Second, cfg.StreamTTL)
	assert.Equal(t, 0, cfg.TableAssignments["users"])
	assert.Equal(t, 1, cfg.TableAssignments["orders"])
	assert.Equal(t, 8, cfg.Replication.Workers)
	assert.Equal(t, 512, cfg.Replication.QueueCapacity)
	assert.True(t, cfg.Transaction.CrossMasterEnabled)
	assert.Equal(t, 45*time.Second, cfg.Transaction.CommitTimeout)
}

func TestLoad_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "no masters",
			yaml:    `listen_addr: ":8080"`,
			wantErr: "at least one master",
		},
		{
			name: "master without url",
			yaml: `
masters:
  - token: "tok"
`,
			wantErr: "master[0].url is required",
		},
		{
			name: "table assignment out of range",
			yaml: `
masters:
  - url: "http://localhost:8081"
    token: "tok"
table_assignments:
  users: 5
`,
			wantErr: "master index 5",
		},
		{
			name: "unknown read policy",
			yaml: `
masters:
  - url: "http://localhost:8081"
    token: "tok"
read_policy: "magic"
`,
			wantErr: "unknown read_policy",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := writeTemp(t, tc.yaml)
			_, err := config.Load(path)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestLoad_FileNotFound(t *testing.T) {
	t.Parallel()
	_, err := config.Load("/nonexistent/path/config.yaml")
	require.Error(t, err)
}
