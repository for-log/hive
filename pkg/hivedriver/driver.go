package hivedriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"hive/gen/hivepb"

	_ "modernc.org/sqlite" // register "sqlite" driver
)

type Connector struct {
	remote     hivepb.HiveSQLClient
	localDB    *sql.DB
	syncer     *Syncer
	log        *slog.Logger
	syncTicker *time.Ticker
	routerHTTP string
}

// DSN format: grpc-host:port?local_db=./local.db[&http_addr=host:port][&sync_interval=60s][&log_level=info]
//
// http_addr defaults to the same host as grpc with port 8080.
func NewConnector(dsn string) (*Connector, error) {
	return newConnector(dsn)
}

func NewConnectorWithDialer(dsn string, opts ...grpc.DialOption) (*Connector, error) {
	return newConnector(dsn, opts...)
}

func newConnector(dsn string, extraOpts ...grpc.DialOption) (*Connector, error) {
	host, params, err := parseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("hivedriver: parse dsn: %w", err)
	}

	localDBPath := params.Get("local_db")
	if localDBPath == "" {
		return nil, fmt.Errorf("hivedriver: local_db param is required in DSN")
	}

	logLevel := params.Get("log_level")
	log := newDriverLogger(logLevel)

	localDSN := localDBPath + "?_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)"
	localDB, err := sql.Open("sqlite", localDSN)
	if err != nil {
		return nil, fmt.Errorf("hivedriver: open local db: %w", err)
	}

	var setupErr error
	defer func() {
		if setupErr != nil {
			if closeErr := localDB.Close(); closeErr != nil {
				setupErr = fmt.Errorf("%w (also close local db: %v)", setupErr, closeErr)
			}
		}
	}()

	dialOpts := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, extraOpts...)
	conn, err := grpc.NewClient(host, dialOpts...)
	if err != nil {
		setupErr = fmt.Errorf("hivedriver: dial router: %w", err)
		return nil, setupErr
	}

	remote := hivepb.NewHiveSQLClient(conn)

	httpAddr := params.Get("http_addr")
	if httpAddr == "" {
		httpAddr = httpHostFromGRPC(host)
	}
	routerHTTP := "http://" + httpAddr

	c := &Connector{
		remote:     remote,
		localDB:    localDB,
		log:        log,
		routerHTTP: routerHTTP,
		syncer:     newSyncer(localDB, routerHTTP, log),
	}

	if intervalStr := params.Get("sync_interval"); intervalStr != "" {
		d, parseErr := time.ParseDuration(intervalStr)
		if parseErr != nil {
			setupErr = fmt.Errorf("hivedriver: parse sync_interval: %w", parseErr)
			return nil, setupErr
		}
		if d > 0 {
			c.syncTicker = time.NewTicker(d)
			go c.autoSync()
		}
	}

	return c, nil
}

func (c *Connector) Connect(_ context.Context) (driver.Conn, error) {
	return newConn(c.remote, c.localDB, c.log), nil
}

func (c *Connector) Driver() driver.Driver { return nil }

func (c *Connector) Sync(ctx context.Context) error {
	return c.syncer.Sync(ctx)
}

func (c *Connector) Close() error {
	if c.syncTicker != nil {
		c.syncTicker.Stop()
	}
	return c.localDB.Close()
}

func (c *Connector) autoSync() {
	for range c.syncTicker.C {
		ctx, cancel := context.WithTimeout(context.Background(), syncHTTPTimeout)
		if err := c.syncer.Sync(ctx); err != nil {
			c.log.Error("hivedriver: auto-sync", "err", err)
		}
		cancel()
	}
}

func httpHostFromGRPC(grpcAddr string) string {
	host, _, found := strings.Cut(grpcAddr, ":")
	if !found {
		return grpcAddr + ":8080"
	}
	return host + ":8080"
}

func parseDSN(dsn string) (host string, params url.Values, err error) {
	// Support both "host:port?key=val" and "host:port" (no query).
	idx := strings.IndexByte(dsn, '?')
	if idx < 0 {
		return dsn, url.Values{}, nil
	}
	host = dsn[:idx]
	params, err = url.ParseQuery(dsn[idx+1:])
	if err != nil {
		return "", nil, fmt.Errorf("parse query: %w", err)
	}
	return host, params, nil
}

func newDriverLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelWarn
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}
