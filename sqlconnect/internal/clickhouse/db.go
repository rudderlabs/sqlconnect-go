package clickhouse

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"net"
	"strconv"
	"time"

	ch "github.com/rudderlabs/clickhouse-go/v2"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/base"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chpolicy"
)

// Pool limits, set on the *sql.DB.
const (
	maxOpenConns    = 10
	maxIdleConns    = 5
	connMaxLifetime = 30 * time.Minute
	// dialTimeout bounds the lookup and the TCP dial of one connection.
	dialTimeout = 90 * time.Second
)

// DB is the ClickHouse client.
type DB struct {
	*base.DB
	cfg  Config
	env  openEnv
	opts *ch.Options
	// validateSem bounds concurrent ValidateContext calls.
	validateSem chan struct{}
}

// openEnv is what NewDB takes from the process. Tests replace it.
type openEnv struct {
	policy    chpolicy.Policy
	policySet bool
	resolver  resolver
	// rootCAs is test-only; production passes nil and uses the system roots.
	rootCAs                  *x509.CertPool
	dialTimeout, readTimeout time.Duration
}

func productionEnv() openEnv {
	p, ok := chpolicy.Current()
	return openEnv{
		policy: p, policySet: ok, resolver: net.DefaultResolver,
		dialTimeout: dialTimeout, readTimeout: clickhousequery.MaxRunBudget + 60*time.Second,
	}
}

// NewDB parses the account config and opens a lazy pool. It sends no request.
// It fails until clickhousequery.SetDialPolicy has run.
func NewDB(configJSON json.RawMessage) (*DB, error) { return newDB(configJSON, productionEnv()) }

func newDB(configJSON json.RawMessage, env openEnv) (*DB, error) {
	if !env.policySet {
		return nil, cherr.New(cherr.CodeConfigInvalid, "", "the ClickHouse dial policy is not installed; call clickhousequery.SetDialPolicy at start")
	}
	cfg, err := parseConfig(configJSON, env.policy.AllowPlainHTTP)
	if err != nil {
		return nil, err
	}
	var tlsConfig *tls.Config
	if *cfg.Secure {
		// ServerName stays empty: net/http takes it from Options.Addr, which
		// keeps the configured hostname while the dialer dials the checked IP.
		tlsConfig = &tls.Config{RootCAs: env.rootCAs, MinVersion: tls.VersionTLS12}
	}
	dialer := newGuardedDialer(cfg.Host, env.policy, env.resolver, env.dialTimeout)
	opts := &ch.Options{
		Protocol:         ch.HTTP,
		Addr:             []string{net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.PortOrDefault()))},
		Auth:             ch.Auth{Database: cfg.Database, Username: cfg.User, Password: cfg.Password},
		TLS:              tlsConfig,
		DialContext:      dialer.DialContext,
		DialTimeout:      env.dialTimeout,
		ReadTimeout:      env.readTimeout,
		MaxOpenConns:     maxOpenConns,
		MaxIdleConns:     maxIdleConns,
		ConnMaxLifetime:  connMaxLifetime,
		ConnOpenStrategy: ch.ConnOpenInOrder,
		Compression:      &ch.Compression{Method: ch.CompressionLZ4},
		TransportFunc:    newTransportFunc(),
	}
	sqldb := sql.OpenDB(&connectGuard{next: ch.Connector(opts)})
	sqldb.SetMaxOpenConns(maxOpenConns)
	sqldb.SetMaxIdleConns(maxIdleConns)
	sqldb.SetConnMaxLifetime(connMaxLifetime)
	d := &DB{cfg: cfg, env: env, opts: opts, validateSem: make(chan struct{}, maxConcurrentValidations)}
	d.DB = base.NewDB(sqldb, func() error { return nil },
		base.WithDialect(newDialect()),
		base.WithColumnTypeMapper(func(c base.ColumnType) string { return canonicalType(c.DatabaseTypeName()) }),
	)
	return d, nil
}

// connectGuard opens every pool connection. The fork sends a hello query when
// it opens a connection; the guard gives that query the control map and a
// fresh query id, never the caller's statement map, and bounds any error.
// The hello reads only constants. Validation stage 2 checks the driver
// settings, so a constraint on one of them names that setting there and does
// not fail the open.
type connectGuard struct{ next driver.Connector }

func (g *connectGuard) Connect(parent context.Context) (driver.Conn, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if dl, ok := parent.Deadline(); ok {
		var cancelDeadline context.CancelFunc
		ctx, cancelDeadline = context.WithDeadline(ctx, dl)
		defer cancelDeadline()
	}
	// The caller's cancellation still ends the dial and the hello.
	stop := context.AfterFunc(parent, cancel)
	defer stop()
	ctx = ch.Context(ctx, ch.WithSettings(ch.Settings(controlSettings())), ch.WithQueryID(clickhousequery.NewQueryID()))
	conn, err := g.next.Connect(ctx)
	if err != nil {
		return nil, bound("connect", "", err)
	}
	return &guardConn{inner: conn}, nil
}

func (g *connectGuard) Driver() driver.Driver { return g.next.Driver() }

// ClassifyError implements sqlconnect.ErrorClassifier.
func (db *DB) ClassifyError(err error) sqlconnect.ErrorInfo { return classify(err) }

// Begin is not supported: ClickHouse has no transactions on this path.
func (db *DB) Begin() (*sql.Tx, error) { return nil, txUnsupported() }

// BeginTx is not supported: ClickHouse has no transactions on this path.
func (db *DB) BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error) {
	return nil, txUnsupported()
}

// Prepare is not supported: the driver sends every statement once.
func (db *DB) Prepare(string) (*sql.Stmt, error) { return nil, prepUnsupported() }

// PrepareContext is not supported: the driver sends every statement once.
func (db *DB) PrepareContext(context.Context, string) (*sql.Stmt, error) {
	return nil, prepUnsupported()
}

func txUnsupported() error {
	return wrap(cherr.CodeTransactionsUnsupported, "", fixedMessages[cherr.CodeTransactionsUnsupported], sqlconnect.ErrNotSupported)
}

func prepUnsupported() error {
	return wrap(cherr.CodePrepareUnsupported, "", fixedMessages[cherr.CodePrepareUnsupported], sqlconnect.ErrNotSupported)
}

func init() {
	sqlconnect.RegisterDBFactory(DatabaseType, func(credentialsJSON json.RawMessage) (sqlconnect.DB, error) {
		return NewDB(credentialsJSON)
	})
}
