package clickhouse

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"net"
	"net/http"
	"time"

	ch "github.com/rudderlabs/clickhouse-go/v2"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chpolicy"
)

// ParseConfigForTest exposes the parser with the test-only plain HTTP switch.
// The switch never appears in an exported production signature.
var ParseConfigForTest = parseConfig

// Bound exposes the error bounding for external tests.
var Bound = bound

// TestEnv replaces the production environment of NewDB in tests.
type TestEnv struct {
	Policy   chpolicy.Policy
	Roots    *x509.CertPool
	Resolver interface {
		LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
	}
	ReadTimeout time.Duration // zero means the production value
}

// NewDBForTest opens a DB under policy, trusting roots.
func NewDBForTest(cfg json.RawMessage, policy chpolicy.Policy, roots *x509.CertPool) (*DB, error) {
	return NewDBForTestWith(cfg, TestEnv{Policy: policy, Roots: roots})
}

// NewDBForTestWith opens a DB with the given environment. A nil Resolver means
// net.DefaultResolver.
func NewDBForTestWith(cfg json.RawMessage, o TestEnv) (*DB, error) {
	env := productionEnv()
	env.policy, env.rootCAs = o.Policy, o.Roots
	env.dynamicPolicy = false
	if o.Resolver != nil {
		env.resolver = o.Resolver
	}
	if o.ReadTimeout != 0 {
		env.readTimeout = o.ReadTimeout
	}
	return newDB(cfg, env)
}

// InspectOptions returns the options the pool was opened with.
func InspectOptions(db *DB) *ch.Options { return db.opts }

// InspectTransport returns a transport as the fork builds it for each
// connection, after the driver's TransportFunc has changed it.
func InspectTransport(db *DB) *http.Transport {
	rt := &http.Transport{Proxy: http.ProxyFromEnvironment}
	if _, err := db.opts.TransportFunc(rt); err != nil {
		panic(err)
	}
	return rt
}

// DriverScratchSettings exposes the driver write map, so test executors can
// attach it with their own query ids.
var DriverScratchSettings = driverScratchSettings

// CodeOf returns the registry code that the classifier gives err.
func CodeOf(err error) string { return classify(err).Code }
