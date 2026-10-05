package clickhouse

import (
	"context"
	"net"
	"strconv"
	"time"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chpolicy"
)

// resolver is the part of *net.Resolver the dialer uses. Production passes
// net.DefaultResolver.
type resolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

// guardedDialer resolves the configured host before every new connection,
// checks every answer against the dial policy and dials only the address it
// checked. It never makes a second lookup between the check and the dial.
type guardedDialer struct {
	host          string
	policy        chpolicy.Policy
	dynamicPolicy bool
	resolver      resolver
	dial          func(ctx context.Context, network, addr string) (net.Conn, error)
	// timeout bounds the lookup and the TCP dial together. The fork applies
	// DialTimeout only to its own dialer, not to a custom DialContext.
	timeout time.Duration
}

func newGuardedDialer(host string, policy chpolicy.Policy, r resolver, timeout time.Duration) *guardedDialer {
	return &guardedDialer{
		host: host, policy: policy, resolver: r, timeout: timeout,
		dial: (&net.Dialer{}).DialContext,
	}
}

// DialContext has the signature of clickhouse.Options.DialContext. Only the
// port comes from addr; the host is always the configured one, so a caller
// cannot steer the dial to an unchecked address.
//
// Every refusal is a *cherr.Error and never a *net.OpError, so database/sql
// never replays a refused dial as a bad connection.
func (g *guardedDialer) DialContext(ctx context.Context, addr string) (net.Conn, error) {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, cherr.New(cherr.CodeConfigInvalid, "port", "the connection address is not valid")
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 || strconv.Itoa(n) != port {
		return nil, cherr.New(cherr.CodeConfigInvalid, "port", "the connection address is not valid")
	}
	dctx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()
	ip, err := g.pick(dctx)
	if err != nil {
		return nil, err
	}
	return g.dial(dctx, "tcp", net.JoinHostPort(ip.String(), port))
}

// pick returns the address to dial: the first IPv4 answer, else the first
// answer. One refused answer refuses the whole host, so a mixed answer set
// cannot win a race toward an internal address.
func (g *guardedDialer) pick(ctx context.Context) (net.IP, error) {
	policy := g.policy
	if g.dynamicPolicy {
		policy, _ = chpolicy.Current()
	}
	refused := cherr.New(cherr.CodeHostNotAllowed, "host", "the host resolves to an address RudderStack does not connect to")
	if lit := net.ParseIP(g.host); lit != nil {
		// The host rule admits only dotted-decimal IPv4 literals.
		if lit.To4() == nil || policy.RefusedReason(lit) != "" {
			return nil, refused
		}
		return lit.To4(), nil
	}
	answers, err := g.resolver.LookupIPAddr(ctx, g.host)
	if err != nil {
		return nil, wrap(cherr.CodeDNSFailed, "host", "the host name could not be resolved", err)
	}
	if len(answers) == 0 {
		return nil, refused
	}
	var first, firstV4 net.IP
	for _, a := range answers {
		if policy.RefusedReason(a.IP) != "" {
			return nil, refused
		}
		if first == nil {
			first = a.IP
		}
		if firstV4 == nil && a.IP.To4() != nil {
			firstV4 = a.IP.To4()
		}
	}
	if firstV4 != nil {
		return firstV4, nil
	}
	return first, nil
}
