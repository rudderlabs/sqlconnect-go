package clickhouse

import (
	"context"
	"database/sql/driver"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/chpolicy"
)

// fakeResolver returns one answer set per call; the last set repeats.
type fakeResolver struct {
	mu      sync.Mutex
	answers [][]string
	calls   int
	err     error
}

func (f *fakeResolver) LookupIPAddr(_ context.Context, _ string) ([]net.IPAddr, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if len(f.answers) == 0 {
		return nil, nil
	}
	set := f.answers[min(f.calls, len(f.answers))-1]
	out := make([]net.IPAddr, 0, len(set))
	for _, a := range set {
		out = append(out, net.IPAddr{IP: net.ParseIP(a)})
	}
	return out, nil
}

// blockingResolver blocks until its context ends.
type blockingResolver struct{}

func (blockingResolver) LookupIPAddr(ctx context.Context, _ string) ([]net.IPAddr, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// recordingDialer replaces g.dial with a net.Pipe stub and returns the dialled addresses.
func recordingDialer(g *guardedDialer) *[]string {
	var mu sync.Mutex
	dialed := &[]string{}
	g.dial = func(_ context.Context, network, addr string) (net.Conn, error) {
		mu.Lock()
		defer mu.Unlock()
		if network != "tcp" {
			return nil, errors.New("unexpected network " + network)
		}
		*dialed = append(*dialed, addr)
		c, peer := net.Pipe()
		_ = peer.Close()
		return c, nil
	}
	return dialed
}

func TestSQ26_Refusals(t *testing.T) {
	for name, answers := range map[string][]string{
		"unspecified": {"0.0.0.0"}, "loopback": {"127.0.0.1"}, "link-local": {"169.254.10.1"},
		"metadata": {"169.254.169.254"}, "metadata v6": {"fd00:ec2::254"}, "multicast": {"224.0.0.1"},
		"embedded metadata": {"64:ff9b::a9fe:a9fe"}, "mixed": {"8.8.8.8", "169.254.169.254"}, "empty": {},
		"mapped loopback": {"::ffff:127.0.0.1"}, "loopback v6": {"::1"},
	} {
		t.Run(name, func(t *testing.T) {
			g := newGuardedDialer("ch.example.com", chpolicy.Policy{}, &fakeResolver{answers: [][]string{answers}}, time.Second)
			dialed := recordingDialer(g)
			_, err := g.DialContext(context.Background(), "ch.example.com:8443")
			d, _ := clickhousequery.Describe(err)
			require.Equal(t, "CH_HOST_NOT_ALLOWED", d.Code)
			require.Equal(t, "host", d.Field)
			for _, a := range answers {
				require.NotContains(t, err.Error(), a, "the error names the field, never the address")
			}
			require.Empty(t, *dialed)
		})
	}
}

func TestSQ26_OperatorBlockList(t *testing.T) {
	p := chpolicy.Policy{Blocked: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}, AllowLoopback: true}
	for name, host := range map[string]string{"answer": "h", "literal": "10.1.2.3"} {
		t.Run(name, func(t *testing.T) {
			g := newGuardedDialer(host, p, &fakeResolver{answers: [][]string{{"10.1.2.3"}}}, time.Second)
			dialed := recordingDialer(g)
			_, err := g.DialContext(context.Background(), host+":8443")
			requireCode(t, err, "CH_HOST_NOT_ALLOWED")
			require.Empty(t, *dialed)
		})
	}
}

func TestSQ26_RotationRebindAndPinning(t *testing.T) {
	r := &fakeResolver{answers: [][]string{{"203.0.113.10"}, {"203.0.113.20"}, {"169.254.169.254"}}}
	g := newGuardedDialer("h", chpolicy.Policy{}, r, time.Second)
	dialed := recordingDialer(g)
	_, err := g.DialContext(context.Background(), "h:8443")
	require.NoError(t, err)
	_, err = g.DialContext(context.Background(), "h:8443") // rotation: next new connection dials the new answer
	require.NoError(t, err)
	require.Equal(t, []string{"203.0.113.10:8443", "203.0.113.20:8443"}, *dialed)
	_, err = g.DialContext(context.Background(), "h:8443") // rebind to a refused answer
	require.Error(t, err)
	require.Equal(t, 3, r.calls, "exactly one lookup per new connection, none between resolve and dial")
	require.Len(t, *dialed, 2)
}

func TestSQ26_DialsConfiguredHostNotAddrHost(t *testing.T) {
	r := &fakeResolver{answers: [][]string{{"203.0.113.10"}}}
	g := newGuardedDialer("h", chpolicy.Policy{}, r, time.Second)
	dialed := recordingDialer(g)
	_, err := g.DialContext(context.Background(), "127.0.0.1:9000")
	require.NoError(t, err)
	require.Equal(t, []string{"203.0.113.10:9000"}, *dialed, "only the port comes from the dial address")

	for _, addr := range []string{"h", "h:", "h:0", "h:65536", "h:https", "h:-1"} {
		_, err := g.DialContext(context.Background(), addr)
		requireCode(t, err, "CH_CONFIG_INVALID")
	}
	require.Len(t, *dialed, 1)
}

func TestSQ26_PrivateAnswersPreferIPv4AndLiterals(t *testing.T) {
	for answers, want := range map[[2]string]string{
		{"10.1.2.3", ""}: "10.1.2.3:8443", {"192.168.1.1", ""}: "192.168.1.1:8443",
		{"fd12:3456::1", ""}: "[fd12:3456::1]:8443", {"2001:db8::1", "203.0.113.5"}: "203.0.113.5:8443",
	} {
		set := []string{answers[0]}
		if answers[1] != "" {
			set = append(set, answers[1])
		}
		g := newGuardedDialer("h", chpolicy.Policy{}, &fakeResolver{answers: [][]string{set}}, time.Second)
		dialed := recordingDialer(g)
		_, err := g.DialContext(context.Background(), "h:8443")
		require.NoError(t, err, "RFC 1918 and ULA answers pass; the first IPv4 answer wins")
		require.Equal(t, want, (*dialed)[0])
	}
	r := &fakeResolver{answers: [][]string{{"203.0.113.5"}}}
	lit := newGuardedDialer("169.254.169.254", chpolicy.Policy{}, r, time.Second)
	dialed := recordingDialer(lit)
	_, err := lit.DialContext(context.Background(), "169.254.169.254:8443")
	require.Error(t, err)
	require.Zero(t, r.calls, "an IP literal skips resolution")
	require.Empty(t, *dialed, "a refused literal makes zero dial attempts")

	ok := newGuardedDialer("203.0.113.7", chpolicy.Policy{}, r, time.Second)
	dialed = recordingDialer(ok)
	_, err = ok.DialContext(context.Background(), "203.0.113.7:8443")
	require.NoError(t, err)
	require.Equal(t, []string{"203.0.113.7:8443"}, *dialed)

	v6 := newGuardedDialer("2606:4700::1", chpolicy.Policy{}, r, time.Second)
	dialed = recordingDialer(v6)
	_, err = v6.DialContext(context.Background(), "[2606:4700::1]:8443")
	requireCode(t, err, "CH_HOST_NOT_ALLOWED")
	require.Empty(t, *dialed, "an IPv6 literal host is refused")
	require.Zero(t, r.calls)
}

func TestSQ26_DNSFailureAndLoopbackOption(t *testing.T) {
	g := newGuardedDialer("h", chpolicy.Policy{}, &fakeResolver{err: &net.DNSError{Err: "no such host", Name: "h", IsNotFound: true}}, time.Second)
	_, err := g.DialContext(context.Background(), "h:8443")
	requireCode(t, err, "CH_DNS_FAILED")
	d, _ := clickhousequery.Describe(err)
	require.Equal(t, "host", d.Field)
	lo := newGuardedDialer("localhost", chpolicy.Policy{AllowLoopback: true}, &fakeResolver{answers: [][]string{{"127.0.0.1"}}}, time.Second)
	recordingDialer(lo)
	_, err = lo.DialContext(context.Background(), "localhost:8443")
	require.NoError(t, err)
}

func TestSQ26_RefusalIsNotOpError(t *testing.T) {
	g := newGuardedDialer("h", chpolicy.Policy{}, &fakeResolver{answers: [][]string{{"127.0.0.1"}}}, time.Second)
	tr := &http.Transport{DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) { return g.DialContext(ctx, addr) }}
	resp, err := (&http.Client{Transport: tr}).Post("https://h:8443/", "text/plain", strings.NewReader("SELECT 1"))
	if resp != nil {
		_ = resp.Body.Close()
	}
	require.Error(t, err)
	var op *net.OpError
	require.False(t, errors.As(err, &op), "database/sql must not see a replayable *net.OpError")
	require.False(t, errors.Is(err, driver.ErrBadConn))
	d, _ := clickhousequery.Describe(err)
	require.Equal(t, "CH_HOST_NOT_ALLOWED", d.Code)
}

func TestSQ26_TimeoutBoundsLookupAndDial(t *testing.T) {
	g := newGuardedDialer("h", chpolicy.Policy{}, &fakeResolver{answers: [][]string{{"203.0.113.5"}}}, 50*time.Millisecond)
	g.dial = func(ctx context.Context, _, _ string) (net.Conn, error) { <-ctx.Done(); return nil, ctx.Err() }
	slow := newGuardedDialer("slow.example.com", chpolicy.Policy{}, blockingResolver{}, 50*time.Millisecond)
	for _, d := range []*guardedDialer{g, slow} {
		start := time.Now()
		_, err := d.DialContext(context.Background(), d.host+":8443")
		require.Error(t, err)
		require.Less(t, time.Since(start), time.Second, "the 90 s bound (50 ms here) covers the lookup and the TCP dial")
	}
}
