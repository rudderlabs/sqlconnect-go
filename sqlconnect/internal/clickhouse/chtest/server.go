// Package chtest starts digest-pinned ClickHouse containers through the
// rudder-go-kit ClickHouse resource and puts a fault-injecting proxy in front
// of them. It is for tests only.
package chtest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	chresource "github.com/rudderlabs/rudder-go-kit/testhelper/docker/resource/clickhouse"
)

const (
	repository = "clickhouse/clickhouse-server"
	// memoryLimit caps every fixture container (host load limits).
	memoryLimit = 1 << 30
	adminUser   = "admin"
)

//go:embed images.json
var imagesJSON []byte

var imageDigests = func() map[string]string {
	m := map[string]string{}
	if err := json.Unmarshal(imagesJSON, &m); err != nil {
		panic(fmt.Sprintf("chtest: images.json: %v", err))
	}
	return m
}()

// Image returns the digest-pinned reference for a tag listed in images.json.
// It panics for any other tag, so a test can never run an unpinned image.
func Image(tag string) string {
	return repository + "@" + digestOf(tag)
}

func digestOf(tag string) string {
	d, ok := imageDigests[tag]
	if !ok {
		panic(fmt.Sprintf("chtest: tag %q has no pinned digest in images.json", tag))
	}
	return d
}

// Options configure Start.
type Options struct {
	Tag       string // a key of images.json, e.g. "26.3"
	Timezone  string // the server TZ, UTC when empty
	ConfigXML string // an extra config.d document, optional
}

// Server is a running ClickHouse container.
type Server struct {
	Host      string // "localhost": a name that both certificates carry
	HTTPSPort int    // the server's own HTTPS port, with the go-kit certificate
	// HTTPPort is a plain HTTP port on 127.0.0.1. The go-kit resource
	// publishes only TLS ports in TLS mode, so a local bridge relays this port
	// to HTTPSPort.
	HTTPPort int
	// CA trusts the server's own certificate and the front certificate that
	// proxies and balancers present.
	CA            *x509.CertPool
	AdminUser     string
	AdminPassword string

	// frontCert carries the SANs localhost and rebind.test and no IP SAN, so
	// 127.0.0.1 through a proxy gives a hostname mismatch. go-kit does not
	// expose its server key, so proxies and balancers present this one.
	frontCert   tls.Certificate
	pool        *dockertest.Pool
	containerID string
	httpClient  *http.Client
}

// Start runs a container of the pinned image for o.Tag, waits until it answers
// and removes it when the test ends.
func Start(t *testing.T, o Options) *Server {
	t.Helper()
	digest := digestOf(o.Tag)
	tz := o.Timezone
	if tz == "" {
		tz = "UTC"
	}

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 5 * time.Minute

	password := randomString(t, 24)
	opts := []chresource.Opt{
		// go-kit pulls by digest and refuses a container whose image has another digest.
		chresource.WithImage(repository + ":" + o.Tag + "@" + digest),
		chresource.WithTLS(),
		chresource.WithUser(adminUser),
		chresource.WithPassword(password),
		chresource.WithDatabase("default"),
		chresource.WithEnv("TZ=" + tz),
		chresource.WithMemory(memoryLimit),
		chresource.WithPrintLogsOnError(true),
	}
	if o.ConfigXML != "" {
		opts = append(opts, chresource.WithConfig(o.ConfigXML))
	}
	res, err := chresource.Setup(pool, t, opts...)
	require.NoError(t, err)

	httpsPort, err := strconv.Atoi(res.HTTPPort)
	require.NoError(t, err)
	frontCA, frontCert := newFrontCert(t)
	caPool := x509.NewCertPool()
	require.True(t, caPool.AppendCertsFromPEM(res.CAPEM), "the go-kit CA parses")
	caPool.AddCert(frontCA)

	bridgeTLS := res.TLSConfig.Clone()
	bridgeTLS.ServerName = "localhost"
	s := &Server{
		Host:          "localhost",
		HTTPSPort:     httpsPort,
		HTTPPort:      startPlainBridge(t, net.JoinHostPort("127.0.0.1", res.HTTPPort), bridgeTLS),
		CA:            caPool,
		AdminUser:     adminUser,
		AdminPassword: password,
		frontCert:     frontCert,
		pool:          pool,
		containerID:   res.ContainerID,
		httpClient:    newLoopbackClient(),
	}
	t.Cleanup(s.httpClient.CloseIdleConnections)

	version := s.AdminQuery(t, "SELECT version()")[0][0]
	require.True(t, strings.HasPrefix(version, o.Tag+"."), "server version %s for tag %s", version, o.Tag)
	t.Logf("clickhouse image=%s version=%s", s.ImageDigest(t), version)
	return s
}

// startPlainBridge listens on 127.0.0.1 and relays each plain connection to
// upstream over TLS. It returns the port.
func startPlainBridge(t *testing.T, upstream string, cfg *tls.Config) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Go(func() {
				relay(c, func() (net.Conn, error) {
					d := tls.Dialer{Config: cfg}
					return d.Dial("tcp", upstream)
				})
			})
		}
	})
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})
	return ln.Addr().(*net.TCPAddr).Port
}

// ImageDigest returns the digest of the running container's image, read from
// Docker and not from images.json.
func (s *Server) ImageDigest(t *testing.T) string {
	t.Helper()
	c, err := s.pool.Client.InspectContainer(s.containerID)
	require.NoError(t, err)
	img, err := s.pool.Client.InspectImage(c.Image)
	require.NoError(t, err)
	for _, rd := range img.RepoDigests {
		if name, digest, ok := strings.Cut(rd, "@"); ok && name == repository {
			return digest
		}
	}
	require.FailNow(t, "the running image has no repository digest", "%v", img.RepoDigests)
	return ""
}

// Config returns the eight account fields for the driver. HTTPS uses the host
// "localhost" so the fixture certificate matches.
func (s *Server) Config(user, password, database string, secure bool) json.RawMessage {
	port := s.HTTPPort
	if secure {
		port = s.HTTPSPort
	}
	b, err := json.Marshal(struct {
		Host       string `json:"host"`
		Port       int    `json:"port"`
		Database   string `json:"database"`
		User       string `json:"user"`
		Password   string `json:"password"`
		Secure     bool   `json:"secure"`
		SkipVerify bool   `json:"skipVerify"`
	}{s.Host, port, database, user, password, secure, false})
	if err != nil {
		panic(err)
	}
	return b
}

// AdminExec runs one statement as the admin over plain HTTP on loopback.
func (s *Server) AdminExec(t *testing.T, sql string) {
	t.Helper()
	_ = s.adminDo(t, sql)
}

// AdminQuery runs one query as the admin and returns the TSV rows.
func (s *Server) AdminQuery(t *testing.T, sql string) [][]string {
	t.Helper()
	body := s.adminDo(t, sql)
	var rows [][]string
	for line := range strings.SplitSeq(strings.TrimSuffix(body, "\n"), "\n") {
		if line == "" && body == "" {
			break
		}
		fields := strings.Split(line, "\t")
		for i := range fields {
			fields[i] = unescapeTSV(fields[i])
		}
		rows = append(rows, fields)
	}
	return rows
}

func (s *Server) adminDo(t *testing.T, sql string) string {
	t.Helper()
	return s.post(t, s.httpClient, s.plainURL("/", url.Values{"default_format": {"TabSeparated"}}), sql)
}

// post sends sql as the admin to u and returns the body of a 200 answer.
func (s *Server) post(t *testing.T, client *http.Client, u, sql string) string {
	t.Helper()
	return s.postAs(t, client, u, s.AdminUser, s.AdminPassword, sql)
}

// postAs sends sql as user to u and returns the body of a 200 answer.
func (s *Server) postAs(t *testing.T, client *http.Client, u, user, password, sql string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(sql))
	require.NoError(t, err)
	req.SetBasicAuth(user, password)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	if resp.StatusCode != http.StatusOK {
		require.FailNow(t, "admin statement failed", adminFailure(resp.StatusCode, string(b)))
	}
	return string(b)
}

// FlushLogs writes the buffered system log tables, such as system.query_log.
func (s *Server) FlushLogs(t *testing.T) {
	t.Helper()
	s.AdminExec(t, "SYSTEM FLUSH LOGS")
}

// customerGrants is the grant set customers run, in the order of the Setup
// SQL panel. It mirrors GRANTS in rudder-lookout src/lib/clickhouse-grants.ts
// (rudder-lookout c9d9d407a), the one source of truth. Change both together.
var customerGrants = []struct {
	privileges string
	scope      string // customer, working, working.sync_log, or a system table
}{
	{"SELECT", "customer"},
	{"SELECT, INSERT, CREATE TABLE, DROP TABLE", "working"},
	{"ALTER DELETE", "working.sync_log"},
	{"SELECT", "system.processes"},
	{"SELECT", "system.query_log"},
}

// scopedUserStatements renders CreateScopedUser's statements: the user, both
// databases, then customerGrants. syncLog also creates working.sync_log.
func scopedUserStatements(name, password, customerDB, rudderDB string, syncLog bool) []string {
	user, customer, working := quoteIdent(name), quoteIdent(customerDB), quoteIdent(rudderDB)
	stmts := []string{
		fmt.Sprintf("CREATE USER %s IDENTIFIED WITH sha256_password BY %s", user, quoteString(password)),
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", working),
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", customer),
	}
	for _, g := range customerGrants {
		scope := g.scope
		switch g.scope {
		case "customer":
			scope = customer + ".*"
		case "working":
			scope = working + ".*"
		case "working.sync_log":
			scope = working + ".`sync_log`"
		}
		stmts = append(stmts, fmt.Sprintf("GRANT %s ON %s TO %s", g.privileges, scope, user))
	}
	if syncLog {
		stmts = append(stmts, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.`sync_log` (id UInt8) ENGINE = MergeTree ORDER BY id", working))
	}
	return stmts
}

// CreateScopedUser provisions a runtime user with the customer grant set,
// customerGrants. syncLog also creates the working database's sync_log table.
func (s *Server) CreateScopedUser(t *testing.T, name, password, customerDB, rudderDB string, syncLog bool) {
	t.Helper()
	for _, stmt := range scopedUserStatements(name, password, customerDB, rudderDB, syncLog) {
		s.AdminExec(t, stmt)
	}
}

// User is a runtime user that a test created.
type User struct {
	Name, Password string
}

// CreateUserWithProfile creates a user whose settings profile sets each
// setting to its value, quoted as a string literal.
func (s *Server) CreateUserWithProfile(t *testing.T, settings map[string]string) User {
	t.Helper()
	u := User{Name: "u_" + strings.ToLower(randomString(t, 10)), Password: "pw_" + randomString(t, 16)}
	profile := quoteIdent("p_" + u.Name)
	var parts []string
	for k, v := range settings {
		parts = append(parts, quoteIdent(k)+" = "+quoteString(v))
	}
	stmt := "CREATE SETTINGS PROFILE " + profile
	if len(parts) > 0 {
		slices.Sort(parts)
		stmt += " SETTINGS " + strings.Join(parts, ", ")
	}
	s.AdminExec(t, stmt)
	s.AdminExec(t, fmt.Sprintf("CREATE USER %s IDENTIFIED WITH sha256_password BY %s SETTINGS PROFILE %s",
		quoteIdent(u.Name), quoteString(u.Password), profile))
	return u
}

// QueryAs runs one statement as u over plain HTTP on loopback and returns the
// TSV body.
func (s *Server) QueryAs(t *testing.T, u User, sql string) string {
	t.Helper()
	return s.postAs(t, s.httpClient, s.plainURL("/", url.Values{"default_format": {"TabSeparated"}}), u.Name, u.Password, sql)
}

// Show returns SHOW CREATE TABLE for table, run as the admin. table is
// "db.name"; both parts are quoted.
func (s *Server) Show(t *testing.T, table string) string {
	t.Helper()
	db, name, ok := strings.Cut(table, ".")
	require.True(t, ok, "Show wants db.name")
	rows := s.AdminQuery(t, "SHOW CREATE TABLE "+quoteIdent(db)+"."+quoteIdent(name))
	require.Len(t, rows, 1, "SHOW CREATE TABLE answer shape")
	return rows[0][0]
}

func (s *Server) plainURL(path string, q url.Values) string {
	u := url.URL{Scheme: "http", Host: net.JoinHostPort("127.0.0.1", strconv.Itoa(s.HTTPPort)), Path: path, RawQuery: q.Encode()}
	return u.String()
}

// newFrontCert creates a test CA and a front certificate with the SANs
// DNS:localhost and DNS:rebind.test only.
func newFrontCert(t *testing.T) (*x509.Certificate, tls.Certificate) {
	t.Helper()
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTmpl := &x509.Certificate{
		SerialNumber:          serial(t),
		Subject:               pkix.Name{CommonName: "chtest front CA"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: serial(t),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost", "rebind.test"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	require.NoError(t, err)
	return caCert, tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func serial(t *testing.T) *big.Int {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	require.NoError(t, err)
	return n
}

func randomString(t *testing.T, n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		k, err := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
		require.NoError(t, err)
		b[i] = alphabet[k.Int64()]
	}
	return string(b)
}

var errorCodeRE = regexp.MustCompile(`Code: (\d+)\.`)

// adminFailure describes a failed admin statement by status and numeric server
// error code only. The exception text can echo SQL literals, such as
// a password, so it is never shown.
func adminFailure(status int, body string) string {
	msg := fmt.Sprintf("status %d", status)
	if m := errorCodeRE.FindStringSubmatch(body); m != nil {
		msg += ", code " + m[1]
	}
	return msg
}

// newLoopbackClient never follows redirects, so a request and its
// credentials reach only the address it names.
func newLoopbackClient() *http.Client {
	return &http.Client{
		Transport:     &http.Transport{Proxy: nil, DisableCompression: true},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

func quoteIdent(s string) string {
	return "`" + strings.NewReplacer(`\`, `\\`, "`", "\\`").Replace(s) + "`"
}

func quoteString(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

func unescapeTSV(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	return strings.NewReplacer(`\t`, "\t", `\n`, "\n", `\r`, "\r", `\0`, "\x00", `\'`, "'", `\\`, `\`).Replace(s)
}

// QueryLogRow is one finished statement in system.query_log.
type QueryLogRow struct {
	QueryID  string
	Query    string
	Kind     string            // query_kind, such as Select or Create
	Settings map[string]string // only the settings the statement changed
	IsHello  bool              // the query the driver sends when it opens a connection
}

// driverRows keeps finished statements that the Go driver sent. It leaves out
// the admin statements this package sends, which carry the Go HTTP user agent.
const driverRows = "type != 'QueryStart' AND http_user_agent LIKE '%clickhouse-go/%'"

const helloPrefix = "SELECT displayName(), version(), revision(), timezone()"

func (s *Server) queryLog(t *testing.T, where string) []QueryLogRow {
	t.Helper()
	rows := s.AdminQuery(t, "SELECT query_id, query_kind, toJSONString(Settings), base64Encode(query) FROM system.query_log WHERE "+
		driverRows+" AND ("+where+") ORDER BY event_time_microseconds")
	out := make([]QueryLogRow, 0, len(rows))
	for _, r := range rows {
		require.Len(t, r, 4, "query_log row shape")
		settings := map[string]string{}
		require.NoError(t, json.Unmarshal([]byte(r[2]), &settings))
		q, err := base64.StdEncoding.DecodeString(r[3])
		require.NoError(t, err)
		query := string(q)
		out = append(out, QueryLogRow{
			QueryID: r[0], Kind: r[1], Settings: settings, Query: query,
			IsHello: strings.HasPrefix(strings.TrimSpace(query), helloPrefix),
		})
	}
	return out
}

// QueryLogSettings returns the settings of the finished statement with the
// query id. Call FlushLogs first.
func (s *Server) QueryLogSettings(t *testing.T, queryID string) map[string]string {
	t.Helper()
	rows := s.queryLog(t, "query_id = "+quoteString(queryID))
	require.NotEmpty(t, rows, "no query_log row for %s", queryID)
	return rows[0].Settings
}

// QueryLogCount returns the number of finished statements with the query id.
func (s *Server) QueryLogCount(t *testing.T, queryID string) int {
	t.Helper()
	return len(s.queryLog(t, "query_id = "+quoteString(queryID)))
}

// QueryLogKind returns the query_kind of the finished statement with the query id.
func (s *Server) QueryLogKind(t *testing.T, queryID string) string {
	t.Helper()
	rows := s.queryLog(t, "query_id = "+quoteString(queryID))
	require.NotEmpty(t, rows, "no query_log row for %s", queryID)
	return rows[0].Kind
}

// QueryLogSince returns every finished driver statement that started at or
// after since.
func (s *Server) QueryLogSince(t *testing.T, since time.Time) []QueryLogRow {
	t.Helper()
	return s.queryLog(t, "event_time_microseconds >= fromUnixTimestamp64Micro("+strconv.FormatInt(since.UnixMicro(), 10)+")")
}

// QueryLogLike returns every finished driver statement whose text matches the
// LIKE pattern.
func (s *Server) QueryLogLike(t *testing.T, pattern string) []QueryLogRow {
	t.Helper()
	return s.queryLog(t, "query LIKE "+quoteString(pattern))
}

// QueryLogCountLike counts the finished driver statements of one query_kind
// whose text matches the LIKE pattern.
func (s *Server) QueryLogCountLike(t *testing.T, pattern, kind string) int {
	t.Helper()
	return len(s.queryLog(t, "query LIKE "+quoteString(pattern)+" AND toString(query_kind) = "+quoteString(kind)))
}

// RawHTTPS runs one statement as the admin over the HTTPS port with the
// fixture CA and returns the raw response body. It sets no driver settings, so
// a test can see what the server answers without the driver in between.
func (s *Server) RawHTTPS(t *testing.T, sql string, q url.Values) string {
	t.Helper()
	tr := &http.Transport{
		Proxy:              nil,
		DisableCompression: true,
		TLSClientConfig:    &tls.Config{RootCAs: s.CA, ServerName: s.Host, MinVersion: tls.VersionTLS12},
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, net.JoinHostPort("127.0.0.1", strconv.Itoa(s.HTTPSPort)))
		},
	}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	u := url.URL{Scheme: "https", Host: net.JoinHostPort(s.Host, strconv.Itoa(s.HTTPSPort)), Path: "/", RawQuery: q.Encode()}
	return s.post(t, client, u.String(), sql)
}

// LatestHello returns the most recent connection-open query.
func (s *Server) LatestHello(t *testing.T) QueryLogRow {
	t.Helper()
	rows := s.queryLog(t, "startsWith(trimLeft(query), "+quoteString(helloPrefix)+")")
	require.NotEmpty(t, rows, "no connection-open query in query_log")
	return rows[len(rows)-1]
}

// Running is a statement that StartAs sent in the background.
type Running struct{ done chan string }

// Wait blocks until the statement ends and fails the test unless it
// succeeded.
func (r *Running) Wait(t *testing.T) {
	t.Helper()
	if failure := <-r.done; failure != "" {
		require.FailNow(t, "background statement failed", failure)
	}
}

// StartAs sends sql as user under queryID in the background and returns once
// system.processes shows the statement.
func (s *Server) StartAs(t *testing.T, user, password, queryID, sql string) *Running {
	t.Helper()
	r := &Running{done: make(chan string, 1)}
	u := s.plainURL("/", url.Values{"query_id": {queryID}, "default_format": {"TabSeparated"}})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(sql))
		if err != nil {
			r.done <- "build request"
			return
		}
		req.SetBasicAuth(user, password)
		resp, err := s.httpClient.Do(req)
		if err != nil {
			r.done <- "transport error"
			return
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			r.done <- adminFailure(resp.StatusCode, string(b))
			return
		}
		r.done <- ""
	}()
	require.Eventually(t, func() bool { return s.ProcessCount(t, queryID) == 1 }, time.Minute, 50*time.Millisecond,
		"the background statement never showed in system.processes")
	return r
}

// ProcessCount returns the number of system.processes rows with the query
// id, for every user.
func (s *Server) ProcessCount(t *testing.T, queryID string) int {
	t.Helper()
	rows := s.AdminQuery(t, "SELECT count() FROM system.processes WHERE query_id = "+quoteString(queryID))
	require.Len(t, rows, 1)
	n, err := strconv.Atoi(rows[0][0])
	require.NoError(t, err)
	return n
}

// KillStatements flushes the logs and returns the text of every KILL
// statement the Go driver sent.
func (s *Server) KillStatements(t *testing.T) []string {
	t.Helper()
	s.FlushLogs(t)
	var out []string
	for _, r := range s.queryLog(t, "startsWith(upper(trimLeft(query)), 'KILL')") {
		out = append(out, r.Query)
	}
	return out
}

// CreateUserWithConstraint creates a user whose settings profile pins setting
// to value with a CONST constraint, so a statement that sends another value
// fails with server code 452.
func (s *Server) CreateUserWithConstraint(t *testing.T, setting, value string) User {
	t.Helper()
	u := User{Name: "u_" + strings.ToLower(randomString(t, 10)), Password: "pw_" + randomString(t, 16)}
	profile := quoteIdent("p_" + u.Name)
	s.AdminExec(t, "CREATE SETTINGS PROFILE "+profile+" SETTINGS "+quoteIdent(setting)+" = "+quoteString(value)+" CONST")
	s.AdminExec(t, fmt.Sprintf("CREATE USER %s IDENTIFIED WITH sha256_password BY %s SETTINGS PROFILE %s",
		quoteIdent(u.Name), quoteString(u.Password), profile))
	return u
}

// ProbeTables returns the names of the validation probe tables in _rudderstack.
func (s *Server) ProbeTables(t *testing.T) []string {
	t.Helper()
	var out []string
	for _, r := range s.AdminQuery(t, "SELECT name FROM system.tables WHERE database = '_rudderstack' AND startsWith(name, '_rudder_probe_')") {
		if len(r) == 1 && r[0] != "" {
			out = append(out, r[0])
		}
	}
	return out
}

// DropProbes drops every validation probe table in _rudderstack.
func (s *Server) DropProbes(t *testing.T) {
	t.Helper()
	for _, name := range s.ProbeTables(t) {
		s.AdminExec(t, "DROP TABLE IF EXISTS `_rudderstack`."+quoteIdent(name)+" SYNC")
	}
}
