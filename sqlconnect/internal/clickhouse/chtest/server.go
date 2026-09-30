// Package chtest starts digest-pinned ClickHouse containers with a TLS fixture
// and puts a fault-injecting proxy in front of them. It is for tests only.
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
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
)

const (
	repository = "clickhouse/clickhouse-server"
	// memoryLimit caps every fixture container (host load limits).
	memoryLimit = 1 << 30
	adminUser   = "admin"
	// containerLabel marks fixture containers so a stale one is easy to find and remove.
	containerLabel = "sqlconnect-go.chtest"
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
	Host          string // "localhost": the name the fixture certificate carries
	HTTPSPort     int
	HTTPPort      int
	CA            *x509.CertPool
	AdminUser     string
	AdminPassword string

	serverCert  tls.Certificate
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

	caPool, serverCert, certPEM, keyPEM := newTLSFixture(t)
	dir := t.TempDir()
	certDir := filepath.Join(dir, "certs")
	require.NoError(t, os.Mkdir(certDir, 0o755))
	// The server runs as uid 101 inside the container and must read the
	// mounted files. The key is a throwaway test key.
	require.NoError(t, os.Chmod(certDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(certDir, "server.crt"), certPEM, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(certDir, "server.key"), keyPEM, 0o644)) //nolint:gosec // throwaway test key
	tlsXML := filepath.Join(dir, "tls.xml")
	require.NoError(t, os.WriteFile(tlsXML, []byte(tlsConfigXML), 0o644))
	// Mount single files into config.d: the image's own docker_related_config.xml
	// there sets listen_host and must stay.
	mounts := []string{
		certDir + ":/etc/clickhouse-server/certs:ro",
		tlsXML + ":/etc/clickhouse-server/config.d/tls.xml:ro",
	}
	if o.ConfigXML != "" {
		extra := filepath.Join(dir, "extra.xml")
		require.NoError(t, os.WriteFile(extra, []byte(o.ConfigXML), 0o644))
		mounts = append(mounts, extra+":/etc/clickhouse-server/config.d/zz_extra.xml:ro")
	}

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = 5 * time.Minute

	// dockertest builds the reference as repository + ":" + tag, so the digest
	// is split across both fields.
	pinned := repository + "@" + digest
	if _, err := pool.Client.InspectImage(pinned); err != nil {
		require.NoError(t, pool.Client.PullImage(docker.PullImageOptions{
			Repository: repository, Tag: digest, // the Engine API accepts a digest as the tag
		}, docker.AuthConfiguration{}))
	}

	password := randomString(t, 24)
	loopback := func() []docker.PortBinding { return []docker.PortBinding{{HostIP: "127.0.0.1", HostPort: ""}} }
	res, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: repository + "@sha256",
		Tag:        strings.TrimPrefix(digest, "sha256:"),
		Env: []string{
			"CLICKHOUSE_USER=" + adminUser,
			"CLICKHOUSE_PASSWORD=" + password,
			"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1",
			"TZ=" + tz,
		},
		Mounts:       mounts,
		ExposedPorts: []string{"8443/tcp", "8123/tcp"},
		// Publish on loopback only: the admin password must not reach other hosts.
		PortBindings: map[docker.Port][]docker.PortBinding{"8443/tcp": loopback(), "8123/tcp": loopback()},
		Labels:       map[string]string{containerLabel: t.Name()},
	}, func(hc *docker.HostConfig) {
		hc.AutoRemove = true
		hc.PublishAllPorts = false
		hc.Memory = memoryLimit
		hc.MemorySwap = memoryLimit
		hc.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := pool.Purge(res); err != nil {
			t.Logf("chtest: purge container %s: %v", res.Container.ID, err)
		}
	})

	img, err := pool.Client.InspectImage(res.Container.Image)
	require.NoError(t, err)
	require.Contains(t, img.RepoDigests, pinned, "the container runs the pinned image")
	require.EqualValues(t, memoryLimit, res.Container.HostConfig.Memory, "the container has the memory limit")

	httpsPort, err := strconv.Atoi(res.GetPort("8443/tcp"))
	require.NoError(t, err)
	httpPort, err := strconv.Atoi(res.GetPort("8123/tcp"))
	require.NoError(t, err)

	s := &Server{
		Host:          "localhost",
		HTTPSPort:     httpsPort,
		HTTPPort:      httpPort,
		CA:            caPool,
		AdminUser:     adminUser,
		AdminPassword: password,
		serverCert:    serverCert,
		pool:          pool,
		containerID:   res.Container.ID,
		httpClient:    newLoopbackClient(),
	}
	t.Cleanup(s.httpClient.CloseIdleConnections)

	require.NoError(t, pool.Retry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.plainURL("/ping", nil), nil)
		if err != nil {
			return err
		}
		resp, err := s.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("ping status %d", resp.StatusCode)
		}
		return nil
	}), "the server answers /ping")

	version := s.AdminQuery(t, "SELECT version()")[0][0]
	require.True(t, strings.HasPrefix(version, o.Tag+"."), "server version %s for tag %s", version, o.Tag)
	t.Logf("clickhouse image=%s version=%s", s.ImageDigest(t), version)
	return s
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
func (s *Server) Config(user, password, database, scratch string, secure bool) json.RawMessage {
	port := s.HTTPPort
	if secure {
		port = s.HTTPSPort
	}
	b, err := json.Marshal(struct {
		Host            string `json:"host"`
		Port            int    `json:"port"`
		Database        string `json:"database"`
		User            string `json:"user"`
		Password        string `json:"password"`
		Secure          bool   `json:"secure"`
		SkipVerify      bool   `json:"skipVerify"`
		ScratchDatabase string `json:"scratchDatabase"`
	}{s.Host, port, database, user, password, secure, false, scratch})
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, strings.NewReader(sql))
	require.NoError(t, err)
	req.SetBasicAuth(s.AdminUser, s.AdminPassword)
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

// CreateScopedUser provisions a runtime user with the published grant script:
// grants-and-security section 2.1, plus section 2.2 when pruning is true.
func (s *Server) CreateScopedUser(t *testing.T, name, password, customerDB, scratchDB string, pruning bool) {
	t.Helper()
	user, customer, scratch := quoteIdent(name), quoteIdent(customerDB), quoteIdent(scratchDB)
	stmts := []string{
		fmt.Sprintf("CREATE USER %s IDENTIFIED WITH sha256_password BY %s", user, quoteString(password)),
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", scratch),
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", customer),
		fmt.Sprintf("GRANT SELECT ON %s.* TO %s", customer, user),
		fmt.Sprintf("GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE ON %s.* TO %s", scratch, user),
		fmt.Sprintf("GRANT SELECT ON system.processes TO %s", user),
		fmt.Sprintf("GRANT SELECT ON system.query_log TO %s", user),
	}
	if pruning {
		stmts = append(stmts,
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.`sync_log` (id UInt8) ENGINE = MergeTree ORDER BY id", scratch),
			fmt.Sprintf("GRANT ALTER DELETE ON %s.`sync_log` TO %s", scratch, user),
		)
	}
	for _, stmt := range stmts {
		s.AdminExec(t, stmt)
	}
}

func (s *Server) plainURL(path string, q url.Values) string {
	u := url.URL{Scheme: "http", Host: net.JoinHostPort("127.0.0.1", strconv.Itoa(s.HTTPPort)), Path: path, RawQuery: q.Encode()}
	return u.String()
}

const tlsConfigXML = `<clickhouse>
    <http_port>8123</http_port>
    <https_port>8443</https_port>
    <openSSL>
        <server>
            <certificateFile>/etc/clickhouse-server/certs/server.crt</certificateFile>
            <privateKeyFile>/etc/clickhouse-server/certs/server.key</privateKeyFile>
            <verificationMode>none</verificationMode>
            <loadDefaultCAFile>false</loadDefaultCAFile>
            <disableProtocols>sslv2,sslv3,tlsv1,tlsv1_1</disableProtocols>
        </server>
    </openSSL>
</clickhouse>
`

// newTLSFixture creates a test CA and a server certificate with the SANs
// DNS:localhost and DNS:rebind.test only. It has no IP SAN, so 127.0.0.1 gives
// a hostname mismatch.
func newTLSFixture(t *testing.T) (*x509.CertPool, tls.Certificate, []byte, []byte) {
	t.Helper()
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTmpl := &x509.Certificate{
		SerialNumber:          serial(t),
		Subject:               pkix.Name{CommonName: "chtest CA"},
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
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)
	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)
	return caPool, pair, certPEM, keyPEM
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
