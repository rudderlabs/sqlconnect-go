package clickhouse_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/testhelper/rand"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/clickhouse"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/clickhouse/chtest"
)

func TestSQ4_FloorMatrix(t *testing.T) {
	for tag, ok := range map[string]bool{"26.3": true, "25.8": false, "24.8": false} {
		t.Run(tag, func(t *testing.T) {
			srv := chtest.Start(t, chtest.Options{Tag: tag})
			srv.CreateScopedUser(t, "rudder_retl", "pw_Retl_123", "customer_db", "scratch_db", false)
			res, err := openScoped(t, srv, "rudder_retl", "pw_Retl_123").ValidateContext(context.Background())
			if ok {
				require.NoError(t, err)
				require.True(t, strings.HasPrefix(res.ServerVersion, "26.3."))
				return
			}
			requireCode(t, err, "CH_VERSION_BELOW_FLOOR")
			requireStage(t, err, 1, "version")
			require.Contains(t, err.Error(), tag+".")
			require.Contains(t, err.Error(), "26.3.0")
			require.Equal(t, "0", srv.AdminQuery(t, "SELECT count() FROM system.tables WHERE database = 'scratch_db'")[0][0], "no scratch DDL")
		})
	}
	srv := chtest.Start(t, chtest.Options{Tag: "26.3"})
	srv.CreateScopedUser(t, "rudder_retl", "pw_Retl_123", "customer_db", "scratch_db", false)
	_, err := openScoped(t, srv, "rudder_retl", "wrong-pw").ValidateContext(context.Background())
	requireCode(t, err, "CH_AUTHENTICATION")
	_, err = openScopedOnDatabase(t, srv, "rudder_retl", "pw_Retl_123", "no_such_db", "scratch_db").ValidateContext(context.Background())
	require.Contains(t, []string{"CH_OBJECT_NOT_FOUND", "CH_PERMISSION"}, clickhouse.CodeOf(err), "a missing database fails, classified")
}

func TestSQ14_ReadonlyAndConstraints(t *testing.T) {
	srv := chtest.Start(t, chtest.Options{Tag: "26.3"})
	_, err := openAs(t, srv, srv.CreateUserWithProfile(t, map[string]string{"readonly": "1"})).ValidateContext(context.Background())
	d, _ := clickhousequery.Describe(err)
	require.Equal(t, clickhousequery.Details{Code: "CH_PERMISSION", Category: "permission", Field: "max_execution_time", ServerCode: 164}, d)
	requireStage(t, err, 1, "settings") // the hello SELECT fails inside stage 1
	_, err = openAs(t, srv, srv.CreateUserWithConstraint(t, "select_sequential_consistency", "0")).ValidateContext(context.Background())
	d, _ = clickhousequery.Describe(err)
	require.Equal(t, "select_sequential_consistency", d.Field)
	require.EqualValues(t, 452, d.ServerCode)
	requireStage(t, err, 2, "settings")
	_, err = openAs(t, srv, srv.CreateUserWithConstraint(t, "send_progress_in_http_headers", "1")).ValidateContext(context.Background())
	d, _ = clickhousequery.Describe(err)
	require.Equal(t, [2]any{"send_progress_in_http_headers", int32(452)}, [2]any{d.Field, d.ServerCode}, "the open names the setting it refused")
	requireStage(t, err, 1, "settings")
}

func TestValidation_Grants(t *testing.T) {
	srv := chtest.Start(t, chtest.Options{Tag: "26.3"})
	fresh := func(t *testing.T, pruning bool) (string, *clickhouse.DB) {
		user := "u_" + strings.ToLower(rand.String(6))
		srv.CreateScopedUser(t, user, "pw_U_12345", "customer_db", "scratch_db", pruning)
		return user, openScoped(t, srv, user, "pw_U_12345")
	}
	pruning := sqlconnect.WithValidationOptions(context.Background(), sqlconnect.ValidationOptions{SyncLogPruning: true})
	_, db := fresh(t, false)
	res, err := db.ValidateContext(context.Background())
	require.NoError(t, err, "the published script validates")
	require.False(t, res.GrantsChecked)
	require.Len(t, filterOp(res.Warnings, "inspect_grants"), 1)
	require.Len(t, filterOp(res.Warnings, "check_alter_delete"), 1, "no options: a missing ALTER DELETE is a warning")
	for privilege, revoke := range map[string]string{
		"CREATE TABLE": "REVOKE CREATE TABLE ON scratch_db.* FROM ", "SELECT ON system.processes": "REVOKE SELECT ON system.processes FROM ",
		"SELECT ON system.query_log": "REVOKE SELECT ON system.query_log FROM ", "ALTER DELETE": "", // pruning on, no sync_log grant
	} {
		user, db := fresh(t, false)
		ctx := pruning
		if revoke != "" {
			ctx = context.Background()
			srv.AdminExec(t, revoke+user)
		}
		_, err := db.ValidateContext(ctx)
		d, _ := clickhousequery.Describe(err)
		require.Equal(t, [2]string{"CH_PERMISSION", privilege}, [2]string{d.Code, d.Field})
		requireStage(t, err, 3, "grant_check")
	}
	_, db = fresh(t, true)
	_, err = db.ValidateContext(pruning)
	require.NoError(t, err, "pruning on with the sync_log grant")
	for _, g := range []string{
		"CREATE USER retl_user IDENTIFIED WITH sha256_password BY 'pw_Role_123'", "CREATE ROLE retl_permissions", "CREATE ROLE retl_login",
		"GRANT SELECT ON customer_db.* TO retl_permissions", "GRANT SELECT, INSERT, CREATE TABLE, DROP TABLE ON scratch_db.* TO retl_permissions",
		"GRANT SELECT ON system.processes TO retl_permissions", "GRANT SELECT ON system.query_log TO retl_permissions",
		"GRANT retl_permissions TO retl_login", "GRANT retl_login TO retl_user", "SET DEFAULT ROLE retl_login TO retl_user",
	} {
		srv.AdminExec(t, g)
	}
	_, err = openScoped(t, srv, "retl_user", "pw_Role_123").ValidateContext(context.Background())
	require.NoError(t, err, "CHECK GRANT sees privileges through the inherited role")
}

func TestValidation_EngineAndCluster(t *testing.T) {
	srv := chtest.Start(t, chtest.Options{Tag: "26.3"})
	srv.AdminExec(t, "CREATE DATABASE s_mem ENGINE = Memory")
	srv.CreateScopedUser(t, "e_mem", "pw_E_12345", "customer_db", "s_mem", false)
	_, err := openScopedOnDatabase(t, srv, "e_mem", "pw_E_12345", "customer_db", "s_mem").ValidateContext(context.Background())
	d, _ := clickhousequery.Describe(err)
	require.Equal(t, [2]string{"CH_CONFIG_INVALID", "scratchDatabase"}, [2]string{d.Code, d.Field})
	require.Contains(t, err.Error(), "Memory")
	// One container at a time (load limits): the second "host" is a proxy
	// that answers hostName() with another name.
	srv.CreateScopedUser(t, "rudder_retl", "pw_Retl_123", "customer_db", "scratch_db", false)
	other := chtest.NewProxy(t, srv, chtest.ProxyOptions{PlainHTTP: true, RewriteSQL: func(q string) string {
		return strings.ReplaceAll(q, "SELECT hostName()", "SELECT 'replica-2'")
	}})
	_, err = openScopedVia(t, srv, chtest.NewBalancer(t, srv, srv.HTTPPort, other.Port()), "rudder_retl", "pw_Retl_123").ValidateContext(context.Background())
	requireCode(t, err, "CH_CLUSTER_UNSUPPORTED")
	requireStage(t, err, 3, "engine")
	srv.CreateScopedUser(t, "lb_user", "pw_Lb_12345", "customer_db", "scratch_db", false)
	_, err = openScopedVia(t, srv, chtest.NewBalancer(t, srv, srv.HTTPPort), "lb_user", "pw_Lb_12345").ValidateContext(context.Background())
	require.NoError(t, err, "one host behind the balancer passes")
}

func TestSQ25_Probe(t *testing.T) {
	srv := chtest.Start(t, chtest.Options{Tag: "26.3"})
	srv.CreateScopedUser(t, "rudder_retl", "pw_Retl_123", "customer_db", "scratch_db", false)
	srv.AdminExec(t, "CREATE TABLE scratch_db.test_table (k UInt8) ENGINE = MergeTree ORDER BY k")
	db := openScoped(t, srv, "rudder_retl", "pw_Retl_123")
	var wg sync.WaitGroup
	for range 10 { // more calls than pool slots: no call waits on a slot another call holds
		wg.Go(func() { _, err := db.ValidateContext(context.Background()); require.NoError(t, err) })
	}
	wg.Wait()
	require.Equal(t, "1", srv.AdminQuery(t, "SELECT count() FROM system.tables WHERE database='scratch_db' AND name='test_table'")[0][0], "a customer test_table survives")
	require.Equal(t, "0", probeCount(t, srv))
	srv.FlushLogs(t)
	seen := map[string]bool{}
	for _, c := range srv.QueryLogLike(t, "CREATE TABLE `scratch_db`.`_rudder_probe_%") {
		require.Regexp(t, "^CREATE TABLE `scratch_db`\\.`_rudder_probe_[0-9a-f]{16}` \\(probe UInt8\\)\\nENGINE = MergeTree", c.Query, "plain CREATE")
		seen[c.Query] = true
	}
	require.Len(t, seen, 10, "the probe name is unique per call")
}

func TestSQ25_ProbeCleanup(t *testing.T) {
	srv := chtest.Start(t, chtest.Options{Tag: "26.3"})
	srv.CreateScopedUser(t, "rudder_retl", "pw_Retl_123", "customer_db", "scratch_db", false)
	isProbe := func(prefix string) func(chtest.Request) bool {
		return func(r chtest.Request) bool { return strings.HasPrefix(r.SQL, prefix+" `scratch_db`.`_rudder_probe_") }
	}
	validate := func(p *chtest.Proxy, ctx context.Context) error {
		_, err := openScopedVia(t, srv, p, "rudder_retl", "pw_Retl_123").ValidateContext(ctx)
		return err
	}
	p := chtest.NewProxy(t, srv, chtest.ProxyOptions{})
	p.InjectOnce(isProbe("DROP TABLE"), 500, http.Header{"X-ClickHouse-Exception-Code": {"497"}}, "Code: 497. DB::Exception: Not enough privileges.")
	err := validate(p, context.Background())
	requireCode(t, err, "CH_SCRATCH_CLEANUP_FAILED")
	requireStage(t, err, 4, "scratch_cleanup")
	srv.DropProbes(t)
	p = chtest.NewProxy(t, srv, chtest.ProxyOptions{})
	p.InjectOnce(isProbe("INSERT INTO"), 500, http.Header{"X-ClickHouse-Exception-Code": {"241"}}, "Code: 241. DB::Exception: Memory limit.")
	require.Error(t, validate(p, context.Background()))
	require.Equal(t, "0", probeCount(t, srv), "INSERT failure: probe still dropped")
	p = chtest.NewProxy(t, srv, chtest.ProxyOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	created := false
	p.OnRequest(func(r chtest.Request) {
		if isProbe("CREATE TABLE")(r) {
			created = true
		} else if created && strings.Contains(r.SQL, "FROM system.tables") {
			cancel() // the CREATE returned success and the visibility read started
		}
	})
	require.ErrorIs(t, validate(p, ctx), context.Canceled)
	require.Equal(t, "0", probeCount(t, srv), "caller cancel: cleanup ran on its own context")
	p = chtest.NewProxy(t, srv, chtest.ProxyOptions{})
	p.DropResponseOnce(isProbe("CREATE TABLE"))
	require.Error(t, validate(p, context.Background()))
	require.Equal(t, "0", probeCount(t, srv), "unknown CREATE outcome: reconciled with DROP TABLE IF EXISTS")
	srv.AdminExec(t, "REVOKE DROP TABLE ON scratch_db.* FROM rudder_retl")
	_, err = openScoped(t, srv, "rudder_retl", "pw_Retl_123").ValidateContext(context.Background())
	_, stage := stageOf(err)
	require.Equal(t, "grant_check", stage, "a missing DROP grant stops before the probe")
	require.Equal(t, "0", probeCount(t, srv))
}

func TestSQ25_SQ30_ValidationHelloFaults(t *testing.T) {
	srv := chtest.Start(t, chtest.Options{Tag: "26.3"})
	var logs bytes.Buffer
	for _, secure := range []bool{true, false} {
		p := chtest.NewProxy(t, srv, chtest.ProxyOptions{PlainHTTP: !secure})
		p.InjectOnce(isHello, 500, http.Header{"X-ClickHouse-Exception-Code": {"516"}}, "Code: 516. DB::Exception: sentinel-pw-7f3a is wrong. (AUTHENTICATION_FAILED)")
		_, err := openViaWithPassword(t, srv, p, secure, "sentinel-pw-7f3a").ValidateContext(context.Background())
		fmt.Fprintf(&logs, "%v %+v %#v", err, err, err)
		requireCode(t, err, "CH_AUTHENTICATION")
		require.Contains(t, err.Error(), "the server refused the credentials")
	}
	require.NotContains(t, logs.String(), "sentinel-pw-7f3a", "SQ25: the validation response carries no server text")
	var second atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { second.Add(1) }))
	defer other.Close()
	p := chtest.NewProxy(t, srv, chtest.ProxyOptions{})
	p.InjectOnce(isHello, 308, http.Header{"Location": {other.URL + "/"}}, "")
	_, err := openVia(t, srv, p, true).ValidateContext(context.Background())
	requireCode(t, err, "CH_REDIRECT_REFUSED")
	requireStage(t, err, 0, "network")
	require.Zero(t, second.Load(), "SQ30: the second host receives zero requests")
}

func TestValidation_PingDelegates(t *testing.T) {
	srv := chtest.Start(t, chtest.Options{Tag: "26.3"})
	srv.CreateScopedUser(t, "rudder_retl", "pw_Retl_123", "customer_db", "scratch_db", false)
	db := openScoped(t, srv, "rudder_retl", "pw_Retl_123")
	require.NoError(t, db.Ping(), "metadata warnings never fail Ping")
	r1, err := db.ValidateContext(context.Background())
	require.NoError(t, err)
	r2, _ := db.ValidateContext(context.Background())
	r1.Warnings = append(r1.Warnings, sqlconnect.ValidationWarning{Operation: "mutated"})
	require.NotContains(t, r2.Warnings, sqlconnect.ValidationWarning{Operation: "mutated"}, "each call owns its result")
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, db.PingContext(cctx))
}
