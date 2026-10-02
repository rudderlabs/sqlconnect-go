package clickhouse

import (
	"context"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/cherr"
)

var (
	_ sqlconnect.ValidationReporter = (*DB)(nil)
	// versionFloor is the lowest supported server version. It never sits below
	// the fork's own minimum, 25.8.0.
	versionFloor = [3]int{26, 3, 0}
)

const (
	// maxConcurrentValidations bounds concurrent ValidateContext calls. Each
	// call holds its own connection plus three for the host comparison, so
	// 2 x 4 stays under the pool's ten.
	maxConcurrentValidations = 2
	// validationOpenTimeout bounds the first validation request: the dial,
	// the TLS handshake and the hello query.
	validationOpenTimeout = 90 * time.Second
	// probeCleanupTimeout bounds the probe cleanup, which runs on a context
	// detached from the caller.
	probeCleanupTimeout = 60 * time.Second
	// probeSettlePolls and probeSettleInterval bound the wait for a probe
	// CREATE with an unknown outcome. The window outlasts the default
	// query_log flush interval of 7.5 s, so a finished CREATE shows up in it.
	probeSettlePolls    = 15
	probeSettleInterval = time.Second
	hostNameSQL         = "SELECT hostName()"
	engineSQL           = "SELECT engine FROM system.databases WHERE name = ?"
)

// probeLookupTimeout bounds one outcome read, and probeCleanupReserve is the
// part of probeCleanupTimeout that the outcome wait never uses. A stalled
// read therefore always leaves time for the DROP and the absence check.
// Tests shorten probeLookupTimeout.
var probeLookupTimeout = 5 * time.Second

const probeCleanupReserve = 20 * time.Second

const defaultRudderSchema = "_rudderstack"

func workingDatabase(ctx context.Context) string {
	opts, _ := sqlconnect.ValidationOptionsFrom(ctx)
	if opts.WorkingDatabase == "" {
		return defaultRudderSchema
	}
	return opts.WorkingDatabase
}

// diagnosticSettings are the settings stage 5 reads from system.settings.
var diagnosticSettings = []string{
	"async_insert", "cancel_http_readonly_queries_on_client_close", "date_time_input_format",
	"default_table_engine", "enable_analyzer", "enable_parallel_replicas", "join_use_nulls",
	"max_execution_time", "readonly", "select_sequential_consistency", "send_progress_in_http_headers",
	"session_timezone", "wait_for_async_insert",
}

var settingsSQL = "SELECT name, value, changed\nFROM system.settings\nWHERE name IN ('" +
	strings.Join(diagnosticSettings, "', '") + "')\nORDER BY name"

const (
	currentRolesSQL = "SELECT role_name FROM system.current_roles"
	roleGrantsSQL   = "SELECT granted_role_name\nFROM system.role_grants\nWHERE role_name = ?"
	userGrantsSQL   = "SELECT access_type, database, table, column, is_partial_revoke, grant_option\nFROM system.grants\nWHERE user_name = currentUser()"
	roleGrantSQL    = "SELECT access_type, database, table, column, is_partial_revoke, grant_option\nFROM system.grants\nWHERE role_name = ?"
)

// Ping runs the full validation and returns only its error.
func (db *DB) Ping() error { return db.PingContext(context.Background()) }

// PingContext runs the full validation and returns only its error.
func (db *DB) PingContext(ctx context.Context) error {
	_, err := db.ValidateContext(ctx)
	return err
}

// stageErr is the only form of a validation failure. It wraps the bounded
// error, so no server text reaches the caller.
func stageErr(stage int, tag string, err error) error {
	return sqlconnect.ValidationStageError{Stage: stage, Tag: tag, Err: bound(tag, "", err)}
}

// ValidateContext runs the validation stages in order on one connection.
// Stages 1 to 4 are fatal; stage 5 only adds warnings. Each call owns its
// result.
func (db *DB) ValidateContext(ctx context.Context) (sqlconnect.ValidationResult, error) {
	var res sqlconnect.ValidationResult
	working := workingDatabase(ctx)
	if !namePattern.MatchString(working) {
		return res, stageErr(3, "engine", invalid("workingDatabase", nameErr))
	}
	switch strings.ToLower(working) {
	case strings.ToLower(db.cfg.Database), "default", "system", "information_schema":
		return res, stageErr(3, "engine", invalid("workingDatabase",
			"The _rudderstack database must differ from the customer database, default, system and information_schema."))
	}
	select {
	case db.validateSem <- struct{}{}:
		defer func() { <-db.validateSem }()
	case <-ctx.Done():
		return res, stageErr(0, "network", ctx.Err())
	}
	refused := func(code int32) string { return db.refusedOpenSetting(ctx, code) }
	openCtx, cancel := context.WithTimeout(ctx, validationOpenTimeout)
	defer cancel()
	conn, err := db.Conn(openCtx)
	if err != nil {
		return res, openFailure(err, refused)
	}
	defer func() { _ = conn.Close() }()
	ex := driverExec{conn: conn, settings: driverScratchSettings}

	// Stage 1: connect and version. It carries the control map, as the hello
	// does, so a refused driver setting is named by stage 2.
	control := driverExec{conn: conn, settings: helloSettings}
	var version, current string
	// On a warm pool db.Conn sends nothing, so this read can be the first
	// request: it runs under the same 90 s bound.
	if err := control.QueryRowContext(openCtx, "SELECT version(), currentDatabase()").Scan(&version, &current); err != nil {
		return res, openFailure(err, refused)
	}
	if v, err := parseVersion(version); err != nil || less(v, versionFloor) {
		return res, stageErr(1, "version", cherr.New(cherr.CodeVersionBelowFloor, "",
			fmt.Sprintf("server version %s is below the supported floor %d.%d.%d",
				sanitizeVersion(version), versionFloor[0], versionFloor[1], versionFloor[2])))
	}
	res.ServerVersion = sanitizeVersion(version)
	if current != db.cfg.Database {
		return res, stageErr(1, "configuration", cherr.New(cherr.CodeConfigInvalid, "database",
			"the server's current database differs from the configured database"))
	}

	// Stage 2: settings.
	union := driverExec{conn: conn, settings: func() map[string]any {
		return stage2UnionSettings(int(clickhousequery.MaxRunBudget.Seconds()))
	}}
	var one uint8
	// The fork replaces max_execution_time with the remaining deadline, so the
	// check runs without a deadline and sends the run budget itself.
	budgetCtx, stopBudget := withoutDeadline(ctx)
	defer stopBudget()
	if err := union.QueryRowContext(budgetCtx, "SELECT 1").Scan(&one); err != nil {
		return res, settingFailure(budgetCtx, conn, err)
	}

	// Stage 3: grants and engine.
	warnings, err := db.checkGrants(ctx, ex)
	if err != nil {
		return res, err
	}
	res.Warnings = append(res.Warnings, warnings...)
	if err := db.checkEngine(ctx, ex); err != nil {
		return res, err
	}

	// Stage 4: probe.
	if err := db.probe(ctx, ex); err != nil {
		return res, err
	}

	// Stage 5: diagnostics, never fatal. The control map keeps the driver's
	// own settings out of the system.settings snapshot.
	res.Warnings = append(res.Warnings, db.diagnostics(ctx, control, &res)...)
	return res, nil
}

// checkGrants runs one CHECK GRANT per required privilege, so CH_PERMISSION
// names the failing one, then the diagnostic checks that only warn.
func (db *DB) checkGrants(ctx context.Context, ex sqlconnect.QueryExecutor) ([]sqlconnect.ValidationWarning, error) {
	scratch := db.QuoteIdentifier(workingDatabase(ctx))
	type grant struct{ privilege, stmt string }
	var checks []grant
	for _, p := range []string{"SELECT", "INSERT", "CREATE TABLE", "DROP TABLE"} {
		checks = append(checks, grant{p, "CHECK GRANT " + p + " ON " + scratch + ".*"})
	}
	for _, tbl := range []string{"system.processes", "system.query_log"} {
		checks = append(checks, grant{"SELECT ON " + tbl, "CHECK GRANT SELECT ON " + tbl})
	}
	alterDelete := "CHECK GRANT ALTER DELETE ON " + scratch + ".`sync_log`"
	opts, hasOpts := sqlconnect.ValidationOptionsFrom(ctx)
	if hasOpts && opts.SyncLogPruning {
		checks = append(checks, grant{"ALTER DELETE", alterDelete})
	}
	for _, c := range checks {
		ok, err := checkGrant(ctx, ex, c.stmt)
		if err != nil {
			return nil, stageErr(3, "grant_check", err)
		}
		if !ok {
			return nil, stageErr(3, "grant_check", cherr.New(cherr.CodePermission, c.privilege, "the user lacks a required privilege"))
		}
	}
	warn := func(op string) sqlconnect.ValidationWarning {
		return sqlconnect.ValidationWarning{Info: info(cherr.CodePermission, 0), Operation: op}
	}
	var warnings []sqlconnect.ValidationWarning
	// Account-only validation has no options: a missing ALTER DELETE warns.
	if !hasOpts {
		if ok, err := checkGrant(ctx, ex, alterDelete); err == nil && !ok {
			warnings = append(warnings, warn("check_alter_delete"))
		}
	}
	// Narrower customer grants are valid, so this check only warns.
	if ok, err := checkGrant(ctx, ex, "CHECK GRANT SELECT ON "+db.QuoteIdentifier(db.cfg.Database)+".*"); err == nil && !ok {
		warnings = append(warnings, warn("check_database_select"))
	}
	return warnings, nil
}

// checkGrant runs one CHECK GRANT. The statement accepts no FORMAT clause.
func checkGrant(ctx context.Context, ex sqlconnect.QueryExecutor, stmt string) (bool, error) {
	var v uint8
	if err := ex.QueryRowContext(ctx, stmt).Scan(&v); err != nil {
		return false, bound("grant check", "", err)
	}
	return v == 1, nil
}

// checkEngine refuses a scratch engine other than Atomic or Shared. For
// Atomic it compares the host of four connections.
func (db *DB) checkEngine(ctx context.Context, ex sqlconnect.QueryExecutor) error {
	var engine string
	switch err := ex.QueryRowContext(ctx, engineSQL, workingDatabase(ctx)).Scan(&engine); {
	case errors.Is(err, sql.ErrNoRows):
		return stageErr(3, "engine", cherr.New(cherr.CodeConfigInvalid, "workingDatabase", "the _rudderstack database does not exist"))
	case err != nil:
		return stageErr(3, "engine", err)
	}
	switch engine {
	case "Shared": // Cloud replicas differ by design, so no host comparison.
		return nil
	case "Atomic":
		if err := db.sameHost(ctx, ex); err != nil {
			return stageErr(3, "engine", err)
		}
		return nil
	default:
		return stageErr(3, "engine", cherr.New(cherr.CodeConfigInvalid, "workingDatabase",
			"the _rudderstack database engine "+engineLabel(engine)+" is not Atomic or Shared"))
	}
}

// sameHost compares hostName() on ex and on three more connections that it
// holds at the same time. It is a heuristic and can miss a load balancer.
func (db *DB) sameHost(ctx context.Context, ex sqlconnect.QueryExecutor) error {
	var first string
	if err := ex.QueryRowContext(ctx, hostNameSQL).Scan(&first); err != nil {
		return bound("host check", "", err)
	}
	conns := make([]*sql.Conn, 0, 3)
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()
	for range 3 {
		c, err := db.Conn(ctx)
		if err != nil {
			return bound("connect", "", err)
		}
		conns = append(conns, c)
	}
	for _, c := range conns {
		var host string
		if err := (driverExec{conn: c, settings: driverReadSettings}).QueryRowContext(ctx, hostNameSQL).Scan(&host); err != nil {
			return bound("host check", "", err)
		}
		if host != first {
			return cherr.New(cherr.CodeClusterUnsupported, "", "the connections reached different hosts; a cluster behind one address is not supported")
		}
	}
	return nil
}

// openFailure maps a failure of the first request to its stage and tag.
// refused names the setting behind server code 164 or 452.
func openFailure(err error, refused func(code int32) string) error {
	i := classify(err)
	if i.ServerCode == 164 || i.ServerCode == 452 {
		e := wrap(cherr.CodePermission, refused(i.ServerCode), fixedMessages[cherr.CodePermission], err)
		e.ServerCode = i.ServerCode
		return stageErr(1, "settings", e)
	}
	switch i.Code {
	case cherr.CodeTLS:
		return stageErr(1, "tls", err)
	case cherr.CodeAuthentication:
		return stageErr(1, "authentication", err)
	case cherr.CodeHostNotAllowed, cherr.CodeDNSFailed, cherr.CodeNetwork, cherr.CodeRedirectRefused,
		cherr.CodeTimeout, cherr.CodeCancelled, cherr.CodeUnavailable:
		return stageErr(0, "network", err)
	default:
		return stageErr(1, "configuration", err)
	}
}

// refusedOpenSetting names the setting that a connection open refused with
// code. The error text is redacted, so it opens connections outside the pool,
// one per hello setting with only that setting and no deadline, and returns
// the first one refused with the same code. If none is, it opens one with an
// empty map and a deadline, which adds max_execution_time. The result is
// always a fixed name, or "".
func (db *DB) refusedOpenSetting(ctx context.Context, code int32) string {
	if db.connector == nil {
		return ""
	}
	refusedWith := func(octx context.Context, m map[string]any) bool {
		c, err := db.connector.Connect(context.WithValue(octx, helloOverrideKey{}, m))
		if err == nil {
			_ = c.Close()
			return false
		}
		return classify(err).ServerCode == code
	}
	octx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	timer := time.AfterFunc(validationOpenTimeout, cancel) // a deadline would add max_execution_time
	defer timer.Stop()
	hello := helloSettings()
	for _, k := range slices.Sorted(maps.Keys(hello)) {
		if refusedWith(octx, map[string]any{k: hello[k]}) {
			return k
		}
	}
	dctx, dcancel := context.WithTimeout(octx, validationOpenTimeout)
	defer dcancel()
	if refusedWith(dctx, map[string]any{}) {
		return "max_execution_time"
	}
	return ""
}

// withoutDeadline returns a context that ends when ctx ends but carries no
// deadline. The fork adds max_execution_time from a context deadline and
// overwrites the statement's own value.
func withoutDeadline(ctx context.Context) (context.Context, func()) {
	c, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stop := context.AfterFunc(ctx, cancel)
	return c, func() {
		stop()
		cancel()
	}
}

// settingFailure maps a stage 2 failure. Codes 164 and 452 are CH_PERMISSION
// and code 115 is CH_QUERY_INVALID, each naming the refused setting.
func settingFailure(ctx context.Context, conn *sql.Conn, err error) error {
	i := classify(err)
	var code string
	switch i.ServerCode {
	case 164, 452:
		code = cherr.CodePermission
	case 115:
		code = cherr.CodeQueryInvalid
	default:
		return stageErr(2, "settings", err)
	}
	e := wrap(code, refusedSetting(ctx, conn, i.ServerCode), fixedMessages[code], err)
	e.ServerCode = i.ServerCode
	return stageErr(2, "settings", e)
}

// refusedSetting finds the stage 2 key that the server refuses with code. The
// error text is redacted before it reaches the driver, so it sends SELECT 1
// once per key, in name order, and returns the first key refused with the
// same code. The result is always a fixed key, never server text, or "".
func refusedSetting(ctx context.Context, conn *sql.Conn, code int32) string {
	full := stage2UnionSettings(int(clickhousequery.MaxRunBudget.Seconds()))
	keys := slices.Sorted(maps.Keys(full))
	for _, k := range keys {
		single := driverExec{conn: conn, settings: func() map[string]any {
			return map[string]any{"send_progress_in_http_headers": 0, k: full[k]}
		}}
		var one uint8
		if err := single.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil && classify(err).ServerCode == code {
			return k
		}
	}
	return ""
}

// diagnostics reads the settings snapshot and the enabled-role closure. It
// never fails: a refused read only adds a warning. Stage 3 is the only
// privilege verdict, so no grant row ever produces an error.
func (db *DB) diagnostics(ctx context.Context, ex sqlconnect.QueryExecutor, res *sqlconnect.ValidationResult) []sqlconnect.ValidationWarning {
	var warnings []sqlconnect.ValidationWarning
	if rows, err := ex.QueryContext(ctx, settingsSQL); err == nil {
		known := map[string]bool{}
		for _, s := range diagnosticSettings {
			known[s] = true
		}
		for rows.Next() {
			var name, value string
			var changed uint8
			if rows.Scan(&name, &value, &changed) == nil && changed == 1 && known[name] {
				warnings = append(warnings, sqlconnect.ValidationWarning{Info: sqlconnect.ErrorInfo{Category: "configuration"}, Operation: "settings_snapshot"})
			}
		}
		_ = rows.Err() // a broken snapshot read only loses warnings
		_ = rows.Close()
	}
	res.GrantsChecked = false
	if err := readRoleClosure(ctx, ex); err != nil {
		return append(warnings, sqlconnect.ValidationWarning{Info: classify(err), Operation: "inspect_grants"})
	}
	res.GrantsChecked = true
	return warnings
}

// readRoleClosure walks the role closure with a queue and a visited set, then
// reads the grants of the user and of every role. Every name is bound.
func readRoleClosure(ctx context.Context, ex sqlconnect.QueryExecutor) error {
	queue, err := readNames(ctx, ex, currentRolesSQL)
	if err != nil {
		return err
	}
	visited := map[string]bool{}
	var order []string
	for len(queue) > 0 {
		role := queue[0]
		queue = queue[1:]
		if visited[role] {
			continue
		}
		visited[role] = true
		order = append(order, role)
		granted, err := readNames(ctx, ex, roleGrantsSQL, role)
		if err != nil {
			return err
		}
		queue = append(queue, granted...)
	}
	if err := drain(ctx, ex, userGrantsSQL); err != nil {
		return err
	}
	for _, role := range order {
		if err := drain(ctx, ex, roleGrantSQL, role); err != nil {
			return err
		}
	}
	return nil
}

func readNames(ctx context.Context, ex sqlconnect.QueryExecutor, q string, args ...any) ([]string, error) {
	rows, err := ex.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// drain reads every row of q and keeps none: the rows are diagnostics only.
func drain(ctx context.Context, ex sqlconnect.QueryExecutor, q string, args ...any) error {
	rows, err := ex.QueryContext(ctx, q, args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
	}
	return rows.Err()
}

// probeCleanup drops the probe table. Probe registers it before the CREATE.
type probeCleanup struct {
	db      *DB
	ref     sqlconnect.RelationRef
	created bool // true after an acknowledged CREATE; false for an unknown outcome
	// createID is the query id of a CREATE with an unknown outcome. That
	// CREATE can still be in transit, so the DROP waits until it is over.
	createID string
	// acquire gives the connection for each step. In production it is the
	// validation's own connection while that stays usable, so a busy pool
	// cannot starve the cleanup, and a fresh pool connection after
	// database/sql has closed it.
	acquire func(ctx context.Context) (sqlconnect.QueryExecutor, func(), error)
	policy  sqlconnect.VisibilityPolicy
}

// run drops the probe on a context detached from the caller, so a caller
// cancellation still removes it. Every failure is CH_SCRATCH_CLEANUP_FAILED.
func (c probeCleanup) run(parent context.Context) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), probeCleanupTimeout)
	defer cancel()
	failed := func(err error) error {
		return stageErr(4, "scratch_cleanup", wrap(cherr.CodeScratchCleanupFailed, c.ref.Name, "the probe table could not be removed", err))
	}
	stmt := "DROP TABLE " + c.db.QuoteTable(c.ref) + " SYNC"
	if !c.created {
		stmt = "DROP TABLE IF EXISTS " + c.db.QuoteTable(c.ref) + " SYNC"
	}
	unresolved := c.createID != "" && !c.createSettled(ctx)
	if err := c.step(ctx, func(ex sqlconnect.QueryExecutor) error {
		_, err := ex.ExecContext(ctx, stmt)
		return err
	}); err != nil {
		return failed(err)
	}
	if err := c.step(ctx, func(ex sqlconnect.QueryExecutor) error {
		return c.db.AwaitTableAbsent(ctx, ex, c.ref, c.policy)
	}); err != nil {
		return failed(err)
	}
	if unresolved {
		// The DROP ran, but the CREATE can still land after it.
		return failed(cherr.New(cherr.CodeOutcomeUnknown, "", "the probe CREATE outcome is still unknown, so the probe table can still appear"))
	}
	return nil
}

// step runs fn on the connection from acquire and releases it after. It
// never replays a statement: each step asks acquire again, so a connection
// that an earlier step broke is not reused.
func (c probeCleanup) step(ctx context.Context, fn func(ex sqlconnect.QueryExecutor) error) error {
	ex, done, err := c.acquire(ctx)
	if err != nil {
		return err
	}
	defer done()
	return fn(ex)
}

// createSettled waits until the unknown-outcome CREATE has finished or
// failed, so it cannot create the table after the DROP. It polls at most
// probeSettlePolls times, each read bounded by probeLookupTimeout, and stops
// when less than probeCleanupReserve of ctx remains. Only a finished or
// failed outcome proves that the CREATE is over. A CREATE that is still
// running or never seen, or an outcome read that fails, leaves the outcome
// open, and it returns false.
func (c probeCleanup) createSettled(ctx context.Context) bool {
	for i := range probeSettlePolls {
		if dl, ok := ctx.Deadline(); ok && time.Until(dl) < probeCleanupReserve {
			return false
		}
		var o sqlconnect.QueryOutcome
		lctx, stop := context.WithTimeout(ctx, probeLookupTimeout)
		err := c.step(lctx, func(ex sqlconnect.QueryExecutor) error {
			var err error
			o, err = queryOutcome(func(q string, args ...any) *sql.Row { return ex.QueryRowContext(lctx, q, args...) }, c.createID, "Create")
			return err
		})
		stop()
		switch {
		case err != nil:
			return false
		case o == sqlconnect.QueryFinished, o == sqlconnect.QueryFailed:
			return true
		case i == probeSettlePolls-1:
			return false
		}
		if sleep(ctx, probeSettleInterval) != nil {
			return false
		}
	}
	return false
}

// cleanupAcquire returns the probe cleanup's connection source: the
// validation's own connection while it stays usable, so a busy pool cannot
// starve the cleanup, else a fresh pool connection. database/sql closes a
// connection whose driver returned driver.ErrBadConn, and Raw then reports
// sql.ErrConnDone.
func (db *DB) cleanupAcquire(own driverExec) func(context.Context) (sqlconnect.QueryExecutor, func(), error) {
	return func(ctx context.Context) (sqlconnect.QueryExecutor, func(), error) {
		if own.conn.Raw(func(any) error { return nil }) == nil {
			return own, func() {}, nil
		}
		fresh, err := db.Conn(ctx)
		if err != nil {
			return nil, nil, err
		}
		return driverExec{conn: fresh, settings: driverScratchSettings}, func() { _ = fresh.Close() }, nil
	}
}

// probe creates, fills, reads and drops one table with a random name. The
// CREATE is plain: an existing table with the name fails the stage.
func (db *DB) probe(ctx context.Context, ex driverExec) (err error) {
	var b [8]byte
	_, _ = rand.Read(b[:])
	ref := sqlconnect.NewRelationRef("_rudder_probe_"+hex.EncodeToString(b[:]), sqlconnect.WithSchema(workingDatabase(ctx)))
	t := db.QuoteTable(ref)
	cleanup := probeCleanup{
		db: db, ref: ref,
		acquire: db.cleanupAcquire(ex),
	}
	registered := false
	defer func() {
		if !registered {
			return
		}
		if cerr := cleanup.run(ctx); cerr != nil {
			err = cerr // a cleanup failure wins
		}
	}()
	fail := func(e error) error { return stageErr(4, "scratch_write", e) }
	createID := clickhousequery.NewQueryID()
	if _, cerr := ex.execAs(ctx, createID, "CREATE TABLE "+t+" (probe UInt8)\nENGINE = MergeTree\nORDER BY tuple()"); cerr != nil {
		// An unknown outcome may have created the table, or may still create
		// it: wait for the CREATE, then reconcile with DROP TABLE IF EXISTS.
		// A refused CREATE needs no cleanup.
		registered = unknownOutcome(cerr)
		cleanup.createID = createID
		return fail(cerr)
	}
	registered, cleanup.created = true, true
	if _, werr := db.AwaitTable(ctx, ex, ref, "", sqlconnect.VisibilityPolicy{}); werr != nil {
		return fail(werr)
	}
	if _, werr := ex.ExecContext(ctx, "INSERT INTO "+t+" VALUES (1)"); werr != nil {
		return fail(werr)
	}
	var value uint8
	werr := retryVisibleRead(ctx, sqlconnect.VisibilityPolicy{}, func(rctx context.Context) error {
		return ex.QueryRowContext(rctx, "SELECT probe FROM "+t).Scan(&value)
	})
	if werr == nil && value != 1 {
		werr = cherr.New(cherr.CodeScratchCleanupFailed, "", "the probe read returned an unexpected value")
	}
	if werr != nil {
		return fail(werr)
	}
	return nil
}

// unknownOutcome reports whether a failed statement may still have run.
func unknownOutcome(err error) bool {
	if errors.Is(err, driver.ErrBadConn) {
		return true
	}
	switch classify(err).Code {
	case cherr.CodeNetwork, cherr.CodeTimeout, cherr.CodeCancelled, cherr.CodeUnavailable:
		return true
	}
	return false
}

// parseVersion reads the first three numeric parts of a server version.
func parseVersion(s string) ([3]int, error) {
	var v [3]int
	parts := strings.Split(s, ".")
	if len(parts) < 3 {
		return v, fmt.Errorf("version has fewer than three parts")
	}
	for i := range 3 {
		n, err := strconv.Atoi(parts[i])
		if err != nil || n < 0 {
			return v, fmt.Errorf("version part %d is not a number", i)
		}
		v[i] = n
	}
	return v, nil
}

func less(a, b [3]int) bool {
	for i := range 3 {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return false
}

// versionRE is the whole shape of a ClickHouse version string.
var versionRE = regexp.MustCompile(`^[0-9]{1,4}(\.[0-9]{1,8}){2,3}$`)

// sanitizeVersion returns s when it is a complete version string, else a
// fixed label, so a hostile server cannot put text (or digits taken from
// text) into an error through version().
func sanitizeVersion(s string) string {
	if versionRE.MatchString(s) {
		return s
	}
	return "(unrecognized)"
}

// knownEngines are the database engine names an error may repeat. Any other
// answer is server-controlled text and is never shown.
var knownEngines = map[string]bool{
	"Ordinary": true, "Lazy": true, "Memory": true, "Replicated": true, "MySQL": true,
	"MaterializedMySQL": true, "PostgreSQL": true, "MaterializedPostgreSQL": true, "SQLite": true,
	"DataLakeCatalog": true, "Backup": true, "Filesystem": true, "S3": true, "HDFS": true,
	"Dictionary": true, "Overlay": true,
}

// engineLabel returns the engine name when it is a known engine, else a fixed
// label.
func engineLabel(engine string) string {
	if knownEngines[engine] {
		return engine
	}
	return "(unrecognized)"
}
