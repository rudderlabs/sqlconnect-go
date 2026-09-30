package clickhouse_test

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/clickhousequery"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/clickhouse"
)

func validJSON(mut func(m map[string]any)) json.RawMessage {
	m := map[string]any{
		"host": "ch.example.com", "database": "analytics", "user": "rudder_retl",
		"password": "s3cret", "secure": true, "scratchDatabase": "_rudderstack_ws",
	}
	if mut != nil {
		mut(m)
	}
	b, _ := json.Marshal(m)
	return b
}

func requireConfigInvalid(t *testing.T, err error, field string) {
	t.Helper()
	d, ok := clickhousequery.Describe(err)
	require.True(t, ok, "%v", err)
	require.Equal(t, "CH_CONFIG_INVALID", d.Code)
	require.Equal(t, field, d.Field, "%v", err)
}

func TestSQ2_CredentialContract(t *testing.T) {
	cfg, err := clickhouse.ParseConfigForTest(validJSON(nil), false)
	require.NoError(t, err)
	require.Equal(t, 8443, cfg.PortOrDefault())
	require.Equal(t, clickhouse.DefaultPort, cfg.PortOrDefault())

	cfg, err = clickhouse.ParseConfigForTest(validJSON(func(m map[string]any) { m["port"] = 9440 }), false)
	require.NoError(t, err)
	require.Equal(t, 9440, cfg.PortOrDefault())

	for name, tc := range map[string]struct {
		mut   func(map[string]any)
		field string
	}{
		"blank host":            {func(m map[string]any) { m["host"] = "" }, "host"},
		"blank database":        {func(m map[string]any) { m["database"] = "" }, "database"},
		"blank user":            {func(m map[string]any) { m["user"] = "" }, "user"},
		"blank scratch":         {func(m map[string]any) { m["scratchDatabase"] = "" }, "scratchDatabase"},
		"port fraction":         {func(m map[string]any) { m["port"] = 0.5 }, "port"},
		"port zero":             {func(m map[string]any) { m["port"] = 0 }, "port"},
		"negative port":         {func(m map[string]any) { m["port"] = -1 }, "port"},
		"port 65536":            {func(m map[string]any) { m["port"] = 65536 }, "port"},
		"port string":           {func(m map[string]any) { m["port"] = "8443" }, "port"},
		"dash in database":      {func(m map[string]any) { m["database"] = "my-db" }, "database"},
		"email user":            {func(m map[string]any) { m["user"] = "analyst@example.com" }, "user"},
		"skipVerify":            {func(m map[string]any) { m["skipVerify"] = true }, "skipVerify"},
		"empty password":        {func(m map[string]any) { m["password"] = "" }, "password"},
		"control char password": {func(m map[string]any) { m["password"] = "p\u0000w" }, "password"},
		"edge space password":   {func(m map[string]any) { m["password"] = " pw" }, "password"},
		"nbsp edge password":    {func(m map[string]any) { m["password"] = "pw\u00a0" }, "password"},
		"cluster field":         {func(m map[string]any) { m["cluster"] = "c1" }, "cluster"},
		"secure absent":         {func(m map[string]any) { delete(m, "secure") }, "secure"},
		"secure false":          {func(m map[string]any) { m["secure"] = false }, "secure"},
		"secure null":           {func(m map[string]any) { m["secure"] = nil }, "secure"},
		"secure string":         {func(m map[string]any) { m["secure"] = "true" }, "secure"},
		"password null":         {func(m map[string]any) { m["password"] = nil }, "password"},
		"non-canonical ipv4":    {func(m map[string]any) { m["host"] = "0x7f000001" }, "host"},
		"host with port":        {func(m map[string]any) { m["host"] = "h.example.com:8443" }, "host"},
		"ipv6 literal":          {func(m map[string]any) { m["host"] = "::1" }, "host"},
		"host 254 chars":        {func(m map[string]any) { m["host"] = strings.Repeat("a.", 126) + "ab" }, "host"},
		"key case differs":      {func(m map[string]any) { m["Secure"] = false }, ""},
		"key case only":         {func(m map[string]any) { delete(m, "host"); m["HOST"] = "ch.example.com" }, ""},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := clickhouse.ParseConfigForTest(validJSON(tc.mut), false)
			requireConfigInvalid(t, err, tc.field)
		})
	}

	_, err = clickhouse.ParseConfigForTest(validJSON(func(m map[string]any) { m["database"] = "my-db" }), false)
	require.Contains(t, err.Error(), "Use letters, digits and underscores, start with a letter or underscore, at most 128 characters.")

	cfg, err = clickhouse.ParseConfigForTest(validJSON(func(m map[string]any) { m["password"] = "p w\u00a0x" }), false)
	require.NoError(t, err, "inner whitespace is allowed")
	require.Equal(t, "p w\u00a0x", cfg.Password, "never trimmed")

	_, err = clickhouse.ParseConfigForTest(validJSON(func(m map[string]any) { m["secure"] = false }), true)
	require.NoError(t, err, "secure=false only under the test-only AllowPlainHTTP policy")

	_, err = clickhouse.ParseConfig(validJSON(func(m map[string]any) { m["secure"] = false }))
	requireConfigInvalid(t, err, "secure")
}

func TestSQ2_MalformedDocuments(t *testing.T) {
	valid := string(validJSON(nil))
	for name, raw := range map[string]string{
		"empty":          ``,
		"null":           `null`,
		"array":          `[]`,
		"string":         `"x"`,
		"truncated":      valid[:len(valid)-1],
		"trailing value": valid + ` {}`,
		"trailing brace": valid + `}`,
		"two documents":  valid + valid,
		"duplicate key":  `{"host":"ch.example.com","host":"evil.example.com","database":"analytics","user":"u","password":"pw","secure":true,"scratchDatabase":"_s"}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := clickhouse.ParseConfigForTest(json.RawMessage(raw), false)
			d, ok := clickhousequery.Describe(err)
			require.True(t, ok, "%v", err)
			require.Equal(t, "CH_CONFIG_INVALID", d.Code)
		})
	}
}

func TestSQ2_ErrorsNeverEchoValues(t *testing.T) {
	const sentinel = "Sentinel_Value_42"
	for name, mut := range map[string]func(map[string]any){
		"password":   func(m map[string]any) { m["password"] = " " + sentinel },
		"type error": func(m map[string]any) { m["password"] = []string{sentinel} },
		"host":       func(m map[string]any) { m["host"] = "https://" + sentinel },
		"database":   func(m map[string]any) { m["database"] = sentinel + "-x" },
		"long key":   func(m map[string]any) { m[strings.Repeat("k", 200)+sentinel] = "x" },
		"odd key":    func(m map[string]any) { m["pw="+sentinel] = "x" },
		"short key":  func(m map[string]any) { m[sentinel] = "x" },
	} {
		t.Run(name, func(t *testing.T) {
			_, err := clickhouse.ParseConfigForTest(validJSON(mut), false)
			require.Error(t, err)
			require.NotContains(t, err.Error(), sentinel)
			d, _ := clickhousequery.Describe(err)
			require.NotContains(t, d.Field, sentinel)
		})
	}
}

func TestConfig_FormatRedactsPassword(t *testing.T) {
	const sentinel = "Sentinel_Password_42"
	cfg, err := clickhouse.ParseConfigForTest(validJSON(func(m map[string]any) { m["password"] = sentinel }), false)
	require.NoError(t, err)
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		out := fmt.Sprintf(verb, cfg)
		require.NotContains(t, out, sentinel, verb)
		require.Contains(t, out, "ch.example.com", verb)
		require.NotContains(t, fmt.Sprintf(verb, &cfg), sentinel, verb)
	}
	require.Equal(t, sentinel, cfg.Password, "formatting never changes the value")
}

func TestSQ6_ExcludedConfiguration(t *testing.T) {
	for _, key := range []string{
		"protocol", "nativePort", "caCertificate", "tunnel_info", "sshHost",
		"cluster", "settings", "timeout", "allowLoopback", "allowPlainHTTP", "skipHostValidation",
	} {
		_, err := clickhouse.ParseConfigForTest(validJSON(func(m map[string]any) { m[key] = "x" }), false)
		requireConfigInvalid(t, err, key)
	}
	require.Equal(t, 8, reflect.TypeOf(clickhouse.Config{}).NumField())
}

func TestSQ25_ScratchExclusions(t *testing.T) {
	for _, name := range []string{
		"default", "DEFAULT", "Default", "system", "SYSTEM", "System",
		"information_schema", "INFORMATION_SCHEMA", "Information_Schema", "analytics", "ANALYTICS",
	} {
		_, err := clickhouse.ParseConfigForTest(validJSON(func(m map[string]any) { m["scratchDatabase"] = name }), false)
		requireConfigInvalid(t, err, "scratchDatabase")
		require.Contains(t, err.Error(), "The scratch database must differ from the customer database, default, system and information_schema.")
	}
}

// fieldCase is one entry of the shared rudder-integrations-config fixture.
// configBackendVerdict and schemaVerdict belong to other surfaces; the Go
// parser follows verdict.
type fieldCase struct {
	ID       string `json:"id"`
	Field    string `json:"field"`
	Input    string `json:"input"`
	Verdict  string `json:"verdict"`
	Error    string `json:"error"`
	Database string `json:"database"`
}

// fixtureTargets maps a fixture field onto the account keys the case applies to.
// "name" is the shared pattern of database, user and scratchDatabase.
func fixtureTargets(field string) []string {
	if field == "name" {
		return []string{"database", "user", "scratchDatabase"}
	}
	return []string{field}
}

func TestSQ2_SharedFieldFixtures(t *testing.T) {
	raw, err := os.ReadFile("testdata/clickhouse-fields.json")
	require.NoError(t, err)
	var f struct {
		Cases []fieldCase `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &f))
	require.Len(t, f.Cases, 75, "the fixture is a byte copy of rudder-integrations-config")

	perField := map[string]int{}
	for _, c := range f.Cases {
		require.Contains(t, []string{"pass", "fail"}, c.Verdict, c.ID)
		for _, key := range fixtureTargets(c.Field) {
			t.Run(c.ID+"/"+key, func(t *testing.T) {
				cfg, err := clickhouse.ParseConfigForTest(validJSON(func(m map[string]any) {
					m[key] = c.Input
					switch {
					case c.Database != "":
						m["database"] = c.Database
					case key == "scratchDatabase" && strings.EqualFold(c.Input, "analytics"):
						// Keeps the name case off the scratch rule, which S20 to S27 cover.
						m["database"] = "customer_db"
					case key == "database" && strings.EqualFold(c.Input, "_rudderstack_ws"):
						m["scratchDatabase"] = "_scratch_other"
					}
				}), false)
				if c.Verdict == "pass" {
					require.NoError(t, err, "%s: %s=%q", c.ID, key, c.Input)
					got := map[string]string{
						"host": cfg.Host, "database": cfg.Database, "user": cfg.User,
						"password": cfg.Password, "scratchDatabase": cfg.ScratchDatabase,
					}[key]
					require.Equal(t, c.Input, got, "%s: exact bytes, never trimmed or folded", c.ID)
					return
				}
				requireConfigInvalid(t, err, key)
				require.NotEmpty(t, c.Error, c.ID)
				require.Contains(t, err.Error(), c.Error, "%s: %s=%q", c.ID, key, c.Input)
			})
			perField[c.Field]++
		}
	}
	for _, field := range []string{"host", "name", "scratchDatabase", "password"} {
		require.Positive(t, perField[field], "no case ran for %s", field)
	}
	require.Len(t, perField, 4, "unexpected fixture field: %v", perField)
}
