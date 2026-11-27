package snowflake_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/snowflakedb/gosnowflake/v2"
	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/snowflake"
)

const (
	accountErr = "invalid snowflake account identifier"
	regionErr  = "invalid snowflake region identifier"
)

type identifierCase struct {
	name    string
	account string
	region  string
	wantErr string
}

var identifierCases = []identifierCase{
	{"locator", "xy12345", "", ""},
	{"locator_region", "xy12345.us-east-1", "", ""},
	{"locator_region_cloud", "xy12345.us-east-1.aws", "", ""},
	{"org_account", "myorg-myaccount", "", ""},
	{"privatelink_locator", "xy12345.us-east-1.privatelink", "", ""},
	{"privatelink_org", "myorg-myaccount.privatelink", "", ""},
	{"separate_region", "xy12345", "us-east-1.aws", ""},
	{"china", "xy12345.cn-northwest-1.aws", "", ""},
	{"global", "myorg-myaccount.global", "", ""},
	{"default_region", "xy12345.US-WEST-2", "", ""},
	{"full_domain_is_lexically_valid", "acct.snowflakecomputing.com", "", ""},
	{"case_hyphens_underscores", "-A0_--Z-", "_R--1", ""},
	{"long_label", strings.Repeat("A", 64), "", ""},
	{"empty", "", "", ""},
	{"ascii_spaces_only", "  ", "  ", ""},
	{"ascii_padding", " xy12345 ", " us-east-1 ", ""},
	{"space_at_driver_split", "xy12345. us-east-1", "", ""},
	{"space_before_driver_split", "xy12345 .us-east-1", "", ""},
	{"space_only_embedded_region", "xy12345. ", "", ""},
	{"query_parameters", "xy12345?protocol=http&proxyHost=example.invalid", "", accountErr},
	{"punctuation_after_first_dot_query", "xy12345.us-east-1?authenticator=oauth&proxyPort=1", "", accountErr},
	{"punctuation_after_first_dot_ampersand", "xy12345.aws&proxyHost=x", "", accountErr},
	{"leading_dot", ".acct", "", accountErr},
	{"empty_label", "acct..aws", "", accountErr},
	{"trailing_dot", "acct.", "", accountErr},
	{"space_inside_label", "ac ct", "", accountErr},
	{"tab", "\tacct", "", accountErr},
	{"newline", "acct\n", "", accountErr},
	{"unicode", "acçt", "", accountErr},
	{"percent_escape", "acct%20name", "", accountErr},
	{"slash", "acct/test", "", accountErr},
	{"region_delimiter", "acct", "us-east-1?fixture", regionErr},
	{"region_empty_label", "acct", "us..aws", regionErr},
	{"region_internal_space", "acct", "us. aws", regionErr},
	{"region_tab", "acct", "us-east-1\t", regionErr},
	{"region_unicode", "acct", "éu-west-1", regionErr},
}

func TestConfigParseAccountRegion(t *testing.T) {
	for _, tc := range identifierCases {
		t.Run(tc.name, func(t *testing.T) {
			body, err := json.Marshal(snowflake.Config{Account: tc.account, Region: tc.region})
			require.NoError(t, err)
			var cfg snowflake.Config
			err = cfg.Parse(body)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.account, cfg.Account)
			require.Equal(t, tc.region, cfg.Region)
		})
	}
}

func TestConnectionStringAccountRegion(t *testing.T) {
	for _, tc := range []struct {
		name    string
		account string
		region  string
		wantErr string
	}{
		{"account", "bad name", "", "invalid snowflake account identifier"},
		{"region", "acct", "bad name", "invalid snowflake region identifier"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A struct literal skips Parse. The identifier error must precede key parsing.
			cfg := snowflake.Config{Account: tc.account, Region: tc.region, UseKeyPairAuth: true}
			dsn, err := cfg.ConnectionString()
			require.EqualError(t, err, tc.wantErr)
			require.Empty(t, dsn)
		})
	}

	cfg := snowflake.Config{Host: "acct.privatelink.snowflakecomputing.com", User: "u", Password: "p"}
	_, err := cfg.ConnectionString()
	require.NoError(t, err) // Driver can infer the account from an explicit host, without DNS here.
}

func TestNewDBAccountRegion(t *testing.T) {
	for _, tc := range []struct {
		name    string
		body    string
		wantErr string
	}{
		{"invalid_account_with_host", `{"account":"bad name","host":"192.0.2.1"}`, "invalid snowflake account identifier"},
		{"invalid_region_with_host", `{"account":"acct","region":"bad name","host":"192.0.2.1"}`, "invalid snowflake region identifier"},
		{"empty_account_without_host", `{"user":"u","password":"p"}`, "260000"},
		{"spaces_only_account_without_host", `{"account":"  ","user":"u","password":"p"}`, "260000"},
		{"two_regions", `{"account":"acct.us-east-1","region":"eu-west-1","user":"u","password":"p"}`, "260009"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := snowflake.NewDB([]byte(tc.body))
			require.ErrorContains(t, err, tc.wantErr)
			require.Nil(t, db)
		})
	}

	db, err := snowflake.NewDB([]byte(`{"account":"xy12345.us-east-1.privatelink","user":"u","password":"p"}`))
	require.NoError(t, err)
	require.NoError(t, db.Close()) // Pool opens lazily. No login is needed.
}

func TestSnowflakeHostSyntax(t *testing.T) {
	const hostErr = "invalid snowflake host"
	const hostQuery = "acct?protocol=http&proxyHost=example.invalid&x=.fixture.invalid"
	label63 := strings.Repeat("a", 63)
	name253 := strings.Repeat(label63+".", 3) + strings.Repeat("b", 61)

	for _, tc := range []struct {
		name string
		host string
	}{
		{"derived", ""},
		{"single_label", "warehouse"},
		{"privatelink", "acct.us-east-1.privatelink.snowflakecomputing.com"},
		{"underscore_privatelink", "my_acct.privatelink.snowflakecomputing.com"},
		{"uppercase", "ACCT.SNOWFLAKECOMPUTING.COM"},
		{"root_dot", "acct.snowflakecomputing.com."},
		{"ipv4", "192.0.2.1"},
		{"maximum_name", name253 + "."},
		{"privatelink_org", "org-acct.privatelink.snowflakecomputing.com"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := snowflake.Config{Account: "acct", Host: tc.host, User: "u", Password: "p"}
			dsn, err := cfg.ConnectionString()
			require.NoError(t, err)
			parsed, err := gosnowflake.ParseDSN(dsn)
			require.NoError(t, err)
			want := tc.host
			if want == "" {
				want = "acct.snowflakecomputing.com"
			}
			require.Equal(t, want, parsed.Host)
		})
	}

	for _, tc := range []struct {
		name string
		host string
	}{
		{"query_parameters", hostQuery},
		{"ampersand", "acct&flag"},
		{"userinfo", "user@acct"},
		{"scheme", "https://acct"},
		{"path", "acct/path"},
		{"fragment", "acct#fragment"},
		{"percent", "acct%3Fflag"},
		{"port", "acct:443"},
		{"padding", " acct "},
		{"non_ascii", "acçt.example"},
		{"root_only", "."},
		{"empty_label", "a..example"},
		{"leading_dot", ".example"},
		{"double_root_dot", "acct.example.."},
		{"edge_hyphen", "-a.example"},
		{"final_hyphen", "a-.example"},
		{"overlong_label", label63 + "a.example"},
		{"overlong_name", name253 + "b"},
		{"invalid_octet", "192.0.2.256"},
		{"abbreviated_ipv4", "127.1"},
		{"zero_padded_octet", "192.0.2.01"},
		{"ipv4_root_dot", "192.0.2.1."},
		{"ipv6", "2001:db8::1"},
		{"bracketed_ipv6", "[2001:db8::1]"},
		{"mapped_ipv6", "::ffff:192.0.2.1"},
		{"question_mark", "acct?flag"},
		{"tab", "acct\t.example"},
		{"newline", "acct\n.example"},
		{"space_inside", "ac ct.example"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := snowflake.Config{Account: "acct", Host: tc.host, UseKeyPairAuth: true}
			dsn, err := cfg.ConnectionString()
			require.EqualError(t, err, hostErr)
			require.Empty(t, dsn)
		})
	}

	t.Run("parse_rejects_host_query_before_lookup", func(t *testing.T) {
		body, err := json.Marshal(snowflake.Config{Account: "acct", Host: hostQuery})
		require.NoError(t, err)
		var cfg snowflake.Config
		require.EqualError(t, cfg.Parse(body), hostErr)
	})
}

func TestConnectionStringAccountRegionRows(t *testing.T) {
	for _, tc := range identifierCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := snowflake.Config{Account: tc.account, Region: tc.region, User: "u", Password: "p"}
			dsn, err := cfg.ConnectionString()
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				require.Empty(t, dsn)
				return
			}
			if strings.Trim(tc.account, " ") == "" {
				// Our check passes an empty account; the driver itself then
				// fails with 260000 because there is no host to infer it from.
				require.ErrorContains(t, err, "260000")
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestAccountRegionParityWithDriver fails loudly if a gosnowflake upgrade
// changes which Account and Region values it rejects: the local check must
// reject exactly when the driver returns its invalid-identifier error (260023).
func TestAccountRegionParityWithDriver(t *testing.T) {
	const (
		codeEmptyAccount      = 260000
		codeInvalidIdentifier = 260023
	)
	for _, tc := range identifierCases {
		t.Run(tc.name, func(t *testing.T) {
			_, driverErr := gosnowflake.DSN(&gosnowflake.Config{Account: tc.account, Region: tc.region, User: "u", Password: "p"})
			var sfErr *gosnowflake.SnowflakeError
			driverRejects := false
			if driverErr != nil {
				require.ErrorAs(t, driverErr, &sfErr)
				if sfErr.Number == codeEmptyAccount {
					// The driver fails on an empty account for a different reason
					// (it needs a host to infer one), which is outside this check.
					require.Empty(t, tc.wantErr, "local check must accept what the driver only rejects as an empty account")
					t.Skipf("driver returned %d for an empty account", sfErr.Number)
				}
				require.Equal(t, codeInvalidIdentifier, sfErr.Number, "unexpected driver error: %v", driverErr)
				driverRejects = true
			}
			require.Equal(t, driverRejects, tc.wantErr != "", "local check disagrees with the driver (driver error: %v)", driverErr)
		})
	}
}

func TestProtocolValidation(t *testing.T) {
	const protocolErr = `unsupported protocol "http": only https is supported`

	t.Run("parse rejects http before identifier errors", func(t *testing.T) {
		var cfg snowflake.Config
		require.EqualError(t, cfg.Parse([]byte(`{"account":"bad name","protocol":"http"}`)), protocolErr)
	})
	t.Run("connection string rejects http", func(t *testing.T) {
		cfg := snowflake.Config{Account: "acct", Protocol: "http", UseKeyPairAuth: true}
		dsn, err := cfg.ConnectionString()
		require.EqualError(t, err, protocolErr)
		require.Empty(t, dsn)
	})
	for _, protocol := range []string{"", "https"} {
		t.Run("accepts "+protocol, func(t *testing.T) {
			var cfg snowflake.Config
			require.NoError(t, cfg.Parse([]byte(`{"account":"acct","protocol":"`+protocol+`"}`)))
			cfg = snowflake.Config{Account: "acct", Protocol: protocol, User: "u", Password: "p"}
			_, err := cfg.ConnectionString()
			require.NoError(t, err)
		})
	}
}
