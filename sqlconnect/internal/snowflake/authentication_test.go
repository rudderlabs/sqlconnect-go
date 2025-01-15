package snowflake_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/snowflake"
)

func TestSnowflakeAuthentication(t *testing.T) {
	t.Run("key pair with passphrase", func(t *testing.T) {
		configJSON, ok := os.LookupEnv("SNOWFLAKE_TEST_AUTH_KEYPAIR_ENCRYPTED_CREDENTIALS")
		if !ok {
			if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
				t.Fatal("SNOWFLAKE_TEST_AUTH_KEYPAIR_ENCRYPTED_CREDENTIALS environment variable not set")
			}
			t.Skip("skipping test due to lack of a test environment")
		}

		db, err := sqlconnect.NewDB(snowflake.DatabaseType, []byte(configJSON))
		require.NoError(t, err, "it should be able to create a new DB")
		defer func() { _ = db.Close() }()
		require.NoError(t, db.Ping(), "it should be able to ping the database")
	})
	t.Run("key pair without passphrase", func(t *testing.T) {
		configJSON, ok := os.LookupEnv("SNOWFLAKE_TEST_AUTH_KEYPAIR_UNENCRYPTED_CREDENTIALS")
		if !ok {
			if os.Getenv("FORCE_RUN_INTEGRATION_TESTS") == "true" {
				t.Fatal("SNOWFLAKE_TEST_AUTH_KEYPAIR_UNENCRYPTED_CREDENTIALS environment variable not set")
			}
			t.Skip("skipping test due to lack of a test environment")
		}
		db, err := sqlconnect.NewDB(snowflake.DatabaseType, []byte(configJSON))
		require.NoError(t, err, "it should be able to create a new DB")
		defer func() { _ = db.Close() }()
		require.NoError(t, db.Ping(), "it should be able to ping the database")
	})
	t.Run("oauth", func(t *testing.T) {
		authCode, ok := os.LookupEnv("SNOWFLAKE_TEST_AUTH_OAUTH_CODE")
		if !ok {
			t.Skip("skipping test due to lack of a test environment")
		}

		configJSON, ok := os.LookupEnv("SNOWFLAKE_TEST_ENVIRONMENT_CREDENTIALS")
		require.True(t, ok, "it should be able to get the environment credentials")
		var conf snowflake.Config
		require.NoError(t, json.Unmarshal([]byte(configJSON), &conf), "it should be able to unmarshal the config")
		// reset username and password
		conf.User = ""
		conf.Password = ""

		// Issue a token
		var accessToken string
		{
			var oauthCreds struct {
				ClientID     string `json:"client_id"`
				ClientSecret string `json:"client_secret"`
			}
			oauthCredsJSON, ok := os.LookupEnv("SNOWFLAKE_TEST_AUTH_OAUTH_CREDENTIALS")
			require.True(t, ok, "it should be able to get the oauth creds")
			require.NoError(t, json.Unmarshal([]byte(oauthCredsJSON), &oauthCreds), "it should be able to unmarshal the oauth creds")
			body := url.Values{}
			body.Add("redirect_uri", "https://localhost.com")
			body.Add("code", authCode)
			body.Add("grant_type", "authorization_code")
			body.Add("scope", fmt.Sprintf("session:role:%s", conf.Role))
			r, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("https://%s.snowflakecomputing.com/oauth/token-request", conf.Account), strings.NewReader(body.Encode()))
			r.Header.Add("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8")
			r.SetBasicAuth(oauthCreds.ClientID, oauthCreds.ClientSecret)
			resp, err := http.DefaultClient.Do(r)
			require.NoError(t, err, "it should be able to issue a token")
			defer func() { _ = resp.Body.Close() }()
			respBody, err := io.ReadAll(resp.Body)
			require.NoError(t, err, "it should be able to read the response body")
			require.Equalf(t, http.StatusOK, resp.StatusCode, "it should be able to issue a token: %s", string(respBody))
			var token struct {
				AccessToken string `json:"access_token"`
			}
			require.NoError(t, json.Unmarshal(respBody, &token), "it should be able to decode the token")
			accessToken = token.AccessToken
		}

		conf.UseOAuth = true
		conf.OAuthToken = accessToken
		oauthConfigJSON, err := json.Marshal(conf)
		require.NoError(t, err, "it should be able to marshal the config")
		db, err := sqlconnect.NewDB(snowflake.DatabaseType, oauthConfigJSON)
		require.NoError(t, err, "it should be able to create a new DB")
		defer func() { _ = db.Close() }()
		require.NoError(t, db.Ping(), "it should be able to ping the database")
		require.NoError(t, db.QueryRow("SELECT 1").Err())
	})
}
