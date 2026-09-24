package sqlconnect_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

var errFactoryCalled = errors.New("factory called")

func TestNewDB(t *testing.T) {
	t.Run("returns an error for an invalid database name", func(t *testing.T) {
		_, err := sqlconnect.NewDB("invalid", []byte{})
		require.Error(t, err, "should return error for invalid db name")
	})

	t.Run("forwards Google client options to an option-aware factory", func(t *testing.T) {
		const name = "test-google-client-options"
		customOption := option.WithEndpoint("https://bigquery.example.invalid")
		var capturedOptions []option.ClientOption
		sqlconnect.RegisterDBFactoryWithGoogleClientOptions(name, func(_ json.RawMessage, opts ...option.ClientOption) (sqlconnect.DB, error) {
			capturedOptions = opts
			return nil, errFactoryCalled
		})

		_, err := sqlconnect.NewDB(name, nil, sqlconnect.WithGoogleClientOptions(customOption))
		require.ErrorIs(t, err, errFactoryCalled)
		require.Equal(t, []option.ClientOption{customOption}, capturedOptions)
	})

	t.Run("constructs BigQuery with custom options and no credentials", func(t *testing.T) {
		for name, config := range map[string]string{
			"absent":       `{"project":"test-project"}`,
			"empty string": `{"project":"test-project","credentials":""}`,
			"empty object": `{"project":"test-project","credentials":"{}"}`,
			"whitespace":   `{"project":"test-project","credentials":"  "}`,
		} {
			t.Run(name, func(t *testing.T) {
				db, err := sqlconnect.NewDB(
					"bigquery",
					json.RawMessage(config),
					sqlconnect.WithGoogleClientOptions(option.WithoutAuthentication()),
				)
				require.NoError(t, err)
				require.NoError(t, db.Close())
			})
		}
	})

	t.Run("keeps legacy factories compatible", func(t *testing.T) {
		const name = "test-legacy-factory"
		called := false
		sqlconnect.RegisterDBFactory(name, func(_ json.RawMessage) (sqlconnect.DB, error) {
			called = true
			return nil, errFactoryCalled
		})

		_, err := sqlconnect.NewDB(name, nil, sqlconnect.WithGoogleClientOptions(option.WithoutAuthentication()))
		require.ErrorIs(t, err, errFactoryCalled)
		require.True(t, called)
	})
}
