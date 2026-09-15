package postgres

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectionStringDoesNotSetSessionTimeZone(t *testing.T) {
	dsn := Config{
		Host:     "postgres.example.com",
		Port:     5432,
		DBName:   "dbname",
		User:     "user",
		Password: "password",
		SSLMode:  "require",
	}.ConnectionString()

	parsed, err := url.Parse(dsn)
	require.NoError(t, err)

	values := parsed.Query()
	require.Equal(t, "require", values.Get("sslmode"))
	require.Empty(t, values.Get("TimeZone"), "sqlconnect-go must inherit the Postgres session timezone unless the surrounding deployment/database/user default sets it")
	require.Empty(t, values.Get("timezone"), "sqlconnect-go must not add a lib/pq timezone startup parameter")
	require.NotContains(t, dsn, "TimeZone=")
	require.NotContains(t, dsn, "timezone=")
}
