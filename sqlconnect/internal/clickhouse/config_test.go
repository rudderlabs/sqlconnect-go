package clickhouse

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	t.Parallel()
	input := `{"host":"10.20.30.40","database":"Analytics","user":"Rudder","password":" secret ","scratchDatabase":"Scratch"}`
	var c Config
	require.NoError(t, c.Parse(json.RawMessage(input)))
	require.Equal(t, 9440, c.Port)
	require.True(t, *c.Secure)
	require.False(t, c.SkipVerify)
	require.Equal(t, " secret ", c.Password)
	opts := c.options()
	require.Equal(t, []string{"10.20.30.40:9440"}, opts.Addr)
	require.Equal(t, 90*time.Second, opts.DialTimeout)
	require.Equal(t, "Analytics", opts.Auth.Database)
	require.NotNil(t, opts.TLS)
	require.False(t, opts.TLS.InsecureSkipVerify)
	require.NoError(t, c.Parse(json.RawMessage(input[:len(input)-1]+`,"skipVerify":true}`)))
	require.True(t, c.options().TLS.InsecureSkipVerify)
	require.NoError(t, c.Parse(json.RawMessage(input[:len(input)-1]+`,"secure":false}`)))
	require.Nil(t, c.options().TLS)
	for _, field := range []string{`,"port":-1`, `,"port":65536`, `,"port":1.5`, `,"host":"https://example.com"`, `,"host":"example.com:9440"`, `,"host":"127.0.0.1"`, `,"scratchDatabase":""`, `,"database":""`, `,"user":""`, `,"settings":{}`, `,"password":123`} {
		err := c.Parse(json.RawMessage(input[:len(input)-1] + field + `}`))
		require.Error(t, err)
		require.NotContains(t, err.Error(), "secret")
	}
}
