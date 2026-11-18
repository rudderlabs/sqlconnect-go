package maxcompute

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testSecret = "SENTINEL_SECRET_value_9f3c"
	testKeyID  = "LTAI5tTestKeyId0001"
)

func validConfig() string {
	return `{"endpoint":"https://service.me-east-1.maxcompute.aliyun.com/api","project":"customer_project","accessKeyId":"` + testKeyID + `","accessKeySecret":"` + testSecret + `","schema":"Default"}`
}

func TestConfigParse(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		var c Config
		require.NoError(t, c.Parse([]byte(validConfig())))
		require.Equal(t, "me-east-1", c.Region())
		require.Equal(t, "customer_project", c.Project)
		require.Equal(t, "default", c.Schema, "the schema is folded to lower case")
	})

	t.Run("schema is optional", func(t *testing.T) {
		var c Config
		require.NoError(t, c.Parse([]byte(`{"endpoint":"https://service.cn-shanghai.maxcompute.aliyun.com/api","project":"abc","accessKeyId":"a","accessKeySecret":"b"}`)))
		require.Empty(t, c.Schema)
		require.Equal(t, "cn-shanghai", c.Region())
	})

	refused := map[string]string{
		"not json":                 `invalid`,
		"trailing data":            validConfig() + `{}`,
		"unknown key":              `{"endpoint":"https://service.me-east-1.maxcompute.aliyun.com/api","project":"customer_project","accessKeyId":"a","accessKeySecret":"b","pasted":"` + testSecret + `"}`,
		"rudderSchema":             `{"endpoint":"https://service.me-east-1.maxcompute.aliyun.com/api","project":"customer_project","accessKeyId":"a","accessKeySecret":"b","rudderSchema":"_rudderstack"}`,
		"no /api":                  `{"endpoint":"https://service.me-east-1.maxcompute.aliyun.com","project":"customer_project","accessKeyId":"a","accessKeySecret":"` + testSecret + `"}`,
		"http":                     `{"endpoint":"http://service.me-east-1.maxcompute.aliyun.com/api","project":"customer_project","accessKeyId":"a","accessKeySecret":"` + testSecret + `"}`,
		"other host":               `{"endpoint":"https://service.me-east-1.maxcompute.aliyun.com.evil/api","project":"customer_project","accessKeyId":"a","accessKeySecret":"` + testSecret + `"}`,
		"port":                     `{"endpoint":"https://service.me-east-1.maxcompute.aliyun.com:443/api","project":"customer_project","accessKeyId":"a","accessKeySecret":"` + testSecret + `"}`,
		"user info":                `{"endpoint":"https://u:` + testSecret + `@service.me-east-1.maxcompute.aliyun.com/api","project":"customer_project","accessKeyId":"a","accessKeySecret":"b"}`,
		"project too short":        `{"endpoint":"https://service.me-east-1.maxcompute.aliyun.com/api","project":"ab","accessKeyId":"a","accessKeySecret":"b"}`,
		"project with a dot":       `{"endpoint":"https://service.me-east-1.maxcompute.aliyun.com/api","project":"a.bc","accessKeyId":"a","accessKeySecret":"b"}`,
		"project starts digit":     `{"endpoint":"https://service.me-east-1.maxcompute.aliyun.com/api","project":"1abc","accessKeyId":"a","accessKeySecret":"b"}`,
		"blank key id":             `{"endpoint":"https://service.me-east-1.maxcompute.aliyun.com/api","project":"customer_project","accessKeyId":" ","accessKeySecret":"b"}`,
		"blank secret":             `{"endpoint":"https://service.me-east-1.maxcompute.aliyun.com/api","project":"customer_project","accessKeyId":"a","accessKeySecret":"  "}`,
		"schema with a backtick":   `{"endpoint":"https://service.me-east-1.maxcompute.aliyun.com/api","project":"customer_project","accessKeyId":"a","accessKeySecret":"` + testSecret + `","schema":"a` + "`" + `b"}`,
		"schema with a dot":        `{"endpoint":"https://service.me-east-1.maxcompute.aliyun.com/api","project":"customer_project","accessKeyId":"a","accessKeySecret":"` + testSecret + `","schema":"a.b"}`,
		"role arn":                 `{"endpoint":"https://service.me-east-1.maxcompute.aliyun.com/api","project":"customer_project","accessKeyId":"a","accessKeySecret":"b","roleArn":"acs:ram::1:role/x"}`,
		"external id":              `{"endpoint":"https://service.me-east-1.maxcompute.aliyun.com/api","project":"customer_project","accessKeyId":"a","accessKeySecret":"b","externalId":"x"}`,
		"secret in a wrong type":   `{"endpoint":"https://service.me-east-1.maxcompute.aliyun.com/api","project":"customer_project","accessKeyId":"a","accessKeySecret":["` + testSecret + `"]}`,
		"key in another case":      `{"endpoint":"https://service.me-east-1.maxcompute.aliyun.com/api","project":"customer_project","accessKeyId":"a","AccessKeySecret":"` + testSecret + `"}`,
		"duplicate key":            `{"endpoint":"https://service.me-east-1.maxcompute.aliyun.com/api","project":"customer_project","accessKeyId":"a","accessKeySecret":"b","accessKeySecret":"` + testSecret + `"}`,
		"duplicate key, folded":    `{"endpoint":"https://service.me-east-1.maxcompute.aliyun.com/api","project":"customer_project","accessKeyId":"a","accessKeySecret":"b","accesskeysecret":"` + testSecret + `"}`,
		"array":                    `[1]`,
		"secret in the key id key": `{"endpoint":"https://service.me-east-1.maxcompute.aliyun.com/api","project":"customer_project","accessKeyId":"` + testSecret + `-x","accessKeySecret":"b"}`,
	}
	for name, cfg := range refused {
		t.Run("refuses "+name, func(t *testing.T) {
			var c Config
			err := c.Parse([]byte(cfg))
			require.Error(t, err)
			require.NotContains(t, err.Error(), testSecret, "no error echoes a value")
			require.NotContains(t, err.Error(), "SENTINEL")
		})
	}

	t.Run("role accounts name the reason", func(t *testing.T) {
		var c Config
		err := c.Parse([]byte(refused["role arn"]))
		require.ErrorIs(t, err, errRoleNotSupported)
	})
}

func TestConfigStringHidesSecret(t *testing.T) {
	var c Config
	require.NoError(t, c.Parse([]byte(validConfig())))
	for _, s := range []string{c.String(), fmt.Sprintf("%v", c), fmt.Sprintf("%+v", c), fmt.Sprintf("%#v", c), fmt.Sprint(c)} {
		require.NotContains(t, s, testSecret)
		require.NotContains(t, s, testKeyID)
		require.Contains(t, s, "LTAI")
		require.Contains(t, s, "customer_project")
	}
}

func TestNewDBMakesNoCall(t *testing.T) {
	db, err := NewDB([]byte(validConfig()))
	require.NoError(t, err)
	require.NoError(t, db.Close())
	_, err = NewDB([]byte(`{}`))
	require.Error(t, err)
}
