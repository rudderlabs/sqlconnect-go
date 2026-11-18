package odpsclient

import (
	"errors"
	"testing"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/credentials-go/credentials"
	"github.com/stretchr/testify/require"
)

type odpsTestCredential struct {
	model *credentials.CredentialModel
	err   error
}

func (c *odpsTestCredential) GetCredential() (*credentials.CredentialModel, error) {
	return c.model, c.err
}

func (c *odpsTestCredential) GetType() *string {
	kind := "access_key"
	return &kind
}

func (c *odpsTestCredential) GetAccessKeyId() (*string, error) {
	return nil, errors.New("SECRET from deprecated method")
}

func (c *odpsTestCredential) GetAccessKeySecret() (*string, error) {
	return nil, errors.New("SECRET from deprecated method")
}

func (c *odpsTestCredential) GetSecurityToken() (*string, error) {
	return nil, errors.New("SECRET from deprecated method")
}

func (c *odpsTestCredential) GetBearerToken() *string { return nil }

func TestGuardCredentials(t *testing.T) {
	id, key, token := "sensitive id", "sensitive key", "sensitive token"
	source := &odpsTestCredential{model: &credentials.CredentialModel{
		AccessKeyId: &id, AccessKeySecret: &key, SecurityToken: &token,
	}}
	guard := guardCredentials(source)
	model, err := guard.GetCredential()
	require.NoError(t, err)
	require.Equal(t, id, *model.AccessKeyId)
	require.Equal(t, key, *model.AccessKeySecret)
	require.Equal(t, &token, model.SecurityToken)
	*model.AccessKeySecret = "changed"
	next, err := guard.GetCredential()
	require.NoError(t, err)
	require.Equal(t, key, *next.AccessKeySecret)
	source.err = errors.New("SECRET from refresh")
	_, err = guard.GetCredential()
	require.EqualError(t, err, "ODPS credentials unavailable")
	missingToken := guardCredentials(&odpsTestCredential{model: &credentials.CredentialModel{AccessKeyId: &id, AccessKeySecret: &key}})
	_, err = missingToken.GetCredential()
	require.EqualError(t, err, "ODPS credentials unavailable")
	var nilGuard *odpsCredentialGuard
	_, err = nilGuard.GetCredential()
	require.EqualError(t, err, "ODPS credentials unavailable")
}

func TestNewWithEndpoint(t *testing.T) {
	id, key, token := "id", "key", "token"
	provider := &odpsTestCredential{model: &credentials.CredentialModel{AccessKeyId: &id, AccessKeySecret: &key, SecurityToken: &token}}
	for _, tc := range []struct{ project, region string }{
		{"", "cn-shanghai"},
		{"project/escape", "cn-shanghai"},
		{"project", "cn-shanghai.evil.com"},
		{"project", "cn-shanghai:8080"},
		{"project", "Internal"},
	} {
		_, err := NewWithEndpoint(tc.project, tc.region, "https://service."+tc.region+".maxcompute.aliyun.com/api", provider)
		require.Error(t, err)
	}
	endpoint := "https://service.cn-shanghai.maxcompute.aliyun.com/api"
	_, err := NewWithEndpoint("safe_project", "cn-shanghai", endpoint, nil)
	require.ErrorContains(t, err, "credentials")

	for _, invalid := range []string{
		"http://service.cn-shanghai.maxcompute.aliyun.com/api",
		"https://service.cn-beijing.maxcompute.aliyun.com/api",
		"https://service.cn-shanghai.maxcompute.aliyun.com.evil/api",
		"https://service.cn-shanghai.maxcompute.aliyun.com:443/api",
		"https://service.cn-shanghai.maxcompute.aliyun.com/api/",
		"https://user:SECRET@service.cn-shanghai.maxcompute.aliyun.com/api",
	} {
		_, err := NewWithEndpoint("safe_project", "cn-shanghai", invalid, provider)
		require.EqualError(t, err, "invalid ODPS endpoint")
	}
	configured, err := NewWithEndpoint("safe_project", "cn-shanghai", endpoint, provider)
	require.NoError(t, err)
	configuredRest := configured.RestClient()
	require.Equal(t, endpoint, configuredRest.Endpoint())
	require.Equal(t, "safe_project", configured.DefaultProjectName())
	require.Equal(t, "cn-shanghai", configured.Options.RegionId)
	static, err := NewWithAccessKey("safe_project", "cn-shanghai", endpoint, "id", "secret")
	require.NoError(t, err)
	require.IsType(t, &account.AliyunAccount{}, static.Account())
	otherRegion := "https://service.af-south-1.maxcompute.aliyun.com/api"
	_, err = NewWithAccessKey("safe_project", "af-south-1", otherRegion, "id", "secret")
	require.NoError(t, err)
	_, err = NewWithAccessKey("safe_project", "me-central-1", otherRegion, "id", "secret")
	require.EqualError(t, err, "invalid ODPS endpoint")
}

// HttpError.Error has a value receiver; this wrapper tests the pointer form
// without hiding that the SDK itself may return value errors.

// The source's addition to the destination's tests: the guard is what the SDK
// signs with, so a refresh failure must stop before any request is sent.
func TestGuardedSTSAccountFailsBeforeSigning(t *testing.T) {
	id, secret := "SECRET id", "SECRET secret"
	provider := &odpsTestCredential{model: &credentials.CredentialModel{AccessKeyId: &id, AccessKeySecret: &secret}}
	instance, err := NewWithEndpoint("safe_project", "cn-shanghai", "https://service.cn-shanghai.maxcompute.aliyun.com/api", provider)
	require.NoError(t, err)
	sts, ok := instance.Account().(*account.StsAccount)
	require.True(t, ok)
	_, err = sts.Credential()
	require.EqualError(t, err, "ODPS credentials unavailable")
}
