package fabric

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/stretchr/testify/require"
)

type staticCredential struct {
	token string
	err   error
	scope chan string
}

func TestNewBootstrapperUsesBoundedHTTPClient(t *testing.T) {
	bootstrap := newBootstrapper(nil)
	require.Equal(t, bootstrapHTTPClientTimeout, bootstrap.client.Timeout)
}

func (c staticCredential) GetToken(_ context.Context, opts policy.TokenRequestOptions) (azcore.AccessToken, error) {
	if c.scope != nil {
		c.scope <- opts.Scopes[0]
	}
	return azcore.AccessToken{Token: c.token, ExpiresOn: time.Now().Add(time.Hour)}, c.err
}

func TestBootstrapRequestAndCache(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		require.Equal(t, "/v1/workspaces/11111111-1111-1111-1111-111111111111/items", r.URL.Path)
		require.Equal(t, "false", r.URL.Query().Get("recursive"))
		require.Equal(t, "Bearer token", r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	scope := make(chan string, 1)
	bootstrap := newBootstrapper(server.Client())
	bootstrap.apiHost = server.URL
	bootstrap.credentialFactory = func(Config) (azcore.TokenCredential, error) {
		return staticCredential{token: "token", scope: scope}, nil
	}
	config := Config{TenantID: "tenant", ClientID: "client", FabricWorkspaceID: "11111111-1111-1111-1111-111111111111"}
	require.NoError(t, bootstrap.bootstrap(context.Background(), config))
	require.NoError(t, bootstrap.bootstrap(context.Background(), config))
	require.Equal(t, fabricAPIScope, <-scope)
	require.EqualValues(t, 1, requests.Load())
}

func TestBootstrapCacheIsWorkspaceScoped(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	bootstrap := newBootstrapper(server.Client())
	bootstrap.apiHost = server.URL
	bootstrap.credentialFactory = func(Config) (azcore.TokenCredential, error) { return staticCredential{token: "token"}, nil }

	reusedPrincipal := Config{TenantID: "tenant", ClientID: "client"}
	firstWorkspace := reusedPrincipal
	firstWorkspace.FabricWorkspaceID = "11111111-1111-1111-1111-111111111111"
	secondWorkspace := reusedPrincipal
	secondWorkspace.FabricWorkspaceID = "22222222-2222-2222-2222-222222222222"

	require.NoError(t, bootstrap.bootstrap(context.Background(), firstWorkspace))
	require.NoError(t, bootstrap.bootstrap(context.Background(), secondWorkspace))
	require.Equal(t, []string{
		"/v1/workspaces/11111111-1111-1111-1111-111111111111/items",
		"/v1/workspaces/22222222-2222-2222-2222-222222222222/items",
	}, paths)
}

func TestBootstrapNonSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"errorCode":"Forbidden","requestId":"request","isRetriable":false}}`))
	}))
	defer server.Close()
	bootstrap := newBootstrapper(server.Client())
	bootstrap.apiHost = server.URL
	bootstrap.credentialFactory = func(Config) (azcore.TokenCredential, error) { return staticCredential{token: "token"}, nil }

	err := bootstrap.bootstrap(context.Background(), Config{TenantID: "tenant", ClientID: "client", FabricWorkspaceID: "11111111-1111-1111-1111-111111111111"})
	require.ErrorContains(t, err, "spn_token_bootstrap: Fabric API request failed with HTTP 403")
	require.ErrorContains(t, err, "errorCode=Forbidden")
	require.ErrorContains(t, err, "requestId=request")
	require.ErrorContains(t, err, "retryable=false")
	require.ErrorContains(t, err, "Service principals can use Fabric APIs")
}

func TestBootstrapSingleflight(t *testing.T) {
	var requests atomic.Int32
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	bootstrap := newBootstrapper(server.Client())
	bootstrap.apiHost = server.URL
	bootstrap.credentialFactory = func(Config) (azcore.TokenCredential, error) { return staticCredential{token: "token"}, nil }
	config := Config{TenantID: "tenant", ClientID: "client", FabricWorkspaceID: "11111111-1111-1111-1111-111111111111"}

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Go(func() {
			errs <- bootstrap.bootstrap(context.Background(), config)
		})
	}
	require.Eventually(t, func() bool { return requests.Load() == 1 }, time.Second, time.Millisecond)
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.EqualValues(t, 1, requests.Load())
}

func TestBootstrapWaiterCancellation(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	bootstrap := newBootstrapper(server.Client())
	bootstrap.apiHost = server.URL
	bootstrap.credentialFactory = func(Config) (azcore.TokenCredential, error) { return staticCredential{token: "token"}, nil }
	config := Config{TenantID: "tenant", ClientID: "client", FabricWorkspaceID: "11111111-1111-1111-1111-111111111111"}

	leaderErr := make(chan error, 1)
	go func() { leaderErr <- bootstrap.bootstrap(context.Background(), config) }()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, bootstrap.bootstrap(ctx, config), context.Canceled)
	close(release)
	require.NoError(t, <-leaderErr)
}

func TestBootstrapFailuresAreNotCachedAndReportRetryability(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		request := requests.Add(1)
		if request == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("not-json"))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	bootstrap := newBootstrapper(server.Client())
	bootstrap.apiHost = server.URL
	bootstrap.credentialFactory = func(Config) (azcore.TokenCredential, error) { return staticCredential{token: "token"}, nil }
	config := Config{TenantID: "tenant", ClientID: "client", FabricWorkspaceID: "11111111-1111-1111-1111-111111111111"}

	err := bootstrap.bootstrap(context.Background(), config)
	require.ErrorContains(t, err, "spn_token_bootstrap: Fabric API request failed with HTTP 429")
	require.ErrorContains(t, err, "retryable=true")
	require.NotContains(t, err.Error(), "Service principals can use Fabric APIs")
	require.NoError(t, bootstrap.bootstrap(context.Background(), config))
	require.EqualValues(t, 2, requests.Load())
}

func TestBootstrapCacheExpires(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	bootstrap := newBootstrapper(server.Client())
	bootstrap.apiHost = server.URL
	bootstrap.credentialFactory = func(Config) (azcore.TokenCredential, error) { return staticCredential{token: "token"}, nil }
	bootstrap.ttl = time.Hour
	now := time.Now()
	bootstrap.now = func() time.Time { return now }
	config := Config{TenantID: "tenant", ClientID: "client", FabricWorkspaceID: "11111111-1111-1111-1111-111111111111"}

	require.NoError(t, bootstrap.bootstrap(context.Background(), config))
	now = now.Add(bootstrap.ttl)
	require.NoError(t, bootstrap.bootstrap(context.Background(), config))
	require.EqualValues(t, 2, requests.Load())
}

func TestBootstrapServerErrorIsRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"errorCode":"Unavailable","isRetriable":false}`))
	}))
	defer server.Close()
	bootstrap := newBootstrapper(server.Client())
	bootstrap.apiHost = server.URL
	bootstrap.credentialFactory = func(Config) (azcore.TokenCredential, error) { return staticCredential{token: "token"}, nil }

	err := bootstrap.bootstrap(context.Background(), Config{TenantID: "tenant", ClientID: "client", FabricWorkspaceID: "11111111-1111-1111-1111-111111111111"})
	require.ErrorContains(t, err, "HTTP 503")
	require.ErrorContains(t, err, "errorCode=Unavailable")
	require.ErrorContains(t, err, "retryable=true")
}

func TestBootstrapCancellationAndCredentialError(t *testing.T) {
	bootstrap := newBootstrapper(nil)
	bootstrap.credentialFactory = func(Config) (azcore.TokenCredential, error) {
		return staticCredential{err: context.Canceled}, nil
	}
	err := bootstrap.bootstrap(context.Background(), Config{TenantID: "tenant", ClientID: "client", FabricWorkspaceID: "11111111-1111-1111-1111-111111111111"})
	require.ErrorContains(t, err, "spn_token_bootstrap: acquiring Fabric API token")
	require.ErrorIs(t, err, context.Canceled)

	bootstrap = newBootstrapper(nil)
	bootstrap.credentialFactory = func(Config) (azcore.TokenCredential, error) { return nil, errors.New("credential failed") }
	err = bootstrap.bootstrap(context.Background(), Config{TenantID: "tenant", ClientID: "client", FabricWorkspaceID: "11111111-1111-1111-1111-111111111111"})
	require.ErrorContains(t, err, "spn_token_bootstrap: creating credential")
	require.NotContains(t, err.Error(), "secret")
}
