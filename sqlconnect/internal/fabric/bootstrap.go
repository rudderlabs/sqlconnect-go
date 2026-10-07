package fabric

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"golang.org/x/sync/singleflight"
)

const (
	fabricAPIScope = "https://api.fabric.microsoft.com/.default"
	fabricAPIHost  = "https://api.fabric.microsoft.com"
	bootstrapTTL   = 24 * time.Hour
	maxErrorBody   = 64 << 10

	defaultBootstrapTimeout = 10 * time.Second
)

type bootstrapper struct {
	mu                sync.Mutex
	successes         map[string]time.Time
	group             singleflight.Group
	ttl               time.Duration
	now               func() time.Time
	client            *http.Client
	apiHost           string
	credentialFactory func(Config) (azcore.TokenCredential, error)
}

func newBootstrapper(client *http.Client) *bootstrapper {
	if client == nil {
		client = &http.Client{}
	}
	return &bootstrapper{
		successes: make(map[string]time.Time),
		ttl:       bootstrapTTL,
		now:       time.Now,
		client:    client,
		apiHost:   fabricAPIHost,
		credentialFactory: func(config Config) (azcore.TokenCredential, error) {
			return azidentity.NewClientSecretCredential(config.TenantID, config.ClientID, config.ClientSecret, nil)
		},
	}
}

var defaultBootstrapper = newBootstrapper(nil)

func (b *bootstrapper) bootstrap(ctx context.Context, config Config) error {
	key := config.TenantID + "\x00" + config.ClientID + "\x00" + config.FabricWorkspaceID
	b.mu.Lock()
	if at, ok := b.successes[key]; ok && b.now().Sub(at) < b.ttl {
		b.mu.Unlock()
		return nil
	}
	b.mu.Unlock()

	result := b.group.DoChan(key, func() (any, error) {
		b.mu.Lock()
		if at, ok := b.successes[key]; ok && b.now().Sub(at) < b.ttl {
			b.mu.Unlock()
			return struct{}{}, nil
		}
		b.mu.Unlock()

		err := b.request(ctx, config)
		if err == nil {
			b.mu.Lock()
			b.successes[key] = b.now()
			b.mu.Unlock()
		}
		return struct{}{}, err
	})
	select {
	case result := <-result:
		return result.Err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *bootstrapper) request(ctx context.Context, config Config) error {
	credential, err := b.credentialFactory(config)
	if err != nil {
		return fmt.Errorf("spn_token_bootstrap: creating credential: %w", err)
	}
	token, err := credential.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{fabricAPIScope}})
	if err != nil {
		return fmt.Errorf("spn_token_bootstrap: acquiring Fabric API token: %w", err)
	}

	requestURL, err := url.Parse(b.apiHost)
	if err != nil {
		return fmt.Errorf("spn_token_bootstrap: parsing Fabric API URL: %w", err)
	}
	requestURL.Path = strings.TrimRight(requestURL.Path, "/") + "/v1/workspaces/" + url.PathEscape(config.FabricWorkspaceID) + "/items"
	query := requestURL.Query()
	query.Set("recursive", "false")
	requestURL.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return fmt.Errorf("spn_token_bootstrap: creating Fabric API request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token.Token)
	resp, err := b.client.Do(req)
	if err != nil {
		return fmt.Errorf("spn_token_bootstrap: executing Fabric API request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusOK {
		return nil
	}

	var response struct {
		ErrorCode string `json:"errorCode"`
		RequestID string `json:"requestId"`
	}
	_ = json.NewDecoder(io.LimitReader(resp.Body, maxErrorBody)).Decode(&response)
	return fmt.Errorf(
		"spn_token_bootstrap: Fabric API request failed with HTTP %d, errorCode=%s, requestId=%s",
		resp.StatusCode,
		response.ErrorCode,
		response.RequestID,
	)
}
