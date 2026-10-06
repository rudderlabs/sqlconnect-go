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
)

const (
	fabricAPIScope = "https://api.fabric.microsoft.com/.default"
	fabricAPIHost  = "https://api.fabric.microsoft.com"
	bootstrapTTL   = 24 * time.Hour
	maxErrorBody   = 64 << 10

	defaultBootstrapTimeout    = 10 * time.Second
	bootstrapHTTPClientTimeout = 30 * time.Second
)

type bootstrapCall struct {
	done chan struct{}
	err  error
}

type bootstrapper struct {
	mu                sync.Mutex
	successes         map[string]time.Time
	inflight          map[string]*bootstrapCall
	ttl               time.Duration
	now               func() time.Time
	client            *http.Client
	apiHost           string
	credentialFactory func(Config) (azcore.TokenCredential, error)
}

func newBootstrapper(client *http.Client) *bootstrapper {
	if client == nil {
		client = &http.Client{Timeout: bootstrapHTTPClientTimeout}
	}
	return &bootstrapper{
		successes: make(map[string]time.Time),
		inflight:  make(map[string]*bootstrapCall),
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
	if call, ok := b.inflight[key]; ok {
		b.mu.Unlock()
		select {
		case <-call.done:
			return call.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	call := &bootstrapCall{done: make(chan struct{})}
	b.inflight[key] = call
	b.mu.Unlock()

	call.err = b.request(ctx, config)
	b.mu.Lock()
	if call.err == nil {
		b.successes[key] = b.now()
	}
	delete(b.inflight, key)
	close(call.done)
	b.mu.Unlock()
	return call.err
}

func (b *bootstrapper) request(ctx context.Context, config Config) error {
	if err := validateFabricWorkspaceID(config.FabricWorkspaceID); err != nil {
		return fmt.Errorf("spn_token_bootstrap: %w", err)
	}
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
		ErrorCode   string `json:"errorCode"`
		RequestID   string `json:"requestId"`
		IsRetriable bool   `json:"isRetriable"`
		Error       *struct {
			ErrorCode   string `json:"errorCode"`
			RequestID   string `json:"requestId"`
			IsRetriable bool   `json:"isRetriable"`
		} `json:"error"`
	}
	_ = json.NewDecoder(io.LimitReader(resp.Body, maxErrorBody)).Decode(&response)
	if response.Error != nil {
		if response.ErrorCode == "" {
			response.ErrorCode = response.Error.ErrorCode
		}
		if response.RequestID == "" {
			response.RequestID = response.Error.RequestID
		}
		response.IsRetriable = response.IsRetriable || response.Error.IsRetriable
	}
	retryable := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError || response.IsRetriable
	message := fmt.Sprintf("spn_token_bootstrap: Fabric API request failed with HTTP %d", resp.StatusCode)
	if response.ErrorCode != "" {
		message += ", errorCode=" + response.ErrorCode
	}
	if response.RequestID != "" {
		message += ", requestId=" + response.RequestID
	}
	message += fmt.Sprintf(", retryable=%t", retryable)
	if !retryable {
		message += "; enable 'Service principals can use Fabric APIs' and grant the service principal the required workspace role"
	}
	return fmt.Errorf("%s", message)
}
