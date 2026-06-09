package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

type bigQueryDriver struct{}

type bigQueryConfig struct {
	projectID       string
	scopes          []string
	endpoint        string
	disableAuth     bool
	credentialFile  string
	credentialsJSON string
}

func (b bigQueryDriver) Open(uri string) (driver.Conn, error) {
	config, err := configFromUri(uri)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, config.projectID, optionsFor(config)...)
	if err != nil {
		return nil, err
	}

	return &bigQueryConnection{
		ctx:    ctx,
		client: client,
	}, nil
}

func optionsFor(config *bigQueryConfig) []option.ClientOption {
	opts := []option.ClientOption{option.WithScopes(config.scopes...)}
	if config.endpoint != "" {
		opts = append(opts, option.WithEndpoint(config.endpoint))
	}
	if config.disableAuth {
		// When authentication is disabled, skip credential options to avoid
		// passing conflicting auth options to the client and to avoid failing
		// early on missing/unreadable credential files.
		opts = append(opts, option.WithoutAuthentication())
	} else {
		if config.credentialFile != "" {
			opts = append(opts, option.WithAuthCredentialsFile(option.ServiceAccount, config.credentialFile))
		}
		if config.credentialsJSON != "" {
			opts = append(opts, option.WithAuthCredentialsJSON(option.ServiceAccount, []byte(config.credentialsJSON)))
		}
	}
	return opts
}

func configFromUri(uri string) (*bigQueryConfig, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, invalidConnectionStringError(uri)
	}

	if u.Scheme != "bigquery" {
		return nil, fmt.Errorf("invalid prefix, expected bigquery:// got: %s", uri)
	}

	config := &bigQueryConfig{
		projectID:       u.Hostname(),
		scopes:          getScopes(u.Query()),
		endpoint:        u.Query().Get("endpoint"),
		disableAuth:     u.Query().Get("disable_auth") == "true",
		credentialFile:  u.Query().Get("credential_file"),
		credentialsJSON: u.Query().Get("credentials_json"),
	}

	return config, nil
}

func getScopes(query url.Values) []string {
	q := strings.Trim(query.Get("scopes"), ",")
	if q == "" {
		return []string{}
	}
	return strings.Split(q, ",")
}

func invalidConnectionStringError(uri string) error {
	return fmt.Errorf("invalid connection string: %s", uri)
}
