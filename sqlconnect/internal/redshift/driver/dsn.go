package driver

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
)

type RedshiftConfig struct {
	ClusterIdentifier   string        `json:"clusterIdentifier"`
	Database            string        `json:"database"`
	DbUser              string        `json:"user"`
	WorkgroupName       string        `json:"workgroupName"`
	SecretsARN          string        `json:"secretsARN"`
	Region              string        `json:"region"`
	SharedConfigProfile string        `json:"sharedConfigProfile"`
	AccessKeyID         string        `json:"accessKeyId"`
	SecretAccessKey     string        `json:"secretAccessKey"`
	SessionToken        string        `json:"sessionToken"`
	Timeout             time.Duration `json:"timeout"`          // default: no timeout
	MinPolling          time.Duration `json:"polling"`          // default: 10ms
	MaxPolling          time.Duration `json:"maxPolling"`       // default: 5s
	RetryMaxAttempts    int           `json:"retryMaxAttempts"` // default: 20

	Params url.Values
}

func (cfg *RedshiftConfig) Sanitize() {
	if cfg.ClusterIdentifier != "" {
		cfg.WorkgroupName = ""
	}
	if cfg.WorkgroupName != "" {
		cfg.DbUser = ""
	}
}

func (cfg *RedshiftConfig) LoadOpts() []func(*config.LoadOptions) error {
	var opts []func(*config.LoadOptions) error
	if cfg.SharedConfigProfile != "" {
		opts = append(opts, config.WithSharedConfigProfile(cfg.SharedConfigProfile))
	}
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID,
			cfg.SecretAccessKey,
			cfg.SessionToken,
		)))
	}
	opts = append(opts, config.WithRetryMaxAttempts(cfg.GetRetryMaxAttempts()))
	return opts
}

func (cfg *RedshiftConfig) Opts() []func(*redshiftdata.Options) {
	var opts []func(*redshiftdata.Options)
	if cfg.Region != "" {
		opts = append(opts, func(o *redshiftdata.Options) {
			o.Region = cfg.Region
		})
	}
	return opts
}

func (cfg *RedshiftConfig) String() string {
	base := strings.TrimPrefix(cfg.baseString(), "//")
	if base == "" {
		return ""
	}
	params := url.Values{}
	for key, value := range cfg.Params {
		params[key] = append([]string{}, value...)
	}
	if cfg.Timeout != 0 {
		params.Add("timeout", cfg.Timeout.String())
	} else {
		params.Del("timeout")
	}
	if cfg.MinPolling != 0 {
		params.Add("minPolling", cfg.MinPolling.String())
	} else {
		params.Del("minPolling")
	}
	if cfg.MaxPolling != 0 {
		params.Add("maxPolling", cfg.MaxPolling.String())
	} else {
		params.Del("maxPolling")
	}
	if cfg.RetryMaxAttempts > 0 {
		params.Add("retryMaxAttempts", strconv.Itoa(cfg.RetryMaxAttempts))
	} else {
		params.Del("retryMaxAttempts")
	}
	if cfg.Region != "" {
		params.Add("region", cfg.Region)
	} else {
		params.Del("region")
	}
	if cfg.SharedConfigProfile != "" {
		params.Add("sharedConfigProfile", cfg.SharedConfigProfile)
	} else {
		params.Del("sharedConfigProfile")
	}
	if cfg.AccessKeyID != "" {
		params.Add("accessKeyId", cfg.AccessKeyID)
	} else {
		params.Del("accessKeyId")
	}
	if cfg.SecretAccessKey != "" {
		params.Add("secretAccessKey", cfg.SecretAccessKey)
	} else {
		params.Del("secretAccessKey")
	}
	if cfg.SessionToken != "" {
		params.Add("sessionToken", cfg.SessionToken)
	} else {
		params.Del("sessionToken")
	}
	encodedParams := params.Encode()
	if encodedParams != "" {
		return base + "?" + encodedParams
	}
	return base
}

func (cfg *RedshiftConfig) setParams(params url.Values) error {
	var err error
	cfg.Params = params
	if params.Has("timeout") {
		cfg.Timeout, err = time.ParseDuration(params.Get("timeout"))
		if err != nil {
			return fmt.Errorf("parse timeout as duration: %w", err)
		}
		cfg.Params.Del("timeout")
	}
	if params.Has("minPolling") {
		cfg.MinPolling, err = time.ParseDuration(params.Get("minPolling"))
		if err != nil {
			return fmt.Errorf("parse min polling as duration: %w", err)
		}
		cfg.Params.Del("minPolling")
	}
	if params.Has("maxPolling") {
		cfg.MaxPolling, err = time.ParseDuration(params.Get("maxPolling"))
		if err != nil {
			return fmt.Errorf("parse max polling as duration: %w", err)
		}
		cfg.Params.Del("maxPolling")
	}
	if params.Has("retryMaxAttempts") {
		cfg.RetryMaxAttempts, err = strconv.Atoi(params.Get("retryMaxAttempts"))
		if err != nil {
			return fmt.Errorf("parse retry max attempts as int: %w", err)
		}
		cfg.Params.Del("retryMaxAttempts")
	}
	if params.Has("region") {
		cfg.Region = params.Get("region")
		cfg.Params.Del("region")
	}
	if params.Has("sharedConfigProfile") {
		cfg.SharedConfigProfile = params.Get("sharedConfigProfile")
		cfg.Params.Del("sharedConfigProfile")
	}
	if params.Has("accessKeyId") {
		cfg.AccessKeyID = params.Get("accessKeyId")
		cfg.Params.Del("accessKeyId")
	}
	if params.Has("secretAccessKey") {
		cfg.SecretAccessKey = params.Get("secretAccessKey")
		cfg.Params.Del("secretAccessKey")
	}
	if params.Has("sessionToken") {
		cfg.SessionToken = params.Get("sessionToken")
		cfg.Params.Del("sessionToken")
	}
	if len(cfg.Params) == 0 {
		cfg.Params = nil
	}
	return nil
}

func (cfg *RedshiftConfig) baseString() string {
	if cfg.SecretsARN != "" {
		return cfg.SecretsARN
	}
	var u url.URL
	if cfg.ClusterIdentifier != "" && cfg.DbUser != "" {
		u.Host = fmt.Sprintf("cluster(%s)", cfg.ClusterIdentifier)
		u.User = url.User(cfg.DbUser)
	}
	if cfg.WorkgroupName != "" {
		u.Host = fmt.Sprintf("workgroup(%s)", cfg.WorkgroupName)
	}
	if u.Host == "" || cfg.Database == "" {
		return ""
	}
	u.Path = cfg.Database
	return u.String()
}

func (cfg *RedshiftConfig) GetMinPolling() time.Duration {
	if cfg.MinPolling == 0 {
		return 10 * time.Millisecond
	}
	return cfg.MinPolling
}

func (cfg *RedshiftConfig) GetMaxPolling() time.Duration {
	if cfg.MaxPolling == 0 {
		return 5 * time.Second
	}
	return cfg.MaxPolling
}

func (cfg *RedshiftConfig) GetRetryMaxAttempts() int {
	if cfg.RetryMaxAttempts <= 0 {
		return 20
	}
	return cfg.RetryMaxAttempts
}

func ParseDSN(dsn string) (*RedshiftConfig, error) {
	if dsn == "" {
		return nil, ErrDSNEmpty
	}
	if strings.HasPrefix(dsn, "arn:") {
		parts := strings.Split(dsn, "?")
		cfg := &RedshiftConfig{
			SecretsARN: parts[0],
		}
		if len(parts) >= 2 {
			params, err := url.ParseQuery(strings.Join(parts[1:], "?"))
			if err != nil {
				return nil, fmt.Errorf("dsn is invalid: can not parse query params: %w", err)
			}
			if err := cfg.setParams(params); err != nil {
				return nil, fmt.Errorf("dsn is invalid: set query params: %w", err)
			}
		}
		return cfg, nil
	}
	u, err := url.Parse("redshift-data://" + dsn)
	if err != nil {
		return nil, fmt.Errorf("dsn is invalid: %w", err)
	}
	cfg := &RedshiftConfig{
		Database: strings.TrimPrefix(u.Path, "/"),
	}
	if cfg.Database == "" {
		return nil, errors.New("dsn is invalid: missing database")
	}
	if err := cfg.setParams(u.Query()); err != nil {
		return nil, fmt.Errorf("dsn is invalid: set query params: %w", err)
	}
	if strings.HasPrefix(u.Host, "cluster(") {
		cfg.DbUser = u.User.Username()
		cfg.ClusterIdentifier = strings.TrimSuffix(strings.TrimPrefix(u.Host, "cluster("), ")")
		return cfg, nil
	}
	if strings.HasPrefix(u.Host, "workgroup(") {
		cfg.WorkgroupName = strings.TrimSuffix(strings.TrimPrefix(u.Host, "workgroup("), ")")
		return cfg, nil
	}
	return nil, errors.New("dsn is invalid: workgroup(name)/database or username@cluster(name)/database or secrets_arn")
}
