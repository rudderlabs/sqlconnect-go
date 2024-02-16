package snowflake

import (
	"encoding/json"
	"fmt"

	"github.com/snowflakedb/gosnowflake"
)

type Config struct {
	Account   string `json:"account"`
	Warehouse string `json:"warehouse"`
	DBName    string `json:"dbname"`
	User      string `json:"user"`
	Password  string `json:"password"`
	Schema    string `json:"schema"`
	Role      string `json:"role"`

	// RudderSchema is used to override the default rudder schema name during tests
	RudderSchema      string `json:"rudderSchema"`
	KeepSessionAlive  bool   `json:"keepSessionAlive"`
	UseLegacyMappings bool   `json:"useLegacyMappings"`
}

func (c Config) ConnectionString() (dsn string, err error) {
	sc := gosnowflake.Config{
		User:      c.User,
		Password:  c.Password,
		Account:   c.Account,
		Database:  c.DBName,
		Warehouse: c.Warehouse,
		Schema:    c.Schema,
		// since omitempty is not used, default value of role would be "" (empty string).
		// this will ensure backwards compatibility, check line 137 on dsn.go (if cfg.Role != "" {params.Add("role", cfg.Role)})
		Role: c.Role,
	}

	if c.KeepSessionAlive {
		params := make(map[string]*string)
		valueTrue := "true"
		params["client_session_keep_alive"] = &valueTrue
		sc.Params = params
	}

	dsn, err = gosnowflake.DSN(&sc)
	if err != nil {
		err = fmt.Errorf("creating dsn: %v", err)
	}
	return
}

func (c *Config) Parse(configJSON json.RawMessage) error {
	return json.Unmarshal(configJSON, c)
}
