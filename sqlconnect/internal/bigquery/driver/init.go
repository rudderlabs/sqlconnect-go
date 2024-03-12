package driver

import (
	"database/sql"
)

func init() {
	sql.Register("bigquery", &bigQueryDriver{})
}
