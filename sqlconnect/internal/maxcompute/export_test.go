package maxcompute

import (
	"context"
	"io"
	"net/http"
	"net/url"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect"
)

// TableLifecycleForTest returns a table's lifecycle in days, read over REST.
func (db *DB) TableLifecycleForTest(ctx context.Context, relation sqlconnect.RelationRef) (int, error) {
	table, err := db.table(relation)
	if err != nil {
		return 0, err
	}
	if err := readCall(ctx, table.Load); err != nil {
		return 0, err
	}
	return table.LifeCycle(), nil
}

// JobSourceForTest returns the job definition of an instance, as stored by
// the server (GET …/instances/<id>?source).
func (db *DB) JobSourceForTest(ctx context.Context, instanceID string) (string, error) {
	rb := common.ResourceBuilder{ProjectName: db.client.DefaultProjectName()}
	rc := db.client.RestClient()
	var body string
	err := readCall(ctx, func() error {
		return rc.GetWithParseFunc(rb.Instance(instanceID), url.Values{"source": {""}}, nil, func(res *http.Response) error {
			b, err := io.ReadAll(res.Body)
			body = string(b)
			return err
		})
	})
	return body, err
}
