package driver

import (
	"context"
	"database/sql/driver"
	"regexp"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
)

var namedParamsRegexp = regexp.MustCompile(`@[\w]+`)

type bigQueryStatement struct {
	connection *bigQueryConnection
	query      string
}

func (statement bigQueryStatement) Close() error {
	return nil
}

func (statement bigQueryStatement) NumInput() int {
	params := strings.Count(statement.query, "?")
	if params > 0 {
		return params
	}
	uniqueMatches := lo.Uniq(namedParamsRegexp.FindAllString(statement.query, -1))
	return len(uniqueMatches)
}

func (bigQueryStatement) CheckNamedValue(*driver.NamedValue) error {
	return nil
}

func (statement *bigQueryStatement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	query, err := statement.buildQuery(convertParameters(args))
	if err != nil {
		return nil, err
	}

	rowIterator, err := statement.connection.readWithBackoff(ctx, query)
	if err != nil {
		return nil, err
	}

	return &bigQueryResult{rowIterator}, nil
}

func (statement *bigQueryStatement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	query, err := statement.buildQuery(convertParameters(args))
	if err != nil {
		return nil, err
	}

	rowIterator, err := statement.connection.readWithBackoff(ctx, query)
	if err != nil {
		return nil, err
	}

	return &bigQueryRows{
		source: createSourceFromRowIterator(rowIterator),
	}, nil
}

func (statement bigQueryStatement) Exec(args []driver.Value) (driver.Result, error) {
	return nil, driver.ErrSkip
}

func (statement bigQueryStatement) Query(args []driver.Value) (driver.Rows, error) {
	return nil, driver.ErrSkip
}

func (statement bigQueryStatement) buildQuery(args []driver.Value) (*bigquery.Query, error) {
	query, err := statement.connection.query(statement.query)
	if err != nil {
		return nil, err
	}
	query.Parameters, err = statement.buildParameters(args)
	if err != nil {
		return nil, err
	}

	return query, err
}

func (statement bigQueryStatement) buildParameters(args []driver.Value) ([]bigquery.QueryParameter, error) { // nolint: unparam
	if args == nil {
		return nil, nil
	}

	var parameters []bigquery.QueryParameter
	for _, arg := range args {
		parameters = buildParameter(arg, parameters)
	}
	return parameters, nil
}

func buildParameter(arg driver.Value, parameters []bigquery.QueryParameter) []bigquery.QueryParameter {
	namedValue, ok := arg.(driver.NamedValue)
	if ok {
		return buildParameterFromNamedValue(namedValue, parameters)
	}

	logrus.Debugf("-param:%s", arg)

	return append(parameters, bigquery.QueryParameter{
		Value: arg,
	})
}

func buildParameterFromNamedValue(namedValue driver.NamedValue, parameters []bigquery.QueryParameter) []bigquery.QueryParameter {
	if namedValue.Name == "" {
		return append(parameters, bigquery.QueryParameter{
			Value: namedValue.Value,
		})
	} else {
		return append(parameters, bigquery.QueryParameter{
			Name:  namedValue.Name,
			Value: namedValue.Value,
		})
	}
}

func convertParameters(args []driver.NamedValue) []driver.Value {
	var values []driver.Value
	for _, arg := range args {
		values = append(values, arg)
	}
	return values
}
