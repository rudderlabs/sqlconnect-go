package clickhouse

type driverError struct {
	code, message string
	cause         error
}

func (e *driverError) Error() string                       { return e.code + ": " + e.message }
func (e *driverError) Unwrap() error                       { return e.cause }
func adapterError(code, message string, cause error) error { return &driverError{code, message, cause} }
