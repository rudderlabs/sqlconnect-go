package sqlconnect

// ErrorInfo classifies an error without granting permission to retry an operation.
type ErrorInfo struct {
	Category   string
	Code       string
	ServerCode int32
}

// ErrorClassifier is an optional capability implemented by warehouse drivers.
type ErrorClassifier interface {
	ClassifyError(error) ErrorInfo
}
