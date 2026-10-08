package sqlconnect

// ErrorInfo is the classification of a driver error.
type ErrorInfo struct {
	Category   string
	Code       string
	ServerCode int32
}

// ErrorClassifier is an optional interface that a [DB] implements when it can
// classify its own errors. It sits beside [DB] and does not change it.
type ErrorClassifier interface {
	ClassifyError(error) ErrorInfo
}
