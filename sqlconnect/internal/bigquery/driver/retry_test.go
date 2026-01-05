package driver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
)

func TestExecuteWithRetry_Success(t *testing.T) {
	ctx := context.Background()
	callCount := 0

	err := ExecuteWithRetry(ctx, DefaultRetryOptions(), func() error {
		callCount++
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, callCount, "should succeed on first attempt")
}

func TestExecuteWithRetry_NonRetryableError(t *testing.T) {
	ctx := context.Background()
	callCount := 0
	syntaxError := &googleapi.Error{
		Code:    400,
		Message: "Syntax error at position 10",
		Errors: []googleapi.ErrorItem{
			{Reason: "invalidQuery", Message: "Syntax error at position 10"},
		},
	}

	err := ExecuteWithRetry(ctx, DefaultRetryOptions(), func() error {
		callCount++
		return syntaxError
	})

	assert.Error(t, err)
	assert.Equal(t, 1, callCount, "should not retry non-retryable errors")
	assert.True(t, errors.Is(err, syntaxError), "should return the original error")
}

func TestExecuteWithRetry_RetryableError_EventualSuccess(t *testing.T) {
	ctx := context.Background()
	callCount := 0
	rateLimitError := &googleapi.Error{
		Code:    400,
		Message: "too many table update operations",
		Errors: []googleapi.ErrorItem{
			{Reason: "invalidQuery", Message: "Exceeded rate limits: too many table update operations"},
		},
	}

	opts := DefaultRetryOptions()
	opts.InitialBackoff = 10 * time.Millisecond // Speed up test
	opts.MaxBackoff = 50 * time.Millisecond
	opts.Jitter = false

	err := ExecuteWithRetry(ctx, opts, func() error {
		callCount++
		if callCount < 3 {
			return rateLimitError
		}
		return nil // Success on 3rd attempt
	})

	assert.NoError(t, err)
	assert.Equal(t, 3, callCount, "should retry until success")
}

func TestExecuteWithRetry_MaxRetriesExceeded(t *testing.T) {
	ctx := context.Background()
	callCount := 0
	rateLimitError := &googleapi.Error{
		Code:    400,
		Message: "too many table update operations",
		Errors: []googleapi.ErrorItem{
			{Reason: "invalidQuery", Message: "Exceeded rate limits: too many table update operations"},
		},
	}

	opts := DefaultRetryOptions()
	opts.MaxRetries = 3
	opts.InitialBackoff = 1 * time.Millisecond // Speed up test
	opts.MaxBackoff = 5 * time.Millisecond
	opts.Jitter = false

	err := ExecuteWithRetry(ctx, opts, func() error {
		callCount++
		return rateLimitError
	})

	assert.Error(t, err)
	assert.Equal(t, 4, callCount, "should attempt MaxRetries+1 times (initial + retries)")
	assert.Contains(t, err.Error(), "exceeded max retries")
}

func TestExecuteWithRetry_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	callCount := 0
	rateLimitError := &googleapi.Error{
		Code:    400,
		Message: "too many table update operations",
		Errors: []googleapi.ErrorItem{
			{Reason: "invalidQuery", Message: "Exceeded rate limits: too many table update operations"},
		},
	}

	opts := DefaultRetryOptions()
	opts.InitialBackoff = 100 * time.Millisecond // Long enough to cancel

	// Cancel after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := ExecuteWithRetry(ctx, opts, func() error {
		callCount++
		return rateLimitError
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context")
}

func TestExecuteWithRetry_MaxDurationExceeded(t *testing.T) {
	ctx := context.Background()
	callCount := 0
	rateLimitError := &googleapi.Error{
		Code:    400,
		Message: "too many table update operations",
		Errors: []googleapi.ErrorItem{
			{Reason: "invalidQuery", Message: "Exceeded rate limits: too many table update operations"},
		},
	}

	opts := DefaultRetryOptions()
	opts.MaxRetries = 100                    // High limit
	opts.MaxDuration = 50 * time.Millisecond // Short duration
	opts.InitialBackoff = 20 * time.Millisecond
	opts.Jitter = false

	err := ExecuteWithRetry(ctx, opts, func() error {
		callCount++
		return rateLimitError
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exceeded max retry duration")
}

func TestExecuteWithRetry_ExponentialBackoff(t *testing.T) {
	ctx := context.Background()
	callCount := 0
	timestamps := []time.Time{}
	rateLimitError := &googleapi.Error{
		Code:    400,
		Message: "too many table update operations",
		Errors: []googleapi.ErrorItem{
			{Reason: "invalidQuery", Message: "Exceeded rate limits: too many table update operations"},
		},
	}

	opts := RetryOptions{
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		Multiplier:     2.0,
		MaxRetries:     3,
		Jitter:         false, // Disable jitter for predictable timing
		RetryableFunc:  IsBigQueryRateLimitError,
	}

	_ = ExecuteWithRetry(ctx, opts, func() error {
		callCount++
		timestamps = append(timestamps, time.Now())
		return rateLimitError
	})

	require.Equal(t, 4, callCount)
	require.Len(t, timestamps, 4)

	// Check that delays are approximately following exponential backoff
	// First interval: ~20ms, Second: ~40ms, Third: ~80ms
	if len(timestamps) >= 2 {
		interval1 := timestamps[1].Sub(timestamps[0])
		assert.True(t, interval1 >= 15*time.Millisecond && interval1 <= 30*time.Millisecond,
			"first interval should be ~20ms, got %v", interval1)
	}
	if len(timestamps) >= 3 {
		interval2 := timestamps[2].Sub(timestamps[1])
		assert.True(t, interval2 >= 30*time.Millisecond && interval2 <= 60*time.Millisecond,
			"second interval should be ~40ms, got %v", interval2)
	}
	if len(timestamps) >= 4 {
		interval3 := timestamps[3].Sub(timestamps[2])
		assert.True(t, interval3 >= 60*time.Millisecond && interval3 <= 120*time.Millisecond,
			"third interval should be ~80ms, got %v", interval3)
	}
}

func TestRetryOptionsFromConfig(t *testing.T) {
	t.Run("nil config uses defaults", func(t *testing.T) {
		opts := RetryOptionsFromConfig(nil)
		assert.Equal(t, DefaultInitialBackoff, opts.InitialBackoff)
		assert.Equal(t, DefaultMaxBackoff, opts.MaxBackoff)
		assert.Equal(t, DefaultMaxRetries, opts.MaxRetries)
	})

	t.Run("config overrides defaults", func(t *testing.T) {
		maxRetries := 5
		maxDuration := 10 * time.Minute
		config := &RetryConfig{
			MaxRetries:       &maxRetries,
			MaxRetryDuration: &maxDuration,
		}

		opts := RetryOptionsFromConfig(config)
		assert.Equal(t, 5, opts.MaxRetries)
		assert.Equal(t, 10*time.Minute, opts.MaxDuration)
		// Other fields should still have defaults
		assert.Equal(t, DefaultInitialBackoff, opts.InitialBackoff)
		assert.Equal(t, DefaultMaxBackoff, opts.MaxBackoff)
	})
}

func TestDefaultRetryOptions(t *testing.T) {
	opts := DefaultRetryOptions()

	assert.Equal(t, 1*time.Second, opts.InitialBackoff)
	assert.Equal(t, 32*time.Second, opts.MaxBackoff)
	assert.Equal(t, 2.0, opts.Multiplier)
	assert.Equal(t, 10, opts.MaxRetries)
	assert.True(t, opts.Jitter)
	assert.NotNil(t, opts.RetryableFunc)
}

func TestExecuteWithDefaultRetry(t *testing.T) {
	ctx := context.Background()
	callCount := 0

	err := ExecuteWithDefaultRetry(ctx, func() error {
		callCount++
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, callCount)
}

func TestExecuteWithRetry_HTTP5xxRetry(t *testing.T) {
	ctx := context.Background()
	callCount := 0
	serverError := &googleapi.Error{
		Code:    503,
		Message: "Service Unavailable",
	}

	opts := DefaultRetryOptions()
	opts.InitialBackoff = 10 * time.Millisecond
	opts.MaxBackoff = 50 * time.Millisecond
	opts.Jitter = false
	// Use the broader retryable check that includes 5xx
	opts.RetryableFunc = IsBigQueryRetryableError

	err := ExecuteWithRetry(ctx, opts, func() error {
		callCount++
		if callCount < 2 {
			return serverError
		}
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 2, callCount, "should retry 503 errors")
}



