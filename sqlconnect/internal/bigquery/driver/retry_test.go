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

func TestRetry_NilConfig_NoRetry(t *testing.T) {
	ctx := context.Background()
	callCount := 0

	// With nil config, retry() should execute once without retry
	err := retry(ctx, nil, func() error {
		callCount++
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, callCount, "should execute once")
}

func TestRetry_NilConfig_ReturnsErrorDirectly(t *testing.T) {
	ctx := context.Background()
	testErr := errors.New("test error")

	// With nil config, retry() should return the error directly without retry
	err := retry(ctx, nil, func() error {
		return testErr
	})

	assert.Equal(t, testErr, err)
}

func TestRetry_Success(t *testing.T) {
	ctx := context.Background()
	callCount := 0
	config := DefaultRetryConfig()

	err := retry(ctx, &config, func() error {
		callCount++
		return nil
	})

	assert.NoError(t, err)
	assert.Equal(t, 1, callCount, "should succeed on first attempt")
}

func TestRetry_NonRetryableError(t *testing.T) {
	ctx := context.Background()
	callCount := 0
	syntaxError := &googleapi.Error{
		Code:    400,
		Message: "Syntax error at position 10",
		Errors: []googleapi.ErrorItem{
			{Reason: "invalidQuery", Message: "Syntax error at position 10"},
		},
	}
	config := DefaultRetryConfig()

	err := retry(ctx, &config, func() error {
		callCount++
		return syntaxError
	})

	assert.Error(t, err)
	assert.Equal(t, 1, callCount, "should not retry non-retryable errors")
	assert.True(t, errors.Is(err, syntaxError), "should return the original error")
}

func TestRetry_RetryableError_EventualSuccess(t *testing.T) {
	ctx := context.Background()
	callCount := 0
	rateLimitError := &googleapi.Error{
		Code:    400,
		Message: "too many table update operations",
		Errors: []googleapi.ErrorItem{
			{Reason: "invalidQuery", Message: "Exceeded rate limits: too many table update operations"},
		},
	}

	config := RetryConfig{
		InitialInterval:     10 * time.Millisecond,
		RandomizationFactor: 0, // No jitter for predictable timing
		Multiplier:          1.5,
		MaxInterval:         50 * time.Millisecond,
		MaxRetries:          10,
		MaxElapsedTime:      5 * time.Second,
	}

	err := retry(ctx, &config, func() error {
		callCount++
		if callCount < 3 {
			return rateLimitError
		}
		return nil // Success on 3rd attempt
	})

	assert.NoError(t, err)
	assert.Equal(t, 3, callCount, "should retry until success")
}

func TestRetry_MaxRetriesExceeded(t *testing.T) {
	ctx := context.Background()
	callCount := 0
	rateLimitError := &googleapi.Error{
		Code:    400,
		Message: "too many table update operations",
		Errors: []googleapi.ErrorItem{
			{Reason: "invalidQuery", Message: "Exceeded rate limits: too many table update operations"},
		},
	}

	config := RetryConfig{
		InitialInterval:     1 * time.Millisecond,
		RandomizationFactor: 0,
		Multiplier:          1.5,
		MaxInterval:         5 * time.Millisecond,
		MaxRetries:          3, // 3 retries = 4 total attempts (initial + 3 retries)
		MaxElapsedTime:      5 * time.Second,
	}

	err := retry(ctx, &config, func() error {
		callCount++
		return rateLimitError
	})

	assert.Error(t, err)
	assert.Equal(t, 4, callCount, "should attempt MaxRetries+1 times (initial + retries)")
	assert.Contains(t, err.Error(), "too many table update operations")
}

func TestRetry_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	callCount := 0
	rateLimitError := &googleapi.Error{
		Code:    400,
		Message: "too many table update operations",
		Errors: []googleapi.ErrorItem{
			{Reason: "invalidQuery", Message: "Exceeded rate limits: too many table update operations"},
		},
	}

	config := RetryConfig{
		InitialInterval:     100 * time.Millisecond, // Long enough to cancel
		RandomizationFactor: 0,
		Multiplier:          1.5,
		MaxInterval:         1 * time.Second,
		MaxRetries:          0, // Unlimited
		MaxElapsedTime:      0, // No limit
	}

	// Cancel after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := retry(ctx, &config, func() error {
		callCount++
		return rateLimitError
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context")
}

func TestRetry_MaxElapsedTimeExceeded(t *testing.T) {
	ctx := context.Background()
	callCount := 0
	rateLimitError := &googleapi.Error{
		Code:    400,
		Message: "too many table update operations",
		Errors: []googleapi.ErrorItem{
			{Reason: "invalidQuery", Message: "Exceeded rate limits: too many table update operations"},
		},
	}

	config := RetryConfig{
		InitialInterval:     20 * time.Millisecond,
		RandomizationFactor: 0,
		Multiplier:          1.5,
		MaxInterval:         100 * time.Millisecond,
		MaxRetries:          100, // High limit
		MaxElapsedTime:      50 * time.Millisecond,
	}

	err := retry(ctx, &config, func() error {
		callCount++
		return rateLimitError
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "too many table update operations")
	assert.GreaterOrEqual(t, callCount, 1) // At least one attempt was made
}

func TestRetry_ExponentialBackoff(t *testing.T) {
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

	config := RetryConfig{
		InitialInterval:     20 * time.Millisecond,
		RandomizationFactor: 0, // Disable jitter for predictable timing
		Multiplier:          2.0,
		MaxInterval:         100 * time.Millisecond,
		MaxRetries:          3,
		MaxElapsedTime:      5 * time.Second,
	}

	_ = retry(ctx, &config, func() error {
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

func TestDefaultRetryConfig(t *testing.T) {
	config := DefaultRetryConfig()

	assert.Equal(t, 500*time.Millisecond, config.InitialInterval)
	assert.Equal(t, 0.5, config.RandomizationFactor)
	assert.Equal(t, 1.5, config.Multiplier)
	assert.Equal(t, 60*time.Second, config.MaxInterval)
	assert.Equal(t, uint(0), config.MaxRetries) // 0 = unlimited
	assert.Equal(t, 15*time.Minute, config.MaxElapsedTime)
}
