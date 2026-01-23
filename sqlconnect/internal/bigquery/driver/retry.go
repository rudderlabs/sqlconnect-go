package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
)

// RetryConfig configures the retry behavior for BigQuery operations.
// This is the execution-time struct with concrete values.
type RetryConfig struct {
	// InitialInterval is the initial delay before the first retry.
	// Default: 500ms
	InitialInterval time.Duration
	// RandomizationFactor adds jitter to prevent thundering herd.
	// Default: 0.5
	RandomizationFactor float64
	// Multiplier is the factor by which backoff increases after each retry.
	// Default: 1.5
	Multiplier float64
	// MaxInterval is the maximum delay between retries.
	// Default: 60 seconds
	MaxInterval time.Duration
	// MaxRetries is the maximum number of retry attempts.
	// Default: 0 (unlimited)
	MaxRetries uint
	// MaxElapsedTime limits the total time spent retrying.
	// Default: 15 minutes
	MaxElapsedTime time.Duration
}

// DefaultRetryConfig returns retry config with sensible defaults.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		InitialInterval:     500 * time.Millisecond,
		RandomizationFactor: 0.5,
		Multiplier:          1.5,
		MaxInterval:         60 * time.Second,
		MaxRetries:          0, // unlimited
		MaxElapsedTime:      15 * time.Minute,
	}
}

// retry executes the given function with retry logic for BigQuery rate limit errors.
// This is necessary because google-cloud-go does NOT automatically retry "invalidQuery"
// errors, which includes table metadata rate limits.
//
// If config is nil, the function is executed once without retry.
//
// Retry stops when:
// - Operation succeeds (returns nil)
// - Non-retryable error occurs (returns that error immediately)
// - MaxRetries exceeded (returns last error)
// - MaxElapsedTime exceeded (returns last error)
// - Context cancelled/deadline exceeded (returns context error)
func retry(ctx context.Context, c *RetryConfig, fn func() error) error {
	if c == nil {
		return fn()
	}

	b := &backoff.ExponentialBackOff{
		InitialInterval:     c.InitialInterval,
		RandomizationFactor: c.RandomizationFactor,
		Multiplier:          c.Multiplier,
		MaxInterval:         c.MaxInterval,
		MaxElapsedTime:      c.MaxElapsedTime,
		Clock:               backoff.SystemClock,
	}
	b.Reset()

	var retryBackoff backoff.BackOff = b
	// WithMaxRetries(b, n) stops after n retries (n+1 total attempts)
	// So MaxRetries=3 means initial attempt + 3 retries = 4 total attempts
	if c.MaxRetries > 0 {
		retryBackoff = backoff.WithMaxRetries(b, uint64(c.MaxRetries))
	}
	retryBackoff = backoff.WithContext(retryBackoff, ctx)

	var lastErr error
	err := backoff.Retry(func() error {
		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err
		if !IsBigQueryRateLimitError(err) {
			return backoff.Permanent(err)
		}
		return err
	}, retryBackoff)

	if err != nil && lastErr != nil && err != lastErr {
		return fmt.Errorf("retry failed: %w (last error: %v)", err, lastErr)
	}
	return err
}
