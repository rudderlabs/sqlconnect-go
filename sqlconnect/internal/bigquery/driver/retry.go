package driver

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

// Default retry parameters (aligned with google-cloud-go/bigquery defaults)
const (
	DefaultInitialBackoff = 1 * time.Second
	DefaultMaxBackoff     = 32 * time.Second
	DefaultMultiplier     = 2.0
	DefaultMaxRetries     = 0 // 0 = unlimited, matching google-cloud-go's default behavior
)

// RetryOptions configures the retry behavior for BigQuery operations.
type RetryOptions struct {
	// InitialBackoff is the initial delay before the first retry.
	// Default: 1 second
	InitialBackoff time.Duration

	// MaxBackoff is the maximum delay between retries.
	// Default: 32 seconds
	MaxBackoff time.Duration

	// Multiplier is the factor by which backoff increases after each retry.
	// Default: 2.0
	Multiplier float64

	// MaxRetries is the maximum number of retry attempts.
	// Default: 10 (set to 0 for unlimited, though not recommended)
	MaxRetries int

	// MaxDuration limits the total time spent retrying.
	// If set, retries will stop when this duration is exceeded.
	// Default: 0 (no limit, relies on MaxRetries and context)
	MaxDuration time.Duration

	// Jitter adds randomness to backoff to prevent thundering herd.
	// When true, actual backoff is between 0.5x and 1.0x of calculated backoff.
	// Default: true
	Jitter bool

	// RetryableFunc determines if an error should be retried.
	// Default: IsBigQueryRateLimitError (only retry rate limit errors)
	// Use IsBigQueryRetryableError for broader retry coverage
	RetryableFunc func(error) bool
}

// DefaultRetryOptions returns retry options with sensible defaults.
func DefaultRetryOptions() RetryOptions {
	return RetryOptions{
		InitialBackoff: DefaultInitialBackoff,
		MaxBackoff:     DefaultMaxBackoff,
		Multiplier:     DefaultMultiplier,
		MaxRetries:     DefaultMaxRetries,
		Jitter:         true,
		RetryableFunc:  IsBigQueryRateLimitError,
	}
}

// RetryOptionsFromConfig creates RetryOptions from RetryConfig.
func RetryOptionsFromConfig(config *RetryConfig) RetryOptions {
	opts := DefaultRetryOptions()
	if config == nil {
		return opts
	}
	if config.MaxRetries != nil {
		opts.MaxRetries = *config.MaxRetries
	}
	if config.MaxRetryDuration != nil {
		opts.MaxDuration = *config.MaxRetryDuration
	}
	return opts
}

// ExecuteWithRetry executes the given function with retry logic for BigQuery
// rate limit errors. This is necessary because google-cloud-go does NOT
// automatically retry "invalidQuery" errors, which includes table metadata
// rate limits.
//
// The function implements exponential backoff with optional jitter:
// - Backoff sequence: 1s → 2s → 4s → 8s → 16s → 32s → 32s → ...
// - Jitter: randomizes actual delay to 50-100% of calculated backoff
//
// Retry stops when:
// - Operation succeeds (returns nil)
// - Non-retryable error occurs (returns that error)
// - MaxRetries exceeded (returns wrapped error)
// - MaxDuration exceeded (returns wrapped error)
// - Context cancelled/deadline exceeded (returns context error)
func ExecuteWithRetry(ctx context.Context, opts RetryOptions, fn func() error) error {
	// Validate and set defaults
	if opts.InitialBackoff <= 0 {
		opts.InitialBackoff = DefaultInitialBackoff
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = DefaultMaxBackoff
	}
	if opts.Multiplier <= 0 {
		opts.Multiplier = DefaultMultiplier
	}
	if opts.RetryableFunc == nil {
		opts.RetryableFunc = IsBigQueryRateLimitError
	}

	var (
		startTime = time.Now()
		backoff   = opts.InitialBackoff
		lastErr   error
	)

	// MaxRetries = 0 means unlimited (but not recommended)
	maxAttempts := opts.MaxRetries + 1
	if opts.MaxRetries == 0 {
		maxAttempts = int(^uint(0) >> 1) // Max int for "unlimited"
	}

	for attempt := 0; attempt < maxAttempts; attempt++ {
		// Check context before each attempt
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return fmt.Errorf("context error after %d attempts: %w (last error: %v)", attempt, err, lastErr)
			}
			return err
		}

		// Check max duration
		if opts.MaxDuration > 0 && time.Since(startTime) > opts.MaxDuration {
			if lastErr != nil {
				return fmt.Errorf("exceeded max retry duration %v after %d attempts: %w", opts.MaxDuration, attempt, lastErr)
			}
			return fmt.Errorf("exceeded max retry duration %v", opts.MaxDuration)
		}

		// Execute the operation
		err := fn()
		if err == nil {
			return nil // Success
		}
		lastErr = err

		// Check if error is retryable
		if !opts.RetryableFunc(err) {
			return err // Non-retryable error
		}

		// Don't sleep after the last attempt
		if attempt == maxAttempts-1 {
			break
		}

		// Calculate sleep duration with optional jitter
		sleepDuration := backoff
		if opts.Jitter {
			// Jitter: random value between 50% and 100% of backoff
			jitterFactor := 0.5 + rand.Float64()*0.5
			sleepDuration = time.Duration(float64(backoff) * jitterFactor)
		}

		// Wait before retry
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled during retry backoff: %w (last error: %v)", ctx.Err(), lastErr)
		case <-time.After(sleepDuration):
		}

		// Increase backoff for next iteration
		backoff = time.Duration(float64(backoff) * opts.Multiplier)
		if backoff > opts.MaxBackoff {
			backoff = opts.MaxBackoff
		}
	}

	return fmt.Errorf("exceeded max retries (%d) for BigQuery operation: %w", opts.MaxRetries, lastErr)
}

// ExecuteWithDefaultRetry is a convenience function that uses default retry options.
func ExecuteWithDefaultRetry(ctx context.Context, fn func() error) error {
	return ExecuteWithRetry(ctx, DefaultRetryOptions(), fn)
}



