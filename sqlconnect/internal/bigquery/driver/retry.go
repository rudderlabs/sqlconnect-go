package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
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
// Note: This uses the application-level retry config (QueryRetryAttempts, QueryRetryDuration),
// not the driver-level MaxRetries which is passed to google-cloud-go.
func RetryOptionsFromConfig(config *RetryConfig) RetryOptions {
	opts := DefaultRetryOptions()
	if config == nil {
		return opts
	}
	if config.QueryRetryAttempts != nil {
		opts.MaxRetries = *config.QueryRetryAttempts
	}
	if config.QueryRetryDuration != nil {
		opts.MaxDuration = *config.QueryRetryDuration
	}
	return opts
}

// ExecuteWithRetry executes the given function with retry logic for BigQuery
// rate limit errors. This is necessary because google-cloud-go does NOT
// automatically retry "invalidQuery" errors, which includes table metadata
// rate limits.
//
// Uses cenkalti/backoff for exponential backoff with jitter:
// - Backoff sequence: 1s → 2s → 4s → 8s → 16s → 32s → 32s → ...
// - Jitter: randomizes actual delay (default: 0.5 randomization factor)
//
// Retry stops when:
// - Operation succeeds (returns nil)
// - Non-retryable error occurs (returns that error immediately)
// - MaxRetries exceeded (returns last error)
// - MaxDuration exceeded (returns last error)
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

	// Configure exponential backoff
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = opts.InitialBackoff
	b.MaxInterval = opts.MaxBackoff
	b.Multiplier = opts.Multiplier
	b.MaxElapsedTime = opts.MaxDuration
	if !opts.Jitter {
		b.RandomizationFactor = 0
	}

	// Wrap with max retries if specified (0 = unlimited)
	var retryBackoff backoff.BackOff = b
	if opts.MaxRetries > 0 {
		retryBackoff = backoff.WithMaxRetries(b, uint64(opts.MaxRetries))
	}

	// Wrap with context for cancellation support
	retryBackoff = backoff.WithContext(retryBackoff, ctx)

	var lastErr error
	err := backoff.Retry(func() error {
		err := fn()
		if err == nil {
			return nil // Success
		}
		lastErr = err

		// Check if error is retryable
		if !opts.RetryableFunc(err) {
			return backoff.Permanent(err) // Don't retry non-retryable errors
		}
		return err // Retryable error, will be retried
	}, retryBackoff)

	if err != nil {
		// Provide more context in error message
		if lastErr != nil && err != lastErr {
			return fmt.Errorf("retry failed: %w (last error: %v)", err, lastErr)
		}
		return err
	}
	return nil
}

// ExecuteWithDefaultRetry is a convenience function that uses default retry options.
func ExecuteWithDefaultRetry(ctx context.Context, fn func() error) error {
	return ExecuteWithRetry(ctx, DefaultRetryOptions(), fn)
}
