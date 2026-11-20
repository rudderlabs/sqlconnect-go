package driver

import (
	"context"
	"time"

	"github.com/rudderlabs/sqlconnect-go/sqlconnect/internal/maxcompute/mcerr"
)

// transportRetries bounds the retries of an idempotent REST call after a
// transport error. The SDK's HTTP client keeps idle connections forever, and
// Alibaba's load balancer closes them, so a call can meet a plain EOF on a
// stale connection (RC2). Job submits are never retried: a POST may have
// created the job.
const transportRetries = 2

// retryBackoff is the wait before retry n (n = 1, 2, …).
var retryBackoff = func(n int) time.Duration { return time.Duration(n) * 200 * time.Millisecond }

// retryCall is call with up to transportRetries retries of transport errors,
// for idempotent calls only.
func retryCall(ctx context.Context, fn func() error) error {
	for attempt := 0; ; attempt++ {
		err := call(ctx, fn)
		if err == nil || attempt >= transportRetries || ctx.Err() != nil || !mcerr.IsTransport(err) {
			return err
		}
		if !sleep(ctx, retryBackoff(attempt+1)) {
			return ctx.Err()
		}
	}
}
