package dsql

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql/driver"
)

const rateLimiterFallbackMetric = "dsql_rate_limiter_fallbacks_total"
const rateLimiterFallbackCooldown = 30 * time.Second

type dynamoDBUpdater interface {
	UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
}

// Mark errors at the SDK boundary, keeping limiter policy errors fail-closed.
type dynamoDBLimiterError struct{ err error }

func (e *dynamoDBLimiterError) Error() string { return "DynamoDB connection limiter: " + e.err.Error() }
func (e *dynamoDBLimiterError) Unwrap() error { return e.err }

type failOpenRateLimiter struct {
	distributed   driver.RateLimiter
	local         driver.RateLimiter
	logger        log.Logger
	fallbacks     metrics.CounterIface
	now           func() time.Time
	mu            sync.Mutex
	fallbackUntil time.Time
	nextWarning   time.Time
}

func newFailOpenRateLimiterWithSettings(distributed driver.RateLimiter, resolved settings, logger log.Logger, handler metrics.Handler) *failOpenRateLimiter {
	if logger == nil {
		logger = log.NewNoopLogger()
	}
	if handler == nil {
		handler = metrics.NoopMetricsHandler
	}
	return &failOpenRateLimiter{
		distributed: distributed,
		local:       newConnectionRateLimiter(resolved),
		logger:      logger,
		fallbacks:   handler.Counter(rateLimiterFallbackMetric),
		now:         time.Now,
	}
}

func (l *failOpenRateLimiter) Wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.mu.Lock()
	fallback := l.now().Before(l.fallbackUntil)
	l.mu.Unlock()
	if fallback {
		return l.waitLocal(ctx)
	}

	err := l.distributed.Wait(ctx)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var condition *types.ConditionalCheckFailedException
	var ddbErr *dynamoDBLimiterError
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.As(err, &condition) || !errors.As(err, &ddbErr) {
		return err
	}
	l.mu.Lock()
	now := l.now()
	if !now.Before(l.fallbackUntil) {
		l.fallbackUntil = now.Add(rateLimiterFallbackCooldown)
	}
	warn := !now.Before(l.nextWarning)
	if warn {
		l.nextWarning = now.Add(time.Minute)
	}
	l.mu.Unlock()
	if warn {
		l.logger.Warn("DynamoDB connection limiter failed; using local rate limiting for 30 seconds", tag.Error(err))
	}
	return l.waitLocal(ctx)
}

func (l *failOpenRateLimiter) waitLocal(ctx context.Context) error {
	l.fallbacks.Record(1)
	return l.local.Wait(ctx)
}
