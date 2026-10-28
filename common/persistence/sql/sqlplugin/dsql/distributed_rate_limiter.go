package dsql

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	// DistributedRateLimiterEnabledEnvVar enables/disables the distributed rate limiter.
	DistributedRateLimiterEnabledEnvVar = "DSQL_DISTRIBUTED_RATE_LIMITER_ENABLED"

	// DistributedRateLimiterTableEnvVar specifies the DynamoDB table name.
	DistributedRateLimiterTableEnvVar = "DSQL_DISTRIBUTED_RATE_LIMITER_TABLE"

	// DistributedRateLimiterLimitEnvVar overrides the default limit per second.
	DistributedRateLimiterLimitEnvVar = "DSQL_DISTRIBUTED_RATE_LIMITER_LIMIT"

	// DistributedRateLimiterMaxWaitEnvVar overrides the default max wait time.
	DistributedRateLimiterMaxWaitEnvVar = "DSQL_DISTRIBUTED_RATE_LIMITER_MAX_WAIT"

	// DefaultDistributedRateLimiterLimit is the DSQL cluster-wide connection rate limit.
	DefaultDistributedRateLimiterLimit = 100

	// DefaultDistributedRateLimiterMaxWait is the default max wait time for acquiring a permit.
	DefaultDistributedRateLimiterMaxWait = 30 * time.Second

	// DefaultDistributedRateLimiterBackoff is the base backoff duration.
	DefaultDistributedRateLimiterBackoff = 25 * time.Millisecond

	// DefaultDistributedRateLimiterTTLWindow is how long per-second items live.
	DefaultDistributedRateLimiterTTLWindow = 3 * time.Minute
)

// DistributedRateLimiter enforces a distributed "new connections per second" limit via DynamoDB.
//
// Call Wait() immediately before opening a NEW DB connection (TCP/TLS + AUTH).
// Do NOT call for every query; only for connection establishment.
//
// This implements the driver.RateLimiter interface so it can be used with the
// pgxpool BeforeConnect hook to rate-limit all connection attempts, including
// pool growth initiated by database/sql internally.
type DistributedRateLimiter struct {
	ddb       *dynamodb.Client
	tableName string
	endpoint  string

	// pkPrefix allows multiple independent limiters in the same table.
	pkPrefix string

	// LimitPerSecond is the global maximum number of connection opens per second per endpoint.
	LimitPerSecond int64

	// MaxWait caps how long Wait will wait before returning an error.
	MaxWait time.Duration

	// BackoffBase controls retry pacing when the limit is exceeded.
	BackoffBase time.Duration

	// EnableTTL sets ttl_epoch for per-second items.
	EnableTTL bool

	// TTLSafetyWindow controls how long per-second items live (if EnableTTL).
	TTLSafetyWindow time.Duration
}

// NewDistributedRateLimiter creates a distributed rate limiter backed by DynamoDB.
//
// The endpoint parameter is the DSQL cluster endpoint (e.g., "cluster-id.dsql.region.on.aws").
// This is used to partition rate limits per DSQL cluster.
func NewDistributedRateLimiter(ddb *dynamodb.Client, tableName, endpoint string) *DistributedRateLimiter {
	limitPerSecond := int64(getEnvInt(DistributedRateLimiterLimitEnvVar, DefaultDistributedRateLimiterLimit))
	maxWait := getEnvDuration(DistributedRateLimiterMaxWaitEnvVar, DefaultDistributedRateLimiterMaxWait)

	return &DistributedRateLimiter{
		ddb:             ddb,
		tableName:       tableName,
		endpoint:        normalizeEndpoint(endpoint),
		pkPrefix:        "dsqlconnect#",
		LimitPerSecond:  limitPerSecond,
		MaxWait:         maxWait,
		BackoffBase:     DefaultDistributedRateLimiterBackoff,
		EnableTTL:       true,
		TTLSafetyWindow: DefaultDistributedRateLimiterTTLWindow,
	}
}

// Wait blocks until a connection permit can be acquired within rate limits.
// This implements the driver.RateLimiter interface.
func (l *DistributedRateLimiter) Wait(ctx context.Context) error {
	return l.Acquire(ctx, 1)
}

// Acquire reserves n connection-open permits for the current second.
// For typical use cases, n should be 1.
func (l *DistributedRateLimiter) Acquire(ctx context.Context, n int64) error {
	if n <= 0 {
		return nil
	}
	if l.LimitPerSecond <= 0 {
		// disabled
		return nil
	}

	deadline := time.Now().Add(l.MaxWait)

	// Check if context already has a shorter deadline
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}

	for {
		now := time.Now().UTC()
		sec := now.Unix()

		ok, err := l.tryAcquireOnce(ctx, sec, n)
		if err == nil && ok {
			return nil
		}
		if err != nil && !isConditionalFail(err) {
			return err
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("dsql connect limiter: timeout acquiring permit (endpoint=%s n=%d)", l.endpoint, n)
		}

		sleep := l.jitteredBackoff(time.Now(), deadline)
		select {
		case <-time.After(sleep):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// tryAcquireOnce attempts to acquire n permits for the given second.
// Returns (true, nil) on success, (false, nil) if limit exceeded, or (false, err) on error.
func (l *DistributedRateLimiter) tryAcquireOnce(ctx context.Context, sec int64, n int64) (bool, error) {
	// pk includes the second so rollover is trivial and race-safe.
	// pk = dsqlconnect#<endpoint>#<unix_second>
	pk := fmt.Sprintf("%s%s#%d", l.pkPrefix, l.endpoint, sec)

	limitMinusN := l.LimitPerSecond - n
	if limitMinusN < 0 {
		return false, fmt.Errorf("requested permits n=%d exceeds limit_per_second=%d", n, l.LimitPerSecond)
	}

	nowms := time.Now().UTC().UnixMilli()

	exprVals := map[string]types.AttributeValue{
		":n":           &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", n)},
		":nowms":       &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", nowms)},
		":limitMinusN": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", limitMinusN)},
	}

	updateExpr := "SET updated_at_ms = :nowms ADD #cnt :n"
	exprNames := map[string]string{
		"#cnt": "count", // count is a reserved word in DynamoDB
	}

	if l.EnableTTL {
		ttlEpoch := time.Now().UTC().Add(l.TTLSafetyWindow).Unix()
		exprVals[":ttl"] = &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttlEpoch)}
		updateExpr = "SET updated_at_ms = :nowms, ttl_epoch = :ttl ADD #cnt :n"
	}

	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(l.tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
		},
		UpdateExpression:          aws.String(updateExpr),
		ExpressionAttributeValues: exprVals,
		ExpressionAttributeNames:  exprNames,
		// Allow if:
		// - item is new (no count), OR
		// - current count is <= limitMinusN, so (count + n) <= limit
		ConditionExpression: aws.String("attribute_not_exists(#cnt) OR #cnt <= :limitMinusN"),
	}

	_, err := l.ddb.UpdateItem(ctx, input)
	if err != nil {
		if isConditionalFail(err) {
			return false, err
		}
		return false, err
	}

	return true, nil
}

// jitteredBackoff calculates the next backoff duration with jitter. It reads the
// time only from now, so tests can pin it.
func (l *DistributedRateLimiter) jitteredBackoff(now time.Time, deadline time.Time) time.Duration {
	remaining := deadline.Sub(now)
	if remaining <= 0 {
		return 0
	}

	base := l.BackoffBase
	if base <= 0 {
		base = DefaultDistributedRateLimiterBackoff
	}

	// If we're near the next second boundary, wait for it (plus small jitter).
	nextSec := now.Truncate(time.Second).Add(time.Second)
	untilNext := nextSec.Sub(now)
	if untilNext > 0 && untilNext < 250*time.Millisecond {
		j := randDuration(0, 50*time.Millisecond)
		return minDuration(untilNext+j, remaining)
	}

	// Otherwise small jittered backoff capped at 200ms.
	j := randDuration(0, base)
	sleep := base + j
	if sleep > 200*time.Millisecond {
		sleep = 200 * time.Millisecond
	}
	return minDuration(sleep, remaining)
}

// IsDistributedRateLimiterEnabled checks if the distributed rate limiter should be used.
func IsDistributedRateLimiterEnabled() bool {
	val := os.Getenv(DistributedRateLimiterEnabledEnvVar)
	if val == "" {
		return false // disabled by default
	}
	enabled, err := strconv.ParseBool(val)
	if err != nil {
		return false
	}
	return enabled
}

// GetDistributedRateLimiterTable returns the configured DynamoDB table name.
func GetDistributedRateLimiterTable() string {
	return os.Getenv(DistributedRateLimiterTableEnvVar)
}

// normalizeEndpoint normalizes the endpoint string for consistent key generation.
func normalizeEndpoint(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	// Strip any port if present.
	if i := strings.IndexByte(s, ':'); i >= 0 {
		s = s[:i]
	}
	return s
}

// randDuration returns a random duration between min and max.
func randDuration(min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	delta := max - min
	var b [8]byte
	_, _ = rand.Read(b[:])
	u := binary.LittleEndian.Uint64(b[:])
	n := time.Duration(u % uint64(delta))
	return min + n
}

// minDuration returns the smaller of two durations.
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// isConditionalFail checks if the error is a DynamoDB conditional check failure.
func isConditionalFail(err error) bool {
	var cfe *types.ConditionalCheckFailedException
	if errors.As(err, &cfe) {
		return true
	}
	return strings.Contains(err.Error(), "ConditionalCheckFailed")
}
