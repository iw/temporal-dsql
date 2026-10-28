package dsql

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	TokenBucketEnabledEnvVar   = "DSQL_TOKEN_BUCKET_ENABLED"
	TokenBucketRateEnvVar      = "DSQL_TOKEN_BUCKET_RATE"
	TokenBucketCapacityEnvVar  = "DSQL_TOKEN_BUCKET_CAPACITY"
	TokenBucketMaxWaitEnvVar   = "DSQL_TOKEN_BUCKET_MAX_WAIT"
	DefaultTokenBucketRate     = 100
	DefaultTokenBucketCapacity = 1000
	DefaultTokenBucketMaxWait  = 30 * time.Second

	// A new key isolates reservations from the old read-modify-write bucket.
	tokenBucketPKPrefix = "dsql_connect_gcra#"
	gcraMaxAttempts     = 4
)

var errGCRAHorizon = errors.New("DSQL connection reservation exceeds max wait")

// TokenBucketLimiter implements GCRA reservations. The shared theoretical arrival
// time only advances; callers sleep locally instead of polling an empty bucket.
// Configuration must be consistent across hosts and immutable after first use.
type TokenBucketLimiter struct {
	ddb       dynamoDBUpdater
	tableName string
	endpoint  string
	Rate      int64
	Capacity  int64
	MaxWait   time.Duration
	TTLWindow time.Duration
	now       func() time.Time
	sleep     func(context.Context, time.Duration) error
}

func newTokenBucketLimiter(ddb dynamoDBUpdater, resolved settings) *TokenBucketLimiter {
	return &TokenBucketLimiter{
		ddb:       ddb,
		tableName: resolved.Limiter.Table,
		endpoint:  normalizeEndpoint(resolved.ClusterEndpoint),
		Rate:      int64(resolved.Limiter.TokenBucketRate),
		Capacity:  int64(resolved.Limiter.TokenBucketCapacity),
		MaxWait:   resolved.Limiter.TokenBucketMaxWait,
		TTLWindow: time.Hour,
		now:       time.Now,
		sleep:     sleepForReservation,
	}
}

func (l *TokenBucketLimiter) Wait(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if l.Rate <= 0 || l.Capacity <= 0 {
		return nil
	}
	// Round up to microseconds so non-divisor rates never exceed their budget.
	if l.Rate > int64(time.Second/time.Microsecond) || l.MaxWait < 0 {
		return errors.New("invalid GCRA rate or max wait")
	}
	interval := (int64(time.Second/time.Microsecond) + l.Rate - 1) / l.Rate
	if l.Capacity-1 > (math.MaxInt64/int64(time.Microsecond)-l.MaxWait.Microseconds())/interval {
		return errors.New("GCRA reservation horizon overflows duration")
	}
	tau := (l.Capacity - 1) * interval
	horizon := tau + l.MaxWait.Microseconds()
	var lastErr error
	for range gcraMaxAttempts {
		if err := ctx.Err(); err != nil {
			return err
		}
		now := l.now()
		t := now.UnixMicro()
		ttl := numberAttribute(now.Add(l.TTLWindow).Unix())
		input := &dynamodb.UpdateItemInput{
			TableName:           aws.String(l.tableName),
			Key:                 map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: tokenBucketPKPrefix + l.endpoint}},
			UpdateExpression:    aws.String("SET tat_us = tat_us + :T, ttl_epoch = :ttl"),
			ConditionExpression: aws.String("tat_us BETWEEN :t AND :limit"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":T": numberAttribute(interval), ":t": numberAttribute(t),
				":limit": numberAttribute(t + horizon), ":ttl": ttl,
			},
			ReturnValues:                        types.ReturnValueUpdatedOld,
			ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
		}
		out, err := l.ddb.UpdateItem(ctx, input)
		if err == nil {
			old, present, err := theoreticalArrival(out.Attributes)
			if err != nil {
				return err
			}
			if !present {
				return errors.New("GCRA update returned no tat_us")
			}
			reserved := max(t, old-tau)
			return l.sleep(ctx, max(0, time.UnixMicro(reserved).Sub(l.now())))
		}
		var condition *types.ConditionalCheckFailedException
		if !errors.As(err, &condition) {
			return &dynamoDBLimiterError{err: err}
		}
		old, present, err := theoreticalArrival(condition.Item)
		if err != nil {
			return err
		}
		if present && old > t+horizon {
			return fmt.Errorf("%w (endpoint=%s)", errGCRAHorizon, l.endpoint)
		}
		if present && old >= t {
			return errors.New("GCRA condition failed inside reservation horizon")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		input.UpdateExpression = aws.String("SET tat_us = :t_plus_T, ttl_epoch = :ttl")
		input.ConditionExpression = aws.String("attribute_not_exists(tat_us) OR tat_us < :t")
		input.ExpressionAttributeValues = map[string]types.AttributeValue{
			":t_plus_T": numberAttribute(t + interval), ":t": numberAttribute(t), ":ttl": ttl,
		}
		_, err = l.ddb.UpdateItem(ctx, input)
		if err == nil {
			return ctx.Err()
		}
		if !errors.As(err, &condition) {
			return &dynamoDBLimiterError{err: err}
		}
		lastErr = err
	}
	return fmt.Errorf("GCRA idle reservation contention after %d attempts: %w", gcraMaxAttempts, lastErr)
}

func numberAttribute(n int64) *types.AttributeValueMemberN {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(n, 10)}
}

func theoreticalArrival(item map[string]types.AttributeValue) (int64, bool, error) {
	value, present := item["tat_us"]
	if !present {
		return 0, false, nil
	}
	n, ok := value.(*types.AttributeValueMemberN)
	if !ok {
		return 0, true, errors.New("GCRA tat_us is not a number")
	}
	t, err := strconv.ParseInt(n.Value, 10, 64)
	if err != nil {
		return 0, true, fmt.Errorf("invalid GCRA tat_us: %w", err)
	}
	return t, true, nil
}

func sleepForReservation(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}
