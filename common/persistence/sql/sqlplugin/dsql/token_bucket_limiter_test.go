package dsql

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

type updateItemFunc func(context.Context, *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error)

func (f updateItemFunc) UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return f(ctx, in)
}

// The fake executes the actual expressions atomically, including old images on
// both success and condition failure. It has no read operation.
type gcraFake struct {
	mu      sync.Mutex
	items   map[string]map[string]types.AttributeValue
	history []int64
	calls   int
}

func (f *gcraFake) UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if in.ReturnValues != types.ReturnValueUpdatedOld || in.ReturnValuesOnConditionCheckFailure != types.ReturnValuesOnConditionCheckFailureAllOld {
		return nil, errors.New("missing old-value options")
	}
	pk := in.Key["pk"].(*types.AttributeValueMemberS).Value
	if f.items == nil {
		f.items = make(map[string]map[string]types.AttributeValue)
	}
	old := f.items[pk]
	tat, present, err := theoreticalArrival(old)
	if err != nil {
		return nil, err
	}
	number := func(key string) int64 {
		var n int64
		_, _ = fmt.Sscan(in.ExpressionAttributeValues[key].(*types.AttributeValueMemberN).Value, &n)
		return n
	}
	t := number(":t")
	var next int64
	var allowed bool
	switch aws.ToString(in.ConditionExpression) {
	case "tat_us BETWEEN :t AND :limit":
		if aws.ToString(in.UpdateExpression) != "SET tat_us = tat_us + :T, ttl_epoch = :ttl" {
			return nil, errors.New("bad busy expression")
		}
		allowed = present && tat >= t && tat <= number(":limit")
		next = tat + number(":T")
	case "attribute_not_exists(tat_us) OR tat_us < :t":
		if aws.ToString(in.UpdateExpression) != "SET tat_us = :t_plus_T, ttl_epoch = :ttl" {
			return nil, errors.New("bad idle expression")
		}
		allowed = !present || tat < t
		next = number(":t_plus_T")
	default:
		return nil, errors.New("unknown condition")
	}
	if !allowed {
		return nil, &types.ConditionalCheckFailedException{Item: old}
	}
	f.items[pk] = map[string]types.AttributeValue{"tat_us": numberAttribute(next), "ttl_epoch": in.ExpressionAttributeValues[":ttl"]}
	f.history = append(f.history, next)
	return &dynamodb.UpdateItemOutput{Attributes: old}, nil
}

func gcraTestLimiter(t *testing.T, client dynamoDBUpdater) *TokenBucketLimiter {
	t.Helper()
	l := NewTokenBucketLimiter(client, "test-rate-table", "test-endpoint", nil)
	l.Rate, l.Capacity, l.MaxWait = 100, 1000, 30*time.Second
	l.now = func() time.Time { return time.Unix(1000, 0) }
	l.sleep = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	return l
}

func TestTokenBucketConfiguration(t *testing.T) {
	for _, name := range []string{TokenBucketRateEnvVar, TokenBucketCapacityEnvVar, TokenBucketMaxWaitEnvVar} {
		t.Setenv(name, "")
	}
	l := NewTokenBucketLimiter(nil, "test-table", " TEST-ENDPOINT:5432 ", nil)
	require.Equal(t, int64(100), l.Rate)
	require.Equal(t, int64(1000), l.Capacity)
	require.Equal(t, 30*time.Second, l.MaxWait)
	require.Equal(t, "test-endpoint", l.endpoint)
	t.Setenv(TokenBucketRateEnvVar, "50")
	t.Setenv(TokenBucketCapacityEnvVar, "500")
	t.Setenv(TokenBucketMaxWaitEnvVar, "10s")
	l = NewTokenBucketLimiter(nil, "test-table", "test-endpoint", nil)
	require.Equal(t, int64(50), l.Rate)
	require.Equal(t, int64(500), l.Capacity)
	require.Equal(t, 10*time.Second, l.MaxWait)
	for _, value := range []string{"", "true", "false", "1", "0", "invalid"} {
		t.Setenv(TokenBucketEnabledEnvVar, value)
		require.Equal(t, value == "true" || value == "1", IsTokenBucketEnabled())
	}
}

func TestGCRABurstAndRate(t *testing.T) {
	fake := &gcraFake{}
	l := gcraTestLimiter(t, fake)
	var delay time.Duration
	l.sleep = func(_ context.Context, d time.Duration) error { delay = d; return nil }
	for i := range 1200 {
		delay = 0
		require.NoError(t, l.Wait(t.Context()))
		require.Equal(t, time.Duration(max(0, i-999))*10*time.Millisecond, delay)
	}
	require.Equal(t, 1201, fake.calls) // One extra conditional write to initialize.
	require.Len(t, fake.items, 1)
	item := fake.items["dsql_connect_gcra#test-endpoint"]
	require.Equal(t, numberAttribute(l.now().Add(time.Hour).Unix()), item["ttl_epoch"])
}

func TestGCRAConcurrentReservations(t *testing.T) {
	fake := &gcraFake{}
	l := gcraTestLimiter(t, fake)
	l.Capacity = 1
	delays := make(chan time.Duration, 80)
	l.sleep = func(_ context.Context, d time.Duration) error { delays <- d; return nil }
	results := make(chan error, 80)
	start := make(chan struct{})
	for range 80 {
		go func() { <-start; results <- l.Wait(t.Context()) }()
	}
	close(start)
	for range 80 {
		require.NoError(t, <-results)
	}
	// The idle winner returns immediately without sleeping.
	close(delays)
	got := []time.Duration{0}
	for d := range delays {
		got = append(got, d)
	}
	slices.Sort(got)
	require.Len(t, got, 80)
	for i, d := range got {
		require.Equal(t, time.Duration(i)*10*time.Millisecond, d)
	}
}

func TestGCRAHorizon(t *testing.T) {
	fake := &gcraFake{}
	l := gcraTestLimiter(t, fake)
	l.Capacity, l.MaxWait = 1, 20*time.Millisecond
	for range 3 {
		require.NoError(t, l.Wait(t.Context()))
	}
	require.ErrorIs(t, l.Wait(t.Context()), errGCRAHorizon)
	require.Len(t, fake.history, 3)
}

func TestGCRAIdleRaceAndFailureImage(t *testing.T) {
	fake := &gcraFake{}
	l := gcraTestLimiter(t, fake)
	other := gcraTestLimiter(t, fake)
	var raced bool
	l.ddb = updateItemFunc(func(ctx context.Context, in *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
		if !raced && aws.ToString(in.ConditionExpression) == "attribute_not_exists(tat_us) OR tat_us < :t" {
			raced = true
			if err := other.Wait(ctx); err != nil {
				return nil, err
			}
			out, err := fake.UpdateItem(ctx, in)
			var cfe *types.ConditionalCheckFailedException
			require.ErrorAs(t, err, &cfe)
			require.Equal(t, numberAttribute(l.now().UnixMicro()+10_000), cfe.Item["tat_us"])
			return out, err
		}
		return fake.UpdateItem(ctx, in)
	})
	require.NoError(t, l.Wait(t.Context()))
	require.True(t, raced)
	require.Len(t, fake.history, 2)
	require.Equal(t, int64(10_000), fake.history[1]-fake.history[0])
	// An idle clock jump also advances, rather than overwrites, the old TAT.
	l.now = func() time.Time { return time.Unix(1010, 0) }
	require.NoError(t, l.Wait(t.Context()))
	require.Greater(t, fake.history[2], fake.history[1])
}

func TestGCRABoundedContention(t *testing.T) {
	calls := 0
	l := gcraTestLimiter(t, updateItemFunc(func(context.Context, *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
		calls++
		return nil, &types.ConditionalCheckFailedException{}
	}))
	var condition *types.ConditionalCheckFailedException
	require.ErrorAs(t, l.Wait(t.Context()), &condition)
	require.Equal(t, 2*gcraMaxAttempts, calls)
}

func TestGCRAMonotonic(t *testing.T) {
	fake := &gcraFake{}
	l := gcraTestLimiter(t, fake)
	rng := rand.New(rand.NewSource(7))
	now := l.now()
	l.now = func() time.Time { return now }
	for range 3000 {
		now = now.Add(time.Duration(rng.Intn(2001)-1000) * time.Millisecond)
		err := l.Wait(t.Context())
		if err != nil {
			require.ErrorIs(t, err, errGCRAHorizon)
		}
	}
	require.Greater(t, len(fake.history), 1)
	for i := 1; i < len(fake.history); i++ {
		require.Greater(t, fake.history[i], fake.history[i-1])
	}
}

func TestGCRACancellationAndErrors(t *testing.T) {
	fake := &gcraFake{}
	l := gcraTestLimiter(t, fake)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, l.Wait(ctx), context.Canceled)
	require.Zero(t, fake.calls)
	l.Capacity = 1
	require.NoError(t, l.Wait(t.Context()))
	l.sleep = func(ctx context.Context, d time.Duration) error {
		require.Positive(t, d)
		cancelled, stop := context.WithCancel(ctx)
		stop()
		return sleepForReservation(cancelled, d)
	}
	require.ErrorIs(t, l.Wait(t.Context()), context.Canceled)
	// Canceled reservations remain spent; reclaiming them could admit twice.
	require.Len(t, fake.history, 2)
	for _, err := range []error{errors.New("network unavailable"), &types.ProvisionedThroughputExceededException{}} {
		l.ddb = updateItemFunc(func(context.Context, *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) { return nil, err })
		var marked *dynamoDBLimiterError
		require.ErrorAs(t, l.Wait(t.Context()), &marked)
		require.ErrorIs(t, marked, err)
	}
}

func TestGCRAClockSkew(t *testing.T) {
	const hosts, steps = 40, 1000
	const tick = 10 * time.Millisecond
	const period = steps * tick
	fake := &gcraFake{}
	base := time.Unix(1000, 0)
	next := make([]time.Duration, hosts)
	admissions := 0
	type result struct {
		host int
		at   time.Duration
		err  error
	}
	for step := 0; step <= steps; step++ {
		elapsed := time.Duration(step) * tick
		results := make(chan result, hosts)
		pending := 0
		for host := range hosts {
			if next[host] > elapsed {
				continue
			}
			pending++
			// Forty independently clocked hosts span -50ms to +50ms.
			offset := -50*time.Millisecond + time.Duration(host)*100*time.Millisecond/(hosts-1)
			l := gcraTestLimiter(t, fake)
			l.now = func() time.Time { return base.Add(elapsed + offset) }
			go func() {
				at := elapsed
				l.sleep = func(_ context.Context, delay time.Duration) error { at += delay; return nil }
				err := l.Wait(t.Context())
				results <- result{host, at, err}
			}()
		}
		for range pending {
			r := <-results
			require.NoError(t, r.err)
			if r.at <= period {
				admissions++
			}
			next[r.host] = r.at + tick
		}
	}
	// The full clock spread is 100ms, so its one-time allowance is 10 permits.
	bound := 100*int(period/time.Second) + 1000 + 10
	require.LessOrEqual(t, admissions, bound)
	require.GreaterOrEqual(t, admissions, 1990)
	for i := 1; i < len(fake.history); i++ {
		require.Greater(t, fake.history[i], fake.history[i-1])
	}

	// Replay the old algorithm with the same 40 offsets and 10ms demand ticks.
	// Serialize each read/write pair, giving the old CAS its best case (no
	// same-millisecond double spends). This schedule admits 2,307 connections.
	// Backward last_refill_ms still repeatedly
	// credits the 90ms between the last fast host and the next slow host.
	tokens, last := int64(1000*1000), base.Add(-50*time.Millisecond).UnixMilli()
	oldAdmissions := 0
	for step := 0; step <= steps; step++ {
		for host := range hosts {
			offset := -50*time.Millisecond + time.Duration(host)*100*time.Millisecond/(hosts-1)
			now := base.Add(time.Duration(step)*tick + offset).UnixMilli()
			refilled := min(int64(1000*1000), tokens+max(int64(0), now-last)*100)
			if refilled >= 1000 {
				tokens, last = refilled-1000, now
				oldAdmissions++
			}
		}
	}
	require.Greater(t, oldAdmissions, bound)
	t.Logf("40 hosts, +/-50ms, %s: old=%d new=%d bound=%d", period, oldAdmissions, admissions, bound)
}
