package dsql

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql/driver"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestRateLimiterFallbackRecovery(t *testing.T) {
	for _, mode := range []string{"gcra", "counter"} {
		t.Run(mode, func(t *testing.T) {
			for _, cause := range []error{
				&types.InternalServerError{}, &types.ProvisionedThroughputExceededException{},
				&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")},
			} {
				t.Run(fmt.Sprintf("%T", cause), func(t *testing.T) {
					fake := &gcraFake{}
					var calls atomic.Int64
					var healthy atomic.Bool
					client := updateItemFunc(func(ctx context.Context, in *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
						calls.Add(1)
						if !healthy.Load() {
							return nil, cause
						}
						if mode == "counter" {
							return &dynamodb.UpdateItemOutput{}, nil
						}
						return fake.UpdateItem(ctx, in)
					})
					var distributed driver.RateLimiter = gcraTestLimiter(t, client)
					if mode == "counter" {
						counter := NewDistributedRateLimiter(nil, "test-table", "test-endpoint")
						counter.ddb = client
						distributed = counter
					}
					core, observed := observer.New(zap.WarnLevel)
					handler := metricstest.NewCaptureHandler()
					capture := handler.StartCapture()
					defer handler.StopCapture(capture)
					l := newFailOpenRateLimiter(distributed, log.NewZapLogger(zap.New(core)), handler)
					now := time.Unix(1000, 0)
					l.now = func() time.Time { return now }
					var locals atomic.Int64
					l.local = poolTestLimiter(func(ctx context.Context) error { locals.Add(1); return ctx.Err() })
					require.NoError(t, l.Wait(t.Context()))
					require.Equal(t, int64(1), calls.Load())
					require.Equal(t, 1, observed.Len())
					results := make(chan error, 40)
					for range 40 {
						go func() { results <- l.Wait(t.Context()) }()
					}
					for range 40 {
						require.NoError(t, <-results)
					}
					require.Equal(t, int64(1), calls.Load())
					now = now.Add(30*time.Second - time.Nanosecond)
					require.NoError(t, l.Wait(t.Context()))
					require.Equal(t, int64(1), calls.Load())
					now = now.Add(time.Nanosecond)
					require.NoError(t, l.Wait(t.Context())) // Still down: retry, renew cooldown, suppress warning.
					require.Equal(t, int64(2), calls.Load())
					require.Equal(t, 1, observed.Len())
					now = now.Add(30 * time.Second)
					require.NoError(t, l.Wait(t.Context()))
					require.Equal(t, 2, observed.Len())
					require.Equal(t, int64(44), locals.Load())
					records := capture.Snapshot()[rateLimiterFallbackMetric]
					require.Len(t, records, 44)
					for _, record := range records {
						require.Equal(t, int64(1), record.Value)
					}
					healthy.Store(true)
					now = now.Add(30 * time.Second)
					require.NoError(t, l.Wait(t.Context()))
					require.GreaterOrEqual(t, calls.Load(), int64(4))
					require.Equal(t, int64(44), locals.Load())
				})
			}
		})
	}
}

func TestRateLimiterFallbackExclusions(t *testing.T) {
	for _, cause := range []error{
		&types.ConditionalCheckFailedException{},
		&dynamoDBLimiterError{err: &types.ConditionalCheckFailedException{}},
		errGCRAHorizon,
		context.Canceled,
		&dynamoDBLimiterError{err: context.DeadlineExceeded},
		errors.New("limiter policy failure"),
	} {
		t.Run(cause.Error(), func(t *testing.T) {
			core, observed := observer.New(zap.WarnLevel)
			handler := metricstest.NewCaptureHandler()
			capture := handler.StartCapture()
			defer handler.StopCapture(capture)
			l := newFailOpenRateLimiter(poolTestLimiter(func(context.Context) error { return cause }), log.NewZapLogger(zap.New(core)), handler)
			l.local = poolTestLimiter(func(context.Context) error { t.Error("unexpected local fallback"); return nil })
			require.ErrorIs(t, l.Wait(t.Context()), cause)
			require.Empty(t, capture.Snapshot())
			require.Zero(t, observed.Len())
			require.True(t, l.fallbackUntil.IsZero())
		})
	}
}

func TestRateLimiterFallbackLocalSettings(t *testing.T) {
	t.Setenv(ConnectionRateLimitEnvVar, "1")
	t.Setenv(ConnectionBurstLimitEnvVar, "2")
	l := newFailOpenRateLimiter(poolTestLimiter(func(context.Context) error {
		return &dynamoDBLimiterError{err: errors.New("offline")}
	}), nil, nil)
	require.NoError(t, l.Wait(t.Context()))
	require.NoError(t, l.Wait(t.Context()))
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	require.ErrorContains(t, l.Wait(ctx), "would exceed context deadline")
	cancel()
	require.ErrorIs(t, l.Wait(ctx), context.Canceled)
}

func TestRateLimiterFallbackPolicyErrors(t *testing.T) {
	for _, mode := range []string{"horizon", "contention", "counter_timeout", "canceled_sdk"} {
		t.Run(mode, func(t *testing.T) {
			client := updateItemFunc(func(context.Context, *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
				if mode == "canceled_sdk" {
					return nil, context.Canceled
				}
				return nil, &types.ConditionalCheckFailedException{}
			})
			var distributed driver.RateLimiter
			switch mode {
			case "horizon":
				l := gcraTestLimiter(t, &gcraFake{})
				l.Capacity, l.MaxWait = 1, 0
				require.NoError(t, l.Wait(t.Context()))
				distributed = l
			case "counter_timeout":
				l := NewDistributedRateLimiter(nil, "test-table", "test-endpoint")
				l.ddb, l.MaxWait = client, -time.Second
				distributed = l
			default:
				distributed = gcraTestLimiter(t, client)
			}
			l := newFailOpenRateLimiter(distributed, nil, nil)
			l.local = poolTestLimiter(func(context.Context) error { t.Error("unexpected fallback"); return nil })
			require.Error(t, l.Wait(t.Context()))
			require.True(t, l.fallbackUntil.IsZero())
		})
	}
}
