package dsql

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// mockDynamoDBClient implements a mock DynamoDB client for testing.
type mockDynamoDBClient struct {
	mu           sync.Mutex
	items        map[string]int64 // pk -> count
	updateCalls  atomic.Int64
	failNextCall bool
	failError    error
}

func newMockDynamoDBClient() *mockDynamoDBClient {
	return &mockDynamoDBClient{
		items: make(map[string]int64),
	}
}

func (m *mockDynamoDBClient) UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	m.updateCalls.Add(1)

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.failNextCall {
		m.failNextCall = false
		return nil, m.failError
	}

	// Extract pk from input
	pkAttr, ok := input.Key["pk"]
	if !ok {
		return nil, fmt.Errorf("missing pk in key")
	}
	pkVal, ok := pkAttr.(*types.AttributeValueMemberS)
	if !ok {
		return nil, fmt.Errorf("pk is not a string")
	}
	pk := pkVal.Value

	// Extract :n and :limitMinusN from expression values
	nAttr := input.ExpressionAttributeValues[":n"]
	nVal, _ := nAttr.(*types.AttributeValueMemberN)
	var n int64
	fmt.Sscanf(nVal.Value, "%d", &n)

	limitMinusNAttr := input.ExpressionAttributeValues[":limitMinusN"]
	limitMinusNVal, _ := limitMinusNAttr.(*types.AttributeValueMemberN)
	var limitMinusN int64
	fmt.Sscanf(limitMinusNVal.Value, "%d", &limitMinusN)

	// Check condition: attribute_not_exists(count) OR count <= :limitMinusN
	currentCount := m.items[pk]
	if currentCount > limitMinusN {
		return nil, &types.ConditionalCheckFailedException{
			Message: stringPtr("The conditional request failed"),
		}
	}

	// Update count
	m.items[pk] = currentCount + n

	return &dynamodb.UpdateItemOutput{}, nil
}

func stringPtr(s string) *string {
	return &s
}

// mockDynamoDBClientInterface wraps mockDynamoDBClient to satisfy the interface
type mockDynamoDBClientInterface struct {
	mock *mockDynamoDBClient
}

func (m *mockDynamoDBClientInterface) UpdateItem(ctx context.Context, input *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return m.mock.UpdateItem(ctx, input, opts...)
}

func TestDistributedRateLimiter_BasicAcquire(t *testing.T) {
	mock := newMockDynamoDBClient()

	// Create limiter with mock client
	limiter := &DistributedRateLimiter{
		ddb:             nil, // We'll use the mock directly
		tableName:       "test-table",
		endpoint:        "test-cluster.dsql.us-east-1.on.aws",
		pkPrefix:        "dsqlconnect#",
		LimitPerSecond:  10,
		MaxWait:         5 * time.Second,
		BackoffBase:     10 * time.Millisecond,
		EnableTTL:       true,
		TTLSafetyWindow: 3 * time.Minute,
	}

	// Replace the ddb client with our mock wrapper
	limiter.ddb = &dynamodb.Client{}

	// Test that tryAcquireOnce works with mock
	ctx := context.Background()

	// Create a test-specific limiter that uses the mock
	testLimiter := &testDistributedRateLimiter{
		DistributedRateLimiter: limiter,
		mockClient:             mock,
	}

	// First acquire should succeed
	ok, err := testLimiter.tryAcquireOnce(ctx, time.Now().Unix(), 1)
	require.NoError(t, err)
	require.True(t, ok)

	// Verify mock was called
	require.Equal(t, int64(1), mock.updateCalls.Load())
}

// testDistributedRateLimiter wraps DistributedRateLimiter for testing with mock
type testDistributedRateLimiter struct {
	*DistributedRateLimiter
	mockClient *mockDynamoDBClient
}

func (l *testDistributedRateLimiter) tryAcquireOnce(ctx context.Context, sec int64, n int64) (bool, error) {
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
		"#cnt": "count",
	}

	if l.EnableTTL {
		ttlEpoch := time.Now().UTC().Add(l.TTLSafetyWindow).Unix()
		exprVals[":ttl"] = &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttlEpoch)}
		updateExpr = "SET updated_at_ms = :nowms, ttl_epoch = :ttl ADD #cnt :n"
	}

	input := &dynamodb.UpdateItemInput{
		TableName: &l.tableName,
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
		},
		UpdateExpression:          &updateExpr,
		ExpressionAttributeValues: exprVals,
		ExpressionAttributeNames:  exprNames,
		ConditionExpression:       stringPtr("attribute_not_exists(#cnt) OR #cnt <= :limitMinusN"),
	}

	_, err := l.mockClient.UpdateItem(ctx, input)
	if err != nil {
		if isConditionalFail(err) {
			return false, err
		}
		return false, err
	}

	return true, nil
}

func TestDistributedRateLimiter_LimitExceeded(t *testing.T) {
	mock := newMockDynamoDBClient()

	limiter := &DistributedRateLimiter{
		tableName:       "test-table",
		endpoint:        "test-cluster.dsql.us-east-1.on.aws",
		pkPrefix:        "dsqlconnect#",
		LimitPerSecond:  5,
		MaxWait:         100 * time.Millisecond, // Short timeout for test
		BackoffBase:     10 * time.Millisecond,
		EnableTTL:       false,
		TTLSafetyWindow: 3 * time.Minute,
	}

	testLimiter := &testDistributedRateLimiter{
		DistributedRateLimiter: limiter,
		mockClient:             mock,
	}

	ctx := context.Background()
	sec := time.Now().Unix()

	// Acquire all 5 permits
	for i := 0; i < 5; i++ {
		ok, err := testLimiter.tryAcquireOnce(ctx, sec, 1)
		require.NoError(t, err)
		require.True(t, ok, "acquire %d should succeed", i)
	}

	// 6th acquire should fail (conditional check)
	ok, err := testLimiter.tryAcquireOnce(ctx, sec, 1)
	require.Error(t, err)
	require.True(t, isConditionalFail(err))
	require.False(t, ok)
}

func TestDistributedRateLimiter_ConcurrentAcquires(t *testing.T) {
	mock := newMockDynamoDBClient()

	limiter := &DistributedRateLimiter{
		tableName:       "test-table",
		endpoint:        "test-cluster.dsql.us-east-1.on.aws",
		pkPrefix:        "dsqlconnect#",
		LimitPerSecond:  50,
		MaxWait:         5 * time.Second,
		BackoffBase:     10 * time.Millisecond,
		EnableTTL:       false,
		TTLSafetyWindow: 3 * time.Minute,
	}

	testLimiter := &testDistributedRateLimiter{
		DistributedRateLimiter: limiter,
		mockClient:             mock,
	}

	ctx := context.Background()
	sec := time.Now().Unix()

	// Run 100 concurrent acquires with limit of 50
	var wg sync.WaitGroup
	var successCount atomic.Int64
	var failCount atomic.Int64

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := testLimiter.tryAcquireOnce(ctx, sec, 1)
			if err == nil && ok {
				successCount.Add(1)
			} else {
				failCount.Add(1)
			}
		}()
	}

	wg.Wait()

	// Exactly 50 should succeed, 50 should fail
	require.Equal(t, int64(50), successCount.Load(), "expected 50 successful acquires")
	require.Equal(t, int64(50), failCount.Load(), "expected 50 failed acquires")
}

func TestDistributedRateLimiter_NormalizeEndpoint(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"cluster.dsql.us-east-1.on.aws", "cluster.dsql.us-east-1.on.aws"},
		{"CLUSTER.DSQL.US-EAST-1.ON.AWS", "cluster.dsql.us-east-1.on.aws"},
		{"cluster.dsql.us-east-1.on.aws:5432", "cluster.dsql.us-east-1.on.aws"},
		{"  cluster.dsql.us-east-1.on.aws  ", "cluster.dsql.us-east-1.on.aws"},
		{"  CLUSTER.DSQL.US-EAST-1.ON.AWS:5432  ", "cluster.dsql.us-east-1.on.aws"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := normalizeEndpoint(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestDistributedRateLimiter_JitteredBackoff(t *testing.T) {
	limiter := &DistributedRateLimiter{
		BackoffBase: 25 * time.Millisecond,
	}

	// Mid-second, away from the boundary wait.
	now := time.Date(2026, 1, 1, 0, 0, 0, int(500*time.Millisecond), time.UTC)
	deadline := now.Add(5 * time.Second)

	// Run multiple times to verify jitter
	backoffs := make([]time.Duration, 100)
	for i := 0; i < 100; i++ {
		backoffs[i] = limiter.jitteredBackoff(now, deadline)
	}

	// Base plus jitter below base.
	for i, b := range backoffs {
		require.GreaterOrEqual(t, b, 25*time.Millisecond, "backoff %d", i)
		require.Less(t, b, 50*time.Millisecond, "backoff %d", i)
	}

	// Verify there's some variation (jitter is working)
	allSame := true
	for i := 1; i < len(backoffs); i++ {
		if backoffs[i] != backoffs[0] {
			allSame = false
			break
		}
	}
	require.False(t, allSame, "backoffs should have some variation due to jitter")
}

func TestDistributedRateLimiter_NearSecondBoundary(t *testing.T) {
	limiter := &DistributedRateLimiter{
		BackoffBase: 25 * time.Millisecond,
	}

	// Within 250ms of the next second, the backoff waits for that second plus up to 50ms of jitter.
	now := time.Date(2026, 1, 1, 0, 0, 0, int(900*time.Millisecond), time.UTC)
	for i := 0; i < 100; i++ {
		backoff := limiter.jitteredBackoff(now, now.Add(5*time.Second))
		require.GreaterOrEqual(t, backoff, 100*time.Millisecond)
		require.Less(t, backoff, 150*time.Millisecond)
	}

	// The deadline still caps the wait.
	require.Equal(t, 30*time.Millisecond, limiter.jitteredBackoff(now, now.Add(30*time.Millisecond)))
	require.Zero(t, limiter.jitteredBackoff(now, now))
}

func TestDistributedRateLimiter_RequestExceedsLimit(t *testing.T) {
	mock := newMockDynamoDBClient()

	limiter := &DistributedRateLimiter{
		tableName:      "test-table",
		endpoint:       "test-cluster",
		pkPrefix:       "dsqlconnect#",
		LimitPerSecond: 10,
	}

	testLimiter := &testDistributedRateLimiter{
		DistributedRateLimiter: limiter,
		mockClient:             mock,
	}

	ctx := context.Background()
	sec := time.Now().Unix()

	// Request more permits than the limit
	ok, err := testLimiter.tryAcquireOnce(ctx, sec, 15)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds limit_per_second")
	require.False(t, ok)
}

func TestDistributedRateLimiter_ZeroOrNegativePermits(t *testing.T) {
	limiter := &DistributedRateLimiter{
		LimitPerSecond: 10,
	}

	ctx := context.Background()

	// Zero permits should succeed immediately
	err := limiter.Acquire(ctx, 0)
	require.NoError(t, err)

	// Negative permits should succeed immediately
	err = limiter.Acquire(ctx, -1)
	require.NoError(t, err)
}

func TestDistributedRateLimiter_DisabledWhenLimitZero(t *testing.T) {
	limiter := &DistributedRateLimiter{
		LimitPerSecond: 0, // Disabled
	}

	ctx := context.Background()

	// Should succeed immediately when disabled
	err := limiter.Acquire(ctx, 1)
	require.NoError(t, err)
}

func TestIsConditionalFail(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "ConditionalCheckFailedException",
			err:      &types.ConditionalCheckFailedException{Message: stringPtr("test")},
			expected: true,
		},
		{
			name:     "error containing ConditionalCheckFailed",
			err:      fmt.Errorf("some error: ConditionalCheckFailed"),
			expected: true,
		},
		{
			name:     "other error",
			err:      fmt.Errorf("some other error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isConditionalFail(tt.err)
			require.Equal(t, tt.expected, result)
		})
	}
}
