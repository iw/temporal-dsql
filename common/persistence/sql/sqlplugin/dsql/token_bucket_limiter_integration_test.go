//go:build integration

package dsql

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestGCRADynamoDBIntegration(t *testing.T) {
	table := os.Getenv("DSQL_TEST_DDB_TABLE")
	if table == "" {
		t.Skip("set DSQL_TEST_DDB_TABLE to a table with a pk string key and TTL on ttl_epoch")
	}
	cfg, err := awsconfig.LoadDefaultConfig(t.Context())
	require.NoError(t, err)
	client := dynamodb.NewFromConfig(cfg)
	endpoint := "test-gcra-" + uuid.NewString()
	l := NewTokenBucketLimiter(client, table, endpoint, nil)
	l.Rate, l.Capacity, l.MaxWait = 10, 5, time.Second
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: aws.String(table),
			Key:       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: tokenBucketPKPrefix + endpoint}},
		})
		require.NoError(t, err)
	})
	const duration = 4 * time.Second
	ctx, cancel := context.WithTimeout(t.Context(), duration)
	defer cancel()
	start := time.Now()
	var mu sync.Mutex
	var admitted []time.Duration
	results := make(chan error, 4)
	for range 4 {
		go func() {
			for {
				if err := l.Wait(ctx); err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						results <- nil
					} else {
						results <- err
					}
					return
				}
				mu.Lock()
				admitted = append(admitted, time.Since(start))
				mu.Unlock()
			}
		}()
	}
	for range 4 {
		require.NoError(t, <-results)
	}
	require.NotEmpty(t, admitted)
	for i, elapsed := range admitted {
		require.LessOrEqual(t, int64(i+1), l.Capacity+int64(elapsed/time.Second)*l.Rate+l.Rate*int64(elapsed%time.Second)/int64(time.Second))
	}
	t.Logf("%d admissions in %s at %d/sec with burst %d", len(admitted), duration, l.Rate, l.Capacity)
}
