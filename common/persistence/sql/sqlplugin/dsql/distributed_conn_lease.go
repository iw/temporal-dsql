package dsql

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// Note: DistributedConnLeaseEnabledEnvVar, DistributedConnLeaseTableEnvVar,
// DistributedConnLimitEnvVar, and DefaultDistributedConnLimit are defined
// in conn_lease_config.go to avoid duplicate declarations.

const (
	// DefaultDistributedConnLeaseTTL is the default TTL for connection lease items in DynamoDB.
	// This is used for automatic cleanup if a service crashes without releasing its leases.
	DefaultDistributedConnLeaseTTL = 3 * time.Minute
)

// ConnLeaseManager describes the retained connection lease implementation (unwired pending #33).
// It is implemented by DistributedConnLeases.
//
// Acquire blocks (caller-controlled) until a lease is acquired or an error occurs.
// Release should free the lease as best-effort.
type ConnLeaseManager interface {
	Acquire(ctx context.Context) (leaseID string, err error)
	Release(ctx context.Context, leaseID string) error
}

// DistributedConnLeases enforces a global per-endpoint connection-count limit via DynamoDB.
//
// Two-item approach:
//   - Counter item: pk=dsqllease_counter#<endpoint>, attribute active
//   - Per-connection lease item: pk=dsqllease#<endpoint>#<leaseID>, TTL cleanup
//
// We use TransactWriteItems for atomicity.
type DistributedConnLeases struct {
	ddb         *dynamodb.Client
	table       string
	endpoint    string
	limit       int64
	ttl         time.Duration
	serviceName string
	logger      log.Logger
}

func NewDistributedConnLeases(ddb *dynamodb.Client, table, endpoint, serviceName string, limit int64, ttl time.Duration, logger log.Logger) (*DistributedConnLeases, error) {
	if ddb == nil {
		return nil, fmt.Errorf("ddb client is nil")
	}
	if table == "" {
		return nil, fmt.Errorf("lease table is empty")
	}
	if endpoint == "" {
		return nil, fmt.Errorf("endpoint is empty")
	}
	if limit <= 0 {
		limit = DefaultDistributedConnLimit
	}
	if ttl <= 0 {
		ttl = DefaultDistributedConnLeaseTTL
	}
	if logger == nil {
		logger = log.NewNoopLogger()
	}
	return &DistributedConnLeases{
		ddb:         ddb,
		table:       table,
		endpoint:    endpoint,
		limit:       limit,
		ttl:         ttl,
		serviceName: serviceName,
		logger:      logger,
	}, nil
}

func (l *DistributedConnLeases) Acquire(ctx context.Context) (string, error) {
	leaseID, err := newLeaseID()
	if err != nil {
		return "", err
	}

	now := time.Now().UTC()
	ttlEpoch := now.Add(l.ttl).Unix()
	counterPK := fmt.Sprintf("dsqllease_counter#%s", l.endpoint)
	leasePK := fmt.Sprintf("dsqllease#%s#%s", l.endpoint, leaseID)

	// Update counter item (increment active) iff active < limit.
	updateCounter := &types.TransactWriteItem{
		Update: &types.Update{
			TableName: aws.String(l.table),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: counterPK},
			},
			UpdateExpression:    aws.String("SET active = if_not_exists(active, :zero) + :one, updated_ms = :nowms"),
			ConditionExpression: aws.String("attribute_not_exists(active) OR active < :limit"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":zero":  &types.AttributeValueMemberN{Value: "0"},
				":one":   &types.AttributeValueMemberN{Value: "1"},
				":limit": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", l.limit)},
				":nowms": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", now.UnixMilli())},
			},
		},
	}

	putLease := &types.TransactWriteItem{
		Put: &types.Put{
			TableName: aws.String(l.table),
			Item: map[string]types.AttributeValue{
				"pk":           &types.AttributeValueMemberS{Value: leasePK},
				"ttl_epoch":    &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttlEpoch)},
				"service_name": &types.AttributeValueMemberS{Value: l.serviceName},
				"created_ms":   &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", now.UnixMilli())},
			},
			ConditionExpression: aws.String("attribute_not_exists(pk)"),
		},
	}

	_, err = l.ddb.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{*updateCounter, *putLease},
	})
	if err != nil {
		// Most likely ConditionalCheckFailed due to limit.
		l.logger.Debug("distributed conn lease acquire failed", tag.Error(err), tag.NewStringTag("endpoint", l.endpoint))
		return "", err
	}

	return leaseID, nil
}

func (l *DistributedConnLeases) Release(ctx context.Context, leaseID string) error {
	if leaseID == "" {
		return nil
	}

	counterPK := fmt.Sprintf("dsqllease_counter#%s", l.endpoint)
	leasePK := fmt.Sprintf("dsqllease#%s#%s", l.endpoint, leaseID)

	now := time.Now().UTC()

	deleteLease := &types.TransactWriteItem{
		Delete: &types.Delete{
			TableName: aws.String(l.table),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: leasePK},
			},
		},
	}

	decrementCounter := &types.TransactWriteItem{
		Update: &types.Update{
			TableName: aws.String(l.table),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: counterPK},
			},
			UpdateExpression:    aws.String("SET active = active - :one, updated_ms = :nowms"),
			ConditionExpression: aws.String("attribute_exists(active) AND active >= :one"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":one":   &types.AttributeValueMemberN{Value: "1"},
				":nowms": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", now.UnixMilli())},
			},
		},
	}

	_, err := l.ddb.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{*deleteLease, *decrementCounter},
	})
	if err != nil {
		// Best-effort release. If the lease item already expired or was deleted, this may fail.
		// We log at debug and continue.
		l.logger.Debug("distributed conn lease release failed", tag.Error(err), tag.NewStringTag("endpoint", l.endpoint))
		return err
	}
	return nil
}

func newLeaseID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// IsLeaseDenied attempts to classify lease denial errors.
func IsLeaseDenied(err error) bool {
	if err == nil {
		return false
	}
	var cfe *types.TransactionCanceledException
	if errors.As(err, &cfe) {
		return true
	}
	return false
}
