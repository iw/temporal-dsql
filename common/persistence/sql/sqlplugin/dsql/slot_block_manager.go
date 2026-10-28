package dsql

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// SlotBlockManager manages connection slot blocks for distributed connection limiting.
//
// Instead of incrementing a single counter per connection (hot partition), this allocates
// blocks of connection slots to each service process. Once a process owns a block, it can
// create connections up to the block's slot count without any DynamoDB calls.
//
// Table schema (uses existing conn lease table):
//
//	pk = connslots#<endpoint>#block-<i>
//	owner_id = <uuid> (empty string if unowned)
//	ttl_epoch = <unix_timestamp> (for crash recovery)
//	slots = <int> (number of slots in this block)
//	service_name = <string> (for debugging)
//	acquired_at_ms = <int64>
//
// This spreads load across multiple partition keys (one per block) instead of
// concentrating all writes on a single counter item.
type SlotBlockManager struct {
	ddb         *dynamodb.Client
	table       string
	endpoint    string
	serviceName string
	ownerID     string
	logger      log.Logger
	metrics     SlotBlockMetrics

	// Configuration
	blockSize   int           // Slots per block (default: 100)
	blockCount  int           // Total blocks (default: 100 = 10k total slots)
	ttl         time.Duration // TTL for crash recovery (default: 3m)
	renewPeriod time.Duration // How often to renew TTL (default: 1m)

	// State
	mu          sync.RWMutex
	ownedBlocks map[int]bool // Block indices we own
	totalSlots  int          // Total slots across all owned blocks
	usedSlots   atomic.Int64 // Slots currently in use
	stopC       chan struct{}
	stopOnce    sync.Once
	renewerWg   sync.WaitGroup
	initialized bool
}

// SlotBlockConfig holds configuration for the slot block manager.
type SlotBlockConfig struct {
	BlockSize   int           // Slots per block (default: 100)
	BlockCount  int           // Total number of blocks (default: 100)
	TTL         time.Duration // TTL for crash recovery (default: 3m)
	RenewPeriod time.Duration // How often to renew TTL (default: 1m)
}

// DefaultSlotBlockConfig returns the default configuration.
func DefaultSlotBlockConfig() SlotBlockConfig {
	return SlotBlockConfig{
		BlockSize:   100,
		BlockCount:  100,
		TTL:         3 * time.Minute,
		RenewPeriod: 1 * time.Minute,
	}
}

// NewSlotBlockManager creates a new slot block manager.
func NewSlotBlockManager(
	ddb *dynamodb.Client,
	table string,
	endpoint string,
	serviceName string,
	cfg SlotBlockConfig,
	logger log.Logger,
	metrics SlotBlockMetrics,
) (*SlotBlockManager, error) {
	if ddb == nil {
		return nil, fmt.Errorf("ddb client is nil")
	}
	if table == "" {
		return nil, fmt.Errorf("table name is empty")
	}
	if endpoint == "" {
		return nil, fmt.Errorf("endpoint is empty")
	}

	ownerID, err := generateOwnerID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate owner ID: %w", err)
	}

	if cfg.BlockSize <= 0 {
		cfg.BlockSize = 100
	}
	if cfg.BlockCount <= 0 {
		cfg.BlockCount = 100
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 3 * time.Minute
	}
	if cfg.RenewPeriod <= 0 {
		cfg.RenewPeriod = 1 * time.Minute
	}

	if logger == nil {
		logger = log.NewNoopLogger()
	}
	if metrics == nil {
		metrics = &noOpSlotBlockMetrics{}
	}

	return &SlotBlockManager{
		ddb:         ddb,
		table:       table,
		endpoint:    endpoint,
		serviceName: serviceName,
		ownerID:     ownerID,
		logger:      logger,
		metrics:     metrics,
		blockSize:   cfg.BlockSize,
		blockCount:  cfg.BlockCount,
		ttl:         cfg.TTL,
		renewPeriod: cfg.RenewPeriod,
		ownedBlocks: make(map[int]bool),
		stopC:       make(chan struct{}),
	}, nil
}

// AcquireSlots attempts to acquire enough blocks to have at least targetSlots available.
// This should be called during service startup before the refiller begins.
// Returns the number of slots actually acquired.
func (m *SlotBlockManager) AcquireSlots(ctx context.Context, targetSlots int) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	blocksNeeded := (targetSlots + m.blockSize - 1) / m.blockSize // Ceiling division
	blocksAcquired := 0

	// Randomize starting block index to reduce contention when many services start simultaneously.
	// Without this, all services would race for blocks 0, 1, 2, ... causing thundering herd.
	startIdx := rand.Intn(m.blockCount)

	m.logger.Info("Acquiring slot blocks",
		tag.NewInt("target_slots", targetSlots),
		tag.NewInt("blocks_needed", blocksNeeded),
		tag.NewInt("block_size", m.blockSize),
		tag.NewInt("start_idx", startIdx))

	// Try to acquire blocks until we have enough or run out of blocks to try
	for i := 0; i < m.blockCount && blocksAcquired < blocksNeeded; i++ {
		blockIdx := (startIdx + i) % m.blockCount

		select {
		case <-ctx.Done():
			m.metrics.RecordSlotBlocksOwned(len(m.ownedBlocks))
			return m.totalSlots, ctx.Err()
		default:
		}

		if m.ownedBlocks[blockIdx] {
			continue // Already own this block
		}

		acquired, err := m.tryAcquireBlock(ctx, blockIdx)
		if err != nil {
			m.logger.Debug("Failed to acquire block",
				tag.NewInt("block_idx", blockIdx),
				tag.Error(err))
			continue
		}

		if acquired {
			m.ownedBlocks[blockIdx] = true
			m.totalSlots += m.blockSize
			blocksAcquired++
			m.logger.Info("Acquired slot block",
				tag.NewInt("block_idx", blockIdx),
				tag.NewInt("total_slots", m.totalSlots),
				tag.NewInt("blocks_acquired", blocksAcquired))
		}
	}

	if m.totalSlots > 0 && !m.initialized {
		m.initialized = true
		m.startRenewer()
	}

	// Record metrics
	m.metrics.RecordSlotBlocksOwned(len(m.ownedBlocks))

	return m.totalSlots, nil
}

// tryAcquireBlock attempts to acquire a single block.
// Returns (true, nil) if acquired, (false, nil) if already owned by another, (false, err) on error.
func (m *SlotBlockManager) tryAcquireBlock(ctx context.Context, blockIdx int) (bool, error) {
	pk := m.blockPK(blockIdx)
	now := time.Now().UTC()
	ttlEpoch := now.Add(m.ttl).Unix()

	// Conditional put: succeed if item doesn't exist OR owner_id is empty OR TTL expired
	input := &dynamodb.PutItemInput{
		TableName: aws.String(m.table),
		Item: map[string]types.AttributeValue{
			"pk":             &types.AttributeValueMemberS{Value: pk},
			"owner_id":       &types.AttributeValueMemberS{Value: m.ownerID},
			"ttl_epoch":      &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttlEpoch)},
			"slots":          &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", m.blockSize)},
			"service_name":   &types.AttributeValueMemberS{Value: m.serviceName},
			"acquired_at_ms": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", now.UnixMilli())},
		},
		// Acquire if: not exists OR owner_id is empty OR TTL expired (crash recovery)
		ConditionExpression: aws.String(
			"attribute_not_exists(pk) OR owner_id = :empty OR ttl_epoch < :now",
		),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":empty": &types.AttributeValueMemberS{Value: ""},
			":now":   &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", now.Unix())},
		},
	}

	_, err := m.ddb.PutItem(ctx, input)
	if err != nil {
		var cfe *types.ConditionalCheckFailedException
		if errors.As(err, &cfe) {
			return false, nil // Block owned by another process
		}
		return false, err
	}

	return true, nil
}

// Acquire allocates a connection slot (unwired pending #33).
// It checks if we have available slots and returns a "lease ID" (just a counter).
// The actual slot tracking is done internally.
func (m *SlotBlockManager) Acquire(ctx context.Context) (string, error) {
	m.mu.RLock()
	totalSlots := m.totalSlots
	m.mu.RUnlock()

	if totalSlots == 0 {
		return "", fmt.Errorf("no slot blocks acquired")
	}

	used := m.usedSlots.Load()
	if used >= int64(totalSlots) {
		return "", fmt.Errorf("all %d slots in use", totalSlots)
	}

	// Optimistically increment
	newUsed := m.usedSlots.Add(1)
	if newUsed > int64(totalSlots) {
		// Race condition - back off
		m.usedSlots.Add(-1)
		return "", fmt.Errorf("slot limit exceeded")
	}

	// Record metrics
	m.metrics.RecordSlotBlockSlotsUsed(newUsed)

	// Return a simple lease ID (we don't actually need to track individual leases)
	return fmt.Sprintf("slot-%d", newUsed), nil
}

// Release returns a connection slot (unwired pending #33).
// It decrements the used slot count.
func (m *SlotBlockManager) Release(ctx context.Context, leaseID string) error {
	if leaseID == "" {
		return nil
	}
	newUsed := m.usedSlots.Add(-1)
	m.metrics.RecordSlotBlockSlotsUsed(newUsed)
	return nil
}

// AvailableSlots returns the number of slots currently available.
func (m *SlotBlockManager) AvailableSlots() int {
	m.mu.RLock()
	total := m.totalSlots
	m.mu.RUnlock()
	used := m.usedSlots.Load()
	avail := int64(total) - used
	if avail < 0 {
		return 0
	}
	return int(avail)
}

// TotalSlots returns the total number of slots owned.
func (m *SlotBlockManager) TotalSlots() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.totalSlots
}

// UsedSlots returns the number of slots currently in use.
func (m *SlotBlockManager) UsedSlots() int64 {
	return m.usedSlots.Load()
}

// startRenewer starts the background goroutine that renews TTLs on owned blocks.
func (m *SlotBlockManager) startRenewer() {
	m.renewerWg.Add(1)
	go func() {
		defer m.renewerWg.Done()
		m.renewLoop()
	}()
}

func (m *SlotBlockManager) renewLoop() {
	ticker := time.NewTicker(m.renewPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopC:
			return
		case <-ticker.C:
			m.renewOwnedBlocks()
		}
	}
}

func (m *SlotBlockManager) renewOwnedBlocks() {
	m.mu.RLock()
	blocks := make([]int, 0, len(m.ownedBlocks))
	for idx := range m.ownedBlocks {
		blocks = append(blocks, idx)
	}
	m.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	renewed := 0
	failed := 0

	for _, blockIdx := range blocks {
		if err := m.renewBlock(ctx, blockIdx); err != nil {
			m.logger.Warn("Failed to renew slot block",
				tag.NewInt("block_idx", blockIdx),
				tag.Error(err))
			failed++
		} else {
			renewed++
		}
	}

	if renewed > 0 || failed > 0 {
		m.logger.Debug("Slot block renewal complete",
			tag.NewInt("renewed", renewed),
			tag.NewInt("failed", failed))
	}
}

func (m *SlotBlockManager) renewBlock(ctx context.Context, blockIdx int) error {
	pk := m.blockPK(blockIdx)
	now := time.Now().UTC()
	ttlEpoch := now.Add(m.ttl).Unix()

	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(m.table),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
		},
		UpdateExpression: aws.String("SET ttl_epoch = :ttl, renewed_at_ms = :now"),
		// Only renew if we still own it
		ConditionExpression: aws.String("owner_id = :owner"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":ttl":   &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", ttlEpoch)},
			":now":   &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", now.UnixMilli())},
			":owner": &types.AttributeValueMemberS{Value: m.ownerID},
		},
	}

	_, err := m.ddb.UpdateItem(ctx, input)
	return err
}

// Stop releases all owned blocks and stops the renewer.
func (m *SlotBlockManager) Stop() {
	m.stopOnce.Do(func() {
		close(m.stopC)
		m.renewerWg.Wait()
		m.releaseAllBlocks()
	})
}

func (m *SlotBlockManager) releaseAllBlocks() {
	m.mu.Lock()
	blocks := make([]int, 0, len(m.ownedBlocks))
	for idx := range m.ownedBlocks {
		blocks = append(blocks, idx)
	}
	m.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, blockIdx := range blocks {
		if err := m.releaseBlock(ctx, blockIdx); err != nil {
			m.logger.Warn("Failed to release slot block",
				tag.NewInt("block_idx", blockIdx),
				tag.Error(err))
		} else {
			m.logger.Info("Released slot block", tag.NewInt("block_idx", blockIdx))
		}
	}

	m.mu.Lock()
	m.ownedBlocks = make(map[int]bool)
	m.totalSlots = 0
	m.mu.Unlock()
}

func (m *SlotBlockManager) releaseBlock(ctx context.Context, blockIdx int) error {
	pk := m.blockPK(blockIdx)

	// Clear owner_id to release the block (don't delete - keep for visibility)
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(m.table),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
		},
		UpdateExpression: aws.String("SET owner_id = :empty, released_at_ms = :now"),
		// Only release if we own it
		ConditionExpression: aws.String("owner_id = :owner"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":empty": &types.AttributeValueMemberS{Value: ""},
			":now":   &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().UTC().UnixMilli())},
			":owner": &types.AttributeValueMemberS{Value: m.ownerID},
		},
	}

	_, err := m.ddb.UpdateItem(ctx, input)
	return err
}

func (m *SlotBlockManager) blockPK(blockIdx int) string {
	return fmt.Sprintf("connslots#%s#block-%d", m.endpoint, blockIdx)
}

func generateOwnerID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
