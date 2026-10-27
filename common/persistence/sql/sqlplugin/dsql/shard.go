package dsql

import (
	"context"
	"database/sql"
	"fmt"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	createShardQry = `INSERT INTO
 shards (shard_id, range_id, data, data_encoding) VALUES ($1, $2, $3, $4)`

	getShardQry = `SELECT
 shard_id, range_id, data, data_encoding
 FROM shards WHERE shard_id = $1`

	// NOTE: updateShardQry is intentionally unfenced (matches upstream Postgres plugin semantics).
	// On DSQL, callers should prefer UpdateShardsWithFencing / UpdateShardsAutoFenced for correctness.
	updateShardQry = `UPDATE shards 
 SET range_id = $1, data = $2, data_encoding = $3 
 WHERE shard_id = $4`

	lockShardQry     = `SELECT range_id FROM shards WHERE shard_id = $1 FOR UPDATE`
	readLockShardQry = `SELECT range_id FROM shards WHERE shard_id = $1 FOR KEY SHARE`
)

// InsertIntoShards inserts one or more rows into shards table
func (pdb *db) InsertIntoShards(
	ctx context.Context,
	row *sqlplugin.ShardsRow,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		createShardQry,
		row.ShardID,
		row.RangeID,
		row.Data,
		row.DataEncoding,
	)
}

// UpdateShards updates one or more rows into shards table.
//
// DSQL behavior:
//   - In transaction contexts (pdb.tx != nil), this method must preserve upstream semantics:
//     the caller has already acquired the shard row via FOR UPDATE and relies on the
//     transaction as the fence. We therefore perform the unfenced UPDATE.
//   - Outside a transaction (pdb.tx == nil), we perform an auto-fenced conditional update
//     using the currently observed range_id as the expected fencing token.
func (pdb *db) UpdateShards(
	ctx context.Context,
	row *sqlplugin.ShardsRow,
) (sql.Result, error) {
	// In a transaction, preserve upstream behavior (lock + tx acts as the fence).
	if pdb.tx != nil {
		return pdb.ExecContext(ctx,
			updateShardQry,
			row.RangeID,
			row.Data,
			row.DataEncoding,
			row.ShardID,
		)
	}

	// Outside a transaction, do a safe fenced update.
	return pdb.UpdateShardsAutoFenced(ctx, row)
}

// UpdateShardsAutoFenced performs a fenced update for non-transaction contexts.
// It reads the current range_id and then updates only if that range_id is unchanged.
// This does not impose monotonicity rules; it only enforces "no lost update".
func (pdb *db) UpdateShardsAutoFenced(
	ctx context.Context,
	row *sqlplugin.ShardsRow,
) (sql.Result, error) {
	var currentRangeID int64
	if err := pdb.GetContext(ctx, &currentRangeID, `SELECT range_id FROM shards WHERE shard_id = $1`, row.ShardID); err != nil {
		return nil, err
	}

	// Fenced update: allow any range_id the caller provides (equal, +1, etc.),
	// but only if the row hasn't changed since we read it.
	return pdb.UpdateShardsWithFencing(ctx, row, currentRangeID)
}

// SelectFromShards reads one or more rows from shards table
func (pdb *db) SelectFromShards(
	ctx context.Context,
	filter sqlplugin.ShardsFilter,
) (*sqlplugin.ShardsRow, error) {
	var row sqlplugin.ShardsRow
	err := pdb.GetContext(ctx,
		&row,
		getShardQry,
		filter.ShardID,
	)
	if err != nil {
		return nil, err
	}
	return &row, nil
}

// ReadLockShards acquires a read lock on a single row in shards table.
// FOR KEY SHARE fences subsequent writes against concurrent shard ownership changes
// using FOR UPDATE, without conflicting with other key-share readers.
func (pdb *db) ReadLockShards(
	ctx context.Context,
	filter sqlplugin.ShardsFilter,
) (int64, error) {
	// Use retry manager if available (for non-transaction contexts).
	if pdb.tx == nil && pdb.retryManager != nil {
		result, err := pdb.retryManager.RunTx(ctx, "ReadLockShards", func(tx *sql.Tx) (interface{}, error) {
			var rangeID int64
			err := tx.QueryRowContext(ctx, readLockShardQry, filter.ShardID).Scan(&rangeID)
			return rangeID, err
		})
		if err != nil {
			return 0, err
		}
		return result.(int64), nil
	}

	// Direct execution for transaction contexts (retry handled at higher level).
	var rangeID int64
	err := pdb.GetContext(ctx,
		&rangeID,
		readLockShardQry,
		filter.ShardID,
	)
	return rangeID, err
}

// WriteLockShards acquires a write lock on a single row in shards table
// DSQL Override: Wraps FOR UPDATE operation in retry logic for SQLSTATE 40001 handling.
// Call-site contract: All subsequent updates must use CAS with range_id fencing.
func (pdb *db) WriteLockShards(
	ctx context.Context,
	filter sqlplugin.ShardsFilter,
) (int64, error) {
	// Use retry manager if available (for non-transaction contexts).
	if pdb.tx == nil && pdb.retryManager != nil {
		result, err := pdb.retryManager.RunTx(ctx, "WriteLockShards", func(tx *sql.Tx) (interface{}, error) {
			var rangeID int64
			err := tx.QueryRowContext(ctx, lockShardQry, filter.ShardID).Scan(&rangeID)
			return rangeID, err
		})
		if err != nil {
			return 0, err
		}
		return result.(int64), nil
	}

	// Direct execution for transaction contexts (retry handled at higher level).
	var rangeID int64
	err := pdb.GetContext(ctx,
		&rangeID,
		lockShardQry,
		filter.ShardID,
	)
	return rangeID, err
}

// UpdateShardsWithFencing performs a fenced update on shards table using range_id as fencing token.
// This is the safe method for updating shards in DSQL's optimistic concurrency model.
func (pdb *db) UpdateShardsWithFencing(
	ctx context.Context,
	row *sqlplugin.ShardsRow,
	expectedRangeID int64,
) (sql.Result, error) {
	const updateShardWithFencingQry = `UPDATE shards 
		SET range_id = $1, data = $2, data_encoding = $3 
		WHERE shard_id = $4 AND range_id = $5`

	fmt.Printf("[DSQL-DEBUG] UpdateShardsWithFencing: shard_id=%d, expected_range_id=%d, new_range_id=%d\n",
		int(row.ShardID), expectedRangeID, row.RangeID)

	result, err := pdb.ExecContext(ctx,
		updateShardWithFencingQry,
		row.RangeID,
		row.Data,
		row.DataEncoding,
		row.ShardID,
		expectedRangeID,
	)
	if err != nil {
		return nil, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}

	if rowsAffected == 0 {
		return nil, NewConditionFailedError(
			ConditionFailedShard,
			fmt.Sprintf("shard %d range_id changed from expected %d (fenced update failed)", row.ShardID, expectedRangeID),
		)
	}

	return result, nil
}
