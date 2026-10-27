package dsql

import (
	"context"
	"database/sql"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const (
	// below are templates for history_node table
	addHistoryNodesQuery = `INSERT INTO history_node (` +
		`shard_id, tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding) ` +
		`VALUES (:shard_id, :tree_id, :branch_id, :node_id, :prev_txn_id, :txn_id, :data, :data_encoding) ` +
		`ON CONFLICT (shard_id, tree_id, branch_id, node_id, txn_id) DO ` +
		`UPDATE SET prev_txn_id=:prev_txn_id, data=:data, data_encoding=:data_encoding `

	getHistoryNodesQuery = `SELECT node_id, prev_txn_id, txn_id, data, data_encoding FROM history_node ` +
		`WHERE shard_id = $1 AND tree_id = $2 AND branch_id = $3 AND ((node_id = $4 AND txn_id > $5) OR node_id > $6) AND node_id < $7 ` +
		`ORDER BY shard_id, tree_id, branch_id, node_id, txn_id LIMIT $8 `

	getHistoryNodesReverseQuery = `SELECT node_id, prev_txn_id, txn_id, data, data_encoding FROM history_node ` +
		`WHERE shard_id = $1 AND tree_id = $2 AND branch_id = $3 AND node_id >= $4 AND ((node_id = $5 AND txn_id < $6) OR node_id < $7) ` +
		`ORDER BY shard_id, tree_id, branch_id DESC, node_id DESC, txn_id DESC LIMIT $8 `

	getHistoryNodeMetadataQuery = `SELECT node_id, prev_txn_id, txn_id FROM history_node ` +
		`WHERE shard_id = $1 AND tree_id = $2 AND branch_id = $3 AND ((node_id = $4 AND txn_id > $5) OR node_id > $6) AND node_id < $7 ` +
		`ORDER BY shard_id, tree_id, branch_id, node_id, txn_id LIMIT $8 `

	deleteHistoryNodeQuery = `DELETE FROM history_node WHERE shard_id = $1 AND tree_id = $2 AND branch_id = $3 AND node_id = $4 AND txn_id = $5 `

	deleteHistoryNodesQuery = `DELETE FROM history_node WHERE (shard_id, tree_id, branch_id, node_id, txn_id) IN (
 SELECT shard_id, tree_id, branch_id, node_id, txn_id FROM history_node
 WHERE shard_id = $1 AND tree_id = $2 AND branch_id = $3 AND node_id >= $4
 ORDER BY shard_id, tree_id, branch_id, node_id, txn_id LIMIT $5)`

	// below are templates for history_tree table
	addHistoryTreeQuery = `INSERT INTO history_tree (` +
		`shard_id, tree_id, branch_id, data, data_encoding) ` +
		`VALUES (:shard_id, :tree_id, :branch_id, :data, :data_encoding) ` +
		`ON CONFLICT (shard_id, tree_id, branch_id) DO UPDATE ` +
		`SET data = excluded.data, data_encoding = excluded.data_encoding`

	getHistoryTreeQuery = `SELECT branch_id, data, data_encoding FROM history_tree WHERE shard_id = $1 AND tree_id = $2 `

	paginateBranchesQuery = `SELECT shard_id, tree_id, branch_id, data, data_encoding
        FROM history_tree
        WHERE (shard_id, tree_id, branch_id) > ($1, $2, $3)
        ORDER BY shard_id, tree_id, branch_id
        LIMIT $4`

	deleteHistoryTreeQuery = `DELETE FROM history_tree WHERE shard_id = $1 AND tree_id = $2 AND branch_id = $3 `
)

// For history_node table:

// InsertIntoHistoryNode inserts a row into history_node table
func (pdb *db) InsertIntoHistoryNode(
	ctx context.Context,
	row *sqlplugin.HistoryNodeRow,
) (sql.Result, error) {
	// NOTE: txn_id is *= -1 within DB
	txnID := -row.TxnID

	args := map[string]interface{}{
		"shard_id":      row.ShardID,
		"tree_id":       UUIDToString(row.TreeID),
		"branch_id":     UUIDToString(row.BranchID),
		"node_id":       row.NodeID,
		"prev_txn_id":   row.PrevTxnID,
		"txn_id":        txnID,
		"data":          row.Data,
		"data_encoding": row.DataEncoding,
	}

	return pdb.NamedExecContext(ctx, addHistoryNodesQuery, args)
}

// DeleteFromHistoryNode delete a row from history_node table
func (pdb *db) DeleteFromHistoryNode(
	ctx context.Context,
	row *sqlplugin.HistoryNodeRow,
) (sql.Result, error) {
	// NOTE: txn_id is *= -1 within DB
	txnID := -row.TxnID

	return pdb.ExecContext(ctx,
		deleteHistoryNodeQuery,
		row.ShardID,
		UUIDToString(row.TreeID),
		UUIDToString(row.BranchID),
		row.NodeID,
		txnID,
	)
}

// RangeSelectFromHistoryNode reads one or more rows from history_node table
func (pdb *db) RangeSelectFromHistoryNode(
	ctx context.Context,
	filter sqlplugin.HistoryNodeSelectFilter,
) ([]sqlplugin.HistoryNodeRow, error) {
	var query string
	if filter.MetadataOnly {
		query = getHistoryNodeMetadataQuery
	} else if filter.ReverseOrder {
		query = getHistoryNodesReverseQuery
	} else {
		query = getHistoryNodesQuery
	}

	treeID := UUIDToString(filter.TreeID)
	branchID := UUIDToString(filter.BranchID)

	var args []interface{}
	if filter.ReverseOrder {
		args = []interface{}{
			filter.ShardID,
			treeID,
			branchID,
			filter.MinNodeID,
			filter.MaxTxnID,
			-filter.MaxTxnID,
			filter.MaxNodeID,
			filter.PageSize,
		}
	} else {
		args = []interface{}{
			filter.ShardID,
			treeID,
			branchID,
			filter.MinNodeID,
			-filter.MinTxnID, // NOTE: transaction ID is *= -1 when stored
			filter.MinNodeID,
			filter.MaxNodeID,
			filter.PageSize,
		}
	}

	var rows []sqlplugin.HistoryNodeRow
	err := pdb.SelectContext(ctx, &rows, query, args...)
	if err != nil {
		return nil, err
	}

	// NOTE: since we store txn_id multiplied by -1 when inserting, revert it back here
	for i := range rows {
		rows[i].TxnID = -rows[i].TxnID
	}

	return rows, nil
}

// RangeDeleteFromHistoryNode deletes one or more rows from history_node table
func (pdb *db) RangeDeleteFromHistoryNode(
	ctx context.Context,
	filter sqlplugin.HistoryNodeDeleteFilter,
) (sql.Result, error) {
	return pdb.deleteInBatches(ctx, historyNodeDeleteBatchSize,
		deleteHistoryNodesQuery,
		filter.ShardID,
		UUIDToString(filter.TreeID),
		UUIDToString(filter.BranchID),
		filter.MinNodeID,
	)
}

// For history_tree table:

// InsertIntoHistoryTree inserts a row into history_tree table
func (pdb *db) InsertIntoHistoryTree(
	ctx context.Context,
	row *sqlplugin.HistoryTreeRow,
) (sql.Result, error) {
	args := map[string]interface{}{
		"shard_id":      row.ShardID,
		"tree_id":       UUIDToString(row.TreeID),
		"branch_id":     UUIDToString(row.BranchID),
		"data":          row.Data,
		"data_encoding": row.DataEncoding,
	}

	return pdb.NamedExecContext(ctx, addHistoryTreeQuery, args)
}

// SelectFromHistoryTree reads one or more rows from history_tree table
func (pdb *db) SelectFromHistoryTree(
	ctx context.Context,
	filter sqlplugin.HistoryTreeSelectFilter,
) ([]sqlplugin.HistoryTreeRow, error) {
	var rows []sqlplugin.HistoryTreeRow
	err := pdb.SelectContext(ctx,
		&rows,
		getHistoryTreeQuery,
		filter.ShardID,
		UUIDToString(filter.TreeID),
	)
	return rows, err
}

// PaginateBranchesFromHistoryTree reads up to page.Limit rows from the history_tree table sorted by their primary key,
// while skipping the first page.Offset rows.
func (pdb *db) PaginateBranchesFromHistoryTree(
	ctx context.Context,
	page sqlplugin.HistoryTreeBranchPage,
) ([]sqlplugin.HistoryTreeRow, error) {
	var rows []sqlplugin.HistoryTreeRow
	err := pdb.SelectContext(ctx,
		&rows,
		paginateBranchesQuery,
		page.ShardID,
		UUIDToString(page.TreeID),
		UUIDToString(page.BranchID),
		page.Limit,
	)
	return rows, err
}

// DeleteFromHistoryTree deletes one or more rows from history_tree table
func (pdb *db) DeleteFromHistoryTree(
	ctx context.Context,
	filter sqlplugin.HistoryTreeDeleteFilter,
) (sql.Result, error) {
	return pdb.ExecContext(ctx,
		deleteHistoryTreeQuery,
		filter.ShardID,
		UUIDToString(filter.TreeID),
		UUIDToString(filter.BranchID),
	)
}
