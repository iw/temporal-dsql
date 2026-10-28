package dsql

import (
	"context"
	"database/sql"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

const dsqlVisibilityUnsupportedMsg = "DSQL plugin does not support SQL visibility. Configure Elasticsearch/OpenSearch (recommended) for visibility instead."

// InsertIntoVisibility inserts a row into visibility table.
func (pdb *db) InsertIntoVisibility(
	ctx context.Context,
	row *sqlplugin.VisibilityRow,
) (sql.Result, error) {
	return nil, serviceerror.NewUnimplemented(dsqlVisibilityUnsupportedMsg)
}

// ReplaceIntoVisibility replaces an existing row if it exists or creates a new row in visibility table.
func (pdb *db) ReplaceIntoVisibility(
	ctx context.Context,
	row *sqlplugin.VisibilityRow,
) (sql.Result, error) {
	return nil, serviceerror.NewUnimplemented(dsqlVisibilityUnsupportedMsg)
}

// DeleteFromVisibility deletes a row from visibility table if it exists.
func (pdb *db) DeleteFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilityDeleteFilter,
) (sql.Result, error) {
	return nil, serviceerror.NewUnimplemented(dsqlVisibilityUnsupportedMsg)
}

// SelectFromVisibility reads one or more rows from visibility table.
func (pdb *db) SelectFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) ([]sqlplugin.VisibilityRow, error) {
	return nil, serviceerror.NewUnimplemented(dsqlVisibilityUnsupportedMsg)
}

// GetFromVisibility reads one row from visibility table.
func (pdb *db) GetFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilityGetFilter,
) (*sqlplugin.VisibilityRow, error) {
	return nil, serviceerror.NewUnimplemented(dsqlVisibilityUnsupportedMsg)
}

func (pdb *db) CountFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) (int64, error) {
	return 0, serviceerror.NewUnimplemented(dsqlVisibilityUnsupportedMsg)
}

func (pdb *db) CountGroupByFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) ([]sqlplugin.VisibilityCountRow, error) {
	return nil, serviceerror.NewUnimplemented(dsqlVisibilityUnsupportedMsg)
}