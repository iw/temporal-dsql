package dsql

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"strings"
	"time"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	dsqlschemaversion "go.temporal.io/server/schema/dsql"
)

const searchPathAttribute = "search_path"

var (
	templateInsertWorkflowExecution = fmt.Sprintf(
		`INSERT INTO executions_visibility (%s)
		VALUES (%s)
		ON CONFLICT (namespace_id, run_id) DO NOTHING`,
		strings.Join(sqlplugin.DbFields, ", "),
		sqlplugin.BuildNamedPlaceholder(sqlplugin.DbFields...),
	)

	templateUpsertWorkflowExecution = fmt.Sprintf(
		`INSERT INTO executions_visibility (%s)
		VALUES (%s)
		%s`,
		strings.Join(sqlplugin.DbFields, ", "),
		sqlplugin.BuildNamedPlaceholder(sqlplugin.DbFields...),
		buildOnConflictUpdate(sqlplugin.DbFields...),
	)

	templateDeleteWorkflowExecution = `
		DELETE FROM executions_visibility
		WHERE namespace_id = :namespace_id AND run_id = :run_id`

	templateGetWorkflowExecution = fmt.Sprintf(
		`SELECT %s FROM executions_visibility
		WHERE namespace_id = $1 AND run_id = $2`,
		strings.Join(sqlplugin.DbFields, ", "),
	)
)

// buildOnConflictUpdate leaves the key columns out of the update, since a conflict means they match.
func buildOnConflictUpdate(fields ...string) string {
	items := make([]string, 0, len(fields))
	for _, field := range fields {
		if field == "namespace_id" || field == "run_id" {
			continue
		}
		items = append(items, fmt.Sprintf("%s = excluded.%s", field, field))
	}
	return fmt.Sprintf(
		// The WHERE clause ensures that no update occurs if the version is behind the saved version.
		"ON CONFLICT (namespace_id, run_id) DO UPDATE SET %s WHERE executions_visibility.%s < EXCLUDED.%s",
		strings.Join(items, ", "), sqlplugin.VersionColumnName, sqlplugin.VersionColumnName,
	)
}

// withVisibilitySearchPath points the visibility pool at its own schema unless the
// datastore sets search_path, so the visibility tables and schema_version row stay
// apart from the persistence store's.
func withVisibilitySearchPath(cfg *config.SQL) *config.SQL {
	for key := range cfg.ConnectAttributes {
		if strings.TrimSpace(key) == searchPathAttribute {
			return cfg
		}
	}
	withPath := *cfg
	withPath.ConnectAttributes = make(map[string]string, len(cfg.ConnectAttributes)+1)
	maps.Copy(withPath.ConnectAttributes, cfg.ConnectAttributes)
	withPath.ConnectAttributes[searchPathAttribute] = dsqlschemaversion.VisibilitySchema
	return &withPath
}

// InsertIntoVisibility inserts a row into visibility table. If an row already exist,
// its left as such and no update will be made
func (pdb *db) InsertIntoVisibility(
	ctx context.Context,
	row *sqlplugin.VisibilityRow,
) (sql.Result, error) {
	finalRow := pdb.prepareRowForDB(row)
	return pdb.NamedExecContext(ctx, templateInsertWorkflowExecution, finalRow)
}

// ReplaceIntoVisibility replaces an existing row if it exist or creates a new row in visibility table
func (pdb *db) ReplaceIntoVisibility(
	ctx context.Context,
	row *sqlplugin.VisibilityRow,
) (sql.Result, error) {
	finalRow := pdb.prepareRowForDB(row)
	return pdb.NamedExecContext(ctx, templateUpsertWorkflowExecution, finalRow)
}

// DeleteFromVisibility deletes a row from visibility table if it exist
func (pdb *db) DeleteFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilityDeleteFilter,
) (sql.Result, error) {
	return pdb.NamedExecContext(ctx, templateDeleteWorkflowExecution, filter)
}

// SelectFromVisibility reads one or more rows from visibility table
func (pdb *db) SelectFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) ([]sqlplugin.VisibilityRow, error) {
	if len(filter.Query) == 0 {
		// backward compatibility for existing tests
		err := sqlplugin.GenerateSelectQuery(&filter, pdb.converter.ToDSQLDateTime)
		if err != nil {
			return nil, err
		}
	}

	// Rebind will replace default placeholder `?` with the right placeholder for DSQL.
	var rows []sqlplugin.VisibilityRow
	if err := pdb.SelectContext(ctx, &rows, pdb.Rebind(filter.Query), filter.QueryArgs...); err != nil {
		return nil, err
	}
	for i := range rows {
		if err := pdb.processRowFromDB(&rows[i]); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

// GetFromVisibility reads one row from visibility table
func (pdb *db) GetFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilityGetFilter,
) (*sqlplugin.VisibilityRow, error) {
	var row sqlplugin.VisibilityRow
	if err := pdb.GetContext(
		ctx,
		&row,
		templateGetWorkflowExecution,
		filter.NamespaceID,
		filter.RunID,
	); err != nil {
		return nil, err
	}
	if err := pdb.processRowFromDB(&row); err != nil {
		return nil, err
	}
	return &row, nil
}

func (pdb *db) CountFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) (int64, error) {
	var count int64
	if err := pdb.GetContext(ctx, &count, pdb.Rebind(filter.Query), filter.QueryArgs...); err != nil {
		return 0, err
	}
	return count, nil
}

func (pdb *db) CountGroupByFromVisibility(
	ctx context.Context,
	filter sqlplugin.VisibilitySelectFilter,
) ([]sqlplugin.VisibilityCountRow, error) {
	rows, err := pdb.QueryContext(ctx, pdb.Rebind(filter.Query), filter.QueryArgs...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return sqlplugin.ParseCountGroupByRows(rows, filter.GroupBy)
}

func (pdb *db) prepareRowForDB(row *sqlplugin.VisibilityRow) *sqlplugin.VisibilityRow {
	if row == nil {
		return nil
	}
	finalRow := *row
	finalRow.StartTime = pdb.converter.ToDSQLDateTime(finalRow.StartTime)
	finalRow.ExecutionTime = pdb.converter.ToDSQLDateTime(finalRow.ExecutionTime)
	if finalRow.CloseTime != nil {
		*finalRow.CloseTime = pdb.converter.ToDSQLDateTime(*finalRow.CloseTime)
	}
	if finalRow.SearchAttributes != nil {
		saMap := *finalRow.SearchAttributes
		for name, value := range saMap {
			if dt, ok := value.(time.Time); ok {
				saMap[name] = dt.Format(time.RFC3339Nano)
			}
		}
	}
	return &finalRow
}

func (pdb *db) processRowFromDB(row *sqlplugin.VisibilityRow) error {
	if row == nil {
		return nil
	}
	row.StartTime = pdb.converter.FromDSQLDateTime(row.StartTime)
	row.ExecutionTime = pdb.converter.FromDSQLDateTime(row.ExecutionTime)
	if row.CloseTime != nil {
		closeTime := pdb.converter.FromDSQLDateTime(*row.CloseTime)
		row.CloseTime = &closeTime
	}
	if row.SearchAttributes != nil {
		for saName, saValue := range *row.SearchAttributes {
			typedSaValue, ok := saValue.([]any)
			if !ok {
				continue
			}
			// the only valid type is slice of strings
			strSlice := make([]string, len(typedSaValue))
			for i, item := range typedSaValue {
				v, ok := item.(string)
				if !ok {
					return fmt.Errorf("%w: %T (expected string)", sqlplugin.ErrInvalidKeywordListDataType, item)
				}
				strSlice[i] = v
			}
			(*row.SearchAttributes)[saName] = strSlice
		}
	}
	// CHAR(64) pads the run ID with trailing spaces.
	row.RunID = strings.TrimSpace(row.RunID)
	return nil
}
