package dsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	taskDeleteBatchSize        = 1000
	historyNodeDeleteBatchSize = 100
)

// deleteInBatches requires autocommit so each batch has its own transaction quota.
// The query's last parameter is the batch limit.
func (pdb *db) deleteInBatches(ctx context.Context, batchSize int, query string, args ...any) (sql.Result, error) {
	args = append(args, batchSize)
	var total int64
	for {
		args[len(args)-1] = batchSize
		// Inspect the raw SQLSTATE before maybeConvertError turns it into a size error.
		result, err := pdb.conn().ExecContext(ctx, query, args...)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.SQLState() == "54000" && batchSize > 1 {
				batchSize /= 2
				continue
			}
			return nil, pdb.maybeConvertError(err)
		}
		deleted, err := result.RowsAffected()
		if err != nil {
			return nil, err
		}
		total += deleted
		if deleted < int64(batchSize) {
			return driver.RowsAffected(total), nil
		}
	}
}
