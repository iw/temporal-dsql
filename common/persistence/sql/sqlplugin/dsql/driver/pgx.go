package driver

import (
	"github.com/jackc/pgx/v5/pgconn"
)

type PGXDriver struct{}

func (p *PGXDriver) IsDupEntryError(err error) bool {
	pgxErr, ok := err.(*pgconn.PgError)
	return ok && pgxErr.Code == dupEntryCode
}

func (p *PGXDriver) IsDupDatabaseError(err error) bool {
	pqErr, ok := err.(*pgconn.PgError)
	return ok && pqErr.Code == dupDatabaseCode
}
