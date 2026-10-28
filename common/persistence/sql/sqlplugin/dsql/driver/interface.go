package driver

import (
	"context"

	"github.com/jmoiron/sqlx"
)

const (
	dupEntryCode    = "23505"
	dupDatabaseCode = "42P04"
)

type Driver interface {
	CreateConnection(dsn string) (*sqlx.DB, error)
	IsDupEntryError(error) bool
	IsDupDatabaseError(error) bool
}

// TokenProvider supplies an IAM token before each physical connection.
type TokenProvider func(context.Context) (string, error)

// RateLimiter paces connection attempts, including background pool refills.
type RateLimiter interface {
	Wait(context.Context) error
}
