package session

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/iancoleman/strcase"
	"github.com/jmoiron/sqlx"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql/driver"
	"go.temporal.io/server/common/resolver"
)

const (
	dsnFmt = "postgres://%v:%v@%v/%v?%v"
)

// These defaults are retained for direct Session users. The plugin uses BuildDSN
// and DefaultMaxConns only; connection_pool.go configures its pgxpool lifecycle.
const (
	// DefaultMaxConnLifetime is the legacy direct-session lifetime.
	DefaultMaxConnLifetime time.Duration = 10 * time.Minute

	// DefaultMaxConnIdleTime is 0 (disabled).
	// CRITICAL: Must be 0 to prevent pool decay. Go's database/sql closes
	// connections idle longer than this, even if MaxIdleConns allows them.
	// With DSQL's 100 conn/sec rate limit, we cannot afford to recreate
	// connections under load - the pool must stay at max size always.
	DefaultMaxConnIdleTime time.Duration = 0

	// DefaultMaxConns is the default maximum number of open connections.
	// Set to 100 to handle shard acquisition bursts and high concurrency workloads.
	// Each Temporal service creates multiple pools, so total connections per service
	// can be 4x this value (e.g., 400 connections for history service).
	DefaultMaxConns = 100

	// DefaultMaxIdleConns is the default maximum number of idle connections.
	// MUST equal MaxConns to prevent connection churn - when idle connections
	// exceed this limit, they are closed immediately and must be recreated
	// on the next request, causing unnecessary overhead and rate limit pressure.
	DefaultMaxIdleConns = 100
)

const (
	sslMode     = "sslmode"
	sslModeFull = "verify-full"

	sslCA   = "sslrootcert"
	sslKey  = "sslkey"
	sslCert = "sslcert"
)

type Session struct {
	*sqlx.DB
}

func NewSession(
	cfg *config.SQL,
	d driver.Driver,
	resolver resolver.ServiceResolver,
) (*Session, error) {
	db, err := createConnection(cfg, d, resolver)
	if err != nil {
		return nil, err
	}
	return &Session{DB: db}, nil
}

func (s *Session) Close() {
	if s.DB != nil {
		_ = s.DB.Close()
	}
}

func createConnection(
	cfg *config.SQL,
	d driver.Driver,
	resolver resolver.ServiceResolver,
) (*sqlx.DB, error) {
	dsn, err := buildDSN(cfg, resolver)
	if err != nil {
		return nil, err
	}
	db, err := d.CreateConnection(dsn)
	if err != nil {
		return nil, err
	}

	// Apply DSQL-optimized pool settings with sensible defaults
	if cfg.MaxConns > 0 {
		db.SetMaxOpenConns(cfg.MaxConns)
	} else {
		db.SetMaxOpenConns(DefaultMaxConns)
	}

	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	} else {
		db.SetMaxIdleConns(DefaultMaxIdleConns)
	}

	if cfg.MaxConnLifetime > 0 {
		db.SetConnMaxLifetime(cfg.MaxConnLifetime)
	} else {
		db.SetConnMaxLifetime(DefaultMaxConnLifetime)
	}

	// Set idle time - 0 means connections never expire due to idle time.
	// This is critical for DSQL: pool must stay at max size to avoid
	// connection creation under load (100 conn/sec cluster-wide rate limit).
	db.SetConnMaxIdleTime(DefaultMaxConnIdleTime)

	// Maps struct names in CamelCase to snake without need for db struct tags.
	db.MapperFunc(strcase.ToSnake)
	return db, nil
}

func buildDSN(
	cfg *config.SQL,
	r resolver.ServiceResolver,
) (string, error) {
	tlsAttrs, err := buildDSNAttr(cfg)
	if err != nil {
		return "", err
	}
	resolvedAddr := r.Resolve(cfg.ConnectAddr)[0]
	return fmt.Sprintf(
		dsnFmt,
		cfg.User,
		url.QueryEscape(cfg.Password),
		resolvedAddr,
		cfg.DatabaseName,
		tlsAttrs.Encode(),
	), nil
}

// BuildDSN builds a DSN string from the given configuration.
// The plugin parses this DSN with pgxpool.ParseConfig.
func BuildDSN(
	cfg *config.SQL,
	r resolver.ServiceResolver,
) (string, error) {
	return buildDSN(cfg, r)
}

// nolint: revive
func buildDSNAttr(cfg *config.SQL) (url.Values, error) {
	parameters := make(url.Values, len(cfg.ConnectAttributes))
	for k, v := range cfg.ConnectAttributes {
		key := strings.TrimSpace(k)
		value := strings.TrimSpace(v)
		if parameters.Get(key) != "" {
			panic(fmt.Sprintf("duplicate connection attr: %v:%v, %v:%v",
				key,
				parameters.Get(key),
				key, value,
			))
		}
		parameters.Set(key, value)
	}

	// DSQL requires TLS. The boolean zero values do not opt out of verification.
	if parameters.Get(sslMode) == "" {
		parameters.Set(sslMode, sslModeFull)
	}
	if cfg.TLS != nil && parameters.Get(sslCA) == "" && cfg.TLS.CaFile != "" {
		parameters.Set(sslCA, cfg.TLS.CaFile)
	}
	if cfg.TLS != nil && cfg.TLS.Enabled {
		if parameters.Get(sslKey) == "" {
			if parameters.Get(sslCert) != "" {
				return nil, errors.New("failed to build postgresql DSN: sslcert connectAttribute is set but sslkey is not set")
			}
			if cfg.TLS.KeyFile != "" {
				if cfg.TLS.CertFile == "" {
					return nil, errors.New("failed to build postgresql DSN: TLS keyFile is set but TLS certFile is not set")
				}
				parameters.Set(sslKey, cfg.TLS.KeyFile)
				parameters.Set(sslCert, cfg.TLS.CertFile)
			} else if cfg.TLS.CertFile != "" {
				return nil, errors.New("failed to build postgresql DSN: TLS certFile is set but TLS keyFile is not set")
			}
		} else if parameters.Get(sslCert) == "" {
			return nil, errors.New("failed to build postgresql DSN: sslkey connectAttribute is set but sslcert is not set")
		}
	}

	return parameters, nil
}
