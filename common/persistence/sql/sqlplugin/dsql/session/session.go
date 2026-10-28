package session

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/resolver"
)

const (
	dsnFmt = "postgres://%v:%v@%v/%v?%v"
)

// DefaultMaxConns is the default service pool size.
const DefaultMaxConns = 100

const (
	sslMode     = "sslmode"
	sslModeFull = "verify-full"

	sslCA   = "sslrootcert"
	sslKey  = "sslkey"
	sslCert = "sslcert"
)

// BuildDSN builds the DSN parsed by pgxpool.
func BuildDSN(
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

// nolint: revive
func buildDSNAttr(cfg *config.SQL) (url.Values, error) {
	parameters := make(url.Values, len(cfg.ConnectAttributes))
	for k, v := range cfg.ConnectAttributes {
		key := strings.TrimSpace(k)
		value := strings.TrimSpace(v)
		if parameters.Get(key) != "" {
			// The values stay out of the error, since an attribute can hold a secret.
			return nil, fmt.Errorf("duplicate connection attribute %q", key)
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
