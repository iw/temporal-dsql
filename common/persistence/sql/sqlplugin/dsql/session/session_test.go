package session

import (
	"net/url"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
)

func TestBuildDSNAttr_DefaultVerifyFull(t *testing.T) {
	for _, tls := range []*auth.TLS{nil, {}, {Enabled: true}, {EnableHostVerification: true}, {Enabled: true, EnableHostVerification: true}} {
		cfg := &config.SQL{User: "admin", ConnectAddr: "db.example.invalid:5432", DatabaseName: "postgres", TLS: tls}
		result, err := buildDSNAttr(cfg)
		require.NoError(t, err)
		require.Equal(t, "verify-full", result.Get(sslMode))
		dsn, err := BuildDSN(cfg, &mockServiceResolver{addr: cfg.ConnectAddr})
		require.NoError(t, err)
		parsed, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		require.NotNil(t, parsed.TLSConfig)
		require.Equal(t, "db.example.invalid", parsed.TLSConfig.ServerName)
		require.False(t, parsed.TLSConfig.InsecureSkipVerify)
		require.Empty(t, parsed.Fallbacks)
	}
}

func TestBuildDSNAttr_CAFileWithoutEnabled(t *testing.T) {
	result, err := buildDSNAttr(&config.SQL{TLS: &auth.TLS{CaFile: "/path/to/ca.crt"}})
	require.NoError(t, err)
	require.Equal(t, "/path/to/ca.crt", result.Get(sslCA))
	require.Equal(t, "verify-full", result.Get(sslMode))
}

func TestBuildDSNAttr_TLSEnabled_WithCertificates(t *testing.T) {
	cfg := &config.SQL{
		TLS: &auth.TLS{
			Enabled:                true,
			EnableHostVerification: true,
			CaFile:                 "/path/to/ca.crt",
			CertFile:               "/path/to/client.crt",
			KeyFile:                "/path/to/client.key",
		},
	}
	result, err := buildDSNAttr(cfg)
	require.NoError(t, err)
	require.Equal(t, "verify-full", result.Get(sslMode))
	require.Equal(t, "/path/to/ca.crt", result.Get(sslCA))
	require.Equal(t, "/path/to/client.crt", result.Get(sslCert))
	require.Equal(t, "/path/to/client.key", result.Get(sslKey))
}

func TestBuildDSNAttr_CustomSSLMode_PreferredOverDefault(t *testing.T) {
	cfg := &config.SQL{
		ConnectAttributes: map[string]string{
			"sslmode": "verify-ca",
		},
	}
	result, err := buildDSNAttr(cfg)
	require.NoError(t, err)
	require.Equal(t, "verify-ca", result.Get(sslMode), "should use custom sslmode instead of default verify-full")
}

func TestBuildDSNAttr_TLSEnabledButCustomSSLModeInAttributes_PreferredOverDefault(t *testing.T) {
	cfg := &config.SQL{
		TLS: &auth.TLS{
			Enabled:                true,
			EnableHostVerification: true,
		},
		ConnectAttributes: map[string]string{
			"sslmode": "verify-ca",
		},
	}
	result, err := buildDSNAttr(cfg)
	require.NoError(t, err)
	require.Equal(t, "verify-ca", result.Get(sslMode), "should use custom sslmode instead of default verify-full")
}

func TestBuildDSNAttr_ConnectAttributes(t *testing.T) {
	cfg := &config.SQL{
		ConnectAttributes: map[string]string{
			"connect_timeout":  "10",
			"application_name": "temporal",
		},
	}
	result, err := buildDSNAttr(cfg)
	require.NoError(t, err)
	require.Equal(t, "10", result.Get("connect_timeout"))
	require.Equal(t, "temporal", result.Get("application_name"))
	require.Equal(t, "verify-full", result.Get(sslMode), "should still set default sslmode")
}

func TestBuildDSNAttr_ConnectAttributesWithSpaces(t *testing.T) {
	cfg := &config.SQL{
		ConnectAttributes: map[string]string{
			"  application_name  ": "  temporal  ",
		},
	}
	result, err := buildDSNAttr(cfg)
	require.NoError(t, err)
	require.Equal(t, "temporal", result.Get("application_name"), "should trim spaces from key and value")
}

func TestBuildDSNAttr_DuplicateConnectAttribute(t *testing.T) {
	cfg := &config.SQL{
		TLS: &auth.TLS{
			Enabled:                true,
			EnableHostVerification: true,
		},
		ConnectAttributes: map[string]string{
			"sslmode":  "require",
			"sslmode ": "verify-full",
		},
	}
	_, err := buildDSNAttr(cfg)
	require.ErrorContains(t, err, `duplicate connection attribute "sslmode"`)
	// An attribute can hold a secret, so the error leaves the values out.
	require.NotContains(t, err.Error(), "require")
	require.NotContains(t, err.Error(), "verify-full")
}

func TestBuildDSN(t *testing.T) {
	cfg := &config.SQL{
		User:         "testuser",
		Password:     "testpass",
		ConnectAddr:  "localhost:5432",
		DatabaseName: "testdb",
		ConnectAttributes: map[string]string{
			"connect_timeout": "10",
		},
	}
	mockResolver := &mockServiceResolver{addr: "localhost:5432"}
	result, err := BuildDSN(cfg, mockResolver)
	require.NoError(t, err)
	u, err := url.Parse(result)
	require.NoError(t, err)
	require.Equal(t, "postgres", u.Scheme)
	require.Equal(t, "testuser:testpass", u.User.String())
	require.Equal(t, "localhost:5432", u.Host)
	require.Equal(t, "/testdb", u.Path)
	require.Equal(t, "10", u.Query().Get("connect_timeout"))
	require.Equal(t, "verify-full", u.Query().Get("sslmode"))
}

func TestBuildDSN_PasswordEscaping(t *testing.T) {
	cfg := &config.SQL{
		User:         "testuser",
		Password:     "p@ss:w/rd&special",
		ConnectAddr:  "localhost:5432",
		DatabaseName: "testdb",
	}
	mockResolver := &mockServiceResolver{addr: "localhost:5432"}
	result, err := BuildDSN(cfg, mockResolver)
	require.NoError(t, err)
	parsed, err := url.Parse(result)
	require.NoError(t, err)
	password, _ := parsed.User.Password()
	require.Equal(t, "p@ss:w/rd&special", password, "password should be properly escaped and parsed")
}

func TestBuildDSNAttr_TLSKeyFileWithoutCertFile_ReturnsError(t *testing.T) {
	cfg := &config.SQL{
		TLS: &auth.TLS{
			Enabled: true,
			KeyFile: "/path/to/client.key",
		},
	}
	_, err := buildDSNAttr(cfg)
	require.ErrorContains(t, err, "TLS keyFile is set but TLS certFile is not set")
}

func TestBuildDSNAttr_TLSCertFileWithoutKeyFile_ReturnsError(t *testing.T) {
	cfg := &config.SQL{
		TLS: &auth.TLS{
			Enabled:  true,
			CertFile: "/path/to/client.crt",
		},
	}
	_, err := buildDSNAttr(cfg)
	require.ErrorContains(t, err, "TLS certFile is set but TLS keyFile is not set")
}

func TestBuildDSNAttr_SSLCertAttributeWithoutSSLKey_ReturnsError(t *testing.T) {
	cfg := &config.SQL{
		TLS: &auth.TLS{
			Enabled: true,
		},
		ConnectAttributes: map[string]string{
			"sslcert": "/path/to/client.crt",
		},
	}
	_, err := buildDSNAttr(cfg)
	require.ErrorContains(t, err, "sslcert connectAttribute is set but sslkey is not set")
}

func TestBuildDSNAttr_SSLKeyAttributeWithoutSSLCert_ReturnsError(t *testing.T) {
	cfg := &config.SQL{
		TLS: &auth.TLS{
			Enabled: true,
		},
		ConnectAttributes: map[string]string{
			"sslkey": "/path/to/client.key",
		},
	}
	_, err := buildDSNAttr(cfg)
	require.ErrorContains(t, err, "sslkey connectAttribute is set but sslcert is not set")
}

func TestBuildDSN_PropagatesErrorFromBuildDSNAttr(t *testing.T) {
	cfg := &config.SQL{
		User:         "testuser",
		Password:     "testpass",
		ConnectAddr:  "localhost:5432",
		DatabaseName: "testdb",
		TLS: &auth.TLS{
			Enabled: true,
			KeyFile: "/path/to/client.key",
		},
	}
	mockResolver := &mockServiceResolver{addr: "localhost:5432"}
	_, err := BuildDSN(cfg, mockResolver)
	require.ErrorContains(t, err, "TLS keyFile is set but TLS certFile is not set")
}

type mockServiceResolver struct {
	addr string
}

func (m *mockServiceResolver) Resolve(addr string) []string {
	return []string{m.addr}
}
