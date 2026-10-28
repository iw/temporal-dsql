package dsql

import (
	"net/url"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli"
)

func TestSchemaDSNTLS(t *testing.T) {
	for _, mode := range []string{"", "require", "disable", "verify-ca", "verify-full"} {
		t.Run("sslmode="+mode, func(t *testing.T) {
			app := BuildCLIOptions()
			app.Action = func(c *cli.Context) error {
				expected := mode
				if expected == "" {
					expected = "verify-full"
				}
				dsn := schemaDSN("db.example.invalid", 5432, "admin", "fake-token", "postgres", c.GlobalString("sslmode"))
				parsed, err := url.Parse(dsn)
				require.NoError(t, err)
				require.Equal(t, expected, parsed.Query().Get("sslmode"))
				cfg, err := pgx.ParseConfig(dsn)
				require.NoError(t, err)
				if expected == "verify-full" {
					require.NotNil(t, cfg.TLSConfig)
					require.Equal(t, "db.example.invalid", cfg.TLSConfig.ServerName)
					require.False(t, cfg.TLSConfig.InsecureSkipVerify)
					require.Empty(t, cfg.Fallbacks)
				}
				return nil
			}
			args := []string{"temporal-dsql-tool"}
			if mode != "" {
				args = append(args, "--sslmode", mode)
			}
			require.NoError(t, app.Run(args))
		})
	}
}
