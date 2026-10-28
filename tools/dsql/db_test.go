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

func TestSchemaRegion(t *testing.T) {
	for _, tc := range []struct{ endpoint, region, want string }{
		{"test.dsql.eu-west-1.on.aws", "", "eu-west-1"},
		{"TEST.DSQL.EU-WEST-1.ON.AWS.", "", "eu-west-1"},
		{"test.dsql.eu-west-1.on.aws", " us-east-1 ", "us-east-1"},
		{"db.example.invalid", "us-east-1", "us-east-1"},
	} {
		got, err := schemaRegion(tc.endpoint, tc.region)
		require.NoError(t, err)
		require.Equal(t, tc.want, got)
	}
	for _, endpoint := range []string{"", "db.example.invalid", ".dsql.eu-west-1.on.aws", "test.dsql.eu-west-1.on.aws.example"} {
		_, err := schemaRegion(endpoint, "")
		require.ErrorContains(t, err, "set --region")
	}
}
