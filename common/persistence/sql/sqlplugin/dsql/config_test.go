package dsql_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql"
	dsqlschema "go.temporal.io/server/schema/dsql"
)

func TestDevelopmentDSQLConfig(t *testing.T) {
	configPath := filepath.Join("..", "..", "..", "..", "..", "config", "development-dsql.yaml")
	cfg, err := config.Load(config.WithConfigFile(configPath))
	require.NoError(t, err)
	require.NoError(t, cfg.Validate())
	main := cfg.Persistence.DataStores[cfg.Persistence.DefaultStore].SQL
	require.NotNil(t, main)
	require.Equal(t, dsql.PluginName, main.PluginName)
	require.Equal(t, "postgres", main.DatabaseName)
	require.Equal(t, "admin", main.User)
	visibility := cfg.Persistence.DataStores[cfg.Persistence.VisibilityStore].SQL
	require.NotNil(t, visibility)
	require.Equal(t, dsql.PluginName, visibility.PluginName)
	require.Equal(t, main.ConnectAddr, visibility.ConnectAddr)
	require.Equal(t, "postgres", visibility.DatabaseName)
	require.Equal(t, dsqlschema.VisibilitySchema, visibility.ConnectAttributes["search_path"])
}

func TestDevelopmentDSQLDynamicConfig(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "..", "config", "dynamicconfig", "development-dsql.yaml"))
	require.NoError(t, err)
	loaded := dynamicconfig.LoadYamlFile(contents)
	require.Empty(t, loaded.Errors)
	require.Empty(t, loaded.Warnings)
}

func TestDSQLDeploymentDynamicConfig(t *testing.T) {
	contents, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "..", "docs", "dsql", "deployment.md"))
	require.NoError(t, err)
	_, section, ok := strings.Cut(string(contents), "## 3. Dynamic Configuration")
	require.True(t, ok)
	_, block, ok := strings.Cut(section, "```yaml\n")
	require.True(t, ok)
	block, _, ok = strings.Cut(block, "\n```")
	require.True(t, ok)
	loaded := dynamicconfig.LoadYamlFile([]byte(block))
	require.Empty(t, loaded.Errors)
	require.Empty(t, loaded.Warnings)
}
