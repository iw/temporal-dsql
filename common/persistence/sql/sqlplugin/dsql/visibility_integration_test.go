//go:build integration

package dsql_test

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/temporalio/sqlparser"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql"
	"go.temporal.io/server/common/persistence/tests"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
)

// visibilityTestCluster serves a cluster whose visibility schema temporal-dsql-tool has
// already set up. The suite writes under fresh namespace IDs, so it needs no teardown.
type visibilityTestCluster struct{ cfg config.Persistence }

func (*visibilityTestCluster) SetupTestDatabase()           {}
func (*visibilityTestCluster) TearDownTestDatabase()        {}
func (c *visibilityTestCluster) Config() config.Persistence { return c.cfg }

// Run with DSQL_TEST_ENDPOINT set, using the default AWS credential chain and a
// cluster with the visibility schema installed:
//
// go test -tags integration,disable_grpc_modules,test_dep ./common/persistence/sql/sqlplugin/dsql -run '^TestDSQLVisibility' -count=1 -timeout=10m -v
func TestDSQLVisibilityPersistence(t *testing.T) {
	cluster := &visibilityTestCluster{cfg: config.Persistence{
		VisibilityStore: "visibility",
		DataStores:      map[string]config.DataStore{"visibility": {SQL: visibilityTestConfig(t)}},
	}}
	suite.Run(t, &tests.VisibilityPersistenceSuite{
		TestBase: persistencetests.NewTestBaseForCluster(cluster, log.NewTestLogger()),
	})
}

// Text and KeywordList are the attributes whose DSQL dialect differs from PostgreSQL's,
// so this checks their matching against the generated columns DSQL stores.
func TestDSQLVisibilityTextAndKeywordList(t *testing.T) {
	db, err := persistencesql.NewSQLDB(sqlplugin.DbKindVisibility, visibilityTestConfig(t),
		resolver.NewNoopResolver(), log.NewTestLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	namespaceID := primitives.NewUUID().String()
	now := time.Now().UTC()
	for workflowID, attributes := range map[string]sqlplugin.VisibilitySearchAttributes{
		"tab-separated": {"Text01": "The Quick  brown\tfox", "KeywordList01": []string{"red", "blue"}},
		"lower-case":    {"Text01": "quick thinking", "KeywordList01": []string{"green"}},
		"wildcards":     {"Text01": "50% off_today!"},
	} {
		_, err := db.InsertIntoVisibility(ctx, &sqlplugin.VisibilityRow{
			NamespaceID: namespaceID, RunID: primitives.NewUUID().String(), WorkflowID: workflowID,
			WorkflowTypeName: "visibility-test", StartTime: now, ExecutionTime: now, Status: 1,
			Encoding: "Proto3", TaskQueue: "visibility-test", SearchAttributes: &attributes, Version: 1,
		})
		require.NoError(t, err)
	}

	qc, err := persistencesql.GetPluginVisibilityQueryConverter(dsql.PluginName)
	require.NoError(t, err)
	list := func(expr sqlparser.Expr, err error) []string {
		t.Helper()
		require.NoError(t, err)
		stmt, args := qc.BuildSelectStmt(&query.QueryParams[sqlparser.Expr]{QueryExpr: &sqlparser.AndExpr{
			Left:  &sqlparser.ComparisonExpr{Operator: sqlparser.EqualStr, Left: query.NamespaceIDSAColumn, Right: query.NewUnsafeSQLString(namespaceID)},
			Right: expr,
		}}, 10, nil)
		rows, err := db.SelectFromVisibility(ctx, sqlplugin.VisibilitySelectFilter{Query: stmt, QueryArgs: args})
		require.NoError(t, err)
		var workflowIDs []string
		for _, row := range rows {
			workflowIDs = append(workflowIDs, row.WorkflowID)
		}
		return workflowIDs
	}
	text := query.NewSAColumn("Text01", "Text01", enumspb.INDEXED_VALUE_TYPE_TEXT)
	matchText := func(operator, value string) []string {
		return list(qc.ConvertTextComparisonExpr(operator, text, query.NewUnsafeSQLString(value)))
	}
	require.ElementsMatch(t, []string{"tab-separated"}, matchText(sqlparser.EqualStr, "Quick"))
	require.ElementsMatch(t, []string{"lower-case"}, matchText(sqlparser.EqualStr, "quick"))
	require.ElementsMatch(t, []string{"tab-separated", "lower-case"}, matchText(sqlparser.EqualStr, "Quick thinking"))
	require.ElementsMatch(t, []string{"tab-separated"}, matchText(sqlparser.EqualStr, "fox"))
	require.ElementsMatch(t, []string{"wildcards"}, matchText(sqlparser.EqualStr, "50%"))
	require.Empty(t, matchText(sqlparser.EqualStr, "50"))
	require.Empty(t, matchText(sqlparser.EqualStr, "off"))
	require.ElementsMatch(t, []string{"lower-case", "wildcards"}, matchText(sqlparser.NotEqualStr, "Quick fox"))

	keywordList := query.NewSAColumn("KeywordList01", "KeywordList01", enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST)
	require.ElementsMatch(t, []string{"tab-separated"}, list(qc.ConvertKeywordListComparisonExpr(
		sqlparser.EqualStr, keywordList, query.NewUnsafeSQLString("blue"))))
	require.ElementsMatch(t, []string{"tab-separated", "lower-case"}, list(qc.ConvertKeywordListComparisonExpr(
		sqlparser.InStr, keywordList, sqlparser.ValTuple{query.NewUnsafeSQLString("red"), query.NewUnsafeSQLString("green")})))
}

func visibilityTestConfig(t *testing.T) *config.SQL {
	t.Helper()
	endpoint := os.Getenv("DSQL_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("set DSQL_TEST_ENDPOINT to run against a live DSQL cluster")
	}
	return &config.SQL{
		PluginName: dsql.PluginName, DatabaseName: "postgres",
		ConnectAddr: net.JoinHostPort(endpoint, "5432"), ConnectProtocol: "tcp",
		MaxConns: 3, MaxIdleConns: 3, PoolSizeHint: "ephemeral",
		TLS: &auth.TLS{Enabled: true, EnableHostVerification: true},
	}
}
