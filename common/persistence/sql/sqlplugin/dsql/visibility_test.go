package dsql

import (
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	dsqlschemaversion "go.temporal.io/server/schema/dsql"
)

func TestVisibilitySearchPath(t *testing.T) {
	for _, tc := range []struct {
		name  string
		attrs map[string]string
		want  string
	}{
		{"default", nil, dsqlschemaversion.VisibilitySchema},
		{"default beside other attributes", map[string]string{"application_name": "test-app"}, dsqlschemaversion.VisibilitySchema},
		{"configured", map[string]string{" search_path ": "custom_visibility"}, "custom_visibility"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := settingsConfig()
			cfg.ConnectAttributes = tc.attrs
			before := maps.Clone(tc.attrs)
			pool := testPoolConfig(t, &plugin{}, withVisibilitySearchPath(cfg))
			require.Equal(t, tc.want, pool.ConnConfig.RuntimeParams["search_path"])
			require.Equal(t, before, cfg.ConnectAttributes)
		})
	}
}

func TestCreateDBVisibility(t *testing.T) {
	p := &plugin{retryConfig: DefaultRetryConfig()}
	var connected *config.SQL
	generic, err := p.CreateDB(sqlplugin.DbKindVisibility, &config.SQL{
		Connect: func(cfg *config.SQL) (*sqlx.DB, error) {
			connected = cfg
			return nil, errors.New("no cluster in unit tests")
		},
	}, nil, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	defer func() { require.NoError(t, generic.Close()) }()
	require.Equal(t, dsqlschemaversion.VisibilitySchema, connected.ConnectAttributes["search_path"])
	vis, ok := generic.(*db)
	require.True(t, ok)
	require.Equal(t, dsqlschemaversion.VisibilityVersion, vis.ExpectedVersion())
	require.Equal(t, dsqlschemaversion.Version, (&db{dbKind: sqlplugin.DbKindMain}).ExpectedVersion())
}

func TestVisibilityUpsertLeavesKeyColumns(t *testing.T) {
	_, set, ok := strings.Cut(templateUpsertWorkflowExecution, "DO UPDATE SET ")
	require.True(t, ok)
	set, where, ok := strings.Cut(set, " WHERE ")
	require.True(t, ok)
	var columns []string
	for _, item := range strings.Split(set, ", ") {
		columns = append(columns, strings.Fields(item)[0])
	}
	want := slices.DeleteFunc(slices.Clone(sqlplugin.DbFields), func(field string) bool {
		return field == "namespace_id" || field == "run_id"
	})
	require.Equal(t, want, columns)
	require.Equal(t, "executions_visibility._version < EXCLUDED._version", where)
}

func TestVisibilityRowConversion(t *testing.T) {
	pdb := &db{converter: &converter{}}
	start := time.Date(2026, 10, 28, 12, 0, 0, 123456789, time.FixedZone("UTC+1", 3600))
	written := sqlplugin.VisibilitySearchAttributes{"Datetime01": start, "Keyword01": "k"}
	row := pdb.prepareRowForDB(&sqlplugin.VisibilityRow{StartTime: start, SearchAttributes: &written})
	require.Equal(t, start.UTC().Truncate(time.Microsecond), row.StartTime)
	require.Equal(t, minDSQLDateTime, row.ExecutionTime)
	require.Equal(t, start.Format(time.RFC3339Nano), written["Datetime01"])

	read := sqlplugin.VisibilitySearchAttributes{"KeywordList01": []any{"a", "b"}, "Int01": float64(1)}
	got := sqlplugin.VisibilityRow{RunID: "run-id  ", StartTime: row.StartTime, ExecutionTime: minDSQLDateTime, SearchAttributes: &read}
	require.NoError(t, pdb.processRowFromDB(&got))
	require.Equal(t, "run-id", got.RunID)
	require.True(t, got.ExecutionTime.IsZero())
	require.Equal(t, []string{"a", "b"}, read["KeywordList01"])
	require.InDelta(t, 1.0, read["Int01"], 0)

	invalid := sqlplugin.VisibilitySearchAttributes{"KeywordList01": []any{"a", 1.0}}
	require.ErrorIs(t, pdb.processRowFromDB(&sqlplugin.VisibilityRow{SearchAttributes: &invalid}), sqlplugin.ErrInvalidKeywordListDataType)
}
