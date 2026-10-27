package dsql_test

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
)

func TestInsertIntoBufferedEventsPerRunIDs(t *testing.T) {
	run := bufferedEventsTestRun{7, "11111111-1111-4111-8111-111111111111", "workflow", "22222222-2222-4222-8222-222222222222"}
	otherRun := run
	otherRun.runID = "33333333-3333-4333-8333-333333333333"
	otherShard := run
	otherShard.shardID++
	otherNamespace := run
	otherNamespace.namespaceID = "44444444-4444-4444-8444-444444444444"
	otherWorkflow := run
	otherWorkflow.workflowID = "other-workflow"

	for _, tc := range []struct {
		name    string
		runs    []bufferedEventsTestRun
		maxIDs  map[bufferedEventsTestRun]int64
		wantIDs []int64
	}{
		{name: "empty"},
		{name: "first_event", runs: []bufferedEventsTestRun{run}, wantIDs: []int64{1}},
		{name: "existing_events", runs: []bufferedEventsTestRun{run}, maxIDs: map[bufferedEventsTestRun]int64{run: 41}, wantIDs: []int64{42}},
		{name: "consecutive", runs: []bufferedEventsTestRun{run, run}, maxIDs: map[bufferedEventsTestRun]int64{run: 41}, wantIDs: []int64{42, 43}},
		{name: "interleaved_runs", runs: []bufferedEventsTestRun{run, otherRun, run, otherRun}, maxIDs: map[bufferedEventsTestRun]int64{run: 41}, wantIDs: []int64{42, 1, 43, 2}},
		{name: "different_shards", runs: []bufferedEventsTestRun{run, otherShard}, maxIDs: map[bufferedEventsTestRun]int64{run: 41}, wantIDs: []int64{42, 1}},
		{name: "different_namespaces", runs: []bufferedEventsTestRun{run, otherNamespace}, maxIDs: map[bufferedEventsTestRun]int64{run: 41}, wantIDs: []int64{42, 1}},
		{name: "different_workflows", runs: []bufferedEventsTestRun{run, otherWorkflow}, maxIDs: map[bufferedEventsTestRun]int64{run: 41}, wantIDs: []int64{42, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, fake := newBufferedEventsTestTx(t)
			fake.bufferedEventMaxIDs = tc.maxIDs
			rows := make([]sqlplugin.BufferedEventsRow, len(tc.runs))
			wantQueries := make(map[bufferedEventsTestRun]int)
			for i, run := range tc.runs {
				rows[i] = bufferedEventsTestRow(run)
				wantQueries[run] = 1
			}

			result, err := tx.InsertIntoBufferedEvents(t.Context(), rows)
			require.NoError(t, err)
			statements, _, _ := fake.snapshot()
			var ids []int64
			queries := make(map[bufferedEventsTestRun]int)
			for i, stmt := range statements {
				args := fake.statementArgs(i)
				if stmt == bufferedEventsTestMaxQuery {
					queries[bufferedEventsTestRun{args[0].(int64), args[1].(string), args[2].(string), args[3].(string)}]++
				} else {
					require.True(t, strings.HasPrefix(stmt, "INSERT INTO buffered_events("))
					row := rows[len(ids)]
					require.Equal(t, []driver.Value{int64(row.ShardID), row.NamespaceID.String(), row.WorkflowID, row.RunID.String(), row.Data, row.DataEncoding}, args[1:])
					ids = append(ids, args[0].(int64))
				}
			}
			t.Run("ids", func(t *testing.T) { require.Equal(t, tc.wantIDs, ids) })
			t.Run("one_query_per_run", func(t *testing.T) { require.Equal(t, wantQueries, queries) })
			t.Run("rows_affected", func(t *testing.T) {
				require.NotNil(t, result)
				count, err := result.RowsAffected()
				require.NoError(t, err)
				require.Equal(t, int64(len(rows)), count)
			})
			require.Equal(t, 1, fake.beginCount())
		})
	}
}

func TestInsertIntoBufferedEventsErrors(t *testing.T) {
	for _, operation := range []string{"max_query", "insert"} {
		t.Run(operation, func(t *testing.T) {
			tx, fake := newBufferedEventsTestTx(t)
			conflict := &pgconn.PgError{Code: "40001", Message: "conflict"}
			if operation == "max_query" {
				fake.queryErrors = []error{conflict}
			} else {
				fake.execErrors = []error{conflict}
			}
			row := bufferedEventsTestRow(bufferedEventsTestRun{7, "11111111-1111-4111-8111-111111111111", "workflow", "22222222-2222-4222-8222-222222222222"})
			result, err := tx.InsertIntoBufferedEvents(t.Context(), []sqlplugin.BufferedEventsRow{row, row})
			require.ErrorIs(t, err, conflict)
			require.Nil(t, result)
			statements, _, _ := fake.snapshot()
			if operation == "max_query" {
				require.Equal(t, []string{bufferedEventsTestMaxQuery}, statements)
			} else {
				require.Len(t, statements, 2)
			}
		})
	}
}

func bufferedEventsTestRow(run bufferedEventsTestRun) sqlplugin.BufferedEventsRow {
	return sqlplugin.BufferedEventsRow{
		ShardID:      int32(run.shardID),
		NamespaceID:  primitives.MustParseUUID(run.namespaceID),
		WorkflowID:   run.workflowID,
		RunID:        primitives.MustParseUUID(run.runID),
		Data:         []byte("event"),
		DataEncoding: "Proto3",
	}
}

func newBufferedEventsTestTx(t *testing.T) (sqlplugin.Tx, *shardTestDriver) {
	t.Helper()
	fakeDB, fake := newShardTestDB(t, 0)
	cfg := &config.SQL{
		PluginName:   dsql.PluginName,
		DatabaseName: "buffered_events_test",
		Connect:      func(*config.SQL) (*sqlx.DB, error) { return fakeDB, nil },
	}
	db, err := persistencesql.NewSQLDB(sqlplugin.DbKindMain, cfg,
		resolver.NewNoopResolver(), log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	// t.Context is cancelled before cleanups run, which would roll the transaction back first.
	tx, err := db.BeginTx(context.WithoutCancel(t.Context()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tx.Rollback()) })
	return tx, fake
}
