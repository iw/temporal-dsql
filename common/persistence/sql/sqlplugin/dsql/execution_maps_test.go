package dsql_test

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

func TestExecutionMapUpserts(t *testing.T) {
	namespaceIDs := []primitives.UUID{primitives.NewUUID(), primitives.NewUUID(), primitives.NewUUID()}
	runIDs := []primitives.UUID{primitives.NewUUID(), primitives.NewUUID(), primitives.NewUUID()}
	for _, tc := range []struct {
		name  string
		key   string
		width int
		write func(sqlplugin.DB, int) (sql.Result, error)
	}{
		{name: "activity_info_maps", key: "schedule_id", width: 7, write: func(db sqlplugin.DB, count int) (sql.Result, error) {
			var rows []sqlplugin.ActivityInfoMapsRow
			if count >= 0 {
				rows = make([]sqlplugin.ActivityInfoMapsRow, count)
			}
			for i := range rows {
				rows[i] = sqlplugin.ActivityInfoMapsRow{
					ShardID: int32(i + 1), NamespaceID: namespaceIDs[i], WorkflowID: fmt.Sprintf("workflow-%d", i), RunID: runIDs[i],
					ScheduleID: int64(i + 10),
					Data:       []byte(fmt.Sprintf("data-%d", i)), DataEncoding: "Proto3",
				}
			}
			return db.ReplaceIntoActivityInfoMaps(t.Context(), rows)
		}},
		{name: "timer_info_maps", key: "timer_id", width: 7, write: func(db sqlplugin.DB, count int) (sql.Result, error) {
			var rows []sqlplugin.TimerInfoMapsRow
			if count >= 0 {
				rows = make([]sqlplugin.TimerInfoMapsRow, count)
			}
			for i := range rows {
				rows[i] = sqlplugin.TimerInfoMapsRow{
					ShardID: int32(i + 1), NamespaceID: namespaceIDs[i], WorkflowID: fmt.Sprintf("workflow-%d", i), RunID: runIDs[i],
					TimerID: fmt.Sprintf("key-%d", i),
					Data:    []byte(fmt.Sprintf("data-%d", i)), DataEncoding: "Proto3",
				}
			}
			return db.ReplaceIntoTimerInfoMaps(t.Context(), rows)
		}},
		{name: "child_execution_info_maps", key: "initiated_id", width: 7, write: func(db sqlplugin.DB, count int) (sql.Result, error) {
			var rows []sqlplugin.ChildExecutionInfoMapsRow
			if count >= 0 {
				rows = make([]sqlplugin.ChildExecutionInfoMapsRow, count)
			}
			for i := range rows {
				rows[i] = sqlplugin.ChildExecutionInfoMapsRow{
					ShardID: int32(i + 1), NamespaceID: namespaceIDs[i], WorkflowID: fmt.Sprintf("workflow-%d", i), RunID: runIDs[i],
					InitiatedID: int64(i + 10),
					Data:        []byte(fmt.Sprintf("data-%d", i)), DataEncoding: "Proto3",
				}
			}
			return db.ReplaceIntoChildExecutionInfoMaps(t.Context(), rows)
		}},
		{name: "request_cancel_info_maps", key: "initiated_id", width: 7, write: func(db sqlplugin.DB, count int) (sql.Result, error) {
			var rows []sqlplugin.RequestCancelInfoMapsRow
			if count >= 0 {
				rows = make([]sqlplugin.RequestCancelInfoMapsRow, count)
			}
			for i := range rows {
				rows[i] = sqlplugin.RequestCancelInfoMapsRow{
					ShardID: int32(i + 1), NamespaceID: namespaceIDs[i], WorkflowID: fmt.Sprintf("workflow-%d", i), RunID: runIDs[i],
					InitiatedID: int64(i + 10),
					Data:        []byte(fmt.Sprintf("data-%d", i)), DataEncoding: "Proto3",
				}
			}
			return db.ReplaceIntoRequestCancelInfoMaps(t.Context(), rows)
		}},
		{name: "signal_info_maps", key: "initiated_id", width: 7, write: func(db sqlplugin.DB, count int) (sql.Result, error) {
			var rows []sqlplugin.SignalInfoMapsRow
			if count >= 0 {
				rows = make([]sqlplugin.SignalInfoMapsRow, count)
			}
			for i := range rows {
				rows[i] = sqlplugin.SignalInfoMapsRow{
					ShardID: int32(i + 1), NamespaceID: namespaceIDs[i], WorkflowID: fmt.Sprintf("workflow-%d", i), RunID: runIDs[i],
					InitiatedID: int64(i + 10),
					Data:        []byte(fmt.Sprintf("data-%d", i)), DataEncoding: "Proto3",
				}
			}
			return db.ReplaceIntoSignalInfoMaps(t.Context(), rows)
		}},
		{name: "signals_requested_sets", key: "signal_id", width: 5, write: func(db sqlplugin.DB, count int) (sql.Result, error) {
			var rows []sqlplugin.SignalsRequestedSetsRow
			if count >= 0 {
				rows = make([]sqlplugin.SignalsRequestedSetsRow, count)
			}
			for i := range rows {
				rows[i] = sqlplugin.SignalsRequestedSetsRow{
					ShardID: int32(i + 1), NamespaceID: namespaceIDs[i], WorkflowID: fmt.Sprintf("workflow-%d", i), RunID: runIDs[i],
					SignalID: fmt.Sprintf("key-%d", i),
				}
			}
			return db.ReplaceIntoSignalsRequestedSets(t.Context(), rows)
		}},
		{name: "chasm_node_maps", key: "chasm_path", width: 9, write: func(db sqlplugin.DB, count int) (sql.Result, error) {
			var rows []sqlplugin.ChasmNodeMapsRow
			if count >= 0 {
				rows = make([]sqlplugin.ChasmNodeMapsRow, count)
			}
			for i := range rows {
				rows[i] = sqlplugin.ChasmNodeMapsRow{
					ShardID: int32(i + 1), NamespaceID: namespaceIDs[i], WorkflowID: fmt.Sprintf("workflow-%d", i), RunID: runIDs[i],
					ChasmPath: fmt.Sprintf("key-%d", i),
					Metadata:  []byte(fmt.Sprintf("metadata-%d", i)), MetadataEncoding: "Proto3",
					Data: []byte(fmt.Sprintf("data-%d", i)), DataEncoding: "Proto3",
				}
			}
			return db.ReplaceIntoChasmNodeMaps(t.Context(), rows)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conflict := &pgconn.PgError{Code: "40001", Message: "conflict"}
			for _, test := range []struct {
				name  string
				count int
				err   error
			}{
				{name: "nil", count: -1},
				{name: "empty"},
				{name: "multiple", count: 3},
				{name: "error", count: 3, err: conflict},
			} {
				t.Run(test.name, func(t *testing.T) {
					db, fake := newUUIDTestDB(t)
					fake.execErrors = []error{test.err}
					result, err := tc.write(db, test.count)
					require.ErrorIs(t, err, test.err)
					statements, _, _ := fake.snapshot()
					if test.count <= 0 {
						require.Empty(t, statements)
						require.Nil(t, result)
						return
					}
					require.Len(t, statements, 1)
					require.True(t, strings.HasPrefix(statements[0], "INSERT INTO "+tc.name+" "))
					_, values, ok := strings.Cut(statements[0], " VALUES ")
					require.True(t, ok)
					values, _, ok = strings.Cut(values, " ON CONFLICT ")
					require.True(t, ok)
					var tuples []string
					var wantArgs []driver.Value
					for i := range test.count {
						var placeholders []string
						for j := range tc.width {
							placeholders = append(placeholders, fmt.Sprintf("$%d", i*tc.width+j+1))
						}
						tuples = append(tuples, "("+strings.Join(placeholders, ",")+")")
						wantArgs = append(wantArgs, int64(i+1), namespaceIDs[i].String(), fmt.Sprintf("workflow-%d", i), runIDs[i].String())
						switch tc.key {
						case "schedule_id", "initiated_id":
							wantArgs = append(wantArgs, int64(i+10))
						default:
							wantArgs = append(wantArgs, fmt.Sprintf("key-%d", i))
						}
						if tc.width == 9 {
							wantArgs = append(wantArgs, []byte(fmt.Sprintf("metadata-%d", i)), "Proto3")
						}
						if tc.width > 5 {
							wantArgs = append(wantArgs, []byte(fmt.Sprintf("data-%d", i)), "Proto3")
						}
					}
					require.Equal(t, strings.Join(tuples, ","), strings.ReplaceAll(values, " ", ""))
					require.Equal(t, wantArgs, fake.statementArgs(0))
					if test.err != nil {
						require.Nil(t, result)
						return
					}
					require.NotNil(t, result)
					affected, err := result.RowsAffected()
					require.NoError(t, err)
					require.Equal(t, int64(test.count), affected)
				})
			}
		})
	}
}

func mapTestInsertWidth(query string) int {
	switch strings.Fields(query)[2] {
	case "activity_info_maps", "timer_info_maps", "child_execution_info_maps", "request_cancel_info_maps", "signal_info_maps":
		return 7
	case "signals_requested_sets":
		return 5
	case "chasm_node_maps":
		return 9
	default:
		return 0
	}
}
