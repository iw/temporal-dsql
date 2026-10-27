package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

func TestDSQLBufferedEventsChangeSet(t *testing.T) {
	for _, from := range []string{"1.0", "1.1"} {
		t.Run(from, func(t *testing.T) {
			task := NewUpdateSchemaTask(nil, &UpdateConfig{
				SchemaName:    "dsql/temporal",
				TargetVersion: "1.2",
			}, log.NewNoopLogger())
			changes, err := task.buildChangeSet(from)
			require.NoError(t, err)
			if from == "1.0" {
				require.Len(t, changes, 2)
				require.Equal(t, "1.1", changes[0].version)
				require.Equal(t, "1.0", changes[0].manifest.MinCompatibleVersion)
				require.Len(t, changes[0].cqlStmts, 1)
				require.Contains(t, changes[0].cqlStmts[0], "CREATE TABLE current_chasm_executions(")
				changes = changes[1:]
			}
			require.Len(t, changes, 1)
			require.Equal(t, "1.2", changes[0].version)
			require.Equal(t, "1.1", changes[0].manifest.MinCompatibleVersion)
			require.Equal(t, []string{"DROP INDEX idx_buffered_events_id;"}, changes[0].cqlStmts)
		})
	}
}
