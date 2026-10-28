package dsql

import (
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	dbschemas "go.temporal.io/server/schema"
)

func TestAsyncIndexStatement(t *testing.T) {
	for _, tc := range []struct {
		stmt  string
		async bool
	}{
		{"CREATE INDEX ASYNC cm_idx_lasthb ON cluster_membership (last_heartbeat)", true},
		{"CREATE UNIQUE INDEX ASYNC idx_namespaces_name ON namespaces(name)", true},
		{"create index async idx on t (a)", true},
		{"-- Secondary indexes are built asynchronously.\nCREATE INDEX ASYNC idx ON t (a)", true},
		{"CREATE INDEX idx ON t (a)", false},
		{"CREATE TABLE async_jobs (id INT PRIMARY KEY)", false},
		{"INSERT INTO t VALUES (1)", false},
	} {
		require.Equal(t, tc.async, asyncIndexStatement.MatchString(tc.stmt), tc.stmt)
	}

	full, err := fs.ReadFile(dbschemas.Assets(), "dsql/temporal/schema.sql")
	require.NoError(t, err)
	var indexes int
	for _, stmt := range strings.Split(string(full), ";") {
		if strings.Contains(strings.ToUpper(stmt), "INDEX ASYNC") {
			indexes++
			require.True(t, asyncIndexStatement.MatchString(stmt), stmt)
		}
	}
	require.Positive(t, indexes)
}

type fakeJobs struct {
	waits   map[string][]bool
	status  map[string][2]string
	waitErr error
	calls   []string
}

func (f *fakeJobs) waitForJob(_ context.Context, id string) (bool, error) {
	f.calls = append(f.calls, id)
	if f.waitErr != nil {
		return false, f.waitErr
	}
	results := f.waits[id]
	f.waits[id] = results[1:]
	return results[0], nil
}

func (f *fakeJobs) jobStatus(_ context.Context, id string) (status, details string, err error) {
	s := f.status[id]
	return s[0], s[1], nil
}

func TestWaitForIndexBuild(t *testing.T) {
	t.Run("completes", func(t *testing.T) {
		jobs := &fakeJobs{waits: map[string][]bool{"a": {true}}}
		require.NoError(t, waitForIndexBuild(t.Context(), jobs, "a"))
		require.Equal(t, []string{"a"}, jobs.calls)
	})
	t.Run("waits again while the build runs", func(t *testing.T) {
		jobs := &fakeJobs{
			waits:  map[string][]bool{"a": {false, true}},
			status: map[string][2]string{"a": {"processing", ""}},
		}
		require.NoError(t, waitForIndexBuild(t.Context(), jobs, "a"))
		require.Equal(t, []string{"a", "a"}, jobs.calls)
	})
	t.Run("failed build", func(t *testing.T) {
		jobs := &fakeJobs{
			waits:  map[string][]bool{"a": {false}},
			status: map[string][2]string{"a": {"failed", "Found duplicate key while validating index for UCVs"}},
		}
		err := waitForIndexBuild(t.Context(), jobs, "a")
		require.ErrorContains(t, err, "DSQL index build a failed: Found duplicate key")
	})
	t.Run("wait error", func(t *testing.T) {
		jobs := &fakeJobs{waitErr: errors.New("connection reset")}
		require.ErrorContains(t, waitForIndexBuild(t.Context(), jobs, "a"), "wait for DSQL index build a: connection reset")
	})
}
