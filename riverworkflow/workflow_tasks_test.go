package riverworkflow

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/rivertype"
)

func TestWorkflowTasks_Get(t *testing.T) {
	t.Parallel()

	row := &rivertype.JobRow{}
	wt := &WorkflowTasks{byName: map[string]*rivertype.JobRow{"a": row}}

	got, err := wt.Get("a")
	require.NoError(t, err)
	require.Same(t, row, got)

	_, err = wt.Get("missing")
	require.Error(t, err)
}

func TestWorkflowTasks_Output_Missing(t *testing.T) {
	t.Parallel()

	wt := &WorkflowTasks{byName: map[string]*rivertype.JobRow{
		"a": {Metadata: []byte(`{}`)},
	}}
	var out struct{ V int }
	err := wt.Output("a", &out)
	require.ErrorIs(t, err, ErrWorkflowTaskOutputMissing)
}

func TestWorkflowTasks_Output_RoundTrips(t *testing.T) {
	t.Parallel()

	type payload struct{ N int }
	encoded, err := json.Marshal(payload{N: 42})
	require.NoError(t, err)
	meta, err := json.Marshal(map[string]json.RawMessage{
		rivertype.MetadataKeyOutput: encoded,
	})
	require.NoError(t, err)

	wt := &WorkflowTasks{byName: map[string]*rivertype.JobRow{
		"a": {Metadata: meta},
	}}
	var got payload
	require.NoError(t, wt.Output("a", &got))
	require.Equal(t, 42, got.N)
}

func TestTasksFromRows(t *testing.T) {
	t.Parallel()

	meta := func(taskName string) []byte {
		b, _ := json.Marshal(map[string]any{
			rivercommon.MetadataKeyWorkflowTask: taskName,
		})
		return b
	}
	rows := []*rivertype.JobRow{
		{ID: 1, Metadata: meta("a")},
		{ID: 2, Metadata: meta("b")},
		{ID: 3, Metadata: []byte(`{}`)}, // no task name → skipped
	}
	wt := tasksFromRows(rows)
	require.Len(t, wt.byName, 2)
	require.Equal(t, int64(1), wt.byName["a"].ID)
	require.Equal(t, int64(2), wt.byName["b"].ID)
}

func TestWalkDeps(t *testing.T) {
	t.Parallel()

	meta := func(taskName string, deps ...string) []byte {
		m := map[string]any{rivercommon.MetadataKeyWorkflowTask: taskName}
		if len(deps) > 0 {
			m[rivercommon.MetadataKeyWorkflowDeps] = deps
		}
		b, _ := json.Marshal(m)
		return b
	}
	all := tasksFromRows([]*rivertype.JobRow{
		{ID: 1, Metadata: meta("a")},
		{ID: 2, Metadata: meta("b", "a")},
		{ID: 3, Metadata: meta("c", "b")},
	})

	t.Run("DirectOnly", func(t *testing.T) {
		t.Parallel()
		got := walkDeps(all, "c", false)
		require.Len(t, got.byName, 1)
		require.Contains(t, got.byName, "b")
	})

	t.Run("Recursive", func(t *testing.T) {
		t.Parallel()
		got := walkDeps(all, "c", true)
		require.Len(t, got.byName, 2)
		require.Contains(t, got.byName, "a")
		require.Contains(t, got.byName, "b")
	})

	t.Run("RootHasNoDeps", func(t *testing.T) {
		t.Parallel()
		got := walkDeps(all, "a", true)
		require.Empty(t, got.byName)
	})
}
