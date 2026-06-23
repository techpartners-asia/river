package riverworkflow

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/internal/rivercommon"
)

type sortArgs struct {
	Strings []string `json:"strings"`
}

func (sortArgs) Kind() string { return "sort" }

func TestWorkflow_NewBasic(t *testing.T) {
	t.Parallel()

	w := newWorkflow[any](&WorkflowOpts{Name: "test"}, nil, "")
	require.NotEmpty(t, w.ID())
	require.Equal(t, "test", w.Name())
	require.Empty(t, w.tasks)
}

func TestWorkflow_New_CustomID(t *testing.T) {
	t.Parallel()

	w := newWorkflow[any](&WorkflowOpts{ID: "custom"}, nil, "")
	require.Equal(t, "custom", w.ID())
}

func TestWorkflow_Add(t *testing.T) {
	t.Parallel()

	w := newWorkflow[any](nil, nil, "")
	task := w.Add("a", sortArgs{Strings: []string{"x"}}, nil, nil)
	require.Equal(t, "a", task.Name)
	require.Len(t, w.tasks, 1)
	require.Same(t, task, w.tasks[0])

	taskB := w.Add("b", sortArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"a"}})
	require.Equal(t, []string{"a"}, taskB.deps)
	require.Len(t, w.tasks, 2)
}

func TestWorkflow_Validate(t *testing.T) {
	t.Parallel()

	t.Run("Empty", func(t *testing.T) {
		t.Parallel()
		w := newWorkflow[any](nil, nil, "")
		require.ErrorIs(t, w.validate(), ErrWorkflowEmpty)
	})

	t.Run("EmptyName", func(t *testing.T) {
		t.Parallel()
		w := newWorkflow[any](nil, nil, "")
		w.Add("", sortArgs{}, nil, nil)
		require.ErrorIs(t, w.validate(), ErrWorkflowTaskNameEmpty)
	})

	t.Run("DuplicateName", func(t *testing.T) {
		t.Parallel()
		w := newWorkflow[any](nil, nil, "")
		w.Add("a", sortArgs{}, nil, nil)
		w.Add("a", sortArgs{}, nil, nil)
		require.ErrorIs(t, w.validate(), ErrWorkflowTaskNameDuplicate)
	})

	t.Run("UnknownDep", func(t *testing.T) {
		t.Parallel()
		w := newWorkflow[any](nil, nil, "")
		w.Add("a", sortArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"ghost"}})
		require.ErrorIs(t, w.validate(), ErrWorkflowDepUnknown)
	})

	t.Run("Cycle", func(t *testing.T) {
		t.Parallel()
		w := newWorkflow[any](nil, nil, "")
		w.Add("a", sortArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"b"}})
		w.Add("b", sortArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"a"}})
		require.ErrorIs(t, w.validate(), ErrWorkflowDepCycle)
	})

	t.Run("ValidDiamond", func(t *testing.T) {
		t.Parallel()
		w := newWorkflow[any](nil, nil, "")
		w.Add("a", sortArgs{}, nil, nil)
		w.Add("b1", sortArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"a"}})
		w.Add("b2", sortArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"a"}})
		w.Add("c", sortArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"b1", "b2"}})
		require.NoError(t, w.validate())
	})
}

func TestWorkflow_Prepare(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	w := newWorkflow[any](&WorkflowOpts{Name: "billing"}, nil, "")
	w.Add("a", sortArgs{Strings: []string{"x"}}, nil, nil)
	w.Add("b", sortArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"a"}})

	res, err := w.Prepare(ctx)
	require.NoError(t, err)
	require.Equal(t, w.ID(), res.WorkflowID)
	require.Len(t, res.Jobs, 2)

	// Task "a" — no deps → state defaulted by river (not pending).
	require.NotNil(t, res.Jobs[0].InsertOpts)
	require.False(t, res.Jobs[0].InsertOpts.Pending)

	var metaA map[string]any
	require.NoError(t, json.Unmarshal(res.Jobs[0].InsertOpts.Metadata, &metaA))
	require.Equal(t, w.ID(), metaA[rivercommon.MetadataKeyWorkflowID])
	require.Equal(t, "billing", metaA[rivercommon.MetadataKeyWorkflowName])
	require.Equal(t, "a", metaA[rivercommon.MetadataKeyWorkflowTask])
	require.NotContains(t, metaA, rivercommon.MetadataKeyWorkflowDeps)

	// Task "b" — has deps → Pending=true and Deps recorded.
	require.NotNil(t, res.Jobs[1].InsertOpts)
	require.True(t, res.Jobs[1].InsertOpts.Pending)

	var metaB map[string]any
	require.NoError(t, json.Unmarshal(res.Jobs[1].InsertOpts.Metadata, &metaB))
	require.Equal(t, "b", metaB[rivercommon.MetadataKeyWorkflowTask])
	require.Equal(t, []any{"a"}, metaB[rivercommon.MetadataKeyWorkflowDeps])
}

func TestWorkflow_Prepare_IgnoreFlags(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	w := newWorkflow[any](&WorkflowOpts{IgnoreCancelledDeps: true}, nil, "")
	w.Add("a", sortArgs{}, nil, nil)
	w.Add("b", sortArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"a"}})

	res, err := w.Prepare(ctx)
	require.NoError(t, err)

	var metaB map[string]any
	require.NoError(t, json.Unmarshal(res.Jobs[1].InsertOpts.Metadata, &metaB))
	require.Equal(t, true, metaB[rivercommon.MetadataKeyWorkflowIgnoreCancelledDeps])
	require.NotContains(t, metaB, rivercommon.MetadataKeyWorkflowIgnoreDiscardedDeps)
}

// C3: Large integers in existing metadata must not lose precision through
// float64 conversion when renderTaskOpts merges workflow keys.
func TestWorkflow_Prepare_PreservesLargeIntegers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	w := newWorkflow[any](nil, nil, "")
	preset, err := json.Marshal(map[string]any{"snowflake_id": json.Number("123456789012345678")})
	require.NoError(t, err)
	w.Add("a", sortArgs{}, &river.InsertOpts{Metadata: preset}, nil)

	res, err := w.Prepare(ctx)
	require.NoError(t, err)
	require.Contains(t, string(res.Jobs[0].InsertOpts.Metadata), `"snowflake_id":123456789012345678`,
		"large integer must be preserved exactly; got %s", res.Jobs[0].InsertOpts.Metadata)
}

func TestWorkflow_Prepare_PreservesExistingMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	w := newWorkflow[any](nil, nil, "")
	preset, err := json.Marshal(map[string]any{"user_key": "user_value"})
	require.NoError(t, err)
	w.Add("a", sortArgs{}, &river.InsertOpts{Metadata: preset}, nil)

	res, err := w.Prepare(ctx)
	require.NoError(t, err)

	var meta map[string]any
	require.NoError(t, json.Unmarshal(res.Jobs[0].InsertOpts.Metadata, &meta))
	require.Equal(t, "user_value", meta["user_key"])
	require.Equal(t, w.ID(), meta[rivercommon.MetadataKeyWorkflowID])
}

func TestWorkflowWaitMetadata(t *testing.T) {
	w := newWorkflow[any](&WorkflowOpts{ID: "wf-wait"}, nil, "")
	w.Add("gate", sortArgs{}, nil, &WorkflowTaskOpts{
		Wait: &WaitSpec{
			Terms: []WaitTermSpec{WaitTermSignal("ok", "ok", "payload.ok")},
			Expr:  "ok",
		},
	})

	res, err := w.Prepare(context.Background())
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	job := res.Jobs[0]
	if job.InsertOpts == nil || !job.InsertOpts.Pending {
		t.Fatalf("wait-bearing task must be Pending")
	}
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(job.InsertOpts.Metadata, &meta); err != nil {
		t.Fatalf("metadata: %v", err)
	}
	if _, ok := meta[rivercommon.MetadataKeyWorkflowWait]; !ok {
		t.Fatalf("expected %s in metadata, got %v", rivercommon.MetadataKeyWorkflowWait, meta)
	}
}

func TestWorkflowWaitInvalidRejected(t *testing.T) {
	w := newWorkflow[any](&WorkflowOpts{ID: "wf-bad"}, nil, "")
	w.Add("gate", sortArgs{}, nil, &WorkflowTaskOpts{
		Wait: &WaitSpec{Terms: []WaitTermSpec{WaitTerm("a", "true")}, Expr: ""},
	})
	if _, err := w.Prepare(context.Background()); !errors.Is(err, ErrWaitExprEmpty) {
		t.Fatalf("want ErrWaitExprEmpty, got %v", err)
	}
}
