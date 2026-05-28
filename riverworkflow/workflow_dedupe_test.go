package riverworkflow

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/rivercommon"
)

// Regression: a duplicate dependency name must be collapsed to a set before the
// deps array is written to job metadata. The readiness classifier counts
// declared deps as the array length but matches siblings by set membership, so
// a repeated name would make declared > matchable and wrongly cancel a task
// whose dependency actually completed. See Workflow.validate.
func TestWorkflow_DeduplicatesDuplicateDeps(t *testing.T) {
	t.Parallel()

	w := newWorkflow[any](&WorkflowOpts{Name: "dedupe"}, nil, "")
	w.Add("a", sortArgs{}, nil, nil)
	w.Add("b", sortArgs{}, nil, &WorkflowTaskOpts{Deps: []string{"a", "a", "a"}})

	res, err := w.prepare()
	require.NoError(t, err)

	depsByTask := map[string][]string{}
	for _, job := range res.Jobs {
		var md map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(job.InsertOpts.Metadata, &md))

		taskRaw, ok := md[rivercommon.MetadataKeyWorkflowTask]
		require.True(t, ok)
		var task string
		require.NoError(t, json.Unmarshal(taskRaw, &task))

		if depsRaw, ok := md[rivercommon.MetadataKeyWorkflowDeps]; ok {
			var deps []string
			require.NoError(t, json.Unmarshal(depsRaw, &deps))
			depsByTask[task] = deps
		}
	}

	// "b" declared "a" three times; storage must collapse it to exactly one,
	// so declared count (1) matches the single matchable sibling "a".
	require.Equal(t, []string{"a"}, depsByTask["b"])
}
