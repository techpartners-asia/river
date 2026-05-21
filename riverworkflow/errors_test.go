package riverworkflow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrorSentinels(t *testing.T) {
	t.Parallel()

	for _, err := range []error{
		ErrWorkflowDepCycle,
		ErrWorkflowDepUnknown,
		ErrWorkflowEmpty,
		ErrWorkflowTaskNameDuplicate,
		ErrWorkflowTaskNameEmpty,
		ErrWorkflowTaskOutputMissing,
	} {
		require.Error(t, err)
		require.Contains(t, err.Error(), "riverworkflow:")
	}
}
