package riverworkflow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// C1: NewClient should return an error when driver is nil instead of nil-deref.
func TestNewClient_RejectsNilDriver(t *testing.T) {
	t.Parallel()
	_, err := NewClient[any](nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "driver is required")
}

// C2: WorkflowFromExisting should return an error when the metadata contains
// an empty workflow ID instead of silently creating a new workflow.
func TestWorkflowFromExisting_EmptyIDRejected(t *testing.T) {
	t.Parallel()
	_, err := workflowIDFromMetadata([]byte(`{"river:workflow_id":""}`))
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty")
}
