package rivercommon

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetadataKeyWorkflow(t *testing.T) {
	t.Parallel()

	require.Equal(t, "river:workflow_id", MetadataKeyWorkflowID)
	require.Equal(t, "river:workflow_name", MetadataKeyWorkflowName)
	require.Equal(t, "river:workflow_task", MetadataKeyWorkflowTask)
	require.Equal(t, "river:workflow_deps", MetadataKeyWorkflowDeps)
	require.Equal(t, "river:workflow_ignore_cancelled_deps", MetadataKeyWorkflowIgnoreCancelledDeps)
	require.Equal(t, "river:workflow_ignore_discarded_deps", MetadataKeyWorkflowIgnoreDiscardedDeps)
	require.Equal(t, "river:workflow_ignore_deleted_deps", MetadataKeyWorkflowIgnoreDeletedDeps)
}

func TestMetadataKeyWorkflowWait(t *testing.T) {
	if MetadataKeyWorkflowWait != "river:workflow_wait" {
		t.Fatalf("unexpected key: %q", MetadataKeyWorkflowWait)
	}
}

func TestJobKindRE(t *testing.T) {
	t.Parallel()

	require.Regexp(t, UserSpecifiedIDOrKindRE, "kind")
	require.Regexp(t, UserSpecifiedIDOrKindRE, "kind123")
	require.Regexp(t, UserSpecifiedIDOrKindRE, "with.dot")
	require.Regexp(t, UserSpecifiedIDOrKindRE, "with:colon")
	require.Regexp(t, UserSpecifiedIDOrKindRE, "with+plus")
	require.Regexp(t, UserSpecifiedIDOrKindRE, "with-hyphen")
	require.Regexp(t, UserSpecifiedIDOrKindRE, "with_underscore")
	require.Regexp(t, UserSpecifiedIDOrKindRE, "with[brackets]")
	require.Regexp(t, UserSpecifiedIDOrKindRE, "with<triangle_brackets>")
	require.Regexp(t, UserSpecifiedIDOrKindRE, "with/slash")
	require.Regexp(t, UserSpecifiedIDOrKindRE, "JobArgsReflectKind[github.com/riverqueue/river.JobArgs·12]")

	require.NotRegexp(t, UserSpecifiedIDOrKindRE, "with space")
	require.NotRegexp(t, UserSpecifiedIDOrKindRE, "with,comma")
	require.NotRegexp(t, UserSpecifiedIDOrKindRE, ":no_leading_special_characters")
}
