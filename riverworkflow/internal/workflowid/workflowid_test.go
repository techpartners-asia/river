package workflowid

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Parallel()

	id := New()
	require.Len(t, id, 26)
	for _, r := range id {
		require.True(t, isCrockford(r), "char %q not Crockford base32", r)
	}
}

func TestNew_UniqueAndSortable(t *testing.T) {
	t.Parallel()

	ids := make([]string, 1000)
	for i := range ids {
		ids[i] = New()
	}
	for i := 1; i < len(ids); i++ {
		require.NotEqual(t, ids[i-1], ids[i], "ULIDs must be unique")
		require.True(t, ids[i-1] <= ids[i], "ULIDs must be monotonically non-decreasing (i=%d %q vs %q)", i, ids[i-1], ids[i])
	}
}

func isCrockford(r rune) bool {
	const c = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	for _, x := range c {
		if x == r {
			return true
		}
	}
	return false
}
