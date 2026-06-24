package workflowid

import (
	"encoding/binary"
	"testing"
	"time"

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

func TestTimestamp(t *testing.T) {
	t.Parallel()

	t.Run("RoundTripsWithNew", func(t *testing.T) {
		t.Parallel()

		before := time.Now().UTC().Truncate(time.Millisecond)
		id := New()
		after := time.Now().UTC().Add(time.Millisecond).Truncate(time.Millisecond)

		ts, err := Timestamp(id)
		require.NoError(t, err)
		require.WithinDuration(t, before, ts, after.Sub(before)+time.Millisecond,
			"decoded timestamp must be within the generation window")
		require.True(t, !ts.Before(before), "decoded timestamp must not be before generation start")
		require.True(t, !ts.After(after), "decoded timestamp must not be after generation end")
	})

	t.Run("KnownID", func(t *testing.T) {
		t.Parallel()

		// Encode a known ms value and verify the round-trip.
		// ms = 1700000000000 (2023-11-14 22:13:20 UTC)
		const knownMS = int64(1700000000000)
		knownTime := time.UnixMilli(knownMS).UTC()

		var raw [16]byte
		binary.BigEndian.PutUint64(raw[0:8], uint64(knownMS)<<16) //nolint:gosec
		id := encode(raw)

		ts, err := Timestamp(id)
		require.NoError(t, err)
		require.Equal(t, knownTime, ts)
	})

	t.Run("WrongLengthID", func(t *testing.T) {
		t.Parallel()

		_, err := Timestamp("ABCD")
		require.Error(t, err)
		require.Contains(t, err.Error(), "26-char ULID")
	})

	t.Run("CustomNonULIDDoesNotDecodeToBogusAnchor", func(t *testing.T) {
		t.Parallel()

		// A caller-supplied non-ULID id whose characters happen to be valid
		// Crockford but whose length is not 26 must error, so the caller falls
		// back to a real time source instead of a 1970-era anchor.
		_, err := Timestamp("0000000000ABCDEF")
		require.Error(t, err)
	})

	t.Run("InvalidCharacter", func(t *testing.T) {
		t.Parallel()

		// Lowercase letters are not in the Crockford base32 alphabet.
		_, err := Timestamp("000000000U0000000000000z00")
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid character")
	})
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
