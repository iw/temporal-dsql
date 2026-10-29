package dsql

import (
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChasmPathRoundTrip(t *testing.T) {
	for _, path := range []string{
		"", "plain", "Updates$update-id#Callbacks$cb",
		"\x00", "\x01", "\x02", "\x01\x01", "\x01\x02",
		"key\\\x00with NUL", "key\\\x01with SOH", "\x00\x01\x00\x02\x01\x00",
	} {
		stored := encodeChasmPath(path)
		require.NotContains(t, stored, "\x00")
		decoded, err := decodeChasmPath(stored)
		require.NoError(t, err)
		require.Equal(t, path, decoded)
	}
	require.Equal(t, "plain", encodeChasmPath("plain"))
	require.Equal(t, "a\x01\x01b\x01\x02c", encodeChasmPath("a\x00b\x01c"))
	require.Equal(t, []string{"x", "\x01\x01"}, encodeChasmPaths([]string{"x", "\x00"}))
}

// Stored paths must sort, and prefix one another, exactly as the encoder's paths do.
func TestChasmPathKeepsOrderAndPrefixes(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	alphabet := []byte{0x00, 0x01, 0x02, '#', '$', '\\', 'a'}
	random := func() string {
		b := make([]byte, r.IntN(6))
		for i := range b {
			b[i] = alphabet[r.IntN(len(alphabet))]
		}
		return string(b)
	}
	for range 20000 {
		a, b := random(), random()
		if r.IntN(4) == 0 {
			b = a + b
		}
		storedA, storedB := encodeChasmPath(a), encodeChasmPath(b)
		require.Equal(t, strings.Compare(a, b), strings.Compare(storedA, storedB), "%q vs %q", a, b)
		require.Equal(t, strings.HasPrefix(b, a), strings.HasPrefix(storedB, storedA), "%q vs %q", a, b)
		decoded, err := decodeChasmPath(storedA)
		require.NoError(t, err)
		require.Equal(t, a, decoded)
	}
}

func TestChasmPathDecodeErrors(t *testing.T) {
	_, err := decodeChasmPath("key\x01")
	require.ErrorContains(t, err, "ends with an escape")
	_, err = decodeChasmPath("key\x01\x03")
	require.ErrorContains(t, err, "unknown escape")
}
