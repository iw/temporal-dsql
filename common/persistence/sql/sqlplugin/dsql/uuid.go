package dsql

import (
	"go.temporal.io/server/common/primitives"
)

// NilUUID is the canonical nil UUID string (all zeros).
// Used as a minimum value for UUID comparisons in range queries.
const NilUUID = "00000000-0000-0000-0000-000000000000"

// BytesToUUIDString converts a byte slice to a canonical UUID string format.
// This is used for DSQL UUID column compatibility where the driver expects
// string format rather than raw bytes.
//
// The function handles:
// - 16-byte slices: converts to standard UUID format (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
// - Empty/nil slices: returns empty string
// - Short slices (<16 bytes): pads with zeros and converts
//
// For primitives.UUID types, use UUIDToString instead.
func BytesToUUIDString(id []byte) string {
	if len(id) == 0 {
		return ""
	}
	if len(id) < 16 {
		// Pad with zeros if too short
		padded := make([]byte, 16)
		copy(padded, id)
		id = padded
	}
	return formatUUID(id)
}

// UUIDToString converts a Temporal primitives.UUID to a canonical UUID string.
// This is the preferred method for converting primitives.UUID types.
//
// Panics if the UUID is not exactly 16 bytes (invalid UUID).
func UUIDToString(u primitives.UUID) string {
	if len(u) != 16 {
		panic("invalid primitives.UUID length (expected 16)")
	}
	return formatUUID(u)
}

// formatUUID formats a 16-byte slice as a canonical UUID string.
// Format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
func formatUUID(b []byte) string {
	const hex = "0123456789abcdef"
	out := make([]byte, 36)
	// 8-4-4-4-12 layout with hyphens at positions 8, 13, 18, 23
	j := 0
	for i := 0; i < 16; i++ {
		if j == 8 || j == 13 || j == 18 || j == 23 {
			out[j] = '-'
			j++
		}
		out[j] = hex[b[i]>>4]
		out[j+1] = hex[b[i]&0x0f]
		j += 2
	}
	return string(out)
}
