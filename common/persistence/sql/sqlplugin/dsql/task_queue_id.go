package dsql

import (
	"crypto/sha256"

	"github.com/google/uuid"
)

// TaskQueueIDToUUIDString converts the variable-length task queue ID byte slice produced by
// taskQueueId(namespaceID, taskQueueName, taskType, subqueue) into a fixed-size UUID string.
//
// Why:
// - Aurora DSQL cannot index BYTEA columns, so task_queue_id cannot be BYTEA in primary keys/indexes.
// - We need a stable, fixed-size, index-friendly surrogate key for task_queue_id.
// - A deterministic UUID (derived from a hash) provides a compact canonical representation.
//
// How:
// - Compute SHA-256 over the input bytes.
// - Take the first 16 bytes as UUID bytes.
// - Set RFC4122 variant and version bits to produce a well-formed UUID string.
// - Return the canonical hyphenated UUID string (36 chars).
//
// Notes:
// - This does not preserve any semantic ordering. It provides a stable total ordering over UUID strings
//   for comparisons/pagination, which is sufficient for SQL queries using ORDER BY task_queue_id.
// - Collision risk is negligible for this use case (128-bit truncated SHA-256).
func TaskQueueIDToUUIDString(taskQueueID []byte) string {
	if len(taskQueueID) == 0 {
		return ""
	}

	sum := sha256.Sum256(taskQueueID)

	u, err := uuid.FromBytes(sum[:16])
	if err != nil {
		// FromBytes only fails if len != 16, which cannot happen here.
		panic(err)
	}

	// Make it a well-formed RFC4122 UUID (set version/variant bits).
	u[6] = (u[6] & 0x0f) | 0x40 // version 4-style nibble
	u[8] = (u[8] & 0x3f) | 0x80 // RFC4122 variant

	return u.String()
}