package dsql

import "time"

var (
	// minDSQLDateTime is the minimum datetime value supported by DSQL.
	// DSQL uses PostgreSQL-compatible datetime handling.
	minDSQLDateTime = getMinDSQLDateTime()
)

type (
	// DataConverter defines the API for conversions to/from
	// Go types to DSQL datatypes
	DataConverter interface {
		ToDSQLDateTime(t time.Time) time.Time
		FromDSQLDateTime(t time.Time) time.Time
	}
	converter struct{}
)

// ToDSQLDateTime converts a Go time to DSQL datetime format.
// DSQL uses PostgreSQL-compatible datetime handling with microsecond precision.
func (c *converter) ToDSQLDateTime(t time.Time) time.Time {
	if t.IsZero() {
		return minDSQLDateTime
	}
	return t.UTC().Truncate(time.Microsecond)
}

// FromDSQLDateTime converts a DSQL datetime to Go time.
// Handles timezone normalization to UTC.
func (c *converter) FromDSQLDateTime(t time.Time) time.Time {
	if t.Equal(minDSQLDateTime) {
		return time.Time{}.UTC()
	}
	return t.UTC()
}

func getMinDSQLDateTime() time.Time {
	t, err := time.Parse(time.RFC3339, "1000-01-01T00:00:00Z")
	if err != nil {
		return time.Unix(0, 0).UTC()
	}
	return t.UTC()
}
