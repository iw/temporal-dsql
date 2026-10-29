package dsql

// Version is the DSQL persistence schema version.
const Version = "1.3"

// VisibilityVersion is the DSQL visibility schema version.
const VisibilityVersion = "1.0"

// VisibilitySchema is the PostgreSQL schema that holds the visibility store unless
// the datastore sets search_path. DSQL has one database per cluster, so the
// visibility store sits in its own schema beside the persistence store's.
const VisibilitySchema = "temporal_visibility"
