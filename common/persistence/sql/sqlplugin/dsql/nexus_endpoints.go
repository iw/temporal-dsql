package dsql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

// dsqlNexusEndpointRow is a DSQL-specific scan target for nexus_endpoints.
// DSQL returns UUID columns as strings, but sqlplugin.NexusEndpointsRow.ID
// is []byte. We scan into this struct and convert back.
type dsqlNexusEndpointRow struct {
	ID           string `db:"id"`
	Data         []byte `db:"data"`
	DataEncoding string `db:"data_encoding"`
	Version      int64  `db:"version"`
}

// toPluginRow converts a DSQL-scanned row to the sqlplugin row type.
// The UUID string (e.g. "6365cfab-74bf-4e32-89d1-c2f9e4993763") is
// parsed back to 16 raw bytes.
func (r *dsqlNexusEndpointRow) toPluginRow() sqlplugin.NexusEndpointsRow {
	return sqlplugin.NexusEndpointsRow{
		ID:           uuidStringToBytes(r.ID),
		Data:         r.Data,
		DataEncoding: r.DataEncoding,
		Version:      r.Version,
	}
}

// uuidStringToBytes parses a UUID string like "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
// into 16 raw bytes. Returns nil for empty strings.
func uuidStringToBytes(s string) []byte {
	if s == "" {
		return nil
	}
	s = strings.ReplaceAll(s, "-", "")
	b, err := hex.DecodeString(s)
	if err != nil {
		return []byte(s) // fallback: return raw string bytes
	}
	return b
}

const (
	createEndpointsTableVersionQry    = `INSERT INTO nexus_endpoints_partition_status(version) VALUES(1)`
	incrementEndpointsTableVersionQry = `UPDATE nexus_endpoints_partition_status SET version = $1 WHERE version = $2`
	getEndpointsTableVersionQry       = `SELECT version FROM nexus_endpoints_partition_status`

	createEndpointQry  = `INSERT INTO nexus_endpoints(id, data, data_encoding, version) VALUES ($1, $2, $3, 1)`
	updateEndpointQry  = `UPDATE nexus_endpoints SET data = $1, data_encoding = $2, version = $3 WHERE id = $4 AND version = $5`
	deleteEndpointQry  = `DELETE FROM nexus_endpoints WHERE id = $1`
	getEndpointByIdQry = `SELECT id, data, data_encoding, version FROM nexus_endpoints WHERE id = $1`
	getEndpointsQry    = `SELECT id, data, data_encoding, version FROM nexus_endpoints WHERE id > $1 ORDER BY id LIMIT $2`
)

func (pdb *db) InitializeNexusEndpointsTableVersion(ctx context.Context) (sql.Result, error) {
	return pdb.ExecContext(ctx, createEndpointsTableVersionQry)
}

func (pdb *db) IncrementNexusEndpointsTableVersion(
	ctx context.Context,
	lastKnownTableVersion int64,
) (sql.Result, error) {
	return pdb.ExecContext(ctx, incrementEndpointsTableVersionQry, lastKnownTableVersion+1, lastKnownTableVersion)
}

func (pdb *db) GetNexusEndpointsTableVersion(
	ctx context.Context,
) (int64, error) {
	var version int64
	err := pdb.GetContext(ctx, &version, getEndpointsTableVersionQry)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return version, err
}

func (pdb *db) InsertIntoNexusEndpoints(
	ctx context.Context,
	row *sqlplugin.NexusEndpointsRow,
) (sql.Result, error) {
	// Convert UUID bytes to string for DSQL UUID column compatibility
	idStr := BytesToUUIDString(row.ID)
	return pdb.ExecContext(
		ctx,
		createEndpointQry,
		idStr,
		row.Data,
		row.DataEncoding)
}

func (pdb *db) UpdateNexusEndpoint(
	ctx context.Context,
	row *sqlplugin.NexusEndpointsRow,
) (sql.Result, error) {
	// Convert UUID bytes to string for DSQL UUID column compatibility
	idStr := BytesToUUIDString(row.ID)
	return pdb.ExecContext(
		ctx,
		updateEndpointQry,
		row.Data,
		row.DataEncoding,
		row.Version+1,
		idStr,
		row.Version)
}

func (pdb *db) DeleteFromNexusEndpoints(
	ctx context.Context,
	id []byte,
) (sql.Result, error) {
	// Convert UUID bytes to string for DSQL UUID column compatibility
	idStr := BytesToUUIDString(id)
	return pdb.ExecContext(ctx, deleteEndpointQry, idStr)
}

func (pdb *db) GetNexusEndpointByID(
	ctx context.Context,
	id []byte,
) (*sqlplugin.NexusEndpointsRow, error) {
	var dsqlRow dsqlNexusEndpointRow
	// Convert UUID bytes to string for DSQL UUID column compatibility
	idStr := BytesToUUIDString(id)
	err := pdb.GetContext(ctx, &dsqlRow, getEndpointByIdQry, idStr)
	if err != nil {
		return nil, err
	}
	row := dsqlRow.toPluginRow()
	return &row, nil
}

func (pdb *db) ListNexusEndpoints(
	ctx context.Context,
	request *sqlplugin.ListNexusEndpointsRequest,
) ([]sqlplugin.NexusEndpointsRow, error) {
	var dsqlRows []dsqlNexusEndpointRow
	// Convert UUID bytes to string for DSQL UUID column compatibility.
	// When LastID is empty (first page), use the nil UUID as the minimum bound
	// so that "WHERE id > $1" returns all rows. DSQL's UUID type rejects empty
	// strings, unlike PostgreSQL's BYTEA which accepts empty byte slices.
	lastIDStr := BytesToUUIDString(request.LastID)
	if lastIDStr == "" {
		lastIDStr = NilUUID
	}
	err := pdb.SelectContext(ctx, &dsqlRows, getEndpointsQry, lastIDStr, request.Limit)
	if err != nil {
		return nil, err
	}
	rows := make([]sqlplugin.NexusEndpointsRow, len(dsqlRows))
	for i := range dsqlRows {
		rows[i] = dsqlRows[i].toPluginRow()
	}
	return rows, err
}
