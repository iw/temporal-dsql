package sql_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
)

// Keep real SQLite reads, deletes and transactions while controlling the failure
// and interleaving between physical deletion and the metadata update.
type queueDeleteTestDB struct {
	sqlplugin.DB
	t           *testing.T
	operations  []string
	failUpdate  bool
	afterDelete func()
}

type queueDeleteTestTx struct {
	sqlplugin.Tx
	db *queueDeleteTestDB
}

func (db *queueDeleteTestDB) BeginTx(ctx context.Context) (sqlplugin.Tx, error) {
	db.operations = append(db.operations, "begin")
	tx, err := db.DB.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	return &queueDeleteTestTx{Tx: tx, db: db}, nil
}

func (db *queueDeleteTestDB) RangeDeleteFromQueueV2Messages(ctx context.Context, filter sqlplugin.QueueV2MessagesFilter) (sql.Result, error) {
	db.operations = append(db.operations, "delete")
	result, err := db.DB.RangeDeleteFromQueueV2Messages(ctx, filter)
	if err == nil && db.afterDelete != nil {
		hook := db.afterDelete
		db.afterDelete = nil
		hook()
	}
	return result, err
}

func (tx *queueDeleteTestTx) RangeDeleteFromQueueV2Messages(context.Context, sqlplugin.QueueV2MessagesFilter) (sql.Result, error) {
	tx.db.t.Error("range delete must run outside the metadata transaction")
	return nil, errors.New("range delete inside transaction")
}

func (tx *queueDeleteTestTx) GetLastEnqueuedMessageIDForUpdateV2(context.Context, sqlplugin.QueueV2Filter) (int64, error) {
	tx.db.t.Error("delete range must be computed outside the metadata transaction")
	return 0, errors.New("last ID read inside metadata transaction")
}

func (tx *queueDeleteTestTx) UpdateQueueV2Metadata(ctx context.Context, row *sqlplugin.QueueV2MetadataRow) (sql.Result, error) {
	tx.db.operations = append(tx.db.operations, "update")
	if tx.db.failUpdate {
		tx.db.failUpdate = false
		return nil, errors.New("injected metadata update failure")
	}
	return tx.Tx.UpdateQueueV2Metadata(ctx, row)
}

func (tx *queueDeleteTestTx) Commit() error {
	tx.db.operations = append(tx.db.operations, "commit")
	return tx.Tx.Commit()
}

func (tx *queueDeleteTestTx) Rollback() error {
	tx.db.operations = append(tx.db.operations, "rollback")
	return tx.Tx.Rollback()
}

func newQueueDeleteTest(t *testing.T) (persistence.QueueV2, *queueDeleteTestDB) {
	t.Helper()
	db, err := persistencesql.NewSQLDB(sqlplugin.DbKindMain, &config.SQL{
		PluginName:        sqlite.PluginName,
		DatabaseName:      primitives.NewUUID().String(),
		ConnectAttributes: map[string]string{"mode": "memory", "cache": "private"},
	}, resolver.NewNoopResolver(), log.NewNoopLogger(), metrics.NoopMetricsHandler)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	wrapped := &queueDeleteTestDB{DB: db, t: t}
	queue := persistencesql.NewQueueV2(wrapped, log.NewNoopLogger(), serialization.NewSerializer())
	_, err = queue.CreateQueue(t.Context(), &persistence.InternalCreateQueueRequest{QueueType: persistence.QueueTypeHistoryDLQ, QueueName: "target"})
	require.NoError(t, err)
	var rows []sqlplugin.QueueV2MessageRow
	for i := range 5 {
		rows = append(rows, sqlplugin.QueueV2MessageRow{QueueType: persistence.QueueTypeHistoryDLQ, QueueName: "target", MessageID: int64(i), MessagePayload: []byte("message"), MessageEncoding: "Proto3"})
	}
	_, err = db.InsertIntoQueueV2Messages(t.Context(), rows)
	require.NoError(t, err)
	return queue, wrapped
}

func queueDeleteRequest(maxID int64) *persistence.InternalRangeDeleteMessagesRequest {
	return &persistence.InternalRangeDeleteMessagesRequest{
		QueueType: persistence.QueueTypeHistoryDLQ, QueueName: "target",
		InclusiveMaxMessageMetadata: persistence.MessageMetadata{ID: maxID},
	}
}

func requireQueueMessages(t *testing.T, queue persistence.QueueV2, ids ...int64) {
	t.Helper()
	response, err := queue.ReadMessages(t.Context(), &persistence.InternalReadMessagesRequest{
		QueueType: persistence.QueueTypeHistoryDLQ, QueueName: "target", PageSize: 10,
	})
	require.NoError(t, err)
	actual := make([]int64, 0, len(response.Messages))
	for _, message := range response.Messages {
		actual = append(actual, message.MetaData.ID)
	}
	require.Equal(t, ids, actual)
}

func requireQueueMinimum(t *testing.T, db sqlplugin.DB, minID int64) {
	t.Helper()
	row, err := db.SelectFromQueueV2Metadata(t.Context(), sqlplugin.QueueV2MetadataFilter{QueueType: persistence.QueueTypeHistoryDLQ, QueueName: "target"})
	require.NoError(t, err)
	var metadata persistencespb.Queue
	require.NoError(t, metadata.Unmarshal(row.MetadataPayload))
	require.Equal(t, minID, metadata.Partitions[0].MinMessageId)
}

func TestQueueV2RangeDeleteOutsideTransaction(t *testing.T) {
	queue, db := newQueueDeleteTest(t)
	response, err := queue.RangeDeleteMessages(t.Context(), queueDeleteRequest(1))
	require.NoError(t, err)
	require.Equal(t, int64(2), response.MessagesDeleted)
	require.Equal(t, []string{"delete", "begin", "update", "commit"}, db.operations)
	requireQueueMessages(t, queue, 2, 3, 4)
	requireQueueMinimum(t, db, 2)
}

func TestQueueV2RangeDeleteMetadataFailureRetry(t *testing.T) {
	queue, db := newQueueDeleteTest(t)
	db.failUpdate = true
	response, err := queue.RangeDeleteMessages(t.Context(), queueDeleteRequest(1))
	require.ErrorContains(t, err, "UpdateQueueV2Metadata operation failed")
	require.Nil(t, response)
	require.Equal(t, []string{"delete", "begin", "update", "rollback"}, db.operations)
	requireQueueMinimum(t, db, 0)
	requireQueueMessages(t, queue, 2, 3, 4)

	response, err = queue.RangeDeleteMessages(t.Context(), queueDeleteRequest(1))
	require.NoError(t, err)
	require.Equal(t, int64(2), response.MessagesDeleted)
	requireQueueMinimum(t, db, 2)
	requireQueueMessages(t, queue, 2, 3, 4)
}

func TestQueueV2RangeDeleteOverlapping(t *testing.T) {
	queue, db := newQueueDeleteTest(t)
	db.afterDelete = func() {
		response, err := queue.RangeDeleteMessages(t.Context(), queueDeleteRequest(3))
		require.NoError(t, err)
		require.Equal(t, int64(4), response.MessagesDeleted)
		requireQueueMinimum(t, db, 4)
	}
	response, err := queue.RangeDeleteMessages(t.Context(), queueDeleteRequest(1))
	require.NoError(t, err)
	require.Equal(t, int64(2), response.MessagesDeleted)
	requireQueueMinimum(t, db, 4)
	requireQueueMessages(t, queue, 4)
	require.Equal(t, []string{"delete", "delete", "begin", "update", "commit", "begin", "update", "commit"}, db.operations)
}
