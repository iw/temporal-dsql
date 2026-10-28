//go:build integration

package dsql_test

import (
	"context"
	"database/sql"
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
)

func TestDSQLQueuesIntegration(t *testing.T) {
	db, cfg := newBehaviorIntegrationDB(t)
	conn, ok := db.(sqlplugin.Conn)
	require.True(t, ok)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	const messageCount = 3500
	blob := persistence.NewDataBlob([]byte("queue-test-"+primitives.NewUUID().String()), "Proto3")

	t.Run("v1_dlq", func(t *testing.T) {
		// V1 has no queue names. Reserve both metadata keys before registering cleanup.
		queueType := persistence.QueueType(1_000_000_001 + rand.Int32N(999_999_999))
		tx, err := db.BeginTx(ctx)
		require.NoError(t, err)
		defer func() {
			if err := tx.Rollback(); !errors.Is(err, sql.ErrTxDone) {
				require.NoError(t, err)
			}
		}()
		for _, key := range []persistence.QueueType{queueType, -queueType} {
			_, err := tx.InsertIntoQueueMetadata(ctx, &sqlplugin.QueueMetadataRow{
				QueueType: key, Data: blob.Data, DataEncoding: blob.EncodingType.String(),
			})
			require.NoError(t, err)
		}
		require.NoError(t, tx.Commit())
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
			defer cancel()
			for _, key := range []persistence.QueueType{queueType, -queueType} {
				_, err := db.RangeDeleteFromMessages(ctx, sqlplugin.QueueMessagesRangeFilter{
					QueueType: key, MinMessageID: persistence.EmptyQueueMessageID, MaxMessageID: persistence.MaxQueueMessageID,
				})
				require.NoError(t, err)
				_, err = conn.ExecContext(ctx, "DELETE FROM queue_metadata WHERE queue_type = $1", key)
				require.NoError(t, err)
			}
		})
		factory := persistencesql.NewFactory(cfg, resolver.NewNoopResolver(), "test",
			log.NewTestLogger(), metrics.NoopMetricsHandler, serialization.NewSerializer())
		t.Cleanup(factory.Close)
		queue, err := factory.NewQueue(queueType)
		require.NoError(t, err)
		t.Cleanup(queue.Close)
		require.NoError(t, queue.Init(ctx, blob))
		for start := 0; start < messageCount; start += 1000 {
			var rows []sqlplugin.QueueMessageRow
			for i := start; i < min(start+1000, messageCount); i++ {
				rows = append(rows, sqlplugin.QueueMessageRow{QueueType: -queueType, MessageID: int64(i), MessagePayload: blob.Data, MessageEncoding: blob.EncodingType.String()})
			}
			result, err := db.InsertIntoMessages(ctx, rows)
			require.NoError(t, err)
			inserted, err := result.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, int64(len(rows)), inserted)
		}
		messages, token, err := queue.ReadMessagesFromDLQ(ctx, persistence.EmptyQueueMessageID, persistence.MaxQueueMessageID, messageCount+1, nil)
		require.NoError(t, err)
		require.Empty(t, token)
		require.Len(t, messages, messageCount)
		for i, message := range messages {
			require.Equal(t, int64(i), message.ID)
			require.Equal(t, -queueType, message.QueueType)
			require.Equal(t, blob.Data, message.Data)
			require.Equal(t, blob.EncodingType.String(), message.Encoding)
		}
		require.NoError(t, queue.RangeDeleteMessagesFromDLQ(ctx, persistence.EmptyQueueMessageID, messages[messageCount-2].ID))
		remaining, _, err := queue.ReadMessagesFromDLQ(ctx, persistence.EmptyQueueMessageID, persistence.MaxQueueMessageID, messageCount+1, nil)
		require.NoError(t, err)
		require.Equal(t, messages[messageCount-1:], remaining)
		id, err := queue.EnqueueMessageToDLQ(ctx, blob)
		require.NoError(t, err)
		require.Equal(t, int64(messageCount), id)
	})

	t.Run("v2", func(t *testing.T) {
		queue := persistencesql.NewQueueV2(db, log.NewTestLogger(), serialization.NewSerializer())
		queueType := persistence.QueueTypeHistoryDLQ
		queueName := "dsql-queue-test-" + primitives.NewUUID().String()
		_, err := queue.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{QueueType: queueType, QueueName: queueName})
		require.NoError(t, err)
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
			defer cancel()
			_, err := db.RangeDeleteFromQueueV2Messages(ctx, sqlplugin.QueueV2MessagesFilter{
				QueueType: queueType, QueueName: queueName, MinMessageID: persistence.FirstQueueMessageID, MaxMessageID: persistence.MaxQueueMessageID,
			})
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "DELETE FROM queues WHERE queue_type = $1 AND queue_name = $2", queueType, queueName)
			require.NoError(t, err)
		})
		for start := 0; start < messageCount; start += 1000 {
			var rows []sqlplugin.QueueV2MessageRow
			for i := start; i < min(start+1000, messageCount); i++ {
				rows = append(rows, sqlplugin.QueueV2MessageRow{QueueType: queueType, QueueName: queueName, MessageID: int64(i), MessagePayload: blob.Data, MessageEncoding: blob.EncodingType.String()})
			}
			result, err := db.InsertIntoQueueV2Messages(ctx, rows)
			require.NoError(t, err)
			inserted, err := result.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, int64(len(rows)), inserted)
		}
		read := &persistence.InternalReadMessagesRequest{QueueType: queueType, QueueName: queueName, PageSize: messageCount + 1}
		response, err := queue.ReadMessages(ctx, read)
		require.NoError(t, err)
		require.Len(t, response.Messages, messageCount)
		for i, message := range response.Messages {
			require.Equal(t, int64(i), message.MetaData.ID)
			require.Equal(t, blob.Data, message.Data.Data)
			require.Equal(t, blob.EncodingType, message.Data.EncodingType)
		}
		deleted, err := queue.RangeDeleteMessages(ctx, &persistence.InternalRangeDeleteMessagesRequest{
			QueueType: queueType, QueueName: queueName, InclusiveMaxMessageMetadata: response.Messages[messageCount-1].MetaData,
		})
		require.NoError(t, err)
		require.Equal(t, int64(messageCount), deleted.MessagesDeleted)
		response, err = queue.ReadMessages(ctx, read)
		require.NoError(t, err)
		require.Empty(t, response.Messages)
		// Core retains the last physical row while metadata hides the deleted range.
		physical, err := db.RangeSelectFromQueueV2Messages(ctx, sqlplugin.QueueV2MessagesFilter{
			QueueType: queueType, QueueName: queueName, MinMessageID: 0, PageSize: messageCount + 1,
		})
		require.NoError(t, err)
		require.Len(t, physical, 1)
		require.Equal(t, int64(messageCount-1), physical[0].MessageID)
		enqueued, err := queue.EnqueueMessage(ctx, &persistence.InternalEnqueueMessageRequest{QueueType: queueType, QueueName: queueName, Blob: blob})
		require.NoError(t, err)
		require.Equal(t, int64(messageCount), enqueued.Metadata.ID)
	})
}
