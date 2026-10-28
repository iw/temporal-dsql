package dsql_test

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	persistencesql "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/dsql"
	"go.temporal.io/server/common/resolver"
)

const (
	queueTestLastIDQuery   = "SELECT message_id FROM queue WHERE queue_type=$1 ORDER BY message_id DESC LIMIT 1 FOR UPDATE"
	queueTestLastIDQueryV2 = "SELECT message_id FROM queue_messages WHERE queue_type=$1 AND queue_name=$2 AND queue_partition=$3 ORDER BY message_id DESC LIMIT 1 FOR UPDATE"
)

func TestQueueLastMessageID(t *testing.T) {
	t.Run("v1", func(t *testing.T) {
		db, fake := newUUIDTestDB(t)
		fake.queueMessages = []sqlplugin.QueueMessageRow{
			{QueueType: -7, MessageID: 8},
			{QueueType: -8, MessageID: 99},
			{QueueType: -7, MessageID: 4},
		}
		id, err := db.GetLastEnqueuedMessageIDForUpdate(t.Context(), -7)
		require.NoError(t, err)
		require.Equal(t, int64(8), id)
		statements, _, _ := fake.snapshot()
		require.Equal(t, []string{queueTestLastIDQuery}, statements)
		require.Equal(t, []driver.Value{int64(-7)}, fake.statementArgs(0))
		_, err = db.GetLastEnqueuedMessageIDForUpdate(t.Context(), -9)
		require.ErrorIs(t, err, sql.ErrNoRows)
	})
	t.Run("v2", func(t *testing.T) {
		db, fake := newUUIDTestDB(t)
		fake.queueV2Messages = []sqlplugin.QueueV2MessageRow{
			{QueueType: 2, QueueName: "target", QueuePartition: 1, MessageID: 8},
			{QueueType: 3, QueueName: "target", QueuePartition: 1, MessageID: 99},
			{QueueType: 2, QueueName: "other", QueuePartition: 1, MessageID: 100},
			{QueueType: 2, QueueName: "target", QueuePartition: 2, MessageID: 101},
			{QueueType: 2, QueueName: "target", QueuePartition: 1, MessageID: 4},
		}
		filter := sqlplugin.QueueV2Filter{QueueType: 2, QueueName: "target", Partition: 1}
		id, err := db.GetLastEnqueuedMessageIDForUpdateV2(t.Context(), filter)
		require.NoError(t, err)
		require.Equal(t, int64(8), id)
		statements, _, _ := fake.snapshot()
		require.Equal(t, []string{queueTestLastIDQueryV2}, statements)
		require.Equal(t, []driver.Value{int64(2), "target", int64(1)}, fake.statementArgs(0))
		filter.QueueName = "empty"
		_, err = db.GetLastEnqueuedMessageIDForUpdateV2(t.Context(), filter)
		require.ErrorIs(t, err, sql.ErrNoRows)
	})
}

func TestQueueConsecutiveEnqueues(t *testing.T) {
	t.Run("v1_dlq", func(t *testing.T) {
		fakeDB, fake := newShardTestDB(t, 0)
		factory := persistencesql.NewFactory(config.SQL{
			PluginName: dsql.PluginName, DatabaseName: "queue_test",
			Connect: func(*config.SQL) (*sqlx.DB, error) { return fakeDB, nil },
		}, resolver.NewNoopResolver(), "test", log.NewNoopLogger(), metrics.NoopMetricsHandler, serialization.NewSerializer())
		t.Cleanup(factory.Close)
		queue, err := factory.NewQueue(7)
		require.NoError(t, err)
		t.Cleanup(queue.Close)
		fake.queueMessages = []sqlplugin.QueueMessageRow{{QueueType: -8, MessageID: 99}}
		for range 3 {
			_, err := queue.EnqueueMessageToDLQ(t.Context(), persistence.NewDataBlob([]byte("message"), "Proto3"))
			require.NoError(t, err)
		}
		statements, commits, _ := fake.snapshot()
		require.Equal(t, 3, commits)
		require.Len(t, statements, 6)
		for i := range 3 {
			require.Equal(t, queueTestLastIDQuery, statements[2*i])
			require.Equal(t, []driver.Value{int64(-7), int64(i), []byte("message"), "Proto3"}, fake.statementArgs(2*i+1))
		}
	})
	t.Run("v2", func(t *testing.T) {
		db, fake := newUUIDTestDB(t)
		queue := persistencesql.NewQueueV2(db, log.NewNoopLogger(), serialization.NewSerializer())
		_, err := queue.CreateQueue(t.Context(), &persistence.InternalCreateQueueRequest{QueueType: persistence.QueueTypeHistoryDLQ, QueueName: "target"})
		require.NoError(t, err)
		fake.queueV2Messages = []sqlplugin.QueueV2MessageRow{{QueueType: persistence.QueueTypeHistoryDLQ, QueueName: "other", MessageID: 99}}
		for i := range 3 {
			response, err := queue.EnqueueMessage(t.Context(), &persistence.InternalEnqueueMessageRequest{
				QueueType: persistence.QueueTypeHistoryDLQ, QueueName: "target",
				Blob: persistence.NewDataBlob([]byte("message"), "Proto3"),
			})
			require.NoError(t, err)
			require.Equal(t, int64(i), response.Metadata.ID)
		}
		statements, commits, _ := fake.snapshot()
		require.Equal(t, 3, commits)
		require.Len(t, statements, 10)
		for i := range 3 {
			require.Equal(t, queueTestLastIDQueryV2, statements[3*i+2])
			require.Equal(t, []driver.Value{int64(2), "target", int64(0), int64(i), []byte("message"), "Proto3"}, fake.statementArgs(3*i+3))
		}
	})
}

// These fixtures model successful sequential transactions, not DSQL's OCC engine.
func (d *shardTestDriver) queryQueueRows(query string, args []driver.NamedValue) (driver.Rows, bool) {
	switch query {
	case queueTestLastIDQuery, queueTestLastIDQueryV2:
		rows := &uuidTestRows{columns: []string{"message_id"}}
		var lastID int64
		found := false
		include := func(id int64) {
			if !found || id > lastID {
				lastID, found = id, true
			}
		}
		if query == queueTestLastIDQuery {
			for _, row := range d.queueMessages {
				if int64(row.QueueType) == args[0].Value {
					include(row.MessageID)
				}
			}
		} else {
			for _, row := range d.queueV2Messages {
				if int64(row.QueueType) == args[0].Value && row.QueueName == args[1].Value && row.QueuePartition == args[2].Value {
					include(row.MessageID)
				}
			}
		}
		if found {
			rows.values = [][]driver.Value{{lastID}}
		}
		return rows, true
	case "SELECT metadata_payload, metadata_encoding from queues WHERE queue_type=$1 and queue_name=$2",
		"SELECT metadata_payload, metadata_encoding from queues WHERE queue_type=$1 and queue_name=$2 FOR UPDATE":
		rows := &uuidTestRows{columns: []string{"metadata_payload", "metadata_encoding"}}
		for _, row := range d.queueV2Metadata {
			if int64(row.QueueType) == args[0].Value && row.QueueName == args[1].Value {
				rows.values = append(rows.values, []driver.Value{row.MetadataPayload, row.MetadataEncoding})
			}
		}
		return rows, true
	default:
		return nil, false
	}
}

func (d *shardTestDriver) insertQueueRows(query string, args []driver.NamedValue) (driver.Result, bool) {
	switch {
	case strings.HasPrefix(query, "INSERT INTO queue "):
		d.queueMessages = append(d.queueMessages, sqlplugin.QueueMessageRow{
			QueueType: persistence.QueueType(args[0].Value.(int64)), MessageID: args[1].Value.(int64),
			MessagePayload: args[2].Value.([]byte), MessageEncoding: args[3].Value.(string),
		})
	case strings.HasPrefix(query, "INSERT INTO queue_messages "):
		d.queueV2Messages = append(d.queueV2Messages, sqlplugin.QueueV2MessageRow{
			QueueType: persistence.QueueV2Type(args[0].Value.(int64)), QueueName: args[1].Value.(string),
			QueuePartition: args[2].Value.(int64), MessageID: args[3].Value.(int64),
			MessagePayload: args[4].Value.([]byte), MessageEncoding: args[5].Value.(string),
		})
	case strings.HasPrefix(query, "INSERT INTO queues "):
		d.queueV2Metadata = append(d.queueV2Metadata, sqlplugin.QueueV2MetadataRow{
			QueueType: persistence.QueueV2Type(args[0].Value.(int64)), QueueName: args[1].Value.(string),
			MetadataPayload: args[2].Value.([]byte), MetadataEncoding: args[3].Value.(string),
		})
	default:
		return nil, false
	}
	return driver.RowsAffected(1), true
}

func TestQueueRangeDeleteBatches(t *testing.T) {
	for _, rows := range []int64{0, 1000, 2500, 3500} {
		for _, v2 := range []bool{false, true} {
			t.Run(fmt.Sprintf("v2=%t/rows=%d", v2, rows), func(t *testing.T) {
				db, fake := newUUIDTestDB(t)
				table := "queue"
				query := "DELETE FROM queue WHERE (queue_type, message_id) IN ( SELECT queue_type, message_id FROM queue WHERE queue_type = $1 AND message_id > $2 AND message_id <= $3 ORDER BY queue_type, message_id LIMIT $4)"
				args := []driver.Value{int64(-7), int64(10), int64(4000), int64(1000)}
				if v2 {
					table = "queue_messages"
					query = "DELETE FROM queue_messages WHERE (queue_type, queue_name, queue_partition, message_id) IN ( SELECT queue_type, queue_name, queue_partition, message_id FROM queue_messages WHERE queue_type = $1 AND queue_name = $2 AND queue_partition = $3 AND message_id >= $4 AND message_id <= $5 ORDER BY queue_type, queue_name, queue_partition, message_id LIMIT $6)"
					args = []driver.Value{int64(2), "target", int64(1), int64(10), int64(4000), int64(1000)}
				}
				fake.tables = map[string]*quotaTestTable{table: {rows: rows, rowBytes: 10}}
				var result sql.Result
				var err error
				if v2 {
					result, err = db.RangeDeleteFromQueueV2Messages(t.Context(), sqlplugin.QueueV2MessagesFilter{
						QueueType: 2, QueueName: "target", Partition: 1, MinMessageID: 10, MaxMessageID: 4000,
					})
				} else {
					result, err = db.RangeDeleteFromMessages(t.Context(), sqlplugin.QueueMessagesRangeFilter{
						QueueType: -7, MinMessageID: 10, MaxMessageID: 4000,
					})
				}
				require.NoError(t, err)
				total, err := result.RowsAffected()
				require.NoError(t, err)
				require.Equal(t, rows, total)
				statements, commits, rollbacks := fake.snapshot()
				require.Len(t, statements, int(rows/1000)+1)
				for i, statement := range statements {
					require.Equal(t, query, statement)
					require.Equal(t, args, fake.statementArgs(i))
				}
				require.Zero(t, fake.beginCount())
				require.Zero(t, commits)
				require.Zero(t, rollbacks)
				require.Zero(t, fake.tables[table].rows)
			})
		}
	}
}
