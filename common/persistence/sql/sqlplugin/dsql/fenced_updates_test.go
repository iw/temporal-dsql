package dsql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

func TestFencedUpdateMethodsExist(t *testing.T) {
	db := createTestDB(t)

	// Test that all fenced update methods exist with correct signatures
	t.Run("UpdateShardsWithFencing_exists", func(t *testing.T) {
		assert.NotNil(t, db.UpdateShardsWithFencing)
	})

	t.Run("UpdateTaskQueuesWithFencing_exists", func(t *testing.T) {
		assert.NotNil(t, db.UpdateTaskQueuesWithFencing)
	})

	// Test that primary update methods still exist (backward compatibility)
	t.Run("UpdateShards_exists", func(t *testing.T) {
		assert.NotNil(t, db.UpdateShards)
	})

	t.Run("UpdateTaskQueues_exists", func(t *testing.T) {
		assert.NotNil(t, db.UpdateTaskQueues)
	})

	t.Run("UpdateNamespace_exists", func(t *testing.T) {
		assert.NotNil(t, db.UpdateNamespace)
	})
}

func TestDummyResult(t *testing.T) {
	result := &dummyResult{rowsAffected: 1}

	lastInsertId, err := result.LastInsertId()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), lastInsertId)

	rowsAffected, err := result.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), rowsAffected)
}

// Helper function to create a test database instance
func createTestDB(t *testing.T) *db {
	return &db{
		dbKind: sqlplugin.DbKindMain,
		dbName: "test",
	}
}
