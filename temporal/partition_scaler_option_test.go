package temporal_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/matching"
	"go.temporal.io/server/temporal"
)

// recordingPartitionScalerFactory records the task queues matching asks it to scale, and
// leaves them unscaled.
type recordingPartitionScalerFactory struct {
	mu         sync.Mutex
	taskQueues []string
}

func (f *recordingPartitionScalerFactory) New(_ namespace.Name, taskQueue string, _ enumspb.TaskQueueType) matching.PartitionScaler {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.taskQueues = append(f.taskQueues, taskQueue)
	return nil
}

func TestNewServerWithPartitionScalerFactory(t *testing.T) {
	factory := &recordingPartitionScalerFactory{}
	runAndTestServer(t, temporal.WithPartitionScalerFactory(factory))

	factory.mu.Lock()
	defer factory.mu.Unlock()
	require.Contains(t, factory.taskQueues, "test-task-queue")
}
