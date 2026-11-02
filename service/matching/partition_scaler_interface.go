//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination partition_scaler_mock.go

package matching

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/protobuf/types/known/anypb"
)

// PartitionScalerFactory is a pluggable interface to control partition scaling.
type PartitionScalerFactory interface {
	// New will be called for a new root partition. It should return a new PartitionScaler
	// (or nil to disable).
	New(nsName namespace.Name, tqName string, tqType enumspb.TaskQueueType) PartitionScaler
}

// PartitionScaler is an instance of a scaler for one task queue.
type PartitionScaler interface {
	// OnTasks will be called once per batch of tasks added, either sync match or async. The
	// NumTasks count may be estimated based on assumptions of load balancing across
	// partitions.
	//
	// It will be given the current partition count target, current backlog counts, and its
	// private state (optional). It returns a PartitionScalerDecision, which will usually be
	// "no change". To make a change, it should return a number in NewTarget. Zero means
	// disable managed scaling and requires BacklogCap to also be zero.
	//
	// Changes may be ignored if values are out of range, if changed too often, or other
	// reasons. Private state will not be updated if target doesn't also change, or if the
	// change is ignored due to rate limit or any other reason.
	//
	// It will also be called periodically with less than a full batch, or even zero, to allow
	// timely scale down when there are no/few tasks.
	OnTasks(PartitionScalerInput) PartitionScalerDecision
	// Stop will be called when unloading the partition.
	Stop()
}

// PartitionStatsScaler is a PartitionScaler that reads the task queue's stats. While
// WantsStats returns true, the scale manager describes the task queue's partitions with
// their stats every BackgroundInterval, and passes the latest to OnTasks.
type PartitionStatsScaler interface {
	PartitionScaler
	WantsStats() bool
}

type PartitionScalerInput struct {
	NumTasks      int // estimated tasks added since last call
	CurrentTarget int
	BacklogCounts []byte
	PrivateState  *anypb.Any
	// ReadPartitions is how many partitions tasks are read from while the scale state
	// holds them: while managed scaling is on, or partitions it added still drain.
	// Otherwise it's 0.
	ReadPartitions int
	// Stats is the task queue's latest stats, for a PartitionStatsScaler that wants them,
	// or nil.
	Stats *PartitionScalerStats
}

// PartitionScalerStats are a task queue's rates and backlog, summed over the physical
// queues of the partitions that answered.
type PartitionScalerStats struct {
	// Time is when the partitions were described.
	Time time.Time
	// WritePartitions is how many partitions tasks are added to, and Described how many
	// partitions answered.
	WritePartitions, Described int
	// AddRate and DispatchRate are tasks a second, over the partitions' last 30 seconds.
	AddRate, DispatchRate float32
	BacklogCount          int64
	BacklogAge            time.Duration // of the oldest backlog
	// RateLimited is set when a partition's dispatch is rate limited.
	RateLimited bool
}

type PartitionScalerDecision struct {
	NoChange     bool       // if true, don't do anything
	NewTarget    int        // if zero, disable managed scaling, otherwise set partition target
	BacklogCap   int        // target cap for backlog count; must be zero when NewTarget is zero
	PrivateState *anypb.Any // optional private state to be stored with target
}
