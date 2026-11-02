package history

import (
	"go.temporal.io/server/common/adaptive/taskrate"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	persistenceclient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/signals"
	"go.temporal.io/server/service/history/queues"
)

// coupleSchedulerRateLimiter couples the task scheduler's rate limiter to persistence,
// through taskrate's controller. It's a decorator, so the queue module's own graph,
// which has no signal plane, keeps the limiter as it is.
func coupleSchedulerRateLimiter(
	limiter queues.SchedulerRateLimiter,
	plane *signals.Plane,
	collection *dynamicconfig.Collection,
	persistenceMaxQPS persistenceclient.PersistenceMaxQps,
	handler metrics.Handler,
) queues.SchedulerRateLimiter {
	return taskrate.NewSchedulerLimiter(
		limiter,
		plane,
		collection,
		func() float64 { return float64(persistenceMaxQPS()) },
		handler,
	)
}
