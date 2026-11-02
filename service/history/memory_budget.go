package history

import (
	"go.temporal.io/server/common/adaptive/memory"
	commoncache "go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/signals"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

// budgetCaches sizes history's host-level caches from the memory budget, through
// memory's controllers. Only a cache limited in bytes can take a share of it: the history
// cache while history.cacheSizeBasedLimit is set, and the events cache while it's
// host-level.
func budgetCaches(
	plane *signals.Plane,
	collection *dynamicconfig.Collection,
	config *configs.Config,
	historyCache wcache.Cache,
	eventsCache events.Cache,
	handler metrics.Handler,
) {
	if c, ok := historyCache.(commoncache.Resizable); ok && config.HistoryCacheLimitSizeBased {
		memory.DriveCache(plane, collection, memory.HistoryCache, config.HistoryHostLevelCacheMaxSizeBytes(), c, handler)
	}
	if c, ok := eventsCache.(commoncache.Resizable); ok && config.EnableHostLevelEventsCache() {
		memory.DriveCache(plane, collection, memory.EventsCache, config.EventsHostLevelCacheMaxSizeBytes(), c, handler)
	}
}
