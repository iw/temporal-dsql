package persistence

// StoreFact is a typed fact a persistence plugin reports about what its store did, so
// core can adapt without inferring the store's health from latency or generic errors.
// See docs/dsql/adaptive-store-facts.md.
type StoreFact int

const (
	// StoreFactThrottled means the store, or the plugin's pacing for it, held work back
	// for capacity.
	StoreFactThrottled StoreFact = iota
	// StoreFactContention means conflicting writes used up the plugin's retries.
	StoreFactContention
	// StoreFactLimit means a request exceeded a fixed store limit, which load doesn't change.
	StoreFactLimit
	// StoreFactQueued means a caller waited for one of the plugin's connections.
	StoreFactQueued

	numStoreFacts
)

type (
	// StoreFacts receives store facts. The health signal aggregator implements it.
	StoreFacts interface {
		// DeclareStoreFacts is called once by a plugin that reports facts, so core relies
		// on them instead of inferring the store's health from latency.
		DeclareStoreFacts()
		ReportStoreFact(fact StoreFact, count int64)
	}

	// StoreFactsReceiver is implemented by the data store factories and plugin databases
	// that pass a StoreFacts sink on to where facts happen.
	StoreFactsReceiver interface {
		SetStoreFacts(StoreFacts)
	}
)

func (f StoreFact) String() string {
	switch f {
	case StoreFactThrottled:
		return "throttled"
	case StoreFactContention:
		return "contention"
	case StoreFactLimit:
		return "limit"
	case StoreFactQueued:
		return "queued"
	default:
		return "unknown"
	}
}
