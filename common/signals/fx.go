package signals

import (
	"os"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.uber.org/fx"
)

// Module provides each service's Plane, and the plane as the services' DecisionSink.
var Module = fx.Options(
	fx.Provide(PlaneProvider),
	fx.Provide(func(p *Plane) adaptive.DecisionSink { return p }),
)

type PlaneParams struct {
	fx.In

	Lifecycle   fx.Lifecycle
	Collection  *dynamicconfig.Collection
	ServiceName primitives.ServiceName
	TimeSource  clock.TimeSource
	Logger      log.Logger
	Persistence persistence.HealthSignalAggregator
	// RPCHealth is history's RPC handler health; other services have none.
	RPCHealth interceptor.HealthSignalAggregator `optional:"true"`
}

func PlaneProvider(params PlaneParams) *Plane {
	var rpcHealth RPCHealth
	if params.RPCHealth != nil {
		rpcHealth = params.RPCHealth
	}
	hostname, _ := os.Hostname()
	plane := newPlane(
		Config.Get(params.Collection),
		params.Persistence,
		rpcHealth,
		newCgroupMemory(os.DirFS("/")),
		params.TimeSource,
		Header{Service: string(params.ServiceName), Host: hostname, PID: os.Getpid()},
		params.Logger,
	)
	params.Lifecycle.Append(fx.StartStopHook(plane.Start, plane.Stop))
	return plane
}
