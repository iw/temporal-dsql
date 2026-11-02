package frontend

import (
	"crypto/tls"

	"go.temporal.io/server/common/adaptive"
	"go.temporal.io/server/common/adaptive/fairshare"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/signals"
	"go.uber.org/fx"
	"google.golang.org/grpc"
)

// The fair-share controller's names for the frontend's namespace rate limits.
const (
	fairShareExecution           = "execution"
	fairShareVisibility          = "visibility"
	fairShareReplicationInducing = "replication_inducing"
)

// FairShareProvider gives the frontend the exchange that splits each namespace's limits
// across the cluster between frontends by their demand. The namespace rate limiters add
// their limits to it. See docs/dsql/adaptive-behaviour.md, Proposal 7.
func FairShareProvider(
	serviceName primitives.ServiceName,
	dc *dynamicconfig.Collection,
	resolver membership.ServiceResolver,
	hostInfo membership.HostInfoProvider,
	tlsProvider encryption.TLSConfigProvider,
	plane *signals.Plane,
	handler metrics.Handler,
	logger log.SnTaggedLogger,
	lc fx.Lifecycle,
) (*fairshare.Exchange, error) {
	// A frontend calls the others as the system worker calls a frontend, and an internal
	// frontend calls the others as the other services call it.
	var tlsConfig *tls.Config
	var err error
	if serviceName == primitives.InternalFrontendService {
		tlsConfig, err = tlsProvider.GetInternodeClientConfig()
	} else {
		tlsConfig, err = tlsProvider.GetFrontendClientConfig()
	}
	if err != nil {
		return nil, err
	}
	exchange := fairshare.NewExchange(fairshare.ExchangeParams{
		Settings: fairshare.Config.Get(dc),
		Frozen:   adaptive.Frozen.Get(dc),
		Self:     hostInfo.HostInfo().GetAddress(),
		Members: func() []string {
			hosts := resolver.AvailableMembers()
			addresses := make([]string, len(hosts))
			for i, host := range hosts {
				addresses[i] = host.GetAddress()
			}
			return addresses
		},
		Dial: func(address string) (*grpc.ClientConn, error) {
			return rpc.Dial(address, tlsConfig, logger, handler)
		},
		TimeSource: clock.NewRealTimeSource(),
		Logger:     logger,
		Handler:    handler,
		Sink:       plane,
	})
	cancel := plane.Subscribe(func(*signals.Snapshot) {}, exchange.Active)
	lc.Append(fx.StartStopHook(exchange.Start, func() {
		cancel()
		exchange.Stop()
	}))
	return exchange, nil
}

// RegisterFairShare adds the exchange's service to the frontend's gRPC server.
func RegisterFairShare(server *grpc.Server, exchange *fairshare.Exchange) {
	exchange.Register(server)
}
