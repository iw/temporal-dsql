package fairshare

import (
	"context"
	"maps"
	"math"
	"strings"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/api"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// The exchange's gRPC service has no proto definition: requests and replies are reports in
// a structpb.Struct.
var exchangeMethod = api.FairShareServicePrefix + "Exchange"

// exchangeServer is what the service's handler type asks of a server.
type exchangeServer interface {
	serve(context.Context, *structpb.Struct) (*structpb.Struct, error)
}

// Register adds the exchange's service to a frontend's gRPC server, before it serves.
func (e *Exchange) Register(server *grpc.Server) {
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: strings.Trim(api.FairShareServicePrefix, "/"),
		HandlerType: (*exchangeServer)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Exchange",
			Handler: func(_ any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
				request := &structpb.Struct{}
				if err := dec(request); err != nil {
					return nil, err
				}
				if interceptor == nil {
					return e.serve(ctx, request)
				}
				info := &grpc.UnaryServerInfo{Server: e, FullMethod: exchangeMethod}
				return interceptor(ctx, request, info, func(ctx context.Context, req any) (any, error) {
					request, ok := req.(*structpb.Struct)
					if !ok {
						return nil, serviceerror.NewInvalidArgument("fair share exchange expects a report")
					}
					return e.serve(ctx, request)
				})
			},
		}},
		Metadata: "common/adaptive/fairshare/service.go",
	}, e)
}

// serve answers another frontend's exchange: it keeps the other's report and replies with
// this frontend's latest one, and how long ago it was measured.
func (e *Exchange) serve(_ context.Context, request *structpb.Struct) (*structpb.Struct, error) {
	host, running, _, peer := readReport(request)
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.counting.Load() || e.report == nil {
		return newReport(e.Self, false, nil, nil), nil
	}
	if running && host != "" && host != e.Self {
		peer.received = e.TimeSource.Now()
		e.peers[host] = peer
	}
	fields := maps.Clone(e.report.GetFields())
	fields["age"] = structpb.NewNumberValue(e.TimeSource.Now().Sub(e.at).Seconds())
	return &structpb.Struct{Fields: fields}, nil
}

// A report is a frontend's address, whether its controller runs, the tokens a second each
// namespace asked of each of its limits, and, by limit, the namespaces whose window still
// fills. A reply also says how long ago its report was measured, so a frontend whose rounds
// stopped goes stale. A report without the namespaces whose window fills, from a frontend
// before they were added, has none.
const reportVersion = 1

func newReport(host string, running bool, demand map[string]map[string]float64, filling map[string]map[string]bool) *structpb.Struct {
	return &structpb.Struct{Fields: map[string]*structpb.Value{
		"version": structpb.NewNumberValue(reportVersion),
		"host":    structpb.NewStringValue(host),
		"running": structpb.NewBoolValue(running),
		"demand":  byLimit(demand, structpb.NewNumberValue),
		"filling": byLimit(filling, structpb.NewBoolValue),
	}}
}

// byLimit is a value for each namespace of each limit, as a report carries it.
func byLimit[V any](values map[string]map[string]V, value func(V) *structpb.Value) *structpb.Value {
	limits := make(map[string]*structpb.Value, len(values))
	for name, namespaces := range values {
		fields := make(map[string]*structpb.Value, len(namespaces))
		for namespace, v := range namespaces {
			fields[namespace] = value(v)
		}
		limits[name] = structpb.NewStructValue(&structpb.Struct{Fields: fields})
	}
	return structpb.NewStructValue(&structpb.Struct{Fields: limits})
}

// readReport reads a report. The peerReport it returns has the report's demand and the
// namespaces whose window fills; the caller sets when it was received.
func readReport(report *structpb.Struct) (host string, running bool, age time.Duration, peer peerReport) {
	fields := report.GetFields()
	peer.demand = map[string]map[string]float64{}
	for name, rates := range fields["demand"].GetStructValue().GetFields() {
		m := map[string]float64{}
		for namespace, rate := range rates.GetStructValue().GetFields() {
			if r := rate.GetNumberValue(); r > 0 && !math.IsInf(r, 0) {
				m[namespace] = r
			}
		}
		peer.demand[name] = m
	}
	peer.filling = map[string]map[string]bool{}
	for name, namespaces := range fields["filling"].GetStructValue().GetFields() {
		m := map[string]bool{}
		for namespace, filling := range namespaces.GetStructValue().GetFields() {
			if filling.GetBoolValue() {
				m[namespace] = true
			}
		}
		peer.filling[name] = m
	}
	age = max(0, time.Duration(fields["age"].GetNumberValue()*float64(time.Second)))
	return fields["host"].GetStringValue(), fields["running"].GetBoolValue(), age, peer
}
