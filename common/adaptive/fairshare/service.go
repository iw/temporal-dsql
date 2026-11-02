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
	host, running, _, demand := readReport(request)
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.counting.Load() || e.report == nil {
		return newReport(e.Self, false, nil), nil
	}
	if running && host != "" && host != e.Self {
		e.peers[host] = peerReport{received: e.TimeSource.Now(), demand: demand}
	}
	fields := maps.Clone(e.report.GetFields())
	fields["age"] = structpb.NewNumberValue(e.TimeSource.Now().Sub(e.at).Seconds())
	return &structpb.Struct{Fields: fields}, nil
}

// A report is a frontend's address, whether its controller runs, and the tokens a second
// each namespace asked of each of its limits. A reply also says how long ago its report was
// measured, so a frontend whose rounds stopped goes stale.
const reportVersion = 1

func newReport(host string, running bool, demand map[string]map[string]float64) *structpb.Struct {
	limits := make(map[string]*structpb.Value, len(demand))
	for name, rates := range demand {
		fields := make(map[string]*structpb.Value, len(rates))
		for namespace, rate := range rates {
			fields[namespace] = structpb.NewNumberValue(rate)
		}
		limits[name] = structpb.NewStructValue(&structpb.Struct{Fields: fields})
	}
	return &structpb.Struct{Fields: map[string]*structpb.Value{
		"version": structpb.NewNumberValue(reportVersion),
		"host":    structpb.NewStringValue(host),
		"running": structpb.NewBoolValue(running),
		"demand":  structpb.NewStructValue(&structpb.Struct{Fields: limits}),
	}}
}

func readReport(report *structpb.Struct) (host string, running bool, age time.Duration, demand map[string]map[string]float64) {
	fields := report.GetFields()
	demand = map[string]map[string]float64{}
	for name, rates := range fields["demand"].GetStructValue().GetFields() {
		m := map[string]float64{}
		for namespace, rate := range rates.GetStructValue().GetFields() {
			if r := rate.GetNumberValue(); r > 0 && !math.IsInf(r, 0) {
				m[namespace] = r
			}
		}
		demand[name] = m
	}
	age = max(0, time.Duration(fields["age"].GetNumberValue()*float64(time.Second)))
	return fields["host"].GetStringValue(), fields["running"].GetBoolValue(), age, demand
}
