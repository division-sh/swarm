package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

type routeStatementProbe struct {
	calls int
	args  []any
	err   error
}

func (p *routeStatementProbe) ExecContext(_ context.Context, _ string, args ...any) (sql.Result, error) {
	p.calls++
	p.args = append([]any(nil), args...)
	return nil, p.err
}

func (*routeStatementProbe) QueryRowContext(context.Context, string, ...any) *sql.Row {
	panic("route source lookup must share the physical upsert statement")
}

func TestPostgresRouteUpsertUsesOnePhysicalStatement(t *testing.T) {
	route := runtimebus.FlowInstanceRouteRecord{
		Identity:     flowidentity.RunScopedFlowInstance{RunID: "9f02f84b-3fba-42dc-b6a9-b8bd857baf90", Route: flowidentity.DeriveRoute("review", "instance")},
		EventPattern: "review/instance/input", SubscriberType: "node", SubscriberID: "receiver", SourceFlow: "review",
	}
	want := []any{route.EventPattern, route.SubscriberType, route.SubscriberID, route.Identity.RunID, route.Identity.Route.InstancePath, route.SourceFlow}
	for _, failure := range []error{nil, errors.New("physical route write refused")} {
		p := &routeStatementProbe{err: failure}
		err := upsertPostgresFlowInstanceRoute(context.Background(), p, route)
		if !errors.Is(err, failure) || p.calls != 1 || !reflect.DeepEqual(p.args, want) {
			t.Fatalf("calls=%d args=%v err=%v want error=%v", p.calls, p.args, err, failure)
		}
	}
}
