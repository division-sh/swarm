package storetest

import (
	"context"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"testing"
)

type DeliveryPublicationFixtureStore interface {
	RunFixtureStore
	PipelineObligations() pipelineobligation.Store
	CommitPublication(context.Context, runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error)
	RegisterAuthorActivityEventCatalog(authoractivity.Scope, []authoractivity.EventDescriptor) (*authoractivity.EventCatalogLease, error)
}

// CommitNativeDeliveryPublication uses the original publication owner and exact
// authority. Publication finishes before any claim or process-local dispatch.
func CommitNativeDeliveryPublication(t *testing.T, ctx context.Context, selected DeliveryPublicationFixtureStore, admitted events.AdmittedEvent, routes []events.DeliveryRoute, authority deliverylifecycle.ExecutionAuthority, root *runtimebus.FlowInstanceActivationCommand) {
	t.Helper()
	event := admitted.Event()
	if err := EnsureRunForAdmittedEvent(ctx, selected, admitted, event.CreatedAt()); err != nil {
		t.Fatal(err)
	}
	ledger, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		t.Fatal(err)
	}
	var settlement events.RouteSettlement
	if len(routes) == 0 {
		settlement, err = events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryDeclaredConsumerNoPlan, ledger)
	} else {
		settlement, err = events.NewDeliverySettlement(events.EventWriteNormalPublication, ledger)
	}
	if err != nil {
		t.Fatal(err)
	}
	publication, err := selected.PipelineObligations().ClaimPublication(ctx, event.ID())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := selected.PipelineObligations().Release(context.WithoutCancel(ctx), publication); err != nil {
			t.Error(err)
		}
	}()
	scope := authoractivity.BundleScope(event.ID(), authority.SourceArtifact().BundleHash())
	catalog, err := selected.RegisterAuthorActivityEventCatalog(scope, []authoractivity.EventDescriptor{{EventType: string(event.Type()), Disposition: authoractivity.StoryDifferent}})
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Release()
	command := runtimebus.PublicationCommand{
		Commit:      runtimebus.CommitPublishRequest{Event: admitted, RouteSettlement: settlement, DeliveryRoutes: routes, DeliveryAuthority: authority, ReplayScope: pipelineobligation.ScopeSubscribed, PipelineClaim: publication},
		AuthorScope: scope, HasAuthorScope: true,
		AuthorDescriptor: authoractivity.EventDescriptor{EventType: string(event.Type()), Disposition: authoractivity.StoryDifferent}, HasAuthorDescriptor: true,
	}
	if root != nil {
		command.Activations = []pipeline.FlowInstanceActivationPlan{root.Plan}
		command.RouteTopology = root.RouteTopology
	}
	committed, err := selected.CommitPublication(ctx, command)
	if err != nil || !committed.Acknowledged {
		t.Fatalf("native publication was not acknowledged: %+v,error=%v", committed, err)
	}
}
