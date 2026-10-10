package bus

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

type unscopedConnectIndexTestReader struct {
	constructionIndexTestReader
	requested []pipeline.FlowInstanceLookupScope
}

func TestReplyOriginLookupPreservesPreparedConstructionAndIndependentFailures(t *testing.T) {
	source := connectRoutePlanCarriedKeyResolutionSource(t, contracts.FlowInputResolutionModeSelect)
	ctx := constructionIndexContext(t, source)
	fact, _ := correlation.SourceArtifactFactFromContext(ctx)
	event := admitRunProposalEvent(t, true).Event()
	root := ConstructedFlowInstanceIdentityFixture(source, ".", "", event.RunID())
	child, err := flowidentity.KeylessChild(source, root, "producer")
	if err != nil {
		t.Fatal(err)
	}
	planner := newTestFlowInstanceActivationOwner(nil)
	var plans []pipeline.FlowInstanceActivationPlan
	for _, instance := range []flowidentity.Instance{root, child} {
		plan, err := planner.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{
			ContractBundle: source, Instance: instance, TriggerEvent: event, OccurredAt: event.CreatedAt(),
		})
		if err != nil {
			t.Fatal(err)
		}
		plans = append(plans, plan)
	}
	request, err := pipeline.NewExactFlowInstanceLookup(source, fact, flowidentity.RunScopedFlowInstance{RunID: event.RunID(), Route: child.Route()})
	if err != nil {
		t.Fatal(err)
	}
	storeFailure := errors.New("independent reply-origin read failure")
	resolver := connectRoutePlanResolver{source: source, lifecycle: connectInstanceSelector{index: constructionIndexTestReader{err: storeFailure}}}
	previewCtx := withConnectRoutePlanPreview(ctx)
	previewCtx.Value(connectRoutePlanPreviewRoutesKey{}).(*connectRoutePlanPreviewRoutes).plans = plans
	instance, found, err := resolver.replyOriginWorkflowInstance(previewCtx, request)
	if err != nil || !found || instance.StorageRef != child.InstancePath || instance.EntityID != plans[1].Instance.EntityID {
		t.Fatalf("prepared reply origin became native absence/read authority: %+v found=%t err=%v", instance, found, err)
	}
	for _, failure := range []error{context.Canceled, storeFailure, errors.Join(context.Canceled, storeFailure)} {
		resolver.lifecycle.index = constructionIndexTestReader{err: failure}
		if _, found, err := resolver.replyOriginWorkflowInstance(ctx, request); found || !errors.Is(err, failure) {
			t.Fatalf("reply read failure became a stale origin: found=%t err=%v want=%v", found, err, failure)
		}
	}
}

func (r *unscopedConnectIndexTestReader) ListFlowInstances(ctx context.Context, scope pipeline.FlowInstanceLookupScope) ([]pipeline.FlowInstanceObservation, error) {
	r.requested = append(r.requested, scope)
	return slices.Clone(r.observations), r.err
}

func TestConnectLookupScopeRejectsForeignInventoryAndIndependentErrors(t *testing.T) {
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.CopyExample(t, canonicalrouting.TemplateSelectExisting))
	table, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	routing, err := events.NewExternalIngressRoutingSource("account", events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.RunCreatingRootIngressWithRoutingSource(eventtest.UUID("lookup-event"), "account.ready", "provider", "", []byte(`{"account_id":"42"}`), 0,
		busInternalTestRunID, "", events.EventEnvelope{}, routing, time.Now().UTC())
	ctx := constructionIndexContext(t, source)
	for _, test := range []struct {
		name, runID, flowID, id, key string
	}{
		{"foreign run", eventtest.UUID("other-run"), "account", "one", "42"},
		{"foreign declaration", busInternalTestRunID, "producer", "", ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			instance := ConstructedFlowInstanceIdentityFixture(source, test.flowID, test.id, test.runID)
			reader := &unscopedConnectIndexTestReader{constructionIndexTestReader: constructionIndexTestReader{observations: []pipeline.FlowInstanceObservation{
				constructionIndexObservation(t, source, test.runID, instance, test.key),
			}}}
			resolver := newConnectRoutePlanResolver(source, table, nil, reader, nil)
			if scope, bounded, err := resolver.selectedTargetScope(ctx, event); err == nil || bounded || len(scope.instancePaths) != 0 {
				t.Fatalf("foreign inventory admitted or became an unbounded fallback: scope=%+v bounded=%v err=%v", scope, bounded, err)
			}
			if len(reader.requested) != 1 || !slices.Equal(reader.requested[0].FlowIDs(), []string{"account"}) || len(reader.requested[0].Coordinates()) != 0 {
				t.Fatalf("lookup escaped its compiled declaration set: %+v", reader.requested)
			}
		})
	}
	storeFailure := errors.New("independent store failure")
	for _, failure := range []error{context.Canceled, storeFailure, errors.Join(context.Canceled, storeFailure)} {
		reader := &unscopedConnectIndexTestReader{constructionIndexTestReader: constructionIndexTestReader{err: failure}}
		resolver := newConnectRoutePlanResolver(source, table, nil, reader, nil)
		if _, bounded, err := resolver.selectedTargetScope(ctx, event); bounded || !errors.Is(err, failure) {
			t.Fatalf("inventory failure lost at planning boundary: bounded=%v err=%v want=%v", bounded, err, failure)
		}
	}
}
