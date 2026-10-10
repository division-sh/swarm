package bus

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

type unscopedConnectIndexTestReader struct {
	constructionIndexTestReader
	requested []pipeline.FlowInstanceLookupScope
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
