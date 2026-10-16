package bus

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestS1RecipientPreviewAdmitsFrozenRootBeforeSourceConsumers(t *testing.T) {
	source := loadConnectRoutePlanCanonicalSource(t, canonicalrouting.WritePublicTemplateInputRoute(t))
	bundle, found := semanticview.Bundle(source)
	if !found {
		t.Fatal("preview control lacks its compiled bundle")
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		t.Fatal(err)
	}
	foreign, _ := sourceartifactfixture.Rooted(t, "other-root", rooted.Artifact())
	if rooted.Admission().Equal(foreign.Admission()) || rooted.Artifact().BundleHash() != foreign.Artifact().BundleHash() {
		t.Fatal("root conflict control does not preserve equal content")
	}
	fact := sourceartifactfixture.FactFor(rooted.Artifact())
	base := correlation.WithSourceArtifactFact(context.Background(), fact)
	canceled, cancel := context.WithCancel(base)
	cancel()
	for _, api := range []bool{false, true} {
		for _, test := range []struct {
			name     string
			ctx      context.Context
			valid    bool
			canceled bool
		}{
			{name: "plain", ctx: context.Background(), valid: true},
			{name: "content-only-context", ctx: base, valid: true},
			{name: "matching", ctx: correlation.WithRootAdmission(base, rooted.Admission()), valid: true},
			{name: "same-content-other-root", ctx: correlation.WithRootAdmission(base, foreign.Admission())},
			{name: "explicit-clear", ctx: correlation.WithRootAdmission(base, sourceartifact.RootAdmission{})},
			{name: "root-without-content", ctx: correlation.WithRootAdmission(context.Background(), rooted.Admission())},
			{name: "canceled", ctx: canceled, canceled: true},
		} {
			t.Run(fmt.Sprintf("api=%t/%s", api, test.name), func(t *testing.T) {
				selected := newConnectRoutePlanStaticStore()
				payloadCalls := 0
				bus, err := newScopedTestEventBus(selected, EventBusOptions{
					ContractBundle: source, SourceArtifactFact: fact,
					Durable: DurableDependencies{
						RunLifecycle: &publicationRunPreflightTestStore{fault: &runlifecycle.RunNotFoundError{RunID: busInternalTestRunID}},
						Instances:    constructionIndexTestReader{err: errors.New("preview read an uncommitted native instance")},
					},
					TemplateInstancePlanner: newTestFlowInstanceActivationOwner(nil),
					PayloadAdmitter: func(ctx context.Context, event events.Event, flow string) (events.PayloadAdmission, error) {
						payloadCalls++
						root, present, err := correlation.RootAdmissionFromContext(ctx)
						if err != nil || !present || !root.Equal(rooted.Admission()) {
							return events.PayloadAdmission{}, fmt.Errorf("payload consumer has no exact publisher root: present=%t err=%v", present, err)
						}
						if err := ctx.Err(); err != nil {
							return events.PayloadAdmission{}, err
						}
						return eventtest.PayloadAdmission(event, flow, "bootstrap.requested")
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				event := eventtest.RunCreatingRootIngress(eventtest.UUID("rooted-recipient-preview"), "bootstrap.requested", "operator", "", []byte(`{}`), 0, busInternalTestRunID, "", events.EventEnvelope{}, time.Now().UTC())
				var plan PublishRecipientPlan
				if api {
					endpoint, endpointErr := NewRootInputAPIEventPublicationEndpoint(source, "bootstrap.requested")
					if endpointErr != nil {
						t.Fatal(endpointErr)
					}
					plan, err = bus.CheckAPIEventPublishRecipientPlan(test.ctx, event, &endpoint)
				} else {
					plan, err = bus.CheckPublishRecipientPlan(test.ctx, event)
				}
				if test.valid {
					if err != nil || payloadCalls != 1 || len(plan.DeliveryRoutes) != 1 || plan.DeliveryRoutes[0].Target.Route().FlowInstance != busInternalTestRunID {
						t.Fatalf("valid creating preview lost its frozen source: payload_calls=%d plan=%+v err=%v", payloadCalls, plan, err)
					}
				} else if test.canceled {
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("preview concealed caller cancellation: %v", err)
					}
				} else if err == nil || payloadCalls != 0 || len(plan.DeliveryRoutes) != 0 {
					t.Fatalf("invalid root reached source consumers: payload_calls=%d plan=%+v err=%v", payloadCalls, plan, err)
				}
				if len(selected.events) != 0 || len(selected.routes) != 0 {
					t.Fatal("recipient preview persisted construction or delivery")
				}
			})
		}
	}
}
