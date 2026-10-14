package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestReceiverHistoricalMaterializationRefusesCrossedEvidenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, request := newR7SelectionProof(t, backend)
			selected, err := pipeline.PrepareFlowInstanceSelection(f.ctx, f.store.(pipeline.FlowInstanceIndexReader), f.manager, request)
			if err != nil || selected.Activation == nil {
				t.Fatalf("prepare source receiver: %+v %v", selected, err)
			}
			if result, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, *selected.Activation); err != nil || !result.Acknowledged {
				t.Fatalf("construct source receiver: %+v %v", result, err)
			}
			fork := materializeReceiverConstructionProofFork(t, f, request.Lookup.RunID())
			owner := flowidentity.RunScopedFlowInstance{RunID: fork.ForkRunID, Route: selected.Identity().Route()}
			lookup, err := pipeline.NewExactFlowInstanceLookup(request.Lookup.Source(), request.Lookup.SourceFact(), owner)
			if err != nil {
				t.Fatal(err)
			}
			observed, found, err := f.store.(pipeline.FlowInstanceIndexReader).LookupFlowInstance(f.ctx, lookup)
			if err != nil || !found {
				t.Fatalf("observe captured receiver: found=%t %v", found, err)
			}
			fixed, projected := observed.HistoricalConstruction()
			if !projected || fixed.SourceOwner.RunID != request.Lookup.RunID() || fixed.SourceRevision != fork.ForkPoint.Revision {
				t.Fatal("fixture lacks positive fixed-cut construction")
			}
			agentRoute, err := observed.Identity().Route().AgentIdentityRoute()
			if err != nil {
				t.Fatal(err)
			}
			name, err := agentidentity.RuntimeName("worker", "historical-claim-proof")
			if err != nil {
				t.Fatal(err)
			}
			identity, err := agentidentity.New(fork.ForkRunID, name, agentRoute)
			if err != nil {
				t.Fatal(err)
			}
			target := events.RouteIdentity{FlowID: fixed.Identity.TemplateID, FlowInstance: fixed.Identity.InstancePath, EntityID: fixed.Identity.EntityID}
			actor := target
			actor.EntityID = ""
			base := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity, Target: events.MustEntitylessReceiverTarget(actor)}
			for _, variant := range []string{"exact", "source_run", "revision", "scope", "logical_instance", "native_missing_receipt"} {
				t.Run(variant, func(t *testing.T) {
					route := base
					dependency := events.HistoricalReceiverConstruction{SourceRunID: fixed.SourceOwner.RunID, SourceRevision: fixed.SourceRevision, SourceReceiver: agentRoute}
					switch variant {
					case "source_run":
						dependency.SourceRunID = uuid.NewString()
					case "revision":
						dependency.SourceRevision++
					case "scope":
						route.AgentIdentity.Route.ScopeKey = "foreign"
					case "logical_instance":
						route.AgentIdentity.Route.InstanceID = "foreign"
					}
					dependency.SourceReceiver = route.AgentIdentity.Route
					if variant == "native_missing_receipt" {
						route.Initialization, err = events.AdmitStoredFlowReceiverInitialization(fork.ForkRunID, selected.Activation.CreatingInput.EventID, events.MustExistingEntityTarget(target), route.AgentIdentity.Route)
					} else {
						route.Initialization, err = events.AdmitHistoricalFlowReceiverInitialization(fork.ForkRunID, events.MustExistingEntityTarget(target), route.AgentIdentity.Route, dependency)
					}
					if err != nil || events.ValidateDeliveryRoutes([]events.DeliveryRoute{route}) != nil || route.Initialization.ValidateRoute(route) != nil {
						t.Fatalf("hostile dependency must reach real persistence admission: %v", err)
					}
					before, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
					if err != nil {
						t.Fatal(err)
					}
					ready, err := readReceiverConstructionReady(t, f, route)
					if variant == "exact" {
						if err != nil || !ready {
							t.Fatalf("exact historical construction refused: ready=%t %v", ready, err)
						}
					} else {
						want := "receiver dependency contradicts exact historical construction"
						if variant == "native_missing_receipt" {
							want = "native receiver construction lacks its original receipt"
						} else if variant == "scope" || variant == "logical_instance" {
							want = "receiver dependency contradicts captured historical coordinates"
						}
						if ready || err == nil || err.Error() != want {
							t.Fatalf("%s refusal: ready=%t err=%v want=%q", variant, ready, err, want)
						}
					}
					if after, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store); err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("materialization admission changed durable evidence: %v", err)
					}
				})
			}
		})
	}
}

func materializeReceiverConstructionProofFork(t *testing.T, f receiverConfigActivationFixture, runID string) runfork.RunForkMaterialization {
	t.Helper()
	marker := eventtest.ExistingRunRootIngress(uuid.NewString(), "checkpoint", "historical-claim-proof", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	if err := commitSemanticPipelineProcessedEventFixture(f.ctx, f.store, marker); err != nil {
		t.Fatal(err)
	}
	fork, err := f.store.(interface {
		MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
	}).MaterializeRunFork(f.ctx, runfork.RunForkMaterializeRequest{SourceRunID: runID, At: marker.ID()})
	if err != nil {
		t.Fatal(err)
	}
	return fork
}

func conflictingHistoricalReceiverReceipt(t *testing.T, f receiverConfigActivationFixture, target flowidentity.Instance, raw []byte) (string, flowidentity.Instance, []byte, events.HistoricalReceiverConstruction) {
	t.Helper()
	sourceRunID := correlation.RunIDFromContext(f.ctx)
	original, err := pipeline.DecodeStoredFlowConstructionReceipt(raw, sourceRunID, target.EntityID, target.InstancePath, target.TemplateID)
	if err != nil {
		t.Fatal(err)
	}
	fork := materializeReceiverConstructionProofFork(t, f, sourceRunID)
	lookup, err := pipeline.NewExactFlowInstanceLookup(semanticview.Wrap(f.bundle), mustStoreTestSourceArtifactFact(f.bundle.SourceArtifact.BundleHash()), flowidentity.RunScopedFlowInstance{RunID: fork.ForkRunID, Route: target.Route()})
	if err != nil {
		t.Fatal(err)
	}
	observed, found, err := f.store.(pipeline.FlowInstanceIndexReader).LookupFlowInstance(f.ctx, lookup)
	if err != nil || !found {
		t.Fatalf("fixed receiver lookup: found=%t %v", found, err)
	}
	fixed, projected := observed.HistoricalConstruction()
	if !projected {
		t.Fatal("dual evidence fixture lacks positive historical construction")
	}
	creating := original.CreatingInput
	creating.EventID = uuid.NewString()
	projectedReceipt, err := pipeline.ProjectFlowConstructionReceipt(original, flowidentity.RunScopedFlowInstance{RunID: fork.ForkRunID, Route: fixed.Identity.Route()}, fixed.Identity, creating, nil)
	if err != nil {
		t.Fatal(err)
	}
	projectedReceipt.InstanceKey = "conflicting-native-key"
	contradiction, err := pipeline.EncodeFlowConstructionReceipt(projectedReceipt)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := fixed.SourceOwner.Route.AgentIdentityRoute()
	if err != nil {
		t.Fatal(err)
	}
	return fork.ForkRunID, fixed.Identity, contradiction, events.HistoricalReceiverConstruction{SourceRunID: sourceRunID, SourceRevision: fixed.SourceRevision, SourceReceiver: receiver}
}

func TestReceiverHistoricalMaterializationRefusesLaterNativeConstructionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, request := newR7SelectionProof(t, backend)
			fork := materializeReceiverConstructionProofFork(t, f, request.Lookup.RunID())
			lookup, err := pipeline.NewExactFlowInstanceLookup(request.Lookup.Source(), request.Lookup.SourceFact(), flowidentity.RunScopedFlowInstance{RunID: fork.ForkRunID, Route: flowidentity.StoredRoute(".", fork.ForkRunID, fork.ForkRunID)})
			if err != nil {
				t.Fatal(err)
			}
			root, found, err := f.store.(pipeline.FlowInstanceIndexReader).LookupFlowInstance(f.ctx, lookup)
			if err != nil || !found {
				t.Fatalf("fixed parent lookup: found=%t %v", found, err)
			}
			ctx := correlation.WithRunID(f.ctx, fork.ForkRunID)
			constructor := request.Constructor
			constructor.Instance, err = flowidentity.KeyedChild(request.Lookup.Source(), root.Identity(), "worker", "later-native")
			if err != nil {
				t.Fatal(err)
			}
			constructor.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "item.created", "constructor-fixture", "", []byte(`{"item_id":"business-key"}`), 0, fork.ForkRunID, events.EventEnvelope{}, constructor.OccurredAt)
			plan, err := f.manager.PrepareFlowInstanceActivation(ctx, constructor)
			if err != nil {
				t.Fatal(err)
			}
			if result, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(ctx, plan); err != nil || !result.Acknowledged {
				t.Fatalf("later native construction: %+v %v", result, err)
			}
			agentRoute, err := plan.Identity.Route().AgentIdentityRoute()
			if err != nil {
				t.Fatal(err)
			}
			name, _ := agentidentity.RuntimeName("worker", "historical-claim-proof")
			agent, err := agentidentity.New(fork.ForkRunID, name, agentRoute)
			if err != nil {
				t.Fatal(err)
			}
			target := events.RouteIdentity{FlowID: plan.Identity.TemplateID, FlowInstance: plan.Identity.InstancePath, EntityID: plan.Identity.EntityID}
			actor := target
			actor.EntityID = ""
			route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agent.AgentID()), AgentIdentity: agent, Target: events.MustEntitylessReceiverTarget(actor)}
			route.Initialization, err = events.AdmitHistoricalFlowReceiverInitialization(fork.ForkRunID, events.MustExistingEntityTarget(target), agentRoute,
				events.HistoricalReceiverConstruction{SourceRunID: request.Lookup.RunID(), SourceRevision: fork.ForkPoint.Revision, SourceReceiver: agentRoute})
			if err != nil || route.Initialization.ValidateRoute(route) != nil {
				t.Fatalf("later native historical pretence must reach persistence admission: %v", err)
			}
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
			if err != nil {
				t.Fatal(err)
			}
			if ready, err := readReceiverConstructionReady(t, f, route); ready || err == nil || err.Error() != "receiver dependency contradicts exact historical construction" {
				t.Fatalf("absent fixed-cut receiver borrowed later native construction: ready=%t %v", ready, err)
			}
			route.Initialization, err = events.AdmitStoredFlowReceiverInitialization(fork.ForkRunID, plan.CreatingInput.EventID, events.MustExistingEntityTarget(target), agentRoute)
			if err != nil {
				t.Fatal(err)
			}
			if ready, err := readReceiverConstructionReady(t, f, route); !ready || err != nil {
				t.Fatalf("exact later native receipt refused: ready=%t %v", ready, err)
			}
			if after, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store); err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("later native admission changed durable evidence: %v", err)
			}
		})
	}
}
