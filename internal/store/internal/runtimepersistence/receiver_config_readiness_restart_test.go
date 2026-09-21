package runtimepersistence

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

// This models a lost caller acknowledgement after a real creation commit,
// not an ambiguous database COMMIT or a replacement transaction implementation.
type receiverConfigLostCreationAcknowledgement struct {
	pipeline.DynamicFlowRuntimeCreationOccurrencePublisher
}

func (p receiverConfigLostCreationAcknowledgement) CommitDynamicFlowRuntimeCreationOccurrence(ctx context.Context, req pipeline.DynamicFlowRuntimeCreationOccurrenceRequest) error {
	if err := p.DynamicFlowRuntimeCreationOccurrencePublisher.CommitDynamicFlowRuntimeCreationOccurrence(ctx, req); err != nil {
		return err
	}
	return errors.New("injected lost creation acknowledgement")
}

func TestReceiverConfigReadinessRestartPreservesPendingAutoEmitBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, boundary := range []string{"before_readiness", "creation_mark_rollback", "creation_commit_ack_lost", "numeric_substitution"} {
			t.Run(backend+"/"+boundary, func(t *testing.T) {
				f := newReceiverConfigActivationFixtureWithOptions(t, backend, true, true)
				fact, _ := correlation.SourceArtifactFactFromContext(f.ctx)
				source := semanticview.Wrap(f.bundle)
				descriptors, err := runtimepkg.AuthorActivityEventDescriptors(source)
				if err != nil {
					t.Fatal(err)
				}
				scope, _ := authoractivity.ScopeFromContext(f.ctx)
				lease, err := f.store.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope, descriptors)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(lease.Release)
				newPublisher := func() *bus.EventBus {
					publisher, err := newStoreTestEventBus(t, f.store.(storeTestDurableEventBusStore), bus.EventBusOptions{ContractBundle: source, SourceArtifactFact: fact})
					if err != nil {
						t.Fatal(err)
					}
					return publisher
				}
				publisher := newPublisher()
				req := f.request("business-key", "ti-restart", "committed")
				req.Config["nested"] = []any{int64(7), float64(7)}
				req.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "request.started", "operator", "", []byte(`{}`), 0, correlation.RunIDFromContext(f.ctx), events.EventEnvelope{}, req.OccurredAt)
				if err := publisher.Publish(f.ctx, req.TriggerEvent); err != nil {
					t.Fatal(err)
				}
				plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
				if err != nil {
					t.Fatal(err)
				}
				committed, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan)
				if err != nil || !committed.Created || plan.Readiness.CreationEvent == nil {
					t.Fatalf("commit pending typed initialization: created=%v err=%v", committed.Created, err)
				}
				f.requireCounts(t, 1)
				f.requireCreationOccurrence(t, plan, 0)
				if boundary == "numeric_substitution" {
					changed := plan.Readiness
					creation := *changed.CreationEvent
					creation.Payload = []byte(strings.ReplaceAll(string(creation.Payload), "7.0", "7"))
					changed.CreationEvent = &creation
					if err := f.workflows.MarkDynamicFlowRuntimeTopologyReady(f.ctx, changed, req.OccurredAt); err == nil {
						t.Fatal("topology CAS accepted a substituted numeric kind")
					}
					stored, _, err := f.store.LoadDynamicFlowRuntimeReadiness(f.ctx, plan.Readiness.RunID, plan.Identity.Route())
					if err != nil || !stored.TopologyReadyAt.IsZero() {
						t.Fatalf("rejected topology CAS mutated readiness: %#v err=%v", stored, err)
					}
					if err := f.workflows.MarkDynamicFlowRuntimeTopologyReady(f.ctx, plan.Readiness, req.OccurredAt); err != nil {
						t.Fatal(err)
					}
					event := eventtest.PersistedChildForProducer(creation.EventID, events.EventType(creation.EventType), eventtest.Producer(events.EventProducerPlatform, "flow-instance-activator"), "", creation.Payload, 0, creation.RunID, creation.ParentEventID,
						events.EnvelopeForSourceRoute(events.EventEnvelope{EntityID: plan.Identity.EntityID, FlowInstance: plan.Identity.InstancePath}, events.RouteIdentity{FlowID: plan.Identity.TemplateID, FlowInstance: plan.Identity.InstancePath, EntityID: plan.Identity.EntityID}), creation.CreatedAt)
					if err := publisher.CommitDynamicFlowRuntimeCreationOccurrence(f.ctx, pipeline.DynamicFlowRuntimeCreationOccurrenceRequest{RunID: changed.RunID, InstancePath: plan.Identity.InstancePath, Plan: changed, Event: event, OccurredAt: req.OccurredAt}); err == nil {
						t.Fatal("creation commit accepted a substituted numeric kind")
					}
					f.requireCreationOccurrence(t, plan, 0)
					f.requireConfig(t, plan)
					return
				}
				if err := f.manager.Shutdown(); err != nil {
					t.Fatal(err)
				}
				if boundary != "before_readiness" {
					var creationPublisher pipeline.DynamicFlowRuntimeCreationOccurrencePublisher = publisher
					var removeFault func()
					if boundary == "creation_mark_rollback" {
						removeFault = f.rejectCreationCompletionMark(t, backend)
					} else {
						creationPublisher = receiverConfigLostCreationAcknowledgement{publisher}
					}
					attempt := f.restartedReceiverConfigManager(t, creationPublisher)
					if err := attempt.manager.FinalizeCommittedFlowInstanceActivation(f.ctx, committed); err == nil {
						t.Fatal("readiness crossed injected failure")
					}
					want := 0
					if boundary == "creation_commit_ack_lost" {
						want = 1
					}
					f.requireCreationOccurrence(t, plan, want)
					f.requireConfig(t, plan)
					if removeFault != nil {
						removeFault()
					}
					if err := attempt.manager.Shutdown(); err != nil {
						t.Fatal(err)
					}
				}
				// Reconstruct both consumers; only durable config/readiness survives.
				restarted := f.restartedReceiverConfigManager(t, newPublisher())
				req.Config = map[string]any{"request_id": "replacement", "label": false, "undeclared": true}
				for i := 0; i < 2; i++ {
					if created, err := restarted.manager.EnsureFlowInstance(f.ctx, req); err != nil || created {
						t.Fatalf("restart/retry replaced initialization: created=%v err=%v", created, err)
					}
					restarted.requireCounts(t, 1)
					restarted.requireConfig(t, plan)
					restarted.requireCreationOccurrence(t, plan, 1)
				}
				agents, err := f.store.LoadAgents(f.ctx)
				if err != nil || len(agents) != 1 {
					t.Fatalf("restarted agents=%d err=%v", len(agents), err)
				}
				want, err := canonicaljson.MarshalPreservingNumberKinds(plan.Instance.Config)
				if err != nil {
					t.Fatal(err)
				}
				requireReceiverConfigWire(t, agents[0].Config.ReceiverConfig, want)
				for _, route := range restarted.bus.materializationRequests() {
					if route.ActivationVariables["label"] != "committed" || route.ActivationVariables["request_id"] != "business-key" {
						t.Fatalf("restart consumed incoming variables: %#v", route.ActivationVariables)
					}
				}
				if len(restarted.bus.routePaths()) != 1 {
					t.Fatal("restart did not install exact instance route")
				}
			})
		}
	}
}

func (f receiverConfigActivationFixture) restartedReceiverConfigManager(t *testing.T, publisher pipeline.DynamicFlowRuntimeCreationOccurrencePublisher) receiverConfigActivationFixture {
	t.Helper()
	fact, _ := correlation.SourceArtifactFactFromContext(f.ctx)
	coordinate := agenttopology.SourceCoordinate{BundleHash: fact.BundleHash()}
	sourceSet, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, nil)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := agentfixture.AdmitGeneration(t, f.ctx, f.store, sourceSet, coordinate)
	if err != nil {
		t.Fatal(err)
	}
	f.bus = &sqliteFlowActivationBus{}
	f.workflows = configureAgentFixtureFlowLifecycle(t, f.store, f.bus, f.bundle)
	f.manager = ownStoreTestAgentManager(t, manager.NewAgentManagerWithOptions(f.bus, nil, manager.AgentManagerOptions{
		ExecutionPosture: executionposture.Live, BaseContext: f.ctx, SourceArtifactFact: fact,
		SemanticSource: semanticview.Wrap(f.bundle), WorkflowInstances: f.workflows, LLMBackend: "anthropic",
		DeliveryStore: f.store, WorkOwner: storeTestWorkOwner(t),
		PersistenceRoles: manager.PersistenceRoles{
			AgentRoutes: f.bus, FlowActivation: agentFixtureFlowActivationCommitter{store: f.store},
			RouteInstaller: f.bus, RouteVerifier: f.bus, RouteRestorer: f.bus, RouteRetirer: f.bus,
			CreationPublisher: publisher,
		}, ReceiverExecution: eventreceiver.NormalExecution(),
	}, f.store))
	admission, err := agenttopology.StaticAdmission(sourceSet.Revision, fact.BundleHash(), agenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.manager.InstallStartupTopology(grant, admission, sourceSet); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f receiverConfigActivationFixture) rejectCreationCompletionMark(t *testing.T, backend string) func() {
	t.Helper()
	install := `CREATE TRIGGER reject_receiver_config_creation BEFORE UPDATE OF creation_event_emitted_at ON flow_instance_runtime_readiness WHEN NEW.creation_event_emitted_at IS NOT NULL BEGIN SELECT RAISE(ABORT, 'injected receiver creation mark failure'); END`
	remove := `DROP TRIGGER reject_receiver_config_creation`
	if backend == "postgres" {
		install = `ALTER TABLE flow_instance_runtime_readiness ADD CONSTRAINT reject_receiver_config_creation CHECK (creation_event_emitted_at IS NULL)`
		remove = `ALTER TABLE flow_instance_runtime_readiness DROP CONSTRAINT reject_receiver_config_creation`
	}
	if _, err := f.db.ExecContext(f.ctx, install); err != nil {
		t.Fatal(err)
	}
	return func() {
		if _, err := f.db.ExecContext(f.ctx, remove); err != nil {
			t.Fatal(err)
		}
	}
}

func (f receiverConfigActivationFixture) requireCreationOccurrence(t *testing.T, plan pipeline.FlowInstanceActivationPlan, want int) {
	t.Helper()
	var count int
	if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM events WHERE event_id=$1`, plan.Readiness.CreationEvent.EventID).Scan(&count); err != nil || count != want {
		t.Fatalf("creation event count=%d want=%d err=%v", count, want, err)
	}
	readiness, found, err := f.store.LoadDynamicFlowRuntimeReadiness(f.ctx, plan.Readiness.RunID, plan.Identity.Route())
	if err != nil || !found || readiness.CreationEventEmittedAt.IsZero() != (want == 0) {
		t.Fatalf("creation event/mark not atomic: %#v err=%v", readiness, err)
	}
	if want != 0 {
		var payload []byte
		if err := f.db.QueryRowContext(f.ctx, `SELECT payload FROM events WHERE event_id=$1`, plan.Readiness.CreationEvent.EventID).Scan(&payload); err != nil {
			t.Fatal(err)
		}
		requireReceiverConfigWire(t, payload, plan.Readiness.CreationEvent.Payload)
	}
}

func requireReceiverConfigWire(t *testing.T, raw, want []byte) {
	t.Helper()
	var decoded any
	if err := canonicaljson.DecodePreservingNumberLexemes(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	wire, err := canonicaljson.MarshalPreservingNumberKinds(decoded)
	if err != nil || string(wire) != string(want) {
		t.Fatalf("config wire=%s want=%s err=%v", wire, want, err)
	}
}
