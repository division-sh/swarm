package runtimepersistence

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/google/uuid"
)

func TestReceiverPublicInputFinalizesCreationWithoutRecoveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			_, selected := newAgentFixtureAuthorityStore(t, backend)
			repo := pipeline.WorkflowRepoRoot()
			bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, canonicalrouting.CopyProviderReceiverInitialization(t), contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
			if err != nil {
				t.Fatal(err)
			}
			source, err := runtimepkg.SourceWithProviderTriggerEvents(semanticview.Wrap(bundle), packfixture.TriggerCatalog(t))
			if err != nil {
				t.Fatal(err)
			}
			fact := mustStoreTestSourceArtifactFact(bundle.SourceArtifact.BundleHash())
			runID := uuid.NewString()
			ctx := correlation.WithRunID(storeTestWorkContext(t, testAuthorActivityContextForBundle(fact.BundleHash())), runID)
			if err := ensureRunFixtureSourceArtifactForTest(ctx, selected, fact.BundleHash(), bundle.SourceArtifact); err != nil {
				t.Fatal(err)
			}
			descriptors, err := runtimepkg.AuthorActivityEventDescriptors(source)
			if err != nil {
				t.Fatal(err)
			}
			scope, _ := authoractivity.ScopeFromContext(ctx)
			catalog, err := selected.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope, descriptors)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(catalog.Release)
			var am *manager.AgentManager
			var finalized []pipeline.CommittedFlowInstanceActivation
			var finalizationErrors []error
			publisher, err := newStoreTestEventBus(t, selected.(storeTestDurableEventBusStore), bus.EventBusOptions{
				ContractBundle: source, SourceArtifactFact: fact,
				TemplateInstancePlanner: pipeline.FlowInstanceActivationPlannerFunc(func(ctx context.Context, req pipeline.FlowInstanceActivationRequest) (pipeline.FlowInstanceActivationPlan, error) {
					return am.PrepareFlowInstanceActivation(ctx, req)
				}),
				FlowActivationFinalizer: pipeline.CommittedFlowInstanceActivationFinalizerFunc(func(ctx context.Context, activation pipeline.CommittedFlowInstanceActivation) error {
					finalized = append(finalized, activation)
					err := am.FinalizeCommittedFlowInstanceActivation(ctx, activation)
					if err == nil {
						readiness, found, readErr := selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, activation.Plan.Identity.Route())
						if readErr != nil || !found || readiness.TopologyReadyAt.IsZero() || readiness.CreationEventEmittedAt.IsZero() {
							err = fmt.Errorf("readiness incomplete inside initial finalizer: %+v found=%v err=%v", readiness, found, readErr)
						}
					}
					finalizationErrors = append(finalizationErrors, err)
					return err
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			workflow := configureAgentFixtureFlowLifecycle(t, selected, &sqliteFlowActivationBus{}, bundle)
			coordinate := agenttopology.SourceCoordinate{BundleHash: fact.BundleHash()}
			set, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, nil)
			if err != nil {
				t.Fatal(err)
			}
			grant, err := agentfixture.AdmitGeneration(t, ctx, selected, set, coordinate)
			if err != nil {
				t.Fatal(err)
			}
			am = ownStoreTestAgentManager(t, manager.NewAgentManagerWithOptions(publisher, nil, manager.AgentManagerOptions{
				ExecutionPosture: executionposture.Live, BaseContext: ctx, SourceArtifactFact: fact, SemanticSource: source, WorkflowInstances: workflow,
				DeliveryStore: selected, WorkOwner: storeTestWorkOwner(t), ReceiverExecution: eventreceiver.NormalExecution(),
				PersistenceRoles: manager.PersistenceRoles{AgentRoutes: publisher, FlowActivation: agentFixtureFlowActivationCommitter{store: selected}, RouteInstaller: publisher, RouteVerifier: publisher, RouteRestorer: publisher, RouteRetirer: publisher, CreationPublisher: publisher},
			}, selected))
			admission, err := agenttopology.StaticAdmission(set.Revision, fact.BundleHash(), agenttopology.LifetimeDurableManaged)
			if err != nil {
				t.Fatal(err)
			}
			if err := am.InstallStartupTopology(grant, admission, set); err != nil {
				t.Fatal(err)
			}
			// Never Run/Resume/Ensure: only this first publication may finalize
			// readiness. An eventual recovery cannot conceal the callback error.
			association := semanticview.BuildAuthoredEventEndpointCensus(source).ResolveDeclaredInputEndpoint("telegram-chat", "inbound.telegram.text_message")
			endpoint, ok := association.Endpoint()
			if !ok {
				t.Fatal(association.Err())
			}
			apiEndpoint, err := bus.NewTemplateAPIEventPublicationEndpoint(source, endpoint)
			if err != nil {
				t.Fatal(err)
			}
			eventID := uuid.NewString()
			event := eventtest.RunCreatingRootIngress(eventID, "inbound.telegram.text_message", "operator-api", "", []byte(`{"conversation_reference":"2307","conversation_scope":"direct","external_account_reference":"2307","provider_message_reference":7,"text":"first-pass"}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
			request := apiidempotency.Request{Method: "event.publish", Actor: apiidempotency.BearerActor("operator"), IdempotencyKey: "first-pass", RequestHash: "exact-request"}
			completion := apiidempotency.Completion{ResourceID: eventID, Response: json.RawMessage(`{"event_id":"` + eventID + `"}`)}
			initialEvent, err := json.Marshal(map[string]any{"event_name": event.Type(), "payload": json.RawMessage(event.Payload()), "emitter": "operator-api", "entity_id": "", "flow_instance": "", "source_event_id": ""})
			if err != nil {
				t.Fatal(err)
			}
			runCreation := durabledata.RunCreationCommand{RunID: runID, Actor: "operator", BundleHash: fact.BundleHash(), EventID: eventID, InitialEvent: initialEvent}
			got, replay, err := publisher.PublishAPIEventWithRunCreationAcknowledged(ctx, event, &apiEndpoint, request, completion, &runCreation)
			if err != nil || replay || got.ResourceID != eventID {
				t.Fatalf("initial public completion: %+v replay=%v err=%v", got, replay, err)
			}
			if len(finalizationErrors) != 1 {
				t.Fatalf("first-pass finalizations=%d", len(finalizationErrors))
			}
			if finalizationErrors[0] != nil {
				t.Fatalf("initial root finalization failed before duplicate or recovery: %v", finalizationErrors[0])
			}
			plan := finalized[0].Plan
			readiness, found, err := selected.LoadDynamicFlowRuntimeReadiness(ctx, runID, plan.Identity.Route())
			if err != nil || !found || readiness.TopologyReadyAt.IsZero() || readiness.CreationEventEmittedAt.IsZero() {
				t.Fatalf("first-pass readiness incomplete: %+v found=%v err=%v", readiness, found, err)
			}
			if plan.Readiness.CreationEvent == nil {
				t.Fatal("fixture did not require automatic creation event")
			}
			child, found, err := selected.(interface {
				LoadPreparedPublishEvent(context.Context, string) (bus.PreparedPublishEvent, bool, error)
			}).LoadPreparedPublishEvent(ctx, plan.Readiness.CreationEvent.EventID)
			if err != nil || !found {
				t.Fatalf("first-pass automatic publication missing: found=%v err=%v", found, err)
			}
			if err := child.Validate(); err != nil {
				t.Fatal(err)
			}
			if auto := child.Event.Event(); auto.Type() != events.EventType(plan.Identity.Route().InstancePath+"/chat.initialized") || auto.ParentEventID() != eventID || auto.RunID() != runID {
				t.Fatalf("automatic event lost its private type or root lineage: %+v", auto)
			}
			if len(child.DeliveryRoutes) != 1 {
				t.Fatalf("automatic event did not select the exact receiver handler: %+v", child.DeliveryRoutes)
			}
			if node, ok := child.DeliveryRoutes[0].Recipient.Node(); !ok || node.FlowPath() != "telegram-chat" || node.NodeID() != "receiver" {
				t.Fatalf("automatic event selected a foreign handler: %+v", child.DeliveryRoutes)
			}
			wantTarget := events.RouteIdentity{FlowID: plan.Identity.ScopeKey, FlowInstance: plan.Identity.InstancePath, EntityID: plan.Identity.EntityID}
			if target := child.DeliveryRoutes[0].Target.Route(); target != wantTarget {
				t.Fatalf("automatic event target=%+v, want exact activated receiver %+v", target, wantTarget)
			}
			// Readiness was checked inside the initial finalizer, before this
			// join of its foreground dispatch. No recovery worker is running.
			joinCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := publisher.WaitForQuiescence(joinCtx); err != nil {
				t.Fatal(err)
			}
			got, replay, err = publisher.PublishAPIEventWithRunCreationAcknowledged(ctx, event, &apiEndpoint, request, completion, &runCreation)
			if err != nil || !replay || got.ResourceID != eventID || len(finalizationErrors) != 1 {
				t.Fatalf("duplicate replay reran finalization: replay=%v callbacks=%d err=%v", replay, len(finalizationErrors), err)
			}
		})
	}
}
