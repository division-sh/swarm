package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/finalflowinstanceauthoring"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestFinalFlowInstanceAuthoringRuntime_PublishActivatesAndExecutesSelectedTemplateInstance(t *testing.T) {
	bundle := finalflowinstanceauthoring.LoadBundle(t, finalflowinstanceauthoring.Options{})
	source := semanticview.Wrap(bundle)
	report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), source, runtimebootverify.Options{})
	if got := report.HardInvalidities(); len(got) != 0 {
		t.Fatalf("final flow-instance authoring hard invalidities = %#v, want none", got)
	}

	pg := storetest.StartPostgresRuntimeStore(t)

	if bundle.SourceArtifact == nil {
		t.Fatal("flow activation proof requires its compiled source artifact")
	}
	fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	ctx := correlation.WithRunID(correlation.WithSourceArtifactFact(testAuthorActivityContext(context.Background()), fact), templateInstanceDeliveryRunID)
	ctx = correlation.WithRuntimeInstanceID(ctx, authorActivityTestRuntimeInstanceID)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, fact.BundleHash()))

	storetest.RequireRun(t, ctx, pg, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: templateInstanceDeliveryRunID, Artifact: bundle.SourceArtifact, BundleHash: fact.BundleHash()})
	var manager *runtimemanager.AgentManager
	var pc *runtimepipeline.PipelineCoordinator
	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{
		ContractBundle:     source,
		SourceArtifactFact: fact,
		InterceptorProvider: func() []runtimebus.EventInterceptor {
			if pc == nil {
				return nil
			}
			return []runtimebus.EventInterceptor{pc}
		},
		TemplateInstancePlanner: runtimepipeline.FlowInstanceActivationPlannerFunc(func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) (runtimepipeline.FlowInstanceActivationPlan, error) {
			if manager == nil {
				return runtimepipeline.FlowInstanceActivationPlan{}, errors.New("agent manager not initialized")
			}
			return manager.PrepareFlowInstanceActivation(ctx, req)
		}),
		FlowActivationFinalizer: runtimepipeline.CommittedFlowInstanceActivationFinalizerFunc(func(ctx context.Context, committed runtimepipeline.CommittedFlowInstanceActivation) error {
			if manager == nil {
				return errors.New("agent manager not initialized")
			}
			return manager.FinalizeCommittedFlowInstanceActivation(ctx, committed)
		}),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	module := newRuntimeTestWorkflowModule(t, source)
	pc = newExternalRuntimeTestPipelineCoordinator(t, bus, pg, runtimepipeline.PipelineCoordinatorOptions{
		WorkOwner:           runtimeTestEventBusWorkOwner(t, bus),
		Module:              module,
		Persistence:         runtimepipeline.NewWorkflowPersistence(pg),
		RunLifecycle:        pg,
		PipelineObligations: pg.PipelineObligations(),
		DeliveryStore:       pg,
	})

	manager = ownRuntimeTestAgentManager(t, runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		ExecutionPosture:   executionposture.Live,
		SourceArtifactFact: fact,
		SemanticSource:     source,
		WorkOwner:          runtimeTestEventBusWorkOwner(t, bus),
		WorkflowInstances:  pc,
		PersistenceRoles:   externalRuntimeTestManagerBusRoles(bus),
		DeliveryStore:      pg, ReceiverExecution: eventreceiver.NormalExecution(),
	}))

	coordinate := agenttopology.SourceCoordinate{BundleHash: fact.BundleHash()}
	desired, err := manager.CompileStaticTopologyDesiredAgents(source, coordinate)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, desired)
	if err != nil {
		t.Fatal(err)
	}
	capability, err := pg.AcquireProcessCapability(ctx, startupownership.AcquireRequest{
		OwnerID: "final-flow-activation-test", BootID: uuid.NewString(), RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
	})
	if err != nil {
		t.Fatal(err)
	}
	var grant startupownership.LiveGenerationGrant
	t.Cleanup(func() {
		if err := closeExternalManagerTestGeneration(manager, bus, grant, capability); err != nil {
			t.Error(err)
		}
	})
	if _, err := capability.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
		t.Fatal(err)
	}
	grant, err = capability.IssueGenerationGrant(ctx, startupownership.GrantRequest{
		BundleHash: fact.BundleHash(), RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
		RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := grant.AdmitExecution(ctx); err != nil {
		t.Fatal(err)
	}
	installExternalManagerTestGeneration(t, ctx, manager, grant)
	producer := seedRuntimeTestKeylessSource(t, ctx, pg, pc, finalflowinstanceauthoring.ProducerFlowID)

	evt := eventtest.ExistingRunRootIngressWithRoutingSource(
		"99999999-9999-4999-8999-999999999955",
		events.EventType(finalflowinstanceauthoring.ProducerFlowID+"/"+finalflowinstanceauthoring.ProducerOutput),
		finalflowinstanceauthoring.ProducerFlowID,
		"",
		json.RawMessage(`{"account_id":"acct-42","score":"91","decision":"approved"}`),
		0,
		templateInstanceDeliveryRunID,
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{
			FlowID: producer.TemplateID, FlowInstance: producer.InstancePath, EntityID: producer.EntityID,
		}),
		eventtest.StaticFlowRoutingSource(producer.TemplateID, producer.InstancePath, producer.EntityID), time.Now().UTC(),
	)
	preflight, err := bus.CheckPublishRecipientPlan(ctx, evt)
	if err != nil {
		t.Fatalf("CheckPublishRecipientPlan: %v", err)
	}
	if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) != 1 || !preflight.UsesCanonicalRouteAuthority() {
		t.Fatalf("preflight failure/routes/canonical = %q/%#v/%v, want one canonical template route", preflight.TargetFailure, preflight.DeliveryRoutes, preflight.UsesCanonicalRouteAuthority())
	}
	assertRuntimeStorageCount(t, "CountTemplateAccountEntities", 0, func() (int, error) {
		return storetest.CountTemplateAccountEntities(ctx, pg)
	})

	if err := bus.PublishAcknowledged(ctx, evt); err != nil {
		t.Fatalf("PublishAcknowledged final account.ready: %v", err)
	}
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		dumpFinalFlowInstanceAuthoringRuntimeProofState(t, ctx, pg, evt.ID())
	})
	templateNodeID := identitytest.FlowNode(t, finalflowinstanceauthoring.TemplateFlowID, finalflowinstanceauthoring.TemplateNodeID).Key()
	waitRuntimeNodeDeliveryOutcome(t, ctx, pg, evt.ID(), templateNodeID)
	waitRuntimeStorageCount(t, ctx, pg, "CountPostgresReviewedTemplateAccount", 1, func() (int, error) {
		return storetest.CountPostgresReviewedTemplateAccount(ctx, pg, `{"account_id":"acct-42","score":"91","decision":"approved"}`)
	})
	assertRuntimeStorageCount(t, "CountTemplateAccountEntities", 1, func() (int, error) {
		return storetest.CountTemplateAccountEntities(ctx, pg)
	})

	flowInstance, entityID := loadFinalFlowInstanceAuthoringTemplateIdentity(t, ctx, pg)
	assertRuntimeStorageCount(t, "CountPostgresTemplateDeliveredNodeTarget", 1, func() (int, error) {
		return storetest.CountPostgresTemplateDeliveredNodeTarget(ctx, pg, evt.ID(), templateNodeID, finalFlowInstanceAuthoringDeliveryTargetRouteJSON(t, events.RouteIdentity{FlowID: finalflowinstanceauthoring.TemplateFlowID, FlowInstance: flowInstance, EntityID: entityID}))
	})

	loaded, ok, err := pc.Load(ctx, runtimeflowidentity.RunScopedFlowInstance{
		RunID: templateInstanceDeliveryRunID,
		Route: runtimeflowidentity.RouteForInstancePath(flowInstance),
	})
	if err != nil {
		t.Fatalf("workflowStore.Load(%s): %v", entityID, err)
	}
	if !ok {
		t.Fatalf("workflowStore.Load(%s) ok=false", entityID)
	}
	if loaded.StorageRef != flowInstance || loaded.WorkflowName != finalflowinstanceauthoring.TemplateFlowID || loaded.CurrentState != "reviewed" {
		t.Fatalf("loaded account_case instance = storage:%q workflow:%q state:%q, want %s/%s/reviewed", loaded.StorageRef, loaded.WorkflowName, loaded.CurrentState, flowInstance, finalflowinstanceauthoring.TemplateFlowID)
	}
	if loaded.Fields[finalflowinstanceauthoring.TemplateInstanceBy] != "acct-42" ||
		loaded.Fields["score"] != "91" ||
		loaded.Fields["decision"] != "approved" {
		t.Fatalf("loaded account_case fields = %#v, want account_id/score/decision from selected routed payload", loaded.Fields)
	}
}

func loadFinalFlowInstanceAuthoringTemplateIdentity(t *testing.T, ctx context.Context, selected any) (string, string) {
	t.Helper()
	row, err := storetest.ReadLatestTemplateAccountIdentity(ctx, selected)
	if err != nil {
		t.Fatalf("load account_case instance identity: %v", err)
	}
	return row.Path, row.EntityID
}

func finalFlowInstanceAuthoringDeliveryTargetRouteJSON(t *testing.T, target events.RouteIdentity) string {
	t.Helper()
	encoded, err := json.Marshal(events.MustMaterializingEntityTarget(target))
	if err != nil {
		t.Fatalf("marshal final flow-instance authoring delivery target: %v", err)
	}
	return strings.TrimSpace(string(encoded))
}

func dumpFinalFlowInstanceAuthoringRuntimeProofState(t *testing.T, ctx context.Context, selected any, eventID string) {
	t.Helper()
	deliveries, err := storetest.ReadTemplateEventDeliveryDiagnostics(ctx, selected, eventID)
	if err != nil {
		t.Logf("event_deliveries diagnostic query failed: %v", err)
		return
	}
	for _, row := range deliveries {
		t.Logf("event_delivery: type=%s id=%s status=%s reason=%s failure=%s target=%s", row.SubscriberType, row.SubscriberID, row.Status, row.Reason, row.Failure, row.Target)
	}
	receipts, err := storetest.ReadTemplateEventReceiptDiagnostics(ctx, selected, eventID)
	if err != nil {
		t.Logf("event_receipts diagnostic query failed: %v", err)
		return
	}
	for _, row := range receipts {
		t.Logf("event_receipt: type=%s id=%s outcome=%s key=%s side_effects=%s", row.SubscriberType, row.SubscriberID, row.Outcome, row.IdempotencyKey, row.SideEffects)
	}
	states, err := storetest.ReadTemplateAccountDiagnostics(ctx, selected)
	if err != nil {
		t.Logf("entity_state diagnostic query failed: %v", err)
		return
	}
	for _, row := range states {
		t.Logf("entity_state: entity=%s flow_instance=%s state=%s fields=%s", row.EntityID, row.Path, row.State, row.Fields)
	}
}
