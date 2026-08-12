package conformance

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type stageLifecycleIdentityStore interface {
	decisioncard.Store
	ListEventDeliveryRoutes(context.Context, string) ([]events.DeliveryRoute, error)
	ListWorkflowTimerActivations(context.Context, string, string, bool) ([]runtimepipeline.WorkflowTimerActivation, error)
	Snapshot(context.Context, string) (runtimedelivery.Snapshot, error)
}

func TestSingletonStageLifecyclePreservesRouteAndEntityAcrossRestartOnBothBackends(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.ArtifactID("internal/runtime/conformance/testdata/stage-lifecycle-identity"))
	module := loadConformanceWorkflowFixtureModule(t, filepath.Join("testdata", "stage-lifecycle-identity"))

	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (any, stageLifecycleIdentityStore, *sql.DB)
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) (any, stageLifecycleIdentityStore, *sql.DB) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected := storetest.AdmitPostgresRuntimeStore(t, db)
				return selected, selected, db
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (any, stageLifecycleIdentityStore, *sql.DB) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				return selected, selected, storetest.DatabaseForTest(selected)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected, lifecycleStore, db := tc.setup(t)
			runtime := newStageLifecycleIdentityRuntime(t, selected, module)
			startStageLifecycleIdentityRuntime(t, runtime)
			_, activations, err := runtime.EnsureStandingTargets(testAuthorActivityContext(context.Background()))
			if err != nil {
				t.Fatalf("materialize authored standing singleton: %v", err)
			}
			if len(activations) != 1 || !activations[0].Created {
				t.Fatalf("standing activations = %#v, want one newly created singleton", activations)
			}
			activation := activations[0]
			if activation.FlowInstance != "scout" || activation.InstanceID != "scout" {
				t.Fatalf("singleton route = %q/%q, want scout/scout identity fields", activation.FlowInstance, activation.InstanceID)
			}
			if activation.EntityID == "" || activation.EntityID == activation.FlowInstance {
				t.Fatalf("standing route/entity are not distinguishable: route=%q entity=%q", activation.FlowInstance, activation.EntityID)
			}
			if _, err := uuid.Parse(activation.EntityID); err != nil {
				t.Fatalf("standing entity_id %q is not canonical: %v", activation.EntityID, err)
			}

			runCtx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), activation.RunID)
			route := runtimeflowidentity.RouteForInstancePath(activation.FlowInstance)
			assertStageLifecycleInstanceIdentity(t, runCtx, runtime.Pipeline, route, activation.EntityID, "collecting", "active")

			memberA := uuid.NewString()
			memberB := uuid.NewString()
			const batchID = "batch-distinct-from-scout"
			setupEventID := publishStageLifecycleIdentityEvent(t, runCtx, runtime.Bus, module.source, activation, "scout.setup", map[string]any{
				"member_ids": []string{memberA, memberB},
				"batch_id":   batchID,
			})
			assertStageLifecycleDeliveryRoute(t, runCtx, lifecycleStore, db, setupEventID, activation)
			assertStageLifecycleInstanceIdentity(t, runCtx, runtime.Pipeline, route, activation.EntityID, "review", "active")

			card := loadStageLifecycleIdentityCard(t, runCtx, lifecycleStore, activation)
			assertStageLifecycleGateIdentity(t, card, activation)
			assertStageLifecycleTimerIdentity(t, runCtx, lifecycleStore, activation, true)

			if err := runtime.Shutdown(); err != nil {
				t.Fatalf("shutdown before lifecycle restart: %v", err)
			}
			runtime = newStageLifecycleIdentityRuntime(t, selected, module)
			startStageLifecycleIdentityRuntime(t, runtime)
			_, restored, err := runtime.EnsureStandingTargets(testAuthorActivityContext(context.Background()))
			if err != nil {
				t.Fatalf("restore authored standing singleton: %v", err)
			}
			if len(restored) != 1 || restored[0].Created || restored[0].RunID != activation.RunID || restored[0].EntityID != activation.EntityID {
				t.Fatalf("restored standing activation = %#v, want same persisted route/entity/run", restored)
			}
			card = loadStageLifecycleIdentityCard(t, runCtx, lifecycleStore, activation)
			assertStageLifecycleGateIdentity(t, card, activation)
			assertStageLifecycleTimerIdentity(t, runCtx, lifecycleStore, activation, true)

			decisionEventID := uuid.NewString()
			decidedAt := time.Now().UTC()
			if err := runtime.Pipeline.CommitDecision(runCtx, card, decisionEventID, decidedAt); err != nil {
				t.Fatalf("commit gate decision route: %v", err)
			}
			decided, err := lifecycleStore.DecideDecisionCard(runCtx, decisioncard.DecideRequest{
				CardID: card.CardID, Verdict: "approve", ActorTokenID: "operator",
				ObservedContentHash: card.CardContentHash, DecisionEventID: decisionEventID, Now: decidedAt,
			})
			if err != nil {
				t.Fatalf("decide stage gate: %v", err)
			}
			card = decided.Card
			decisionPayload, err := json.Marshal(map[string]any{
				"card_id": card.CardID, "anchor_kind": card.Anchor.Kind(), "anchor": card.Anchor.SemanticValue().Interface(),
				"decision_id": card.Snapshot.Decision, "verdict": card.Verdict, "card_content_hash": card.CardContentHash,
				"decision_schema_hash": card.DecisionSchemaHash, "bundle_hash": card.BundleHash, "fields": card.Fields.Interface(),
			})
			if err != nil {
				t.Fatal(err)
			}
			decisionEvent := eventtest.RuntimeControl(
				decisionEventID,
				events.EventType("mailbox.card_decided"),
				"platform",
				"",
				decisionPayload,
				0,
				activation.RunID,
				"",
				events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, activation.EntityID), activation.FlowInstance),
				decidedAt,
			)
			if err := runtime.Bus.PublishAcknowledged(runCtx, decisionEvent); err != nil {
				t.Fatalf("publish stage gate decision: %v", err)
			}
			assertStageLifecycleInstanceIdentity(t, runCtx, runtime.Pipeline, route, activation.EntityID, "awaiting", "active")
			assertStageLifecycleTimerIdentity(t, runCtx, lifecycleStore, activation, false)

			publishStageLifecycleIdentityEvent(t, runCtx, runtime.Bus, module.source, activation, "scout.member.done", map[string]any{
				"member_id": memberB, "batch_id": batchID, "value": 22,
			})
			instance, join := waitStageLifecycleJoin(t, runCtx, runtime.Pipeline, route, activation.EntityID, batchID, 1, "awaiting", "active")
			if join.Status != joinruntime.StatusOpen || join.Completed() != 1 || join.Expected() != 2 {
				t.Fatalf("partial singleton join = %#v, want open 1/2", join)
			}

			publishStageLifecycleIdentityEvent(t, runCtx, runtime.Bus, module.source, activation, "scout.member.done", map[string]any{
				"member_id": memberA, "batch_id": batchID, "value": 11,
			})
			// Standing singletons remain active service instances at terminal stages;
			// template-flow deactivation is proved separately at the typed terminal owner.
			instance = assertStageLifecycleInstanceIdentity(t, runCtx, runtime.Pipeline, route, activation.EntityID, "complete", "active")
			join = loadStageLifecycleJoin(t, instance, batchID)
			if join.Status != joinruntime.StatusClosed || join.CloseReason != joinruntime.CloseReasonComplete {
				t.Fatalf("completed singleton join = %#v, want closed/complete", join)
			}
			if results := join.Results(); len(results) != 2 || results[0] != float64(11) || results[1] != float64(22) {
				t.Fatalf("singleton join results = %#v, want authored membership order [11 22]", results)
			}
		})
	}
}

func newStageLifecycleIdentityRuntime(t *testing.T, selected any, module conformanceLoadedWorkflowModule) *runtimepkg.Runtime {
	t.Helper()
	cfg := &config.Config{
		LLM:     config.LLMConfig{Backend: "anthropic"},
		Runtime: config.RuntimeConfig{ExecutionPosture: executionposture.Live},
	}
	base := runtimepkg.RuntimeDeps{
		Config: cfg,
		Options: testAuthorActivityRuntimeOptions(t, runtimepkg.RuntimeOptions{
			SelfCheck: false, WorkflowModule: module, LLMRuntime: conformanceNoopLLMRuntime{},
			RuntimeInstanceID: authorActivityTestRuntimeInstanceID, BundleSourceFact: authorActivityTestBundleSourceFact,
		}),
	}
	switch store := selected.(type) {
	case *store.PostgresStore:
		base = stageLifecycleIdentityPostgresDeps(base, store)
	case *store.SQLiteRuntimeStore:
		base = stageLifecycleIdentitySQLiteDeps(base, store)
	default:
		t.Fatalf("unsupported lifecycle identity store %T", selected)
	}
	runtime, err := runtimepkg.NewRuntime(testAuthorActivityContext(context.Background()), base)
	if err != nil {
		t.Fatalf("build stage lifecycle runtime: %v", err)
	}
	if err := runtime.PrepareAuthorActivityCatalog(); err != nil {
		t.Fatalf("prepare stage lifecycle author activity catalog: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Shutdown() })
	return runtime
}

func startStageLifecycleIdentityRuntime(t *testing.T, runtime *runtimepkg.Runtime) {
	t.Helper()
	if err := runtime.Start(testAuthorActivityContext(context.Background())); err != nil {
		t.Fatalf("start stage lifecycle runtime: %v", err)
	}
}

func stageLifecycleIdentityPostgresDeps(deps runtimepkg.RuntimeDeps, selected *store.PostgresStore) runtimepkg.RuntimeDeps {
	deps.WorkflowPersistence = runtimepipeline.NewWorkflowPersistence(selected)
	deps.EventStore = selected
	deps.EventBusDurable = conformanceDurableEventBusDependencies(selected)
	deps.EventPayloadValidationBinder = selected
	deps.InboundPayloadValidationBinder = selected
	deps.AuthorActivityRegistrars = []runtimepkg.AuthorActivityCatalogRegistrar{selected}
	deps.RunBundleAvailability = selected
	deps.RunControlStore = selected
	deps.RunLifecycleCandidates = selected
	deps.RuntimeLogStore = selected
	deps.SessionRegistry = selected
	deps.LiveSessionAcquirer = selected
	deps.SessionResetter = selected
	deps.ManagerStore = selected
	deps.ManagerLifecycleStore = selected
	deps.ManagerLifecycleDiagnostics = selected
	deps.ManagerPersistenceRoles = runtimemanager.PersistenceRoles{
		LifecycleState: selected, LifecycleEffects: selected, LifecycleDiagnostics: selected,
		EffectsRecovery: selected, DeliveryQuiescence: selected, EventExistence: selected,
		DirectiveOperations: selected, DirectiveTargets: selected, FlowRoutes: selected,
	}
	deps.EffectsStore = selected
	deps.CompletionStore = selected
	deps.CompletionHeartbeatStore = selected
	deps.EffectsRecoveryStore = selected
	deps.ManagedCapabilitiesStore = selected
	deps.DeliveryStore = selected
	deps.PipelineObligations = selected.PipelineObligations()
	deps.GenericScheduleStore = selected
	deps.TimerObligationReader = selected
	deps.MailboxMaterializer = selected
	deps.DecisionCards = selected
	deps.ProposedEffects = selected
	deps.DecisionCardHumanTasks = selected
	deps.DecisionCardDraftExpiry = selected
	deps.HumanTaskExpiry = selected
	deps.StartupOwnership = selected
	deps.MailboxStore = selected
	deps.ToolEntityStore = selected
	deps.HumanTaskStore = selected
	deps.BudgetSpendStore = selected
	deps.RuntimeIngressStore = selected
	return deps
}

func stageLifecycleIdentitySQLiteDeps(deps runtimepkg.RuntimeDeps, selected *store.SQLiteRuntimeStore) runtimepkg.RuntimeDeps {
	deps.WorkflowPersistence = runtimepipeline.NewWorkflowPersistence(selected)
	deps.EventStore = selected
	deps.EventBusDurable = conformanceDurableEventBusDependencies(selected)
	deps.EventPayloadValidationBinder = selected
	deps.InboundPayloadValidationBinder = selected
	deps.AuthorActivityRegistrars = []runtimepkg.AuthorActivityCatalogRegistrar{selected}
	deps.RunBundleAvailability = selected
	deps.RunControlStore = selected
	deps.RunLifecycleCandidates = selected
	deps.RuntimeLogStore = selected
	deps.SessionRegistry = selected
	deps.LiveSessionAcquirer = selected
	deps.SessionResetter = selected
	deps.ManagerStore = selected
	deps.ManagerLifecycleStore = selected
	deps.ManagerLifecycleDiagnostics = selected
	deps.ManagerPersistenceRoles = runtimemanager.PersistenceRoles{
		LifecycleState: selected, LifecycleEffects: selected, LifecycleDiagnostics: selected,
		EffectsRecovery: selected, DeliveryQuiescence: selected, EventExistence: selected,
		DirectiveOperations: selected, DirectiveTargets: selected, FlowRoutes: selected,
	}
	deps.EffectsStore = selected
	deps.CompletionStore = selected
	deps.CompletionHeartbeatStore = selected
	deps.EffectsRecoveryStore = selected
	deps.ManagedCapabilitiesStore = selected
	deps.DeliveryStore = selected
	deps.PipelineObligations = selected.PipelineObligations()
	deps.GenericScheduleStore = selected
	deps.TimerObligationReader = selected
	deps.MailboxMaterializer = selected
	deps.DecisionCards = selected
	deps.ProposedEffects = selected
	deps.DecisionCardHumanTasks = selected
	deps.DecisionCardDraftExpiry = selected
	deps.HumanTaskExpiry = selected
	deps.StartupOwnership = selected
	deps.MailboxStore = selected
	deps.ToolEntityStore = selected
	deps.HumanTaskStore = selected
	deps.BudgetSpendStore = selected
	deps.RuntimeIngressStore = selected
	return deps
}

func publishStageLifecycleIdentityEvent(t *testing.T, ctx context.Context, bus *runtimebus.EventBus, source semanticview.Source, activation runtimepkg.StandingActivation, localEvent string, payload map[string]any) string {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s: %v", localEvent, err)
	}
	eventID := uuid.NewString()
	evt := eventtest.ExistingRunRootIngress(
		eventID,
		events.EventType(source.ResolveFlowEventReference("scout", localEvent)),
		"scout",
		"",
		raw,
		0,
		activation.RunID,
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{
			FlowID: "scout", FlowInstance: activation.FlowInstance, EntityID: activation.EntityID,
		}),
		time.Now().UTC(),
	)
	if err := bus.PublishAcknowledged(ctx, evt); err != nil {
		t.Fatalf("publish %s: %v", localEvent, err)
	}
	return eventID
}

func assertStageLifecycleDeliveryRoute(t *testing.T, ctx context.Context, selected stageLifecycleIdentityStore, db *sql.DB, eventID string, activation runtimepkg.StandingActivation) {
	t.Helper()
	routes, err := selected.ListEventDeliveryRoutes(ctx, eventID)
	if err != nil {
		t.Fatalf("list authored singleton delivery routes: %v", err)
	}
	targetRoute := events.RouteIdentity{}
	if len(routes) == 1 {
		targetRoute = routes[0].Target.Route()
	}
	if len(routes) != 1 || targetRoute.FlowInstance != activation.FlowInstance || targetRoute.EntityID != activation.EntityID {
		t.Fatalf("authored singleton delivery routes = %#v, want one route to %q/%q", routes, activation.FlowInstance, activation.EntityID)
	}
	deliveryID, err := runtimedelivery.DeliveryID(eventID, routes[0])
	if err != nil {
		t.Fatalf("derive authored singleton delivery id: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	var snapshot runtimedelivery.Snapshot
	for time.Now().Before(deadline) {
		snapshot, err = selected.Snapshot(ctx, deliveryID)
		if err == nil && snapshot.Terminal() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil || snapshot.Status != runtimedelivery.StatusDelivered {
		failure := ""
		var failureAttributes map[string]any
		if snapshot.Failure != nil {
			failure = snapshot.Failure.Message + " " + snapshot.Failure.Detail.Code
			failureAttributes = snapshot.Failure.Detail.Attributes
			if cause, ok := snapshot.Failure.Detail.Attributes["cause"].(string); ok {
				failure += ": " + cause
			}
		}
		t.Fatalf("authored singleton delivery status=%s reason=%s failure=%q attributes=%#v logs=%s err=%v, want delivered", snapshot.Status, snapshot.ReasonCode, failure, failureAttributes, stageLifecycleRuntimeLogs(db), err)
	}
}

func stageLifecycleRuntimeLogs(db *sql.DB) string {
	if db == nil {
		return ""
	}
	rows, err := db.Query(`SELECT payload FROM events WHERE event_name = 'platform.runtime_log' ORDER BY created_at DESC LIMIT 20`)
	if err != nil {
		return err.Error()
	}
	defer rows.Close()
	var logs []string
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return err.Error()
		}
		logs = append(logs, payload)
	}
	raw, _ := json.Marshal(logs)
	return string(raw)
}

func assertStageLifecycleInstanceIdentity(t *testing.T, ctx context.Context, pipeline *runtimepipeline.PipelineCoordinator, route runtimeflowidentity.Route, entityID, state, status string) runtimepipeline.WorkflowInstance {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last runtimepipeline.WorkflowInstance
	for time.Now().Before(deadline) {
		instance, ok, err := pipeline.Load(ctx, route)
		if err == nil && ok {
			last = instance
			if instance.StorageRef != route.InstancePath || instance.InstanceID != route.InstanceID || instance.Metadata["entity_id"] != entityID {
				t.Fatalf("persisted lifecycle identity = route:%q instance:%q entity:%v, want %q/%q/%q", instance.StorageRef, instance.InstanceID, instance.Metadata["entity_id"], route.InstancePath, route.InstanceID, entityID)
			}
			if instance.CurrentState == state && instance.Status == status {
				return instance
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("lifecycle state/status = %q/%q, want %q/%q", last.CurrentState, last.Status, state, status)
	return runtimepipeline.WorkflowInstance{}
}

func waitStageLifecycleJoin(t *testing.T, ctx context.Context, pipeline *runtimepipeline.PipelineCoordinator, route runtimeflowidentity.Route, entityID, batchID string, completed int, state, status string) (runtimepipeline.WorkflowInstance, joinruntime.Activation) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last runtimepipeline.WorkflowInstance
	for time.Now().Before(deadline) {
		instance, ok, err := pipeline.Load(ctx, route)
		if err == nil && ok {
			last = instance
			if instance.StorageRef != route.InstancePath || instance.Metadata["entity_id"] != entityID {
				t.Fatalf("persisted join identity = route:%q entity:%v, want %q/%q", instance.StorageRef, instance.Metadata["entity_id"], route.InstancePath, entityID)
			}
			activation, found := findStageLifecycleJoin(instance, batchID)
			if found && activation.Completed() == completed && instance.CurrentState == state && instance.Status == status {
				return instance, activation
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("singleton join did not reach completed=%d state=%s status=%s; last instance=%#v", completed, state, status, last)
	return runtimepipeline.WorkflowInstance{}, joinruntime.Activation{}
}

func loadStageLifecycleIdentityCard(t *testing.T, ctx context.Context, selected stageLifecycleIdentityStore, activation runtimepkg.StandingActivation) decisioncard.Card {
	t.Helper()
	items, _, err := selected.ListDecisionCards(ctx, decisioncard.ListOptions{
		RunID: activation.RunID, Limit: 10,
	})
	if err != nil || len(items) != 1 || items[0].Status != decisioncard.StatusPending {
		t.Fatalf("stage gate cards = %#v err=%v, want one pending card", items, err)
	}
	card, err := selected.GetDecisionCard(ctx, items[0].CardID)
	if err != nil {
		t.Fatalf("load stage gate card: %v", err)
	}
	return card
}

func assertStageLifecycleGateIdentity(t *testing.T, card decisioncard.Card, activation runtimepkg.StandingActivation) {
	t.Helper()
	anchor, err := card.Anchor.StageGate()
	if err != nil {
		t.Fatalf("decode stage gate anchor: %v", err)
	}
	if anchor.Route.InstancePath != activation.FlowInstance || anchor.EntityID != activation.EntityID {
		t.Fatalf("gate anchor identity = route:%q entity:%q, want %q/%q", anchor.Route.InstancePath, anchor.EntityID, activation.FlowInstance, activation.EntityID)
	}
	scope, err := card.Anchor.Scope()
	if err != nil {
		t.Fatalf("decode stage gate scope: %v", err)
	}
	if scope.EntityID != activation.EntityID || scope.FlowInstance != activation.FlowInstance {
		t.Fatalf("gate card scope identity = entity:%q route:%q, want %q/%q", scope.EntityID, scope.FlowInstance, activation.EntityID, activation.FlowInstance)
	}
}

func assertStageLifecycleTimerIdentity(t *testing.T, ctx context.Context, selected stageLifecycleIdentityStore, activation runtimepkg.StandingActivation, wantActive bool) {
	t.Helper()
	timers, err := selected.ListWorkflowTimerActivations(ctx, activation.RunID, activation.EntityID, true)
	if err != nil {
		t.Fatalf("list workflow timer activations: %v", err)
	}
	if wantActive {
		if len(timers) != 1 || timers[0].Route.InstancePath != activation.FlowInstance || timers[0].EntityID != activation.EntityID {
			t.Fatalf("active timer identity = %#v, want one exact route/entity timer", timers)
		}
		return
	}
	if len(timers) != 0 {
		t.Fatalf("active timers after gate stage exit = %#v, want none", timers)
	}
}

func loadStageLifecycleJoin(t *testing.T, instance runtimepipeline.WorkflowInstance, batchID string) joinruntime.Activation {
	t.Helper()
	activation, ok := findStageLifecycleJoin(instance, batchID)
	if !ok {
		t.Fatalf("load singleton join %q: activation is missing", batchID)
	}
	return activation
}

func findStageLifecycleJoin(instance runtimepipeline.WorkflowInstance, batchID string) (joinruntime.Activation, bool) {
	carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
	if err != nil {
		return joinruntime.Activation{}, false
	}
	key := joinruntime.ActivationKey("awaiting", "awaiting", batchID)
	activation, ok, err := joinruntime.Load(carrier.StateBuckets, "scout-coordinator", key)
	if err != nil || !ok {
		return joinruntime.Activation{}, false
	}
	return activation, true
}
