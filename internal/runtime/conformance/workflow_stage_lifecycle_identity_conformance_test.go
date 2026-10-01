package conformance

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type stageLifecycleIdentityStore interface {
	decisioncard.Store
	ListEventDeliveryRoutes(context.Context, string) ([]events.DeliveryRoute, error)
	ListFlowInstanceRoutes(context.Context) ([]runtimeflowidentity.RunScopedFlowInstance, error)
	ListWorkflowTimerActivations(context.Context, string, string, bool) ([]runtimepipeline.WorkflowTimerActivation, error)
	Snapshot(context.Context, string) (runtimedelivery.Snapshot, error)
}

type stageLifecycleIdentityActivation struct {
	RunID        string
	FlowInstance string
	InstanceID   string
	EntityID     string
}

type stageLifecycleIdentityPublicationStore struct {
	runtimebus.EventStore
	runtimepipeline.WorkflowInstancePersistenceReader
	owner runtimebus.CommitPublicationOwner
	mu    sync.Mutex
	items map[string]stageLifecycleIdentityPublication
}

type stageLifecycleIdentityPublication struct {
	committed runtimebus.CommittedPublication
	initial   []runtimepipeline.WorkflowInstance
	err       error
}

func (s *stageLifecycleIdentityPublicationStore) CommitPublication(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	committed, err := s.owner.CommitPublication(ctx, command)
	if err == nil && committed.Acknowledged {
		observed := stageLifecycleIdentityPublication{committed: committed}
		// Read the committed initial entry before EventBus dispatches its setup handler.
		for _, activation := range committed.Activations {
			identity, readErr := activation.Plan.Readiness.FlowIdentity()
			if readErr != nil {
				observed.err = readErr
				break
			}
			instance, found, readErr := s.LoadWorkflowInstance(ctx, identity)
			if readErr != nil || !found {
				observed.err = readErr
				break
			}
			observed.initial = append(observed.initial, instance)
		}
		s.mu.Lock()
		s.items[command.Commit.Event.ID()] = observed
		s.mu.Unlock()
	}
	return committed, err
}

func (s *stageLifecycleIdentityPublicationStore) publication(eventID string) stageLifecycleIdentityPublication {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.items[eventID]
}

func TestKeyedStageLifecyclePreservesRouteAndEntityAcrossRestartOnBothBackends(t *testing.T) {
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
			runtime, processCapability, publications := newStageLifecycleIdentityRuntime(t, selected, module)
			startStageLifecycleIdentityRuntime(t, runtime)
			runtimeCtx := testAuthorActivityContextForBundle(context.Background(), runtime.Options.SourceArtifactFact)
			runID := uuid.NewString()
			runCtx := runtimecorrelation.WithRunID(runtimeCtx, runID)
			memberA := uuid.NewString()
			memberB := uuid.NewString()
			const batchID = "batch-distinct-from-scout"
			setupPayload, err := json.Marshal(map[string]any{"member_ids": []string{memberA, memberB}, "batch_id": batchID})
			if err != nil {
				t.Fatal(err)
			}
			setupEventID := uuid.NewString()
			setupEvent := eventtest.RunCreatingRootIngress(setupEventID,
				events.EventType(module.source.ResolveFlowEventReference(".", "scout.setup")), "operator", "", setupPayload, 0,
				runID, "", events.EventEnvelope{}, time.Now().UTC())
			if err := runtime.Bus.PublishAcknowledged(runCtx, setupEvent); err != nil {
				t.Fatalf("publish keyed scout setup: %v", err)
			}
			activation, initial, flowIdentity := discoverStageLifecycleIdentityActivation(t, runCtx, lifecycleStore, publications.publication(setupEventID), runID, batchID)
			assertStageLifecycleEntryIdentity(t, initial, flowIdentity, "construction", "", "")
			if initial.CurrentState != "collecting" || initial.Status != "active" {
				t.Fatalf("committed construction = %#v, want initial collecting/active", initial)
			}
			setupDeliveryID := assertStageLifecycleDeliveryRoute(t, runCtx, lifecycleStore, db, setupEventID, activation)
			instance := assertStageLifecycleInstanceIdentity(t, runCtx, runtime.Pipeline, flowIdentity, activation.EntityID, "review", "active")
			reviewEntry := assertStageLifecycleEntryIdentity(t, instance, flowIdentity, "delivery", setupEventID, setupDeliveryID)

			card := loadStageLifecycleIdentityCard(t, runCtx, lifecycleStore, activation)
			assertStageLifecycleGateIdentity(t, card, activation)
			originalCardID := card.CardID
			originalCardHash := card.CardContentHash
			originalGateAnchor, err := card.Anchor.StageGate()
			if err != nil {
				t.Fatal(err)
			}
			originalTimer := assertStageLifecycleTimerIdentity(t, runCtx, lifecycleStore, activation, true)

			requireStageLifecyclePipelineSettlement(t, runtime, selected, activation.RunID)
			if err := closeConformanceRuntimeGeneration(runtime, processCapability); err != nil {
				t.Fatalf("close generation before lifecycle restart: %v", err)
			}
			runtime, processCapability, _ = newStageLifecycleIdentityRuntime(t, selected, module)
			startStageLifecycleIdentityRuntime(t, runtime)
			runtimeCtx = testAuthorActivityContextForBundle(context.Background(), runtime.Options.SourceArtifactFact)
			runCtx = runtimecorrelation.WithRunID(runtimeCtx, runID)
			assertStageLifecyclePersistedRoute(t, runCtx, lifecycleStore, flowIdentity)
			if deliveryID := assertStageLifecycleDeliveryRoute(t, runCtx, lifecycleStore, db, setupEventID, activation); deliveryID != setupDeliveryID {
				t.Fatal("restart replaced the admitted setup delivery")
			}
			instance = assertStageLifecycleInstanceIdentity(t, runCtx, runtime.Pipeline, flowIdentity, activation.EntityID, "review", "active")
			if entry := assertStageLifecycleEntryIdentity(t, instance, flowIdentity, "delivery", setupEventID, setupDeliveryID); entry != reviewEntry {
				t.Fatal("restart replaced the admitted review stage entry")
			}
			card = loadStageLifecycleIdentityCard(t, runCtx, lifecycleStore, activation)
			assertStageLifecycleGateIdentity(t, card, activation)
			restoredGateAnchor, err := card.Anchor.StageGate()
			if err != nil || card.CardID != originalCardID || card.CardContentHash != originalCardHash || restoredGateAnchor.StageActivationID != originalGateAnchor.StageActivationID {
				t.Fatalf("restart replaced the admitted review gate: card=%#v anchor=%#v err=%v", card, restoredGateAnchor, err)
			}
			restoredTimer := assertStageLifecycleTimerIdentity(t, runCtx, lifecycleStore, activation, true)
			if restoredTimer.Ref.TaskID() != originalTimer.Ref.TaskID() || !restoredTimer.FireAt.Equal(originalTimer.FireAt) || string(restoredTimer.Payload) != string(originalTimer.Payload) {
				t.Fatalf("restart replaced the admitted review timer: original=%#v restored=%#v", originalTimer, restoredTimer)
			}

			decisionEventID := uuid.NewString()
			decidedAt := time.Now().UTC()
			if err := runtime.Pipeline.CommitDecision(runCtx, card, decisionEventID, decidedAt); err != nil {
				t.Fatalf("commit gate decision route: %v", err)
			}
			decided, err := storetest.DecisionCardDomain(lifecycleStore).ApplyDecisionForTest(runCtx, decisioncard.DecideRequest{
				CardID: card.CardID, Verdict: "approve", PrincipalID: "operator",
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
			instance = assertStageLifecycleInstanceIdentity(t, runCtx, runtime.Pipeline, flowIdentity, activation.EntityID, "awaiting", "active")
			awaitingEntry := assertStageLifecycleEntryIdentity(t, instance, flowIdentity, "gate", decisionEventID, card.CardID)
			assertStageLifecycleTimerIdentity(t, runCtx, lifecycleStore, activation, false)

			memberBEventID := publishStageLifecycleIdentityEvent(t, runCtx, runtime.Bus, module.source, activation, "scout.member.done", map[string]any{
				"member_id": memberB, "batch_id": batchID, "value": 22,
			})
			assertStageLifecycleDeliveryRoute(t, runCtx, lifecycleStore, db, memberBEventID, activation)
			instance, join := waitStageLifecycleJoin(t, runCtx, runtime.Pipeline, flowIdentity, activation.EntityID, batchID, 1, "awaiting", "active")
			if join.Status != joinruntime.StatusOpen || join.Completed() != 1 || join.Expected() != 2 {
				t.Fatalf("partial keyed join = %#v, want open 1/2", join)
			}
			retainedJoinRef := join.JoinRef()
			if retainedJoinRef.StageEntry() != awaitingEntry {
				t.Fatal("join arm is not bound to the admitted gate stage entry")
			}

			memberAEventID := publishStageLifecycleIdentityEvent(t, runCtx, runtime.Bus, module.source, activation, "scout.member.done", map[string]any{
				"member_id": memberA, "batch_id": batchID, "value": 11,
			})
			assertStageLifecycleDeliveryRoute(t, runCtx, lifecycleStore, db, memberAEventID, activation)
			instance = assertStageLifecycleInstanceIdentity(t, runCtx, runtime.Pipeline, flowIdentity, activation.EntityID, "complete", "terminated")
			if instance.TerminatedAt.IsZero() {
				t.Fatal("terminal keyed scout has no durable termination occurrence")
			}
			join = loadStageLifecycleJoin(t, runCtx, instance, batchID)
			if !join.JoinRef().Equal(retainedJoinRef) {
				t.Fatal("completion replaced the retained keyed arm")
			}
			if join.Status != joinruntime.StatusClosed || join.CloseReason != joinruntime.CloseReasonComplete {
				t.Fatalf("completed keyed join = %#v, want closed/complete", join)
			}
			results, err := join.Results()
			if err != nil {
				t.Fatalf("read keyed join results: %v", err)
			}
			resultJSON, err := json.Marshal(results)
			if err != nil || string(resultJSON) != "[11,22]" {
				t.Fatalf("keyed join results = %#v err=%v, want authored membership order [11 22]", results, err)
			}
			requireStageLifecyclePipelineSettlement(t, runtime, selected, activation.RunID)
			if err := closeConformanceRuntimeGeneration(runtime, processCapability); err != nil {
				t.Fatalf("close settled lifecycle runtime: %v", err)
			}
		})
	}
}

func requireStageLifecyclePipelineSettlement(t *testing.T, rt *runtimepkg.Runtime, selected any, runID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(testAuthorActivityContextForBundle(context.Background(), rt.Options.SourceArtifactFact), 10*time.Second)
	defer cancel()
	owner := selected.(interface {
		PipelineObligations() runtimepipelineobligation.Store
	}).PipelineObligations()
	var summary runtimepipelineobligation.RunSummary
	for ctx.Err() == nil {
		var err error
		summary, err = owner.SummarizeRun(ctx, runID)
		if err != nil {
			t.Fatalf("read lifecycle pipeline settlement: %v", err)
		}
		if !summary.BlocksCompletion() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("lifecycle pipeline publications remain unsettled: %#v", summary)
}

func newStageLifecycleIdentityRuntime(t *testing.T, selected any, module conformanceLoadedWorkflowModule) (*runtimepkg.Runtime, runtimestartupownership.ProcessCapability, *stageLifecycleIdentityPublicationStore) {
	t.Helper()
	bundle, ok := semanticview.Bundle(module.source)
	if !ok || bundle == nil {
		t.Fatal("stage lifecycle runtime requires a bundle-backed source")
	}
	sourceArtifactFact := conformanceSourceArtifactFact(t, module.source)
	catalogStore, ok := selected.(storetest.DurableDataCatalogStore)
	if !ok {
		t.Fatalf("stage lifecycle store %T does not persist source artifacts", selected)
	}
	runtimeCtx := testAuthorActivityContextForBundle(context.Background(), sourceArtifactFact)
	storetest.RequireBundleDataCatalog(t, runtimeCtx, catalogStore, bundle)
	cfg := &config.Config{
		LLM:     config.LLMConfig{Backend: "anthropic"},
		Runtime: config.RuntimeConfig{RecoveryOnStartup: true},
	}
	base := runtimepkg.RuntimeDeps{
		Config: cfg,
		Options: testAuthorActivityRuntimeOptions(t, runtimepkg.RuntimeOptions{
			ExecutionPosture: executionposture.Live,
			SelfCheck:        false, WorkflowModule: module, LLMRuntime: conformanceNoopLLMRuntime{},
			RuntimeInstanceID: authorActivityTestRuntimeInstanceID, SourceArtifactFact: sourceArtifactFact,
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
	publications := &stageLifecycleIdentityPublicationStore{
		EventStore: base.EventStore, owner: selected.(runtimebus.CommitPublicationOwner),
		WorkflowInstancePersistenceReader: selected.(runtimepipeline.WorkflowInstancePersistenceReader),
		items:                             make(map[string]stageLifecycleIdentityPublication),
	}
	base.EventStore = publications
	runtime, err := runtimepkg.NewRuntime(runtimeCtx, base)
	if err != nil {
		t.Fatalf("build stage lifecycle runtime: %v", err)
	}
	if err := runtime.PrepareAuthorActivityCatalog(); err != nil {
		t.Fatalf("prepare stage lifecycle author activity catalog: %v", err)
	}
	processCapability := installConformanceRuntimeStartupGrant(t, runtimeCtx, selected, runtime)
	t.Cleanup(func() {
		if err := closeConformanceRuntimeGeneration(runtime, processCapability); err != nil {
			t.Errorf("close lifecycle runtime generation: %v", err)
		}
	})
	return runtime, processCapability, publications
}

func startStageLifecycleIdentityRuntime(t *testing.T, runtime *runtimepkg.Runtime) {
	t.Helper()
	ctx := testAuthorActivityContextForBundle(context.Background(), runtime.Options.SourceArtifactFact)
	if err := runtime.Start(ctx); err != nil {
		t.Fatalf("start stage lifecycle runtime: %v", err)
	}
}

func stageLifecycleIdentityPostgresDeps(deps runtimepkg.RuntimeDeps, selected *store.PostgresStore) runtimepkg.RuntimeDeps {
	deps.WorkflowPersistence = runtimepipeline.NewWorkflowPersistence(selected)
	deps.EventStore = selected
	deps.EventBusDurable = conformanceDurableEventBusDependencies(selected)
	deps.EventPayloadAdmissionBinder = selected
	deps.InboundPayloadAdmissionBinder = selected
	deps.AuthorActivityRegistrars = []runtimepkg.AuthorActivityCatalogRegistrar{selected}
	deps.RunBundleAvailability = selected
	deps.RunControlStore = selected
	deps.RunLifecycleCandidates = selected
	deps.RuntimeLogStore = selected
	deps.SessionRegistry = selected
	deps.LiveSessionAcquirer = selected
	deps.SessionResetter = selected
	deps.ManagerStore = selected
	deps.ManagerLifecycleDiagnostics = selected
	deps.ManagerPersistenceRoles = runtimemanager.PersistenceRoles{
		LifecycleCensus: selected, LifecycleState: selected, LifecycleEffects: selected, LifecycleDiagnostics: selected,
		EffectsRecovery: selected, DeliveryQuiescence: selected, EventExistence: selected,
		DirectiveOperations: selected, DirectiveTargets: selected, FlowRoutes: selected, StandingRestarts: selected,
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
	deps.DecisionCards = selected
	deps.ProposedEffects = selected
	deps.DecisionCardHumanTasks = selected
	deps.DecisionCardDraftExpiry = selected
	deps.HumanTaskExpiry = selected
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
	deps.EventPayloadAdmissionBinder = selected
	deps.InboundPayloadAdmissionBinder = selected
	deps.AuthorActivityRegistrars = []runtimepkg.AuthorActivityCatalogRegistrar{selected}
	deps.RunBundleAvailability = selected
	deps.RunControlStore = selected
	deps.RunLifecycleCandidates = selected
	deps.RuntimeLogStore = selected
	deps.SessionRegistry = selected
	deps.LiveSessionAcquirer = selected
	deps.SessionResetter = selected
	deps.ManagerStore = selected
	deps.ManagerLifecycleDiagnostics = selected
	deps.ManagerPersistenceRoles = runtimemanager.PersistenceRoles{
		LifecycleCensus: selected, LifecycleState: selected, LifecycleEffects: selected, LifecycleDiagnostics: selected,
		EffectsRecovery: selected, DeliveryQuiescence: selected, EventExistence: selected,
		DirectiveOperations: selected, DirectiveTargets: selected, FlowRoutes: selected, StandingRestarts: selected,
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
	deps.DecisionCards = selected
	deps.ProposedEffects = selected
	deps.DecisionCardHumanTasks = selected
	deps.DecisionCardDraftExpiry = selected
	deps.HumanTaskExpiry = selected
	deps.MailboxStore = selected
	deps.ToolEntityStore = selected
	deps.HumanTaskStore = selected
	deps.BudgetSpendStore = selected
	deps.RuntimeIngressStore = selected
	return deps
}

func discoverStageLifecycleIdentityActivation(t *testing.T, ctx context.Context, selected stageLifecycleIdentityStore, observed stageLifecycleIdentityPublication, runID, batchID string) (stageLifecycleIdentityActivation, runtimepipeline.WorkflowInstance, runtimeflowidentity.RunScopedFlowInstance) {
	t.Helper()
	publication := observed.committed
	if observed.err != nil || len(observed.initial) != 1 {
		t.Fatalf("committed initial scout = %#v err=%v, want one persisted initial state", observed.initial, observed.err)
	}
	if err := publication.Validate(); err != nil || !publication.Acknowledged || len(publication.Activations) != 1 {
		t.Fatalf("setup publication = %#v err=%v, want one acknowledged creation", publication, err)
	}
	construction := publication.Activations[0]
	if !construction.Created || construction.Plan.Instance.WorkflowName != "scout" {
		t.Fatalf("setup activation = %#v, want one newly committed scout", construction)
	}
	owner, err := construction.Plan.Readiness.FlowIdentity()
	if err != nil || owner.RunID != runID || owner.Route.ScopeKey != "scout" || !strings.HasPrefix(owner.Route.InstanceID, "ti-") || owner.Route.InstanceID == batchID {
		t.Fatalf("keyed scout owner = %#v err=%v, want canonical instance of scout in run %s", owner, err, runID)
	}
	if construction.Plan.Instance.Fields["batch_id"] != batchID {
		t.Fatalf("constructed scout key evidence = %#v, want batch_id %q", construction.Plan.Instance.Fields, batchID)
	}
	activation := stageLifecycleIdentityActivation{
		RunID: runID, FlowInstance: owner.Route.InstancePath,
		InstanceID: owner.Route.InstanceID, EntityID: construction.Plan.Instance.EntityID,
	}
	if activation.EntityID == "" || activation.EntityID == activation.FlowInstance || activation.EntityID == batchID || activation.InstanceID == activation.FlowInstance {
		t.Fatalf("keyed route, instance, entity and business key are not distinguishable: %#v", activation)
	}
	if _, err := uuid.Parse(activation.EntityID); err != nil {
		t.Fatalf("keyed entity_id %q is not canonical: %v", activation.EntityID, err)
	}
	assertStageLifecyclePersistedRoute(t, ctx, selected, owner)
	initial := observed.initial[0]
	if initial.StorageRef != activation.FlowInstance || initial.InstanceID != activation.InstanceID || initial.EntityID != activation.EntityID || initial.Fields["batch_id"] != batchID {
		t.Fatalf("committed initial scout disagrees with its activation and key: %#v, want %#v batch %q", initial, activation, batchID)
	}
	return activation, initial, owner
}

func assertStageLifecyclePersistedRoute(t *testing.T, ctx context.Context, selected stageLifecycleIdentityStore, want runtimeflowidentity.RunScopedFlowInstance) {
	t.Helper()
	routes, err := selected.ListFlowInstanceRoutes(ctx)
	if err != nil {
		t.Fatalf("discover persisted lifecycle routes: %v", err)
	}
	var scouts []runtimeflowidentity.RunScopedFlowInstance
	for _, route := range routes {
		if route.RunID == want.RunID && route.Route.ScopeKey == "scout" {
			scouts = append(scouts, route)
		}
	}
	if len(scouts) != 1 || scouts[0] != want {
		t.Fatalf("persisted scout routes = %#v, want exact %v", scouts, want)
	}
}

func publishStageLifecycleIdentityEvent(t *testing.T, ctx context.Context, bus *runtimebus.EventBus, source semanticview.Source, activation stageLifecycleIdentityActivation, localEvent string, payload map[string]any) string {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal %s: %v", localEvent, err)
	}
	eventID := uuid.NewString()
	evt := eventtest.ExistingRunRootIngressWithRoutingSource(
		eventID,
		events.EventType(source.ResolveFlowEventReference(".", localEvent)),
		"operator",
		"",
		raw,
		0,
		activation.RunID,
		events.EventEnvelope{},
		eventtest.RootRoutingSource(activation.RunID),
		time.Now().UTC(),
	)
	if err := bus.PublishAcknowledged(ctx, evt); err != nil {
		t.Fatalf("publish %s: %v", localEvent, err)
	}
	return eventID
}

func assertStageLifecycleDeliveryRoute(t *testing.T, ctx context.Context, selected stageLifecycleIdentityStore, db *sql.DB, eventID string, activation stageLifecycleIdentityActivation) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var (
		routes []events.DeliveryRoute
		err    error
	)
	for time.Now().Before(deadline) {
		routes, err = selected.ListEventDeliveryRoutes(ctx, eventID)
		if err == nil && len(routes) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	targetRoute := events.RouteIdentity{}
	if len(routes) == 1 {
		targetRoute = routes[0].Target.Route()
	}
	if len(routes) != 1 || targetRoute.FlowInstance != activation.FlowInstance || targetRoute.EntityID != activation.EntityID {
		t.Fatalf("authored keyed delivery routes = %#v, want one route to %q/%q", routes, activation.FlowInstance, activation.EntityID)
	}
	deliveryID, err := runtimedelivery.DeliveryID(eventID, routes[0])
	if err != nil {
		t.Fatalf("derive authored keyed delivery id: %v", err)
	}
	deadline = time.Now().Add(10 * time.Second)
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
		t.Fatalf("authored keyed delivery status=%s reason=%s failure=%q attributes=%#v logs=%s err=%v, want delivered", snapshot.Status, snapshot.ReasonCode, failure, failureAttributes, stageLifecycleRuntimeLogs(db), err)
	}
	return deliveryID
}

func assertStageLifecycleEntryIdentity(t *testing.T, instance runtimepipeline.WorkflowInstance, owner runtimeflowidentity.RunScopedFlowInstance, cause, eventID, occurrenceID string) timeridentity.StageEntryRef {
	t.Helper()
	entry, found, err := workflowlifecycle.LoadStageEntry(instance.Bookkeeping)
	if err != nil || !found {
		t.Fatalf("load committed lifecycle entry: found=%v err=%v", found, err)
	}
	if err := entry.RequireOwner(owner.RunID, owner.Route.ScopeKey, owner.Route.InstanceID, owner.Route.InstancePath, instance.EntityID, instance.CurrentState); err != nil {
		t.Fatalf("committed lifecycle entry owner: %v", err)
	}
	if entry.Cause != cause || entry.EventID != eventID || entry.OccurrenceID != occurrenceID || entry.OriginRunID != "" {
		t.Fatalf("committed lifecycle entry = %+v, want local %s event=%q occurrence=%q", entry, cause, eventID, occurrenceID)
	}
	if cause == "construction" {
		if len(instance.TransitionHistory) != 0 {
			t.Fatal("initial construction borrowed transition evidence")
		}
	} else {
		if len(instance.TransitionHistory) == 0 {
			t.Fatal("admitted stage entry has no committed transition")
		}
		transition := instance.TransitionHistory[len(instance.TransitionHistory)-1]
		if transition.TriggerEventID != eventID || transition.To != entry.Stage || transition.TransitionID != entry.TransitionID || transition.Evidence.ID() != entry.TransitionID {
			t.Fatalf("admitted stage entry disagrees with committed transition: entry=%+v transition=%+v", entry, transition)
		}
	}
	return entry
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

func assertStageLifecycleInstanceIdentity(t *testing.T, ctx context.Context, pipeline *runtimepipeline.PipelineCoordinator, identity runtimeflowidentity.RunScopedFlowInstance, entityID, state, status string) runtimepipeline.WorkflowInstance {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last runtimepipeline.WorkflowInstance
	for time.Now().Before(deadline) {
		instance, ok, err := pipeline.Load(ctx, identity)
		if err == nil && ok {
			last = instance
			if instance.StorageRef != identity.Route.InstancePath || instance.InstanceID != identity.Route.InstanceID || instance.EntityID != entityID {
				t.Fatalf("persisted lifecycle identity = route:%q instance:%q entity:%v, want %q/%q/%q", instance.StorageRef, instance.InstanceID, instance.EntityID, identity.Route.InstancePath, identity.Route.InstanceID, entityID)
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

func waitStageLifecycleJoin(t *testing.T, ctx context.Context, pipeline *runtimepipeline.PipelineCoordinator, identity runtimeflowidentity.RunScopedFlowInstance, entityID, batchID string, completed int, state, status string) (runtimepipeline.WorkflowInstance, joinruntime.Activation) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last runtimepipeline.WorkflowInstance
	for time.Now().Before(deadline) {
		instance, ok, err := pipeline.Load(ctx, identity)
		if err == nil && ok {
			last = instance
			if instance.StorageRef != identity.Route.InstancePath || instance.EntityID != entityID {
				t.Fatalf("persisted join identity = route:%q entity:%v, want %q/%q", instance.StorageRef, instance.EntityID, identity.Route.InstancePath, entityID)
			}
			activation, found := findStageLifecycleJoin(t, ctx, instance, batchID)
			if found && activation.Completed() == completed && instance.CurrentState == state && instance.Status == status {
				return instance, activation
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("keyed join did not reach completed=%d state=%s status=%s; last instance=%#v", completed, state, status, last)
	return runtimepipeline.WorkflowInstance{}, joinruntime.Activation{}
}

func loadStageLifecycleIdentityCard(t *testing.T, ctx context.Context, selected stageLifecycleIdentityStore, activation stageLifecycleIdentityActivation) decisioncard.Card {
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

func assertStageLifecycleGateIdentity(t *testing.T, card decisioncard.Card, activation stageLifecycleIdentityActivation) {
	t.Helper()
	anchor, err := card.Anchor.StageGate()
	if err != nil {
		t.Fatalf("decode stage gate anchor: %v", err)
	}
	if anchor.Route.ScopeKey != "scout" || anchor.Route.InstanceID != activation.InstanceID || anchor.Route.InstancePath != activation.FlowInstance || anchor.EntityID != activation.EntityID {
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

func assertStageLifecycleTimerIdentity(t *testing.T, ctx context.Context, selected stageLifecycleIdentityStore, activation stageLifecycleIdentityActivation, wantActive bool) runtimepipeline.WorkflowTimerActivation {
	t.Helper()
	timers, err := selected.ListWorkflowTimerActivations(ctx, activation.RunID, activation.EntityID, true)
	if err != nil {
		t.Fatalf("list workflow timer activations: %v", err)
	}
	if wantActive {
		if len(timers) != 1 || timers[0].RunID != activation.RunID || timers[0].Route.ScopeKey != "scout" || timers[0].Route.InstanceID != activation.InstanceID || timers[0].Route.InstancePath != activation.FlowInstance || timers[0].EntityID != activation.EntityID {
			t.Fatalf("active timer identity = %#v, want one exact route/entity timer", timers)
		}
		if err := timers[0].Validate(); err != nil {
			t.Fatalf("active review timer is not a valid canonical activation: %v", err)
		}
		return timers[0]
	}
	if len(timers) != 0 {
		t.Fatalf("active timers after gate stage exit = %#v, want none", timers)
	}
	return runtimepipeline.WorkflowTimerActivation{}
}

func loadStageLifecycleJoin(t *testing.T, ctx context.Context, instance runtimepipeline.WorkflowInstance, batchID string) joinruntime.Activation {
	t.Helper()
	activation, ok := findStageLifecycleJoin(t, ctx, instance, batchID)
	if !ok {
		t.Fatalf("load keyed join %q: activation is missing", batchID)
	}
	return activation
}

func findStageLifecycleJoin(t *testing.T, ctx context.Context, instance runtimepipeline.WorkflowInstance, batchID string) (joinruntime.Activation, bool) {
	t.Helper()
	if instance.Fields["batch_id"] != batchID {
		t.Fatalf("keyed business batch = %#v, want %q", instance.Fields["batch_id"], batchID)
	}
	return findConformanceJoinActivation(t, ctx, instance, conformanceFlowPathNode(t, "scout", "scout-coordinator"), "scout.member.done", "awaiting", "awaiting")
}
