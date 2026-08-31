package cataloge2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtime "github.com/division-sh/swarm/internal/runtime"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	runlifecyclefixture "github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

const (
	catalogRuntimeRunID          = "88888888-8888-8888-8888-888888888888"
	catalogRuntimePublishTimeout = 20 * time.Second
)

type catalogRuntimeBackend string

const (
	catalogBackendPostgres catalogRuntimeBackend = "postgres"
	catalogBackendSQLite   catalogRuntimeBackend = "sqlite"
)

type catalogTriggerStep struct {
	Event                         string                   `yaml:"event"`
	Payload                       map[string]any           `yaml:"payload"`
	Target                        catalogTriggerTarget     `yaml:"target"`
	BarrierBefore                 catalogTranscriptBarrier `yaml:"barrier_before"`
	AssertPersistedBeforeDelivery bool                     `yaml:"assert_persisted_before_delivery"`
	ErrorContains                 string                   `yaml:"error_contains"`
	ReceiptOutcome                string                   `yaml:"receipt_outcome"`
	ReceiptFailureClass           string                   `yaml:"receipt_failure_class"`
	ReceiptFailureDetail          string                   `yaml:"receipt_failure_detail"`
	ReceiptFailureAttributes      map[string]any           `yaml:"receipt_failure_attributes"`
	inputKind                     string
	eventID                       string
	createdAt                     time.Time
	sourceAgent                   string
	excludeFromEmitted            bool
}

type catalogTriggerTarget struct {
	FlowID       string `yaml:"flow_id"`
	FlowInstance string `yaml:"flow_instance"`
	EntityID     string `yaml:"entity_id"`
}

func (t catalogTriggerTarget) route() (events.RouteIdentity, bool) {
	route := events.RouteIdentity{
		FlowID:       strings.TrimSpace(t.FlowID),
		FlowInstance: strings.Trim(strings.TrimSpace(t.FlowInstance), "/"),
		EntityID:     strings.TrimSpace(t.EntityID),
	}
	if route.FlowID == "" && route.FlowInstance == "" && route.EntityID == "" {
		return events.RouteIdentity{}, false
	}
	return route, route.FlowID != "" && route.FlowInstance != "" && route.EntityID != ""
}

type catalogExpectedDocument struct {
	Trigger struct {
		Boot                          bool                 `yaml:"boot"`
		Event                         string               `yaml:"event"`
		Concurrent                    []catalogTriggerStep `yaml:"concurrent"`
		Payload                       map[string]any       `yaml:"payload"`
		Sequence                      []catalogTriggerStep `yaml:"sequence"`
		ErrorContains                 string               `yaml:"error_contains"`
		Entity                        map[string]any       `yaml:"entity"`
		EntityStateBefore             string               `yaml:"entity_state_before"`
		EntityFieldsBefore            map[string]any       `yaml:"entity_fields_before"`
		GatesBefore                   map[string]bool      `yaml:"gates_before"`
		AssertPersistedBeforeDelivery bool                 `yaml:"assert_persisted_before_delivery"`
	} `yaml:"trigger"`
	Expected struct {
		BootResult          string                           `yaml:"boot_result"`
		HandlerOutcome      string                           `yaml:"handler_outcome"`
		EntityState         string                           `yaml:"entity_state"`
		ParentState         string                           `yaml:"parent_state"`
		FlowBState          string                           `yaml:"flow_b_state"`
		FlowEntities        map[string]catalogEntityExpected `yaml:"flow_entities"`
		EntityFields        map[string]any                   `yaml:"entity_fields"`
		Gates               map[string]bool                  `yaml:"gates"`
		EmittedEvents       []string                         `yaml:"emitted_events"`
		CausalEvents        []string                         `yaml:"causal_events"`
		AgentReceived       map[string][]string              `yaml:"agent_received"`
		DeadLetter          bool                             `yaml:"dead_letter"`
		ChainDepthExceeded  bool                             `yaml:"chain_depth_exceeded"`
		TemplateInstances   *int                             `yaml:"template_instances"`
		FlowInstanceCreated map[string]any                   `yaml:"flow_instance_created"`
		Entities            map[string]catalogEntityExpected `yaml:"entities"`
	} `yaml:"expected"`
}

type catalogEntityExpected struct {
	HandlerOutcome string          `yaml:"handler_outcome"`
	Exists         *bool           `yaml:"exists"`
	EntityState    string          `yaml:"entity_state"`
	EntityFields   map[string]any  `yaml:"entity_fields"`
	Gates          map[string]bool `yaml:"gates"`
	EmittedEvents  []string        `yaml:"emitted_events"`
	CausalEvents   []string        `yaml:"causal_events"`
	DeadLetter     bool            `yaml:"dead_letter"`
}

func (d catalogExpectedDocument) triggerSequence() []catalogTriggerStep {
	if len(d.Trigger.Concurrent) > 0 {
		return nil
	}
	if len(d.Trigger.Sequence) > 0 {
		return append([]catalogTriggerStep(nil), d.Trigger.Sequence...)
	}
	if strings.TrimSpace(d.Trigger.Event) == "" {
		return nil
	}
	return []catalogTriggerStep{{
		Event:                         strings.TrimSpace(d.Trigger.Event),
		Payload:                       cloneStringAnyMap(d.Trigger.Payload),
		AssertPersistedBeforeDelivery: d.Trigger.AssertPersistedBeforeDelivery,
		ErrorContains:                 strings.TrimSpace(d.Trigger.ErrorContains),
	}}
}

func (d catalogExpectedDocument) triggerFlowPrefix() string {
	for _, step := range d.triggerSequence() {
		eventName := strings.Trim(strings.TrimSpace(step.Event), "/")
		if eventName == "" {
			continue
		}
		lastSlash := strings.LastIndex(eventName, "/")
		if lastSlash <= 0 {
			continue
		}
		return strings.Trim(eventName[:lastSlash], "/")
	}
	return ""
}

type runtimeHarness struct {
	t               *testing.T
	backend         catalogRuntimeBackend
	ctx             context.Context
	cancel          context.CancelFunc
	db              *sql.DB
	pg              *store.PostgresStore
	sqlite          *store.SQLiteRuntimeStore
	reopenedSQLite  *store.SQLiteRuntimeStore
	rt              *runtime.Runtime
	processOwner    *worklifetime.Process
	processTopology runtimestartupownership.ProcessCapability
	workflow        catalogWorkflowPersistence
	llm             *scriptedLLMRuntime
	bundle          *runtimecontracts.WorkflowContractBundle
	initialState    string
	startedAt       time.Time
	publishedIDs    map[string]struct{}
	publishedOrder  []string
	eventEntityIDs  map[string]string
	previews        map[string]runtimepipeline.HandlerPreview
	shutdownOnce    sync.Once
	mu              sync.Mutex
}

type catalogWorkflowPersistence interface {
	Load(context.Context, runtimeflowidentity.Route) (runtimepipeline.WorkflowInstance, bool, error)
	ListWorkflowInstances(context.Context) ([]runtimepipeline.WorkflowInstance, error)
	MaterializeInitialEntry(context.Context, runtimepipeline.WorkflowInstance, time.Time) (runtimepipeline.WorkflowInitialMaterializationResult, error)
}

func catalogExactWorkflowRoute(instancePath string) runtimeflowidentity.Route {
	return runtimeflowidentity.RouteForInstancePath(instancePath)
}

func catalogRootWorkflowRoute() runtimeflowidentity.Route {
	return catalogExactWorkflowRoute(catalogRuntimeRunID)
}

type agentFixtureDoc struct {
	AgentFixtures map[string][]agentFixtureStep `yaml:"agent_fixtures"`
}

type agentFixtureStep struct {
	On    string             `yaml:"on"`
	Emits []agentFixtureEmit `yaml:"emits"`
}

type agentFixtureEmit struct {
	Event   string         `yaml:"event"`
	Payload map[string]any `yaml:"payload"`
}

func newRuntimeHarness(t *testing.T, fixtureRoot string, start bool) *runtimeHarness {
	return newRuntimeHarnessForBackend(t, fixtureRoot, catalogBackendPostgres, start)
}

func newRuntimeHarnessForBackend(t *testing.T, fixtureRoot string, backend catalogRuntimeBackend, start bool) *runtimeHarness {
	return newRuntimeHarnessFromTranscript(t, fixtureRoot, backend, start, nil)
}

func newRuntimeHarnessFromTranscript(t *testing.T, fixtureRoot string, backend catalogRuntimeBackend, start bool, transcript *catalogExecutionTranscript) *runtimeHarness {
	t.Helper()
	strictCatalogFixtureStartupPolicy().apply(t)
	bundle := loadFixtureBundle(t, fixtureRoot)
	if transcript != nil {
		requireCatalogTranscriptIdentity(t, fixtureRoot, bundle, transcript)
	}
	var rootSchema struct {
		InitialState string `yaml:"initial_state"`
	}
	loadContractYAML(t, filepath.Join(fixtureRoot, "schema.yaml"), &rootSchema)
	module, err := newFixtureWorkflowModule(bundle)
	if err != nil {
		t.Fatalf("newFixtureWorkflowModule: %v", err)
	}
	bundleSourceFact := catalogBundleSourceFact(t, bundle)

	cfg := testRuntimeConfig()
	cfg.LLM.Backend = "anthropic"
	llmRuntime := newScriptedLLMRuntime()
	if transcript == nil {
		loadAgentFixtures(t, fixtureRoot, llmRuntime)
	} else {
		installAgentFixtures(llmRuntime, transcript.agentFixtures)
	}

	var (
		db             *sql.DB
		pg             *store.PostgresStore
		sqlite         *store.SQLiteRuntimeStore
		reopenedSQLite *store.SQLiteRuntimeStore
	)
	switch backend {
	case catalogBackendPostgres:
		_, postgresDB, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		db = postgresDB
		waitForCatalogHarnessDB(t, db)
		pg = storetest.AdmitPostgresRuntimeStore(t, db)
		pg.SetSessionLockTTL(cfg.LLM.Session.LockTTL)
	case catalogBackendSQLite:
		sqlite, reopenedSQLite = storetest.StartSQLiteRuntimeStorePair(t)
		db = storetest.DatabaseForTest(sqlite)
		sqlite.SetSessionLockTTL(cfg.LLM.Session.LockTTL)
	default:
		t.Fatalf("unsupported catalog runtime backend %q", backend)
	}

	ctx, cancel := context.WithCancel(runtimecorrelation.WithRunID(testAuthorActivityContextForBundle(context.Background(), bundleSourceFact), catalogRuntimeRunID))
	processOwner := worklifetime.NewProcess()
	fixture := runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: catalogRuntimeRunID, BundleHash: bundleSourceFact.BundleHash(), BundleSource: "ephemeral"}
	var workflowPersistence runtimepipeline.WorkflowPersistence
	var deps runtime.RuntimeDeps
	if pg != nil {
		runlifecyclefixture.RequirePostgres(t, ctx, db, fixture)
		workflowPersistence = runtimepipeline.NewWorkflowPersistence(pg)
		deps = catalogPostgresRuntimeDeps(cfg, pg, workflowPersistence, module, llmRuntime, processOwner, bundleSourceFact)
	} else {
		runlifecyclefixture.RequireSQLite(t, ctx, db, fixture)
		workflowPersistence = runtimepipeline.NewWorkflowPersistence(sqlite)
		deps = catalogSQLiteRuntimeDeps(cfg, sqlite, workflowPersistence, module, llmRuntime, processOwner, bundleSourceFact)
	}

	rt, err := runtime.NewValidationHarnessRuntime(ctx, deps)
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	var selected runtimestartupownership.Store
	if pg != nil {
		selected = pg
	} else {
		selected = sqlite
	}
	processTopology := installCatalogRuntimeStartupGrant(t, ctx, selected, rt)
	if !start {
		installCatalogHarnessDeliveryAuthority(t, ctx, rt, pg, sqlite)
	}
	if err := rt.PrepareAuthorActivityCatalog(); err != nil {
		t.Fatalf("PrepareAuthorActivityCatalog: %v", err)
	}
	if start {
		if err := rt.Start(ctx); err != nil {
			t.Fatalf("runtime.Start: %v", err)
		}
	}
	startedAt := catalogHarnessStartBoundary(t, db, backend)
	if transcript != nil {
		startedAt = transcript.observationBoundary()
	}
	h := &runtimeHarness{
		t:               t,
		backend:         backend,
		ctx:             ctx,
		cancel:          cancel,
		db:              db,
		pg:              pg,
		sqlite:          sqlite,
		reopenedSQLite:  reopenedSQLite,
		rt:              rt,
		processOwner:    processOwner,
		processTopology: processTopology,
		workflow:        rt.Pipeline,
		llm:             llmRuntime,
		bundle:          bundle,
		initialState:    strings.TrimSpace(rootSchema.InitialState),
		startedAt:       startedAt,
		publishedIDs:    map[string]struct{}{},
		publishedOrder:  []string{},
		eventEntityIDs:  map[string]string{},
		previews:        map[string]runtimepipeline.HandlerPreview{},
	}
	t.Cleanup(h.shutdown)
	return h
}

func installCatalogRuntimeStartupGrant(
	t testing.TB,
	ctx context.Context,
	selected runtimestartupownership.Store,
	rt *runtime.Runtime,
) runtimestartupownership.ProcessCapability {
	t.Helper()
	if selected == nil || rt == nil || rt.Manager == nil || rt.Options.WorkflowModule == nil {
		t.Fatal("catalog runtime startup grant requires a selected store and constructed runtime")
	}
	bundleHash, bundleSource := rt.Options.BundleSourceFact.StorageValues()
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: bundleHash, BundleSource: bundleSource}
	desired, err := rt.Manager.CompileStaticTopologyDesiredAgents(rt.Options.WorkflowModule.SemanticSource(), coordinate)
	if err != nil {
		t.Fatalf("compile catalog runtime source set: %v", err)
	}
	plan, err := runtimeagenttopology.NewSourceSetPlan([]runtimeagenttopology.SourceCoordinate{coordinate}, desired)
	if err != nil {
		t.Fatalf("construct catalog runtime source set: %v", err)
	}
	capability, err := selected.AcquireProcessCapability(ctx, runtimestartupownership.AcquireRequest{
		OwnerID: "catalog-runtime-harness", BootID: uuid.NewString(), RuntimeInstanceID: rt.Options.RuntimeInstanceID,
	})
	if err != nil {
		t.Fatalf("acquire catalog runtime process capability: %v", err)
	}
	release := true
	defer func() {
		if release {
			_ = capability.Release(context.Background())
		}
	}()
	current, exists, err := capability.CurrentSourceSet(ctx)
	if err != nil {
		t.Fatalf("load catalog runtime source set: %v", err)
	}
	if !exists || current.Revision != plan.Revision {
		commit := runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}
		if exists {
			commit.ExpectedRevision = current.Revision
			_, err = capability.RestoreSourceSet(ctx, commit)
		} else {
			_, err = capability.InstallCompleteSourceSet(ctx, commit)
		}
		if err != nil {
			t.Fatalf("commit catalog runtime source set: %v", err)
		}
	}
	grant, err := capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
		BundleHash: bundleHash, BundleSource: bundleSource, RuntimeInstanceID: rt.Options.RuntimeInstanceID,
		RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatalf("issue catalog runtime generation grant: %v", err)
	}
	if err := rt.InstallStartupGrant(grant); err != nil {
		t.Fatalf("install catalog runtime generation grant: %v", err)
	}
	release = false
	return capability
}

func catalogPostgresRuntimeDeps(cfg *config.Config, pg *store.PostgresStore, workflowPersistence runtimepipeline.WorkflowPersistence, module runtimepipeline.WorkflowModule, llmRuntime *scriptedLLMRuntime, processOwner *worklifetime.Process, bundleSourceFact runtimecorrelation.BundleSourceFact) runtime.RuntimeDeps {
	return runtime.RuntimeDeps{Config: cfg,
		WorkflowPersistence: workflowPersistence,
		EventStore:          pg,
		EventBusDurable: runtimebus.DurableDependencies{
			ReplyContext: pg, RunLifecycle: pg, DeliveryLifecycle: pg,
			FlowRoutes: pg, FlowRouteRecords: pg, FlowRouteSets: pg, FlowRouteTopology: pg, FlowRouteRollback: pg,
			ActiveAgents: pg, ActiveFlows: pg, TargetOwners: pg, WorkflowInstances: pg, PreparedEvents: pg,
			TargetFailureRecorder: pg, RunOrigins: pg, StandingRestarts: pg,
		},
		EventPayloadValidationBinder:   pg,
		InboundPayloadValidationBinder: pg,
		AuthorActivityRegistrars:       []runtime.AuthorActivityCatalogRegistrar{pg},
		RunBundleAvailability:          pg,
		RunControlStore:                pg,
		RunLifecycleCandidates:         pg,
		RuntimeLogStore:                pg,
		SessionRegistry:                pg,
		LiveSessionAcquirer:            pg,
		SessionResetter:                pg,
		ManagerStore:                   pg,
		ManagerLifecycleDiagnostics:    pg,
		ManagerPersistenceRoles: runtimemanager.PersistenceRoles{
			LifecycleCensus: pg, LifecycleState: pg, LifecycleEffects: pg,
			LifecycleDiagnostics: pg, EffectsRecovery: pg, DeliveryQuiescence: pg,
			EventExistence: pg, DirectiveOperations: pg, DirectiveTargets: pg, FlowRoutes: pg,
			StandingRestarts: pg,
		},
		EffectsStore:             pg,
		CompletionStore:          pg,
		CompletionHeartbeatStore: pg,
		EffectsRecoveryStore:     pg,
		ManagedCapabilitiesStore: pg,
		DeliveryStore:            pg,
		PipelineObligations:      pg.PipelineObligations(),
		GenericScheduleStore:     pg,
		TimerObligationReader:    pg,
		MailboxMaterializer:      pg,
		DecisionCards:            pg,
		ProposedEffects:          pg,
		DecisionCardHumanTasks:   pg,
		DecisionCardDraftExpiry:  pg,
		HumanTaskExpiry:          pg,
		MailboxStore:             pg,
		ToolEntityStore:          pg,
		HumanTaskStore:           pg,
		BudgetSpendStore:         pg,
		RuntimeIngressStore:      pg,
		ConversationStore:        nil,
		Options: runtime.RuntimeOptions{
			SelfCheck:         false,
			WorkflowModule:    module,
			LLMRuntime:        llmRuntime,
			RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
			BundleSourceFact:  bundleSourceFact,
			ProcessWorkOwner:  processOwner,
		}}
}

func catalogSQLiteRuntimeDeps(cfg *config.Config, sqlite *store.SQLiteRuntimeStore, workflowPersistence runtimepipeline.WorkflowPersistence, module runtimepipeline.WorkflowModule, llmRuntime *scriptedLLMRuntime, processOwner *worklifetime.Process, bundleSourceFact runtimecorrelation.BundleSourceFact) runtime.RuntimeDeps {
	return runtime.RuntimeDeps{Config: cfg,
		WorkflowPersistence: workflowPersistence,
		EventStore:          sqlite,
		EventBusDurable: runtimebus.DurableDependencies{
			ReplyContext: sqlite, RunLifecycle: sqlite, DeliveryLifecycle: sqlite,
			FlowRoutes: sqlite, FlowRouteRecords: sqlite, FlowRouteSets: sqlite, FlowRouteTopology: sqlite, FlowRouteRollback: sqlite,
			ActiveAgents: sqlite, ActiveFlows: sqlite, TargetOwners: sqlite, WorkflowInstances: sqlite, PreparedEvents: sqlite,
			TargetFailureRecorder: sqlite, RunOrigins: sqlite, StandingRestarts: sqlite,
		},
		EventPayloadValidationBinder:   sqlite,
		InboundPayloadValidationBinder: sqlite,
		AuthorActivityRegistrars:       []runtime.AuthorActivityCatalogRegistrar{sqlite},
		RunBundleAvailability:          sqlite,
		RunControlStore:                sqlite,
		RunLifecycleCandidates:         sqlite,
		RuntimeLogStore:                sqlite,
		SessionRegistry:                sqlite,
		LiveSessionAcquirer:            sqlite,
		SessionResetter:                sqlite,
		ManagerStore:                   sqlite,
		ManagerLifecycleDiagnostics:    sqlite,
		ManagerPersistenceRoles: runtimemanager.PersistenceRoles{
			LifecycleCensus: sqlite, LifecycleState: sqlite, LifecycleEffects: sqlite,
			LifecycleDiagnostics: sqlite, EffectsRecovery: sqlite, DeliveryQuiescence: sqlite,
			EventExistence: sqlite, DirectiveOperations: sqlite, DirectiveTargets: sqlite, FlowRoutes: sqlite,
			StandingRestarts: sqlite,
		},
		EffectsStore:             sqlite,
		CompletionStore:          sqlite,
		CompletionHeartbeatStore: sqlite,
		EffectsRecoveryStore:     sqlite,
		ManagedCapabilitiesStore: sqlite,
		DeliveryStore:            sqlite,
		PipelineObligations:      sqlite.PipelineObligations(),
		GenericScheduleStore:     sqlite,
		TimerObligationReader:    sqlite,
		MailboxMaterializer:      sqlite,
		DecisionCards:            sqlite,
		ProposedEffects:          sqlite,
		DecisionCardHumanTasks:   sqlite,
		DecisionCardDraftExpiry:  sqlite,
		HumanTaskExpiry:          sqlite,
		MailboxStore:             sqlite,
		ToolEntityStore:          sqlite,
		HumanTaskStore:           sqlite,
		BudgetSpendStore:         sqlite,
		RuntimeIngressStore:      sqlite,
		ConversationStore:        nil,
		Options: runtime.RuntimeOptions{
			SelfCheck:         false,
			WorkflowModule:    module,
			LLMRuntime:        llmRuntime,
			RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
			BundleSourceFact:  bundleSourceFact,
			ProcessWorkOwner:  processOwner,
		}}
}

func installCatalogHarnessDeliveryAuthority(t testing.TB, ctx context.Context, rt *runtime.Runtime, pg *store.PostgresStore, sqlite *store.SQLiteRuntimeStore) {
	t.Helper()
	authority, err := runtimedelivery.NewNormalExecutionAuthority(
		rt.Options.BundleSourceFact,
		authorActivityTestRuntimeInstanceID,
		1,
	)
	if err != nil {
		t.Fatalf("construct catalog test delivery authority: %v", err)
	}
	if pg != nil {
		err = pg.ActivateDeliveryAuthority(ctx, authority)
	} else if sqlite != nil {
		err = sqlite.ActivateDeliveryAuthority(ctx, authority)
	} else {
		err = errors.New("catalog selected store is required")
	}
	if err != nil {
		t.Fatalf("activate catalog test delivery authority: %v", err)
	}
	if err := rt.Bus.SetDeliveryAuthority(authority); err != nil {
		t.Fatalf("install catalog test delivery authority: %v", err)
	}
	if err := rt.Bus.SetDeliveryContinuationOwner(runtimebustest.NewDeliveryContinuationOwner(false)); err != nil {
		t.Fatalf("install catalog test delivery continuation owner: %v", err)
	}
}

func (h *runtimeHarness) shutdown() {
	if h == nil {
		return
	}
	h.shutdownOnce.Do(func() {
		if h.rt != nil {
			if err := h.rt.Shutdown(); err != nil {
				h.t.Errorf("shutdown catalog runtime: %v", err)
			}
		}
		if h.cancel != nil {
			h.cancel()
		}
		if h.processOwner != nil {
			joinCtx, cancelJoin := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelJoin()
			h.processOwner.Retire()
			if _, err := h.processOwner.Join(joinCtx); err != nil {
				h.t.Errorf("join catalog runtime process owner: %v", err)
				return
			}
		}
		if h.processTopology != nil {
			select {
			case <-h.processTopology.Done():
			default:
				if err := h.processTopology.Release(context.Background()); err != nil {
					h.t.Errorf("release catalog process topology capability: %v", err)
				}
			}
		}
	})
}

func (h *runtimeHarness) reopenFromTranscript(transcript *catalogExecutionTranscript) *runtimeHarness {
	h.t.Helper()
	h.shutdown()
	strictCatalogFixtureStartupPolicy().apply(h.t)

	bundle := loadFixtureBundle(h.t, h.bundle.Paths.ContractsRoot)
	requireCatalogTranscriptIdentity(h.t, bundle.Paths.ContractsRoot, bundle, transcript)
	module, err := newFixtureWorkflowModule(bundle)
	if err != nil {
		h.t.Fatalf("reopen fixture workflow module: %v", err)
	}
	bundleSourceFact := catalogBundleSourceFact(h.t, bundle)
	cfg := testRuntimeConfig()
	cfg.LLM.Backend = "anthropic"
	cfg.Runtime.RecoveryOnStartup = true
	llmRuntime := newScriptedLLMRuntime()
	installAgentFixtures(llmRuntime, transcript.agentFixtures)

	var (
		db     *sql.DB
		pg     *store.PostgresStore
		sqlite *store.SQLiteRuntimeStore
	)
	switch h.backend {
	case catalogBackendPostgres:
		db = h.db
		pg = storetest.AdmitPostgresRuntimeStore(h.t, db)
		pg.SetSessionLockTTL(cfg.LLM.Session.LockTTL)
	case catalogBackendSQLite:
		sqlite = h.reopenedSQLite
		if sqlite == nil {
			h.t.Fatal("catalog SQLite reconstruction handle is required")
		}
		db = storetest.DatabaseForTest(sqlite)
		sqlite.SetSessionLockTTL(cfg.LLM.Session.LockTTL)
	default:
		h.t.Fatalf("unsupported catalog runtime backend %q", h.backend)
	}

	ctx, cancel := context.WithCancel(runtimecorrelation.WithRunID(testAuthorActivityContextForBundle(context.Background(), bundleSourceFact), catalogRuntimeRunID))
	processOwner := worklifetime.NewProcess()
	var workflowPersistence runtimepipeline.WorkflowPersistence
	var deps runtime.RuntimeDeps
	if pg != nil {
		workflowPersistence = runtimepipeline.NewWorkflowPersistence(pg)
		deps = catalogPostgresRuntimeDeps(cfg, pg, workflowPersistence, module, llmRuntime, processOwner, bundleSourceFact)
	} else {
		workflowPersistence = runtimepipeline.NewWorkflowPersistence(sqlite)
		deps = catalogSQLiteRuntimeDeps(cfg, sqlite, workflowPersistence, module, llmRuntime, processOwner, bundleSourceFact)
	}
	rt, err := runtime.NewValidationHarnessRuntime(ctx, deps)
	if err != nil {
		cancel()
		h.t.Fatalf("reopen catalog runtime: %v", err)
	}
	var selected runtimestartupownership.Store
	if pg != nil {
		selected = pg
	} else {
		selected = sqlite
	}
	processTopology := installCatalogRuntimeStartupGrant(h.t, ctx, selected, rt)
	if err := rt.PrepareAuthorActivityCatalog(); err != nil {
		cancel()
		h.t.Fatalf("prepare reopened author activity catalog: %v", err)
	}
	if err := rt.Start(ctx); err != nil {
		cancel()
		h.t.Fatalf("start reopened catalog runtime: %v", err)
	}

	reopened := &runtimeHarness{
		t: h.t, backend: h.backend, ctx: ctx, cancel: cancel, db: db, pg: pg, sqlite: sqlite,
		rt: rt, processOwner: processOwner, workflow: rt.Pipeline, llm: llmRuntime, bundle: bundle,
		processTopology: processTopology,
		initialState:    h.initialState, startedAt: h.startedAt,
		publishedIDs: cloneCatalogStringSet(h.publishedIDs), publishedOrder: append([]string(nil), h.publishedOrder...),
		eventEntityIDs: cloneCatalogStringMap(h.eventEntityIDs), previews: cloneCatalogPreviews(h.previews),
	}
	h.t.Cleanup(reopened.shutdown)
	return reopened
}

func cloneCatalogStringSet(in map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for key := range in {
		out[key] = struct{}{}
	}
	return out
}

func cloneCatalogStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneCatalogPreviews(in map[string]runtimepipeline.HandlerPreview) map[string]runtimepipeline.HandlerPreview {
	out := make(map[string]runtimepipeline.HandlerPreview, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func catalogHarnessStartBoundary(t testing.TB, db *sql.DB, backend catalogRuntimeBackend) time.Time {
	t.Helper()
	appTime := time.Now().UTC()
	if backend == catalogBackendSQLite {
		return appTime.Add(-1 * time.Second)
	}
	var out time.Time
	if err := db.QueryRowContext(testAuthorActivityContext(context.Background()), `SELECT NOW()`).Scan(&out); err != nil {
		t.Fatalf("query catalog harness db time: %v", err)
	}
	dbTime := out.UTC()
	if dbTime.Before(appTime) {
		return dbTime.Add(-1 * time.Second)
	}
	return appTime.Add(-1 * time.Second)
}

func loadAgentFixtures(t testing.TB, fixtureRoot string, llmRuntime *scriptedLLMRuntime) {
	t.Helper()
	if llmRuntime == nil {
		return
	}
	path := filepath.Join(fixtureRoot, "fixtures.yaml")
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return
		}
		t.Fatalf("stat %s: %v", path, err)
	}
	var doc agentFixtureDoc
	loadYAML(t, path, &doc)
	installAgentFixtures(llmRuntime, doc)
}

func installAgentFixtures(llmRuntime *scriptedLLMRuntime, doc agentFixtureDoc) {
	for agentID, steps := range doc.AgentFixtures {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		for _, step := range steps {
			llmRuntime.SetAgentFixture(agentID, scriptedAgentFixtureStep{
				On:    strings.TrimSpace(step.On),
				Emits: append([]agentFixtureEmit(nil), step.Emits...),
			})
		}
	}
}

func (h *runtimeHarness) publishAndWait(step catalogTriggerStep, timeout time.Duration) {
	h.t.Helper()
	if strings.TrimSpace(step.eventID) == "" {
		step.excludeFromEmitted = true
	}
	payload := cloneStringAnyMap(step.Payload)
	eventType := strings.TrimSpace(step.Event)
	wantErr := strings.TrimSpace(step.ErrorContains)
	if entityID := triggerPayloadEntityID(payload); entityID != "" {
		h.seedInitialState(entityID)
	}
	if wantErr != "" {
		err := h.publishRuntimeEventResultForStep(step, timeout, true)
		if err == nil {
			h.t.Fatalf("Publish(%s) unexpectedly succeeded, want error containing %q", eventType, wantErr)
		}
		if !strings.Contains(err.Error(), wantErr) {
			h.t.Fatalf("Publish(%s) error = %v, want substring %q", eventType, err, wantErr)
		}
		h.assertTriggerReceipt(step)
		return
	}
	if err := h.publishRuntimeEventResultForStep(step, timeout, true); err != nil {
		h.t.Fatalf("Publish(%s): %v", eventType, err)
	}
	h.assertTriggerReceipt(step)
}

func (h *runtimeHarness) waitForRunTerminal(timeout time.Duration) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var (
			snapshot runtimebus.RunLifecycleSnapshot
			err      error
		)
		if h.pg != nil {
			snapshot, err = h.pg.LoadRunLifecycleSnapshot(ctx, catalogRuntimeRunID)
		} else if h.sqlite != nil {
			snapshot, err = h.sqlite.LoadRunLifecycleSnapshot(ctx, catalogRuntimeRunID)
		} else {
			err = errors.New("catalog selected store is required")
		}
		if err != nil {
			h.t.Fatalf("load catalog run lifecycle: %v", err)
		}
		if snapshot.EndedAt != nil {
			return
		}
		select {
		case <-ctx.Done():
			h.t.Fatalf("wait for catalog run terminal lifecycle: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (h *runtimeHarness) publishConcurrentAndWait(steps []catalogTriggerStep, timeout time.Duration) {
	h.t.Helper()
	if len(steps) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(h.ctx, timeout)
	defer cancel()

	type publishItem struct {
		step catalogTriggerStep
		evt  events.Event
	}
	items := make([]publishItem, 0, len(steps))
	for _, step := range steps {
		payload := cloneStringAnyMap(step.Payload)
		targetRoute, hasTarget := step.Target.route()
		if !hasTarget && step.Target != (catalogTriggerTarget{}) {
			h.t.Fatalf("concurrent trigger %s requires a complete target owner", step.Event)
		}
		if entityID := triggerPayloadEntityID(payload); entityID != "" {
			h.seedInitialState(entityID)
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			h.t.Fatalf("marshal concurrent trigger payload: %v", err)
		}
		eventEnvelope := events.EventEnvelope{}
		if entityID := triggerPayloadEntityID(payload); entityID != "" {
			eventEnvelope = events.EnvelopeForEntityID(eventEnvelope, entityID)
		} else {
			eventEnvelope = events.EnvelopeForEntityID(eventEnvelope, runtimepipeline.FlowInstanceEntityID(catalogRuntimeRunID))
		}
		if hasTarget {
			eventEnvelope = events.EnvelopeForTargetRoute(eventEnvelope, targetRoute)
		}
		eventID := strings.TrimSpace(step.eventID)
		if eventID == "" {
			eventID = uuid.NewString()
		}
		createdAt := step.createdAt
		if createdAt.IsZero() {
			createdAt = time.Now().UTC()
		}
		sourceAgent := strings.TrimSpace(step.sourceAgent)
		if sourceAgent == "" {
			sourceAgent = "cataloge2e"
		}
		evt := eventtest.ExistingRunRootIngress(eventID,
			events.EventType(strings.TrimSpace(step.Event)),
			sourceAgent, "", raw, 0, catalogRuntimeRunID, eventEnvelope, createdAt)
		if hasTarget {
			h.ensureTargetFlowInstance(targetRoute, evt)
		}
		if preview, ok := h.previewHandlerOutcome(evt); ok {
			h.mu.Lock()
			h.previews[evt.ID()] = preview
			h.mu.Unlock()
		}
		h.mu.Lock()
		h.publishedIDs[evt.ID()] = struct{}{}
		h.publishedOrder = append(h.publishedOrder, evt.ID())
		if entityID := triggerPayloadEntityID(payload); entityID != "" {
			h.eventEntityIDs[evt.ID()] = entityID
		}
		h.mu.Unlock()
		items = append(items, publishItem{step: step, evt: evt})
	}

	errCh := make(chan error, len(items))
	var wg sync.WaitGroup
	for _, item := range items {
		wg.Add(1)
		go func(item publishItem) {
			defer wg.Done()
			if err := h.rt.Bus.Publish(ctx, item.evt); err != nil {
				errCh <- err
			}
		}(item)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			h.t.Fatalf("concurrent publish failed: %v", err)
		}
	}
	for _, item := range items {
		h.refreshPublishedEventEntityID(item.evt.ID())
	}
}

func (h *runtimeHarness) publishRuntimeEventResultForStep(step catalogTriggerStep, timeout time.Duration, recordOutcome bool) error {
	eventID := strings.TrimSpace(step.eventID)
	if eventID == "" {
		eventID = uuid.NewString()
	}
	createdAt := step.createdAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	sourceAgent := strings.TrimSpace(step.sourceAgent)
	if sourceAgent == "" {
		sourceAgent = "cataloge2e"
	}
	return h.publishRuntimeEventResultWithIdentity(
		step.Event,
		sourceAgent,
		step.Payload,
		eventID,
		createdAt,
		timeout,
		recordOutcome,
		step.excludeFromEmitted,
	)
}

func (h *runtimeHarness) publishRuntimeEventResultWithIdentity(eventType, sourceAgent string, payload map[string]any, eventID string, createdAt time.Time, timeout time.Duration, recordOutcome bool, excludeFromEmitted bool) error {
	h.t.Helper()
	payload = cloneStringAnyMap(payload)
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	eventEnvelope := events.EventEnvelope{}
	if entityID := triggerPayloadEntityID(payload); entityID != "" {
		eventEnvelope = events.EnvelopeForEntityID(eventEnvelope, entityID)
	} else {
		eventEnvelope = events.EnvelopeForEntityID(eventEnvelope, runtimepipeline.FlowInstanceEntityID(catalogRuntimeRunID))
	}
	evt := eventtest.ExistingRunRootIngress(strings.TrimSpace(eventID),
		events.EventType(strings.TrimSpace(eventType)),
		strings.TrimSpace(sourceAgent), "", raw, 0, catalogRuntimeRunID, eventEnvelope, createdAt.UTC())
	if recordOutcome {
		if preview, ok := h.previewHandlerOutcome(evt); ok {
			h.mu.Lock()
			h.previews[evt.ID()] = preview
			h.mu.Unlock()
		}
	}
	h.mu.Lock()
	if excludeFromEmitted {
		h.publishedIDs[evt.ID()] = struct{}{}
	}
	if recordOutcome {
		h.publishedOrder = append(h.publishedOrder, evt.ID())
		if entityID := triggerPayloadEntityID(payload); entityID != "" {
			h.eventEntityIDs[evt.ID()] = entityID
		}
	}
	h.mu.Unlock()

	ctx, cancel := context.WithTimeout(h.ctx, timeout)
	defer cancel()
	if err := h.publishBusEvent(ctx, evt); err != nil {
		return err
	}
	h.refreshPublishedEventEntityID(evt.ID())
	return nil
}

func (h *runtimeHarness) refreshPublishedEventEntityID(eventID string) {
	h.t.Helper()
	eventID = strings.TrimSpace(eventID)
	if h == nil || h.db == nil || eventID == "" {
		return
	}
	var entityID string
	query := `
		SELECT COALESCE(entity_id::text, '')
		FROM events
		WHERE event_id = $1::uuid
	`
	if h.backend == catalogBackendSQLite {
		query = `SELECT COALESCE(entity_id, '') FROM events WHERE event_id = ?`
	}
	err := h.db.QueryRowContext(h.ctx, query, eventID).Scan(&entityID)
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		h.t.Fatalf("query published event entity_id for %s: %v", eventID, err)
	}
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return
	}
	h.mu.Lock()
	h.eventEntityIDs[eventID] = entityID
	h.mu.Unlock()
}

func (h *runtimeHarness) publishBusEvent(ctx context.Context, evt events.Event) error {
	h.t.Helper()
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(150 * time.Millisecond):
			}
		}
		if err := h.rt.Bus.PublishAndWait(ctx, evt); err != nil {
			lastErr = err
			if isTransientCatalogPublishError(err) {
				continue
			}
			return err
		}
		return nil
	}
	return lastErr
}

func isTransientCatalogPublishError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sql.ErrConnDone) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "bad connection")
}

func waitForCatalogHarnessDB(t testing.TB, db *sql.DB) {
	t.Helper()
	if db == nil {
		t.Fatal("catalog harness db is nil")
	}
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(context.Background()), 5*time.Second)
	defer cancel()
	var lastErr error
	for attempt := 0; attempt < 25; attempt++ {
		lastErr = db.PingContext(ctx)
		if lastErr == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("catalog harness db ping: %v", lastErr)
		case <-time.After(200 * time.Millisecond):
		}
	}
	t.Fatalf("catalog harness db ping: %v", lastErr)
}

func (h *runtimeHarness) waitForExpectedEmittedEvents(expected catalogExpectedDocument, timeout time.Duration) {
	h.t.Helper()
	entityID := h.expectedTriggerEntityID(expected)
	if entityID == "" || len(expected.Expected.EmittedEvents) == 0 {
		return
	}
	flowPrefix := expected.triggerFlowPrefix()
	source := semanticview.Wrap(h.bundle)
	ctx, cancel := context.WithTimeout(h.ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		eventsReady := h.hasExpectedEmittedEvents(ctx, entityID, expected.Expected.EmittedEvents, flowPrefix, source)
		stateReady := flowPrefix != "" || strings.TrimSpace(expected.Expected.EntityState) == "" ||
			h.hasExpectedRootEntityState(ctx, entityID, expected.Expected.EntityState)
		if eventsReady && stateReady {
			return
		}
		select {
		case <-ctx.Done():
			observed, readErr := h.catalogOperatorEvents()
			facts := make([]string, 0, len(observed))
			for _, event := range observed {
				deliveries := make([]string, 0, len(event.Deliveries))
				for _, delivery := range event.Deliveries {
					fact := delivery.SubscriberType + ":" + delivery.SubscriberID + "=" + delivery.Status
					if delivery.Failure != nil {
						fact += "/" + string(delivery.Failure.Class) + "/" + delivery.Failure.Detail.Code
					}
					deliveries = append(deliveries, fact)
				}
				sort.Strings(deliveries)
				facts = append(facts, event.EventName+" parent="+event.SourceEventID+" entity="+event.EntityID+" deliveries="+strings.Join(deliveries, ","))
			}
			sort.Strings(facts)
			h.t.Fatalf("wait for expected emitted events %v for entity %s: %v; observed=%v read_error=%v", expected.Expected.EmittedEvents, entityID, ctx.Err(), facts, readErr)
		case <-ticker.C:
		}
	}
}

func (h *runtimeHarness) hasExpectedRootEntityState(ctx context.Context, entityID, want string) bool {
	h.t.Helper()
	if h == nil || h.workflow == nil {
		return false
	}
	instance, found, err := h.workflow.Load(ctx, catalogRootWorkflowRoute())
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return false
		}
		h.t.Fatalf("load workflow instance while waiting for emitted-event effects: %v", err)
	}
	return found && strings.TrimSpace(instance.CurrentState) == strings.TrimSpace(want)
}

func (h *runtimeHarness) expectedTriggerEntityID(expected catalogExpectedDocument) string {
	if h == nil {
		return ""
	}
	for _, step := range expected.triggerSequence() {
		if entityID := triggerPayloadEntityID(step.Payload); entityID != "" {
			return entityID
		}
	}
	if entityID := triggerPayloadEntityID(expected.Trigger.Payload); entityID != "" {
		return entityID
	}
	return h.firstPublishedEntityID()
}

func (h *runtimeHarness) firstPublishedEntityID() string {
	if h == nil {
		return ""
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, eventID := range h.publishedOrder {
		if entityID := strings.TrimSpace(h.eventEntityIDs[strings.TrimSpace(eventID)]); entityID != "" {
			return entityID
		}
	}
	return ""
}

func (h *runtimeHarness) hasExpectedEmittedEvents(ctx context.Context, entityID string, want []string, flowPrefix string, source semanticview.Source) bool {
	h.t.Helper()
	relevantEventIDs := catalogCausalEventIDs(h.t, h.db, h.startedAt, h.publishedIDs)
	relevantEntityIDs := catalogCausalEntityIDs(h.t, h.db, h.startedAt, h.publishedIDs, entityID)
	rows, err := h.db.QueryContext(ctx, catalogDialectQuery(h.db, `
		SELECT event_id::text, event_name, COALESCE(NULLIF(payload->>'entity_id', ''), COALESCE(entity_id::text, ''))
		FROM events
		WHERE created_at >= $1
		ORDER BY created_at ASC, event_id ASC
	`, `
		SELECT event_id, event_name, COALESCE(NULLIF(json_extract(payload, '$.entity_id'), ''), COALESCE(entity_id, ''))
		FROM events
		WHERE created_at >= ?
		ORDER BY created_at ASC, event_id ASC
	`), h.startedAt)
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return false
		}
		h.t.Fatalf("query emitted events for wait: %v", err)
	}
	defer rows.Close()

	counts := make(map[string]int, len(want))
	wantNames := make(map[string]struct{}, len(want))
	for _, name := range want {
		name = strings.TrimSpace(name)
		if name != "" {
			counts[name]++
			wantNames[name] = struct{}{}
		}
	}
	for rows.Next() {
		var eventID, eventName, payloadEntityID string
		if err := rows.Scan(&eventID, &eventName, &payloadEntityID); err != nil {
			h.t.Fatalf("scan emitted events for wait: %v", err)
		}
		if _, skip := h.publishedIDs[strings.TrimSpace(eventID)]; skip {
			continue
		}
		eventID = strings.TrimSpace(eventID)
		payloadEntityID = strings.TrimSpace(payloadEntityID)
		_, causalEvent := relevantEventIDs[eventID]
		_, causalEntity := relevantEntityIDs[payloadEntityID]
		if !causalEvent && !causalEntity {
			continue
		}
		eventName = strings.TrimSpace(eventName)
		if eventName == "" || shouldIgnoreCatalogE2EEvent(eventName) {
			continue
		}
		eventName = normalizeCatalogObservedEventName(eventName, flowPrefix, source, wantNames)
		if flowPrefix == "" && strings.Contains(eventName, "/") {
			if _, explicitlyExpected := wantNames[eventName]; !explicitlyExpected {
				continue
			}
		}
		if _, ok := counts[eventName]; !ok {
			continue
		}
		counts[eventName]--
	}
	if err := rows.Err(); err != nil {
		if ctx.Err() != nil {
			return false
		}
		h.t.Fatalf("iterate emitted events for wait: %v", err)
	}
	for name, remaining := range counts {
		if remaining > 0 {
			_ = name
			return false
		}
	}
	return true
}

func (h *runtimeHarness) previewHandlerOutcome(evt events.Event) (runtimepipeline.HandlerPreview, bool) {
	if h == nil || h.bundle == nil {
		return runtimepipeline.HandlerPreview{}, false
	}
	node := firstMatchingNodeHandler(h.bundle, strings.TrimSpace(string(evt.Type())))
	if node.Empty() {
		return runtimepipeline.HandlerPreview{}, false
	}
	entityID := strings.TrimSpace(evt.EntityID())
	state := runtimepipeline.WorkflowState{
		EntityID: entityID,
	}
	if strings.TrimSpace(entityID) != "" && h.workflow != nil {
		if instance, ok, err := h.workflow.Load(h.ctx, catalogRootWorkflowRoute()); err == nil && ok {
			state.Stage = runtimepipeline.NormalizeWorkflowStateID(instance.CurrentState)
			state.Metadata = cloneStringAnyMap(instance.Fields)
		}
	}
	preview, err := runtimepipeline.PreviewContractHandlerExecution(h.ctx, h.bundle, node, evt, state, nil)
	if err != nil {
		return runtimepipeline.HandlerPreview{}, false
	}
	return preview, true
}

func firstMatchingNodeHandler(bundle *runtimecontracts.WorkflowContractBundle, eventType string) runtimeidentity.ExecutableNode {
	if bundle == nil || strings.TrimSpace(eventType) == "" {
		return runtimeidentity.ExecutableNode{}
	}
	source := semanticview.Wrap(bundle)
	for _, record := range source.ExecutableNodeRecords() {
		node, err := record.Identity()
		if err != nil {
			continue
		}
		if _, ok := source.ExecutableNodeEventHandler(node, eventType); ok {
			return node
		}
	}
	return runtimeidentity.ExecutableNode{}
}

func (h *runtimeHarness) seedInitialState(entityID string) {
	h.t.Helper()
	if h == nil || h.workflow == nil || h.bundle == nil {
		return
	}
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return
	}
	if _, ok, err := h.workflow.Load(h.ctx, catalogRootWorkflowRoute()); err == nil && ok {
		return
	}
	initialState := strings.TrimSpace(h.initialState)
	if initialState == "" {
		return
	}
	entityType := h.requireRootEntityType()
	ctx := worklifetime.WithOccurrence(h.ctx, h.rt.WorkOccurrence())
	ctx = runtimeeffects.WithExecutionMode(ctx, executionmode.Live)
	if _, err := h.workflow.MaterializeInitialEntry(ctx, runtimepipeline.WorkflowInstance{
		InstanceID:      catalogRuntimeRunID,
		StorageRef:      catalogRuntimeRunID,
		EntityID:        entityID,
		WorkflowName:    h.bundle.WorkflowName(),
		WorkflowVersion: h.bundle.WorkflowVersion(),
		CurrentState:    initialState,
		EnteredStageAt:  h.startedAt,
		CreatedAt:       h.startedAt,
		EntityType:      entityType,
	}, h.startedAt); err != nil {
		h.t.Fatalf("seed initial workflow state for %s: %v", entityID, err)
	}
}

func (h *runtimeHarness) ensureTargetFlowInstance(target events.RouteIdentity, trigger events.Event) {
	h.t.Helper()
	if h == nil || h.rt == nil || h.rt.Manager == nil || h.bundle == nil {
		h.t.Fatal("catalog target activation requires the runtime manager and contract bundle")
	}
	target = target.Normalized()
	if target.FlowID == "" || target.FlowInstance == "" || target.EntityID == "" {
		h.t.Fatalf("catalog target owner is incomplete: %#v", target)
	}
	route := runtimeflowidentity.RouteForInstancePath(target.FlowInstance)
	config := map[string]any{}
	if schema, ok := h.bundle.FlowSchemaByID(target.FlowID); ok && !schema.Instance.Empty() {
		config[schema.Instance.Path()] = route.InstanceID
	}
	ctx := worklifetime.WithOccurrence(h.ctx, h.rt.WorkOccurrence())
	ctx = runtimeeffects.WithExecutionMode(ctx, executionmode.Live)
	_, err := h.rt.Manager.EnsureFlowInstance(ctx, runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: semanticview.Wrap(h.bundle),
		Instance: runtimeflowidentity.Stored(
			semanticview.Wrap(h.bundle),
			target.FlowID,
			route.InstancePath,
			route.InstanceID,
			target.EntityID,
			"",
		),
		Config:       config,
		TriggerEvent: trigger,
		OccurredAt:   trigger.CreatedAt(),
	})
	if err != nil {
		h.t.Fatalf("activate catalog target owner %s/%s: %v", target.FlowInstance, target.EntityID, err)
	}
}

func (h *runtimeHarness) seedEntityFields(expected catalogExpectedDocument) {
	h.t.Helper()
	if h == nil || h.workflow == nil {
		return
	}
	if len(expected.Trigger.EntityFieldsBefore) == 0 && len(expected.Trigger.Entity) == 0 && len(expected.Trigger.GatesBefore) == 0 && strings.TrimSpace(expected.Trigger.EntityStateBefore) == "" {
		return
	}
	entityID := ""
	for _, step := range expected.triggerSequence() {
		if entityID = triggerPayloadEntityID(step.Payload); entityID != "" {
			break
		}
	}
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		entityID = runtimepipeline.FlowInstanceEntityID(catalogRuntimeRunID)
	}
	instance, ok, err := h.workflow.Load(h.ctx, catalogRootWorkflowRoute())
	if err != nil {
		h.t.Fatalf("load workflow instance for entity field seeding %s: %v", entityID, err)
	}
	if !ok {
		entityType := h.requireRootEntityType()
		instance = runtimepipeline.WorkflowInstance{
			InstanceID:      catalogRuntimeRunID,
			StorageRef:      catalogRuntimeRunID,
			EntityID:        entityID,
			WorkflowName:    h.bundle.WorkflowName(),
			WorkflowVersion: h.bundle.WorkflowVersion(),
			CurrentState:    h.initialState,
			EnteredStageAt:  h.startedAt,
			CreatedAt:       h.startedAt,
			EntityType:      entityType,
		}
	}
	if strings.TrimSpace(instance.InstanceID) == "" {
		instance.InstanceID = catalogRuntimeRunID
	}
	if strings.TrimSpace(instance.StorageRef) == "" {
		instance.StorageRef = catalogRuntimeRunID
	}
	if strings.TrimSpace(instance.WorkflowName) == "" {
		instance.WorkflowName = h.bundle.WorkflowName()
	}
	if strings.TrimSpace(instance.WorkflowVersion) == "" {
		instance.WorkflowVersion = h.bundle.WorkflowVersion()
	}
	if strings.TrimSpace(instance.CurrentState) == "" {
		instance.CurrentState = h.initialState
	}
	if seededState := strings.TrimSpace(expected.Trigger.EntityStateBefore); seededState != "" {
		instance.CurrentState = seededState
	}
	if instance.Fields == nil {
		instance.Fields = map[string]any{}
	}
	for key, value := range expected.Trigger.Entity {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		instance.Fields[key] = value
	}
	for key, value := range expected.Trigger.EntityFieldsBefore {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		instance.Fields[key] = value
	}
	if len(expected.Trigger.GatesBefore) > 0 {
		if instance.Gates == nil {
			instance.Gates = map[string]bool{}
		}
		for key, value := range expected.Trigger.GatesBefore {
			key = h.catalogGateKey(h.bundle.WorkflowName(), key)
			if key == "" {
				continue
			}
			instance.Gates[key] = value
		}
	}
	if ok {
		h.t.Fatalf("entity field fixture %s was already materialized before exact fixture projection", entityID)
	}
	materializeCtx := worklifetime.WithOccurrence(h.ctx, h.rt.WorkOccurrence())
	materializeCtx = runtimeeffects.WithExecutionMode(materializeCtx, executionmode.Live)
	if _, err := h.workflow.MaterializeInitialEntry(materializeCtx, instance, h.startedAt); err != nil {
		h.t.Fatalf("seed entity_fields_before for %s: %v", entityID, err)
	}
}

func (h *runtimeHarness) requireRootEntityType() string {
	h.t.Helper()
	contract, ok := entityruntime.ResolveForFlow(semanticview.Wrap(h.bundle), "")
	if !ok || strings.TrimSpace(contract.EntityType) == "" {
		h.t.Fatalf("catalog stateful fixture %s requires one canonical root entity contract", h.bundle.WorkflowName())
	}
	return strings.TrimSpace(contract.EntityType)
}

func (h *runtimeHarness) catalogGateKey(flowID, key string) string {
	key = strings.TrimSpace(key)
	if key == "" || strings.Contains(key, "/") || h == nil || h.bundle == nil {
		return key
	}
	scope := strings.Trim(strings.TrimSpace(semanticview.Wrap(h.bundle).FlowPath(strings.TrimSpace(flowID))), "/")
	if scope == "" {
		scope = strings.Trim(strings.TrimSpace(flowID), "/")
	}
	if scope == "" {
		return key
	}
	return scope + "/" + key
}

func triggerPayloadEntityID(payload map[string]any) string {
	if payload == nil {
		return ""
	}
	return strings.TrimSpace(asString(payload["entity_id"]))
}

func cloneStringAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func asString(v any) string {
	switch typed := v.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return ""
	}
}
