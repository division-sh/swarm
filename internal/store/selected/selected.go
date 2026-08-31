// Package selected owns production selected-store construction and lifetime.
// It projects exact domain ports while keeping backend identity and resources
// private to the composition boundary.
package selected

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	apiv1 "github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunforkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
	runtimerunstalled "github.com/division-sh/swarm/internal/runtime/runstalled"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	runtimestartuprecovery "github.com/division-sh/swarm/internal/runtime/startuprecovery"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeconstruction "github.com/division-sh/swarm/internal/store/construction"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

// RuntimeRequest contains construction inputs only. Backend identity is
// consumed by OpenRuntime and never survives in a runtime projection.
type RuntimeRequest struct {
	Selection      storebackend.Selection
	PostgresDSN    string
	SessionLockTTL time.Duration
}

type closeResource interface {
	Close() error
}

type ownershipState uint8

const (
	ownershipUnactivated ownershipState = iota
	ownershipActivated
	ownershipClosed
)

type lifecycle struct {
	mu       sync.Mutex
	resource closeResource
	state    ownershipState
	process  *worklifetime.Process
}

// Owner is the opaque runtime selected-store construction owner. Only the
// composition root retains it; downstream consumers receive narrow ports.
type Owner struct {
	lifetime lifecycle
	core     runtime.RuntimeDeps
	required requiredPorts
	products productPorts
}

type requiredPorts struct {
	schema                 store.SchemaBootstrapper
	pinger                 apiv1.Pinger
	authorActivity         runtimeauthoractivity.Reader
	operatorChannels       operatorchannel.Store
	channelOnboarding      channelonboarding.Store
	startupOwnership       runtimestartupownership.Store
	runQuiescence          runtimerunquiescence.ServeAbandonStore
	mailboxAPI             apiv1.MailboxAPIStore
	mailboxNoticeAck       apiv1.MailboxNoticeAcknowledgmentStore
	observability          apiv1.ObservabilityReadStore
	agentUsage             apiv1.AgentUsageReadStore
	agentDeliveryLifecycle apiv1.AgentDeliveryLifecycleReadStore
	idempotency            apiv1.APIIdempotencyStore
	runs                   apiv1.RunReadStore
	entities               apiv1.EntityReadStore
	agents                 apiv1.AgentReadStore
	conversations          apiv1.ConversationReadStore
	testSetup              apiv1.TestSetupStore
	runBundleContext       apiv1.RunBundleContextStore
	data                   apiv1.DurableDataStore
	dataAccess             durabledata.ResourceAccessStore
	runBundleAvailability  runbundle.AvailabilityStore
	runStalled             runtimerunstalled.ProjectionReader
	sourceArtifacts        SourceArtifactDataWriter
	sourceArtifactReader   runtimerunforkexecution.SourceArtifactSelectedContractSourceStore
}

type SourceArtifactDataWriter interface {
	EnsureSourceArtifactWithData(context.Context, *sourceartifact.AdmittedSourceArtifact, durabledata.Catalog) (sourceartifact.EnsureResult, error)
}

type productPorts struct {
	conversationFork         ConversationFork
	conversationAvailable    bool
	runFork                  RunFork
	runForkAvailable         bool
	destructiveReset         DestructiveReset
	destructiveAvailable     bool
	startupRecovery          StartupRecovery
	startupRecoveryAvailable bool
}

// ConversationFork is the exact read/lifecycle family projection.
type ConversationFork struct {
	reader    apiv1.ConversationForkReadStore
	lifecycle apiv1.ConversationForkLifecycleStore
}

func (o ConversationFork) Reader() apiv1.ConversationForkReadStore         { return o.reader }
func (o ConversationFork) Lifecycle() apiv1.ConversationForkLifecycleStore { return o.lifecycle }

// DestructiveReset is the exact selected persistence projection consumed by
// the reset planner/coordinator. Process-owned cleanup is intentionally absent.
type DestructiveReset struct {
	inventory  runtimedestructivereset.InventoryReader
	locks      runtimedestructivereset.LockManager
	quiescence runtimedestructivereset.QuiescenceStore
}

func (o DestructiveReset) Inventory() runtimedestructivereset.InventoryReader  { return o.inventory }
func (o DestructiveReset) Locks() runtimedestructivereset.LockManager          { return o.locks }
func (o DestructiveReset) Quiescence() runtimedestructivereset.QuiescenceStore { return o.quiescence }

// StartupRecovery is the exact source-artifact availability projection.
type StartupRecovery struct {
	availability runtimestartuprecovery.AvailabilityReader
}

func (o StartupRecovery) Availability() runtimestartuprecovery.AvailabilityReader {
	return o.availability
}

type runForkPlannerMaterializer interface {
	PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
}

// RunFork is the opaque selected-contract runtime family owner.
type RunFork struct {
	planner        runForkPlannerMaterializer
	availability   apiv1.RunForkAvailabilityStore
	activation     runtimerunforkexecution.SelectedContractActivationStore
	executionOwner runtimerunforkexecution.SelectedContractExecutionOwner
}

func (o RunFork) Availability() apiv1.RunForkAvailabilityStore { return o.availability }

func (o RunFork) Plan(ctx context.Context, req runfork.RunForkPlanRequest) (runfork.RunForkPlan, error) {
	if o.planner == nil {
		return runfork.RunForkPlan{}, errors.New("selected run.fork runtime owner is required")
	}
	return o.planner.PlanRunFork(ctx, req)
}

func (o RunFork) Materialize(ctx context.Context, req runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error) {
	if o.planner == nil {
		return runfork.RunForkMaterialization{}, errors.New("selected run.fork runtime owner is required")
	}
	return o.planner.MaterializeRunFork(ctx, req)
}

func (o RunFork) Activate(ctx context.Context, req runtimerunforkexecution.SelectedContractActivationGateRequest) (runtimerunforkexecution.SelectedContractActivationGateResult, error) {
	if o.activation == nil {
		return runtimerunforkexecution.SelectedContractActivationGateResult{}, errors.New("selected run.fork runtime owner is required")
	}
	req.Store = o.activation
	req.ExecutionOwner = o.executionOwner
	return runtimerunforkexecution.ActivateSelectedContractRunFork(ctx, req)
}

func (o RunFork) Execute(ctx context.Context, req runtimerunforkexecution.SelectedContractExecutionRequest) (runtimerunforkexecution.SelectedContractExecutionResult, error) {
	if o.planner == nil {
		return runtimerunforkexecution.SelectedContractExecutionResult{}, errors.New("selected run.fork runtime owner is required")
	}
	req.Owner = o.executionOwner
	return runtimerunforkexecution.ExecuteSelectedContractRunFork(ctx, req)
}

func OpenRuntime(ctx context.Context, req RuntimeRequest) (*Owner, error) {
	var owner *Owner
	var err error
	switch req.Selection.Backend {
	case storebackend.BackendPostgres:
		owner, err = openPostgresRuntime(req.PostgresDSN, req.SessionLockTTL)
	case storebackend.BackendSQLite:
		owner, err = openSQLiteRuntime(req.Selection.SQLitePath, req.SessionLockTTL)
	default:
		return nil, fmt.Errorf("store backend selection is required; supported backends: %s, %s", storebackend.BackendPostgres, storebackend.BackendSQLite)
	}
	if err != nil {
		return nil, err
	}
	return validateOpenedRuntime(ctx, owner)
}

func validateOpenedRuntime(ctx context.Context, owner *Owner) (*Owner, error) {
	if err := owner.required.pinger.Ping(ctx); err != nil {
		return nil, errors.Join(err, owner.CloseUnactivated())
	}
	return owner, nil
}

func openPostgresRuntime(dsn string, ttl time.Duration) (*Owner, error) {
	selected, _, err := storeconstruction.OpenPostgres(dsn)
	if err != nil {
		return nil, err
	}
	selected.SetSessionLockTTL(ttl)
	owner, err := composePostgres(selected)
	if err != nil {
		return nil, errors.Join(err, selected.Close())
	}
	return owner, nil
}

func openSQLiteRuntime(path string, ttl time.Duration) (*Owner, error) {
	selected, _, err := storeconstruction.OpenSQLiteRuntimeWithOwnershipBinding(path)
	if err != nil {
		return nil, err
	}
	selected.SetSessionLockTTL(ttl)
	owner, err := composeSQLite(selected)
	if err != nil {
		return nil, errors.Join(err, selected.Close())
	}
	return owner, nil
}

func composePostgres(selected *private.PostgresStore) (*Owner, error) {
	workflow := runtimepipeline.NewWorkflowPersistence(selected)
	runFork, err := newPostgresRunFork(selected, workflow)
	if err != nil {
		return nil, err
	}
	return &Owner{
		lifetime: lifecycle{resource: selected, state: ownershipUnactivated},
		core: runtime.RuntimeDeps{
			EventStore: selected,
			EventBusDurable: runtimebus.DurableDependencies{
				ReplyContext: selected, RunLifecycle: selected, DeliveryLifecycle: selected,
				FlowRoutes: selected, FlowRouteRecords: selected, FlowRouteSets: selected,
				FlowRouteTopology: selected, FlowRouteRollback: selected,
				ActiveAgents: selected, ActiveFlows: selected, TargetOwners: selected,
				WorkflowInstances: selected, PreparedEvents: selected,
				TargetFailureRecorder: selected, RunOrigins: selected, StandingRestarts: selected,
			},
			EventPayloadValidationBinder: selected, InboundPayloadValidationBinder: selected,
			AuthorActivityRegistrars: []runtime.AuthorActivityCatalogRegistrar{selected},
			RunControlStore:          selected, RunLifecycleCandidates: selected, RuntimeLogStore: selected,
			WorkflowPersistence: workflow, RunBundleAvailability: selected,
			SessionRegistry: selected, LiveSessionAcquirer: selected, SessionResetter: selected,
			ConversationStore: selected, ManagerStore: selected, ManagerLifecycleDiagnostics: selected,
			ManagerPersistenceRoles: managerRoles(selected),
			EffectsStore:            selected, CompletionStore: selected, CompletionHeartbeatStore: selected,
			EffectsRecoveryStore: selected, ManagedCapabilitiesStore: selected, DeliveryStore: selected,
			PipelineObligations: selected.PipelineObligations(), GenericScheduleStore: selected,
			TimerObligationReader: selected, MailboxMaterializer: selected,
			DecisionCards: selected, ProposedEffects: selected, DecisionCardHumanTasks: selected,
			DecisionCardDraftExpiry: selected, HumanTaskExpiry: selected,
			MailboxStore: selected, ToolEntityStore: selected, HumanTaskStore: selected,
			BudgetSpendStore: selected, InboundStore: selected, RuntimeIngressStore: selected,
			DataAccessStore:           selected,
			ScenarioExecutionProfiles: selected,
		},
		required: requiredPorts{
			schema: selected, pinger: selected, authorActivity: selected,
			operatorChannels: selected, channelOnboarding: selected, startupOwnership: selected, runQuiescence: selected,
			mailboxAPI: selected, mailboxNoticeAck: selected, observability: selected,
			agentUsage: selected, agentDeliveryLifecycle: selected, idempotency: selected,
			runs: selected, entities: selected, agents: selected, conversations: selected,
			testSetup: selected, runBundleContext: selected, data: selected, dataAccess: selected, runBundleAvailability: selected,
			runStalled: selected, sourceArtifacts: selected, sourceArtifactReader: selected,
		},
		products: productPorts{
			conversationFork: ConversationFork{reader: selected, lifecycle: selected}, conversationAvailable: true,
			runFork: runFork, runForkAvailable: true,
			destructiveReset: DestructiveReset{inventory: selected, locks: selected, quiescence: selected}, destructiveAvailable: true,
			startupRecovery: StartupRecovery{availability: selected}, startupRecoveryAvailable: true,
		},
	}, nil
}

func composeSQLite(selected *private.SQLiteRuntimeStore) (*Owner, error) {
	workflow := runtimepipeline.NewWorkflowPersistence(selected)
	runFork, err := newSQLiteRunFork(selected, workflow)
	if err != nil {
		return nil, err
	}
	return &Owner{
		lifetime: lifecycle{resource: selected, state: ownershipUnactivated},
		core: runtime.RuntimeDeps{
			EventStore: selected,
			EventBusDurable: runtimebus.DurableDependencies{
				ReplyContext: selected, RunLifecycle: selected, DeliveryLifecycle: selected,
				FlowRoutes: selected, FlowRouteRecords: selected, FlowRouteSets: selected,
				FlowRouteTopology: selected, FlowRouteRollback: selected,
				ActiveAgents: selected, ActiveFlows: selected, TargetOwners: selected,
				WorkflowInstances: selected, PreparedEvents: selected,
				TargetFailureRecorder: selected, RunOrigins: selected, StandingRestarts: selected,
			},
			EventPayloadValidationBinder: selected, InboundPayloadValidationBinder: selected,
			AuthorActivityRegistrars: []runtime.AuthorActivityCatalogRegistrar{selected},
			RunControlStore:          selected, RunLifecycleCandidates: selected, RuntimeLogStore: selected,
			WorkflowPersistence: workflow, RunBundleAvailability: selected,
			SessionRegistry: selected, LiveSessionAcquirer: selected, SessionResetter: selected,
			ConversationStore: selected, ManagerStore: selected, ManagerLifecycleDiagnostics: selected,
			ManagerPersistenceRoles: sqliteManagerRoles(selected),
			EffectsStore:            selected, CompletionStore: selected, CompletionHeartbeatStore: selected,
			EffectsRecoveryStore: selected, ManagedCapabilitiesStore: selected, DeliveryStore: selected,
			PipelineObligations: selected.PipelineObligations(), GenericScheduleStore: selected,
			TimerObligationReader: selected, MailboxMaterializer: selected,
			DecisionCards: selected, ProposedEffects: selected, DecisionCardHumanTasks: selected,
			DecisionCardDraftExpiry: selected, HumanTaskExpiry: selected,
			MailboxStore: selected, ToolEntityStore: selected, HumanTaskStore: selected,
			BudgetSpendStore: selected, InboundStore: selected, RuntimeIngressStore: selected,
			DataAccessStore:           selected,
			ScenarioExecutionProfiles: selected,
		},
		required: requiredPorts{
			schema: selected, pinger: selected, authorActivity: selected,
			operatorChannels: selected, channelOnboarding: selected, startupOwnership: selected, runQuiescence: selected,
			mailboxAPI: selected, mailboxNoticeAck: selected, observability: selected,
			agentUsage: selected, agentDeliveryLifecycle: selected, idempotency: selected,
			runs: selected, entities: selected, agents: selected, conversations: selected,
			testSetup: selected, runBundleContext: selected, data: selected, dataAccess: selected, runBundleAvailability: selected,
			runStalled: selected, sourceArtifacts: selected, sourceArtifactReader: selected,
		},
		products: productPorts{
			conversationFork: ConversationFork{reader: selected, lifecycle: selected}, conversationAvailable: true,
			runFork: runFork, runForkAvailable: true,
		},
	}, nil
}

func managerRoles(selected *private.PostgresStore) runtimemanager.PersistenceRoles {
	return runtimemanager.PersistenceRoles{
		LifecycleCensus: selected, LifecycleState: selected, LifecycleEffects: selected,
		LifecycleDiagnostics: selected, EffectsRecovery: selected, StandingRestarts: selected, DeliveryQuiescence: selected,
		EventExistence: selected, DirectiveOperations: selected, DirectiveTargets: selected, FlowRoutes: selected,
	}
}

func sqliteManagerRoles(selected *private.SQLiteRuntimeStore) runtimemanager.PersistenceRoles {
	return runtimemanager.PersistenceRoles{
		LifecycleCensus: selected, LifecycleState: selected, LifecycleEffects: selected,
		LifecycleDiagnostics: selected, EffectsRecovery: selected, StandingRestarts: selected, DeliveryQuiescence: selected,
		EventExistence: selected, DirectiveOperations: selected, DirectiveTargets: selected, FlowRoutes: selected,
	}
}

func newPostgresRunFork(selected *private.PostgresStore, workflow runtimepipeline.WorkflowPersistence) (RunFork, error) {
	durable := runtimebus.DurableDependencies{
		ReplyContext: selected, RunLifecycle: selected, DeliveryLifecycle: selected,
		FlowRoutes: selected, FlowRouteRecords: selected, FlowRouteSets: selected,
		FlowRouteTopology: selected, FlowRouteRollback: selected, ActiveAgents: selected,
		ActiveFlows: selected, TargetOwners: selected, WorkflowInstances: selected,
		PreparedEvents: selected, TargetFailureRecorder: selected, RunOrigins: selected, StandingRestarts: selected,
	}
	execution, err := runtimerunforkexecution.NewSelectedContractExecutionOwner(
		workflow, selected, selected, selected, selected, durable, selected.PipelineObligations(),
		selected, managerRoles(selected), selected, selected, selected, selected, selected,
		selected, selected, selected, selected, selected, selected, selected, selected,
	)
	if err != nil {
		return RunFork{}, err
	}
	return RunFork{planner: selected, availability: selected, activation: selected, executionOwner: execution}, nil
}

func newSQLiteRunFork(selected *private.SQLiteRuntimeStore, workflow runtimepipeline.WorkflowPersistence) (RunFork, error) {
	durable := runtimebus.DurableDependencies{
		ReplyContext: selected, RunLifecycle: selected, DeliveryLifecycle: selected,
		FlowRoutes: selected, FlowRouteRecords: selected, FlowRouteSets: selected,
		FlowRouteTopology: selected, FlowRouteRollback: selected, ActiveAgents: selected,
		ActiveFlows: selected, TargetOwners: selected, WorkflowInstances: selected,
		PreparedEvents: selected, TargetFailureRecorder: selected, RunOrigins: selected, StandingRestarts: selected,
	}
	execution, err := runtimerunforkexecution.NewSelectedContractExecutionOwner(
		workflow, selected, selected, selected, selected, durable, selected.PipelineObligations(),
		selected, sqliteManagerRoles(selected), selected, selected, selected, selected, selected,
		selected, selected, selected, selected, selected, selected, selected, selected,
	)
	if err != nil {
		return RunFork{}, err
	}
	return RunFork{planner: selected, availability: selected, activation: selected, executionOwner: execution}, nil
}
