package main

import (
	"context"
	"database/sql"
	"fmt"

	apiv1 "github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime"
	runtimebundledelete "github.com/division-sh/swarm/internal/runtime/bundledelete"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimerunforkadmission "github.com/division-sh/swarm/internal/runtime/runforkadmission"
	runtimerunforkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartuprecovery "github.com/division-sh/swarm/internal/runtime/startuprecovery"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/runbundle"
)

type selectedRuntimeStoreFacade struct {
	stores storeBundle
}

type selectedBundleRuntimeCatalogStore interface {
	LoadBundleCatalogRuntimeRecord(context.Context, string) (store.BundleCatalogRuntimeRecord, error)
}

type selectedBundleSourceCatalogStore interface {
	UpsertBundleCatalog(context.Context, store.BundleCatalogUpsert) (store.BundleCatalogUpsertResult, error)
}

type selectedRunBundleAvailabilityStore interface {
	LoadRunBundleAvailability(context.Context, string) (runbundle.Availability, error)
	ActiveRunBundleAvailabilities(context.Context) ([]runbundle.Availability, error)
	ActiveRunBundleAvailabilityConflicts(context.Context) ([]store.ActiveRunBundleAvailabilityConflict, error)
}

type selectedStartupRecoveryStore interface {
	runtimestartuprecovery.AvailabilityReader
	runtimestartuprecovery.PreservationCleanupStore
}

type selectedSchemaCapabilityBinder interface {
	BindSchemaCapabilities(context.Context) (store.StoreSchemaCapabilities, error)
}

type selectedAPICapabilities struct {
	Database            apiv1.Pinger
	Runs                apiv1.RunReadStore
	Entities            apiv1.EntityReadStore
	AgentConversations  apiv1.AgentConversationReadStore
	Observability       apiv1.ObservabilityReadStore
	RunBundleContext    apiv1.RunBundleContextStore
	BundleCatalog       apiv1.BundleCatalogReadStore
	BundleDelete        apiv1.BundleDeleteExecutor
	ConversationForks   apiv1.ConversationForkLifecycleStore
	RunForkAvailability apiv1.RunForkAvailabilityStore
	RunFork             apiv1.RunForkExecutor
	RuntimeContexts     *runtime.RuntimeContextManager
	ResetCoordinator    apiv1.DestructiveResetCoordinator
	ResetQuiescer       apiv1.DestructiveResetQuiescer
	ResetCleaner        apiv1.DestructiveResetCleaner
}

type selectedAPICapabilityRequest struct {
	RepoRoot         string
	PlatformSpecPath string
	LoadedBundle     serveRuntimeBundle
	RuntimeContexts  []serveRuntimeBundleContext
	Source           semanticview.Source
	ContractsRoot    string
	Config           *config.Config
	Workspaces       serveWorkspaceLifecycle
	Credentials      runtimecredentials.Store
}

type selectedRunForkRuntimeOwner struct {
	store *store.PostgresStore
}

func (s storeBundle) facade() selectedRuntimeStoreFacade {
	return selectedRuntimeStoreFacade{stores: s}
}

func (f selectedRuntimeStoreFacade) runtimeStores() runtime.Stores {
	s := f.stores
	return runtime.Stores{
		SQLDB:               s.RuntimeSQLDB,
		ConstructionBlocker: s.RuntimeBlocker,
		EventStore:          s.EventStore,
		RuntimeLogStore:     s.RuntimeLogStore,
		PipelineStore:       s.PipelineStore,
		SessionRegistry:     s.SessionRegistry,
		ConversationStore:   s.ConversationStore,
		ManagerStore:        s.ManagerStore,
		ScheduleStore:       s.ScheduleStore,
		MailboxMaterializer: s.MailboxMaterializer,
		StartupOwnership:    s.StartupOwnership,
		MailboxStore:        s.MailboxStore,
		ToolEntityStore:     s.ToolEntityStore,
		HumanTaskStore:      s.HumanTaskStore,
		BudgetSpendStore:    s.BudgetSpendStore,
		RuntimeIngressStore: s.RuntimeIngressStore,
		TurnStore:           s.TurnStore,
	}
}

func (f selectedRuntimeStoreFacade) close() {
	closeDB(f.stores.SQLDB)
}

func (f selectedRuntimeStoreFacade) workspaceDB() *sql.DB {
	return f.stores.SQLDB
}

func (f selectedRuntimeStoreFacade) pinger() apiv1.Pinger {
	if f.stores.Postgres != nil {
		return f.stores.Postgres
	}
	if f.stores.SQLDB != nil {
		return sqlDBPinger{db: f.stores.SQLDB}
	}
	return nil
}

func (f selectedRuntimeStoreFacade) apiRunBundleContextStore() apiv1.RunBundleContextStore {
	if f.stores.Postgres != nil {
		return f.stores.Postgres
	}
	runBundleContext, _ := f.stores.ObservabilityStore.(apiv1.RunBundleContextStore)
	return runBundleContext
}

func (f selectedRuntimeStoreFacade) apiReadStores() (apiv1.RunReadStore, apiv1.EntityReadStore, apiv1.AgentConversationReadStore, apiv1.ObservabilityReadStore) {
	if f.stores.Postgres != nil {
		return f.stores.Postgres, f.stores.Postgres, f.stores.Postgres, f.stores.Postgres
	}
	var runs apiv1.RunReadStore
	var entities apiv1.EntityReadStore
	if runStore, ok := f.stores.ObservabilityStore.(apiv1.RunReadStore); ok {
		runs = runStore
	}
	if entityStore, ok := f.stores.ObservabilityStore.(apiv1.EntityReadStore); ok {
		entities = entityStore
	}
	return runs, entities, nil, f.stores.ObservabilityStore
}

func (f selectedRuntimeStoreFacade) bundleRuntimeCatalogStore() selectedBundleRuntimeCatalogStore {
	if f.stores.Postgres == nil {
		return nil
	}
	return f.stores.Postgres
}

func (f selectedRuntimeStoreFacade) bundleSourceCatalogStore() selectedBundleSourceCatalogStore {
	if f.stores.Postgres == nil {
		return nil
	}
	return f.stores.Postgres
}

func (f selectedRuntimeStoreFacade) runBundleAvailabilityStore() selectedRunBundleAvailabilityStore {
	if f.stores.Postgres == nil {
		return nil
	}
	return f.stores.Postgres
}

func (f selectedRuntimeStoreFacade) startupRecoveryStore() selectedStartupRecoveryStore {
	if f.stores.Postgres == nil {
		return nil
	}
	return f.stores.Postgres
}

func (f selectedRuntimeStoreFacade) schemaCapabilityBinder() selectedSchemaCapabilityBinder {
	binder, _ := f.stores.SchemaBootstrapper.(selectedSchemaCapabilityBinder)
	return binder
}

func (f selectedRuntimeStoreFacade) runStalledReader() runStalledReadStore {
	if f.stores.Postgres != nil {
		return f.stores.Postgres
	}
	readStore, _ := f.stores.ObservabilityStore.(runStalledReadStore)
	return readStore
}

func (f selectedRuntimeStoreFacade) apiCapabilities(req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
	runs, entities, agentConversations, observability := f.apiReadStores()
	caps := selectedAPICapabilities{
		Database:           f.pinger(),
		Runs:               runs,
		Entities:           entities,
		AgentConversations: agentConversations,
		Observability:      observability,
		RunBundleContext:   f.apiRunBundleContextStore(),
	}
	if f.stores.Postgres == nil {
		return caps, nil
	}
	resetPlanner := runtimedestructivereset.InventoryPlanner{
		Reader: runtimedestructivereset.CompositeInventoryReader{
			Reader:     f.stores.Postgres,
			Containers: req.Workspaces,
		},
	}
	runForkSourceLoader := runtimerunforkexecution.SelectedContractSourceLoader(runtimerunforkexecution.ContractBundleSourceLoader{
		RepoRoot:         req.RepoRoot,
		PlatformSpecPath: req.PlatformSpecPath,
	})
	if req.LoadedBundle.dbLoaded {
		runForkSourceLoader = runtimerunforkexecution.BundleCatalogSelectedContractSourceLoader{
			RepoRoot: req.RepoRoot,
			Store:    f.stores.Postgres,
		}
	}
	apiRuntimeContexts, err := serveRuntimeContextManager(f.stores.Postgres, req.RuntimeContexts)
	if err != nil {
		return selectedAPICapabilities{}, err
	}
	caps.BundleCatalog = f.stores.Postgres
	caps.BundleDelete = &runtimebundledelete.Coordinator{
		Planner:            f.stores.Postgres,
		Cleaner:            f.stores.Postgres,
		Finalizer:          f.stores.Postgres,
		Locks:              f.stores.Postgres,
		ContainerInventory: req.Workspaces,
		Containers:         runtimedestructivereset.ManagedContainerStopper{Runtime: req.Workspaces},
	}
	caps.ConversationForks = f.stores.Postgres
	caps.RunForkAvailability = f.stores.Postgres
	caps.RunFork = apiv1.SelectedContractRunForkExecutor{
		Store:             f.stores.Postgres,
		SourceLoader:      runForkSourceLoader,
		ContractSelection: runtimerunforkadmission.SelectedContractSelection(req.Source, req.ContractsRoot),
		AgentRuntime: runtimerunforkexecution.SelectedContractAgentRuntimeOptions{
			Config:            req.Config,
			EntityStore:       f.stores.ToolEntityStore,
			HumanTaskStore:    f.stores.HumanTaskStore,
			SessionRegistry:   f.stores.SessionRegistry,
			ConversationStore: f.stores.ConversationStore,
			TurnStore:         f.stores.TurnStore,
			ScheduleStore:     f.stores.ScheduleStore,
			MailboxStore:      f.stores.MailboxStore,
			Workspace:         req.Workspaces,
			Credentials:       req.Credentials,
		},
	}
	caps.RuntimeContexts = apiRuntimeContexts
	caps.ResetCoordinator = &runtimedestructivereset.Coordinator{
		Planner: resetPlanner,
		Locks:   f.stores.Postgres,
	}
	caps.ResetQuiescer = runtimedestructivereset.Quiescer{Store: f.stores.Postgres}
	caps.ResetCleaner = runtimedestructivereset.Cleaner{Store: f.stores.Postgres}
	return caps, nil
}

func (f selectedRuntimeStoreFacade) runForkRuntimeOwner() (selectedRunForkRuntimeOwner, bool) {
	if f.stores.Postgres == nil {
		return selectedRunForkRuntimeOwner{}, false
	}
	return selectedRunForkRuntimeOwner{store: f.stores.Postgres}, true
}

func (o selectedRunForkRuntimeOwner) activate(ctx context.Context, req runtimerunforkexecution.SelectedContractActivationGateRequest) (runtimerunforkexecution.SelectedContractActivationGateResult, error) {
	if o.store == nil {
		return runtimerunforkexecution.SelectedContractActivationGateResult{}, fmt.Errorf("postgres store required")
	}
	req.Store = o.store
	return runtimerunforkexecution.ActivateSelectedContractRunFork(ctx, req)
}

func (o selectedRunForkRuntimeOwner) materialize(ctx context.Context, req store.RunForkMaterializeRequest) (store.RunForkMaterialization, error) {
	if o.store == nil {
		return store.RunForkMaterialization{}, fmt.Errorf("postgres store required")
	}
	return o.store.MaterializeRunFork(ctx, req)
}

func (o selectedRunForkRuntimeOwner) execute(ctx context.Context, req runtimerunforkexecution.SelectedContractExecutionRequest) (runtimerunforkexecution.SelectedContractExecutionResult, error) {
	if o.store == nil {
		return runtimerunforkexecution.SelectedContractExecutionResult{}, fmt.Errorf("postgres store required")
	}
	req.Store = o.store
	return runtimerunforkexecution.ExecuteSelectedContractRunFork(ctx, req)
}

func (o selectedRunForkRuntimeOwner) plan(ctx context.Context, req store.RunForkPlanRequest) (store.RunForkPlan, error) {
	if o.store == nil {
		return store.RunForkPlan{}, fmt.Errorf("postgres store required")
	}
	return o.store.PlanRunFork(ctx, req)
}
