package main

import (
	"context"
	"database/sql"
	"fmt"

	apiv1 "github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
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

type selectedAPIOptionalCapabilityBuilder func(selectedAPICapabilityRequest) (selectedAPICapabilities, error)

type selectedRunForkRuntimeOwner struct {
	activateFunc    func(context.Context, runtimerunforkexecution.SelectedContractActivationGateRequest) (runtimerunforkexecution.SelectedContractActivationGateResult, error)
	materializeFunc func(context.Context, store.RunForkMaterializeRequest) (store.RunForkMaterialization, error)
	executeFunc     func(context.Context, runtimerunforkexecution.SelectedContractExecutionRequest) (runtimerunforkexecution.SelectedContractExecutionResult, error)
	planFunc        func(context.Context, store.RunForkPlanRequest) (store.RunForkPlan, error)
}

func (o selectedRunForkRuntimeOwner) configured() bool {
	return o.activateFunc != nil && o.materializeFunc != nil && o.executeFunc != nil && o.planFunc != nil
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
	return f.stores.Database
}

func (f selectedRuntimeStoreFacade) apiRunBundleContextStore() apiv1.RunBundleContextStore {
	return f.stores.RunBundleContextStore
}

func (f selectedRuntimeStoreFacade) apiReadStores() (apiv1.RunReadStore, apiv1.EntityReadStore, apiv1.AgentConversationReadStore, apiv1.ObservabilityReadStore) {
	return f.stores.RunReadStore, f.stores.EntityReadStore, f.stores.AgentConversationReadStore, f.stores.ObservabilityStore
}

func (f selectedRuntimeStoreFacade) bundleRuntimeCatalogStore() selectedBundleRuntimeCatalogStore {
	return f.stores.BundleRuntimeCatalogStore
}

func (f selectedRuntimeStoreFacade) bundleSourceCatalogStore() selectedBundleSourceCatalogStore {
	return f.stores.BundleSourceCatalogStore
}

func (f selectedRuntimeStoreFacade) runBundleAvailabilityStore() selectedRunBundleAvailabilityStore {
	return f.stores.RunBundleAvailabilityStore
}

func (f selectedRuntimeStoreFacade) startupRecoveryStore() selectedStartupRecoveryStore {
	return f.stores.StartupRecoveryStore
}

func (f selectedRuntimeStoreFacade) schemaCapabilityBinder() selectedSchemaCapabilityBinder {
	binder, _ := f.stores.SchemaBootstrapper.(selectedSchemaCapabilityBinder)
	return binder
}

func (f selectedRuntimeStoreFacade) runStalledReader() runStalledReadStore {
	return f.stores.RunStalledReader
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
	if f.stores.APIOptionalCapabilityBuilder == nil {
		return caps, nil
	}
	optional, err := f.stores.APIOptionalCapabilityBuilder(req)
	if err != nil {
		return selectedAPICapabilities{}, err
	}
	caps.BundleCatalog = optional.BundleCatalog
	caps.BundleDelete = optional.BundleDelete
	caps.ConversationForks = optional.ConversationForks
	caps.RunForkAvailability = optional.RunForkAvailability
	caps.RunFork = optional.RunFork
	caps.RuntimeContexts = optional.RuntimeContexts
	caps.ResetCoordinator = optional.ResetCoordinator
	caps.ResetQuiescer = optional.ResetQuiescer
	caps.ResetCleaner = optional.ResetCleaner
	return caps, nil
}

func (f selectedRuntimeStoreFacade) runForkRuntimeOwner() (selectedRunForkRuntimeOwner, bool) {
	if !f.stores.RunForkRuntimeOwner.configured() {
		return selectedRunForkRuntimeOwner{}, false
	}
	return f.stores.RunForkRuntimeOwner, true
}

func (o selectedRunForkRuntimeOwner) activate(ctx context.Context, req runtimerunforkexecution.SelectedContractActivationGateRequest) (runtimerunforkexecution.SelectedContractActivationGateResult, error) {
	if o.activateFunc == nil {
		return runtimerunforkexecution.SelectedContractActivationGateResult{}, fmt.Errorf("selected run.fork runtime owner is required")
	}
	return o.activateFunc(ctx, req)
}

func (o selectedRunForkRuntimeOwner) materialize(ctx context.Context, req store.RunForkMaterializeRequest) (store.RunForkMaterialization, error) {
	if o.materializeFunc == nil {
		return store.RunForkMaterialization{}, fmt.Errorf("selected run.fork runtime owner is required")
	}
	return o.materializeFunc(ctx, req)
}

func (o selectedRunForkRuntimeOwner) execute(ctx context.Context, req runtimerunforkexecution.SelectedContractExecutionRequest) (runtimerunforkexecution.SelectedContractExecutionResult, error) {
	if o.executeFunc == nil {
		return runtimerunforkexecution.SelectedContractExecutionResult{}, fmt.Errorf("selected run.fork runtime owner is required")
	}
	return o.executeFunc(ctx, req)
}

func (o selectedRunForkRuntimeOwner) plan(ctx context.Context, req store.RunForkPlanRequest) (store.RunForkPlan, error) {
	if o.planFunc == nil {
		return store.RunForkPlan{}, fmt.Errorf("selected run.fork runtime owner is required")
	}
	return o.planFunc(ctx, req)
}
