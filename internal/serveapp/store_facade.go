package serveapp

import (
	"context"
	"fmt"
	"log"
	"sync"

	apiv1 "github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/bundlecatalog"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunforkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartuprecovery "github.com/division-sh/swarm/internal/runtime/startuprecovery"
	"github.com/division-sh/swarm/internal/runtime/workspace"
)

type selectedRuntimeStoreFacade struct {
	stores storeBundle
}

type selectedStoreOwnershipState uint8

const (
	selectedStoreUnactivated selectedStoreOwnershipState = iota
	selectedStoreActivated
	selectedStoreClosed
)

type selectedStoreOwner struct {
	mu      sync.Mutex
	facade  selectedRuntimeStoreFacade
	state   selectedStoreOwnershipState
	process *worklifetime.Process
}

func newSelectedStoreOwner(facade selectedRuntimeStoreFacade) *selectedStoreOwner {
	return &selectedStoreOwner{facade: facade, state: selectedStoreUnactivated}
}

func (o *selectedStoreOwner) Activate(process *worklifetime.Process) error {
	if o == nil || process == nil {
		return fmt.Errorf("selected store activation requires a process work owner")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.state != selectedStoreUnactivated {
		return fmt.Errorf("selected store activation requires unactivated construction state")
	}
	o.process = process
	o.state = selectedStoreActivated
	return nil
}

func (o *selectedStoreOwner) CloseUnactivated() error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.state == selectedStoreClosed {
		return nil
	}
	if o.state != selectedStoreUnactivated {
		return fmt.Errorf("activated selected store requires a process join receipt")
	}
	if err := o.facade.closeWithError(); err != nil {
		return err
	}
	o.state = selectedStoreClosed
	return nil
}

func (o *selectedStoreOwner) CloseActivated(receipt *worklifetime.ProcessJoinReceipt) error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.state == selectedStoreClosed {
		return nil
	}
	if o.state != selectedStoreActivated || o.process == nil {
		return fmt.Errorf("selected store is not activated")
	}
	if err := o.process.ValidateJoinReceipt(receipt); err != nil {
		return err
	}
	if err := o.facade.closeWithError(); err != nil {
		return err
	}
	o.state = selectedStoreClosed
	return nil
}

type selectedBundleRuntimeCatalogStore interface {
	LoadBundleCatalogRuntimeRecord(context.Context, string) (runbundle.BundleCatalogRuntimeRecord, error)
}

type selectedBundleSourceCatalogStore interface {
	UpsertBundleCatalog(context.Context, bundlecatalog.Upsert) (bundlecatalog.UpsertResult, error)
}

type selectedRunBundleAvailabilityStore interface {
	LoadRunBundleAvailability(context.Context, string) (runbundle.Availability, error)
	ActiveRunBundleAvailabilities(context.Context) ([]runbundle.Availability, error)
	ActiveRunBundleAvailabilityConflicts(context.Context) ([]runbundle.Availability, error)
}

type selectedStartupRecoveryStore interface {
	runtimestartuprecovery.AvailabilityReader
	runtimestartuprecovery.PreservationCleanupStore
}

type selectedAPICapabilities struct {
	Database                  apiv1.Pinger
	Runs                      apiv1.RunReadStore
	Entities                  apiv1.EntityReadStore
	Agents                    apiv1.AgentReadStore
	Conversations             apiv1.ConversationReadStore
	Observability             apiv1.ObservabilityReadStore
	RunBundleContext          apiv1.RunBundleContextStore
	TestSetup                 apiv1.TestSetupStore
	BundleCatalog             apiv1.BundleCatalogReadStore
	BundleRegister            apiv1.BundleCatalogRegisterStore
	BundleDelete              apiv1.BundleDeleteExecutor
	ConversationForks         apiv1.ConversationForkReadStore
	ConversationForkLifecycle apiv1.ConversationForkLifecycleStore
	RunForkAvailability       apiv1.RunForkAvailabilityStore
	RunFork                   apiv1.RunForkExecutor
	RunForkSelector           apiv1.RunForkExecutorSelector
	RuntimeContexts           *runtime.RuntimeContextManager
	ResetCoordinator          apiv1.DestructiveResetCoordinator
}

type selectedAPICapabilityRequest struct {
	RepoRoot                string
	PlatformSpecPath        string
	RunningPlatformSpecPath string
	LoadedBundle            serveRuntimeBundle
	RuntimeContexts         []serveRuntimeBundleContext
	RuntimeContextManager   *runtime.RuntimeContextManager
	Source                  semanticview.Source
	ContractsRoot           string
	Config                  *config.Config
	Workspaces              cliapp.ServeWorkspaceLifecycle
	Credentials             runtimecredentials.Store
	ManagedCredentials      runtimemanagedcredentials.Store
	ProviderCredentials     runtimecredentials.Store
}

type selectedAPIOptionalCapabilityBuilder func(selectedAPICapabilityRequest) (selectedAPICapabilities, error)

type selectedRunForkRuntimeOwner struct {
	activateFunc    func(context.Context, runtimerunforkexecution.SelectedContractActivationGateRequest) (runtimerunforkexecution.SelectedContractActivationGateResult, error)
	materializeFunc func(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
	executeFunc     func(context.Context, runtimerunforkexecution.SelectedContractExecutionRequest) (runtimerunforkexecution.SelectedContractExecutionResult, error)
	planFunc        func(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
}

func (o selectedRunForkRuntimeOwner) configured() bool {
	return o.activateFunc != nil && o.materializeFunc != nil && o.executeFunc != nil && o.planFunc != nil
}

func (s storeBundle) facade() selectedRuntimeStoreFacade {
	return selectedRuntimeStoreFacade{stores: s}
}

func (f selectedRuntimeStoreFacade) runtimeDeps() runtime.RuntimeDeps {
	s := f.stores
	return runtime.RuntimeDeps{
		EventStore:                     s.EventStore,
		EventBusDurable:                s.EventBusDurable,
		EventPayloadValidationBinder:   s.EventPayloadValidationBinder,
		InboundPayloadValidationBinder: s.InboundPayloadValidationBinder,
		AuthorActivityRegistrars:       append([]runtime.AuthorActivityCatalogRegistrar(nil), s.AuthorActivityRegistrars...),
		RunControlStore:                s.RunControlStore,
		RunLifecycleCandidates:         s.RunLifecycleCandidates,
		RuntimeLogStore:                s.RuntimeLogStore,
		WorkflowPersistence:            s.WorkflowPersistence,
		RunBundleAvailability:          s.RunBundleAvailabilityStore,
		SessionRegistry:                s.SessionRegistry,
		LiveSessionAcquirer:            s.LiveSessionAcquirer,
		SessionResetter:                s.SessionResetter,
		ConversationStore:              s.ConversationStore,
		ManagerStore:                   s.ManagerStore,
		ManagerLifecycleStore:          s.ManagerLifecycleStore,
		ManagerLifecycleDiagnostics:    s.ManagerLifecycleDiagnostics,
		ManagerPersistenceRoles:        s.ManagerPersistenceRoles,
		EffectsStore:                   s.EffectsStore,
		CompletionStore:                s.CompletionStore,
		CompletionHeartbeatStore:       s.CompletionHeartbeatStore,
		EffectsRecoveryStore:           s.EffectsRecoveryStore,
		ManagedCapabilitiesStore:       s.ManagedCapabilitiesStore,
		DeliveryStore:                  s.DeliveryStore,
		PipelineObligations:            s.PipelineObligations,
		GenericScheduleStore:           s.GenericScheduleStore,
		TimerObligationReader:          s.TimerObligationReader,
		MailboxMaterializer:            s.MailboxMaterializer,
		DecisionCards:                  s.DecisionCards,
		ProposedEffects:                s.ProposedEffects,
		DecisionCardHumanTasks:         s.DecisionCardHumanTasks,
		DecisionCardDraftExpiry:        s.DecisionCardDraftExpiry,
		HumanTaskExpiry:                s.HumanTaskExpiry,
		StartupOwnership:               s.StartupOwnership,
		MailboxStore:                   s.MailboxStore,
		ToolEntityStore:                s.ToolEntityStore,
		HumanTaskStore:                 s.HumanTaskStore,
		BudgetSpendStore:               s.BudgetSpendStore,
		InboundStore:                   s.InboundStore,
		RuntimeIngressStore:            s.RuntimeIngressStore,
	}
}

func (f selectedRuntimeStoreFacade) close() {
	if err := f.closeWithError(); err != nil {
		log.Printf("close db: %v", err)
	}
}

func (f selectedRuntimeStoreFacade) closeWithError() error {
	if f.stores.SQLDB == nil {
		return nil
	}
	return f.stores.SQLDB.Close()
}

func (f selectedRuntimeStoreFacade) workspaceLookup() workspace.Lookup {
	return f.stores.WorkspaceLookup
}

func (f selectedRuntimeStoreFacade) pinger() apiv1.Pinger {
	return f.stores.Database
}

func (f selectedRuntimeStoreFacade) apiRunBundleContextStore() apiv1.RunBundleContextStore {
	return f.stores.RunBundleContextStore
}

func (f selectedRuntimeStoreFacade) apiReadStores() (apiv1.RunReadStore, apiv1.EntityReadStore, apiv1.AgentReadStore, apiv1.ConversationReadStore, apiv1.ObservabilityReadStore) {
	return f.stores.RunReadStore, f.stores.EntityReadStore, f.stores.AgentReadStore, f.stores.ConversationReadStore, f.stores.ObservabilityStore
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

func (f selectedRuntimeStoreFacade) runStalledReader() runStalledReadStore {
	return f.stores.RunStalledReader
}

func (f selectedRuntimeStoreFacade) apiCapabilities(req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
	runs, entities, agents, conversations, observability := f.apiReadStores()
	testSetup, _ := entities.(apiv1.TestSetupStore)
	caps := selectedAPICapabilities{
		Database:         f.pinger(),
		Runs:             runs,
		Entities:         entities,
		Agents:           agents,
		Conversations:    conversations,
		Observability:    observability,
		RunBundleContext: f.apiRunBundleContextStore(),
		TestSetup:        testSetup,
		RuntimeContexts:  req.RuntimeContextManager,
	}
	if f.stores.APIOptionalCapabilityBuilder == nil {
		return caps, nil
	}
	optional, err := f.stores.APIOptionalCapabilityBuilder(req)
	if err != nil {
		return selectedAPICapabilities{}, err
	}
	caps.BundleCatalog = optional.BundleCatalog
	caps.BundleRegister = optional.BundleRegister
	caps.BundleDelete = optional.BundleDelete
	caps.ConversationForks = optional.ConversationForks
	caps.ConversationForkLifecycle = optional.ConversationForkLifecycle
	caps.RunForkAvailability = optional.RunForkAvailability
	caps.RunFork = optional.RunFork
	caps.RunForkSelector = optional.RunForkSelector
	caps.RuntimeContexts = optional.RuntimeContexts
	caps.ResetCoordinator = optional.ResetCoordinator
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

func (o selectedRunForkRuntimeOwner) materialize(ctx context.Context, req runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error) {
	if o.materializeFunc == nil {
		return runfork.RunForkMaterialization{}, fmt.Errorf("selected run.fork runtime owner is required")
	}
	return o.materializeFunc(ctx, req)
}

func (o selectedRunForkRuntimeOwner) execute(ctx context.Context, req runtimerunforkexecution.SelectedContractExecutionRequest) (runtimerunforkexecution.SelectedContractExecutionResult, error) {
	if o.executeFunc == nil {
		return runtimerunforkexecution.SelectedContractExecutionResult{}, fmt.Errorf("selected run.fork runtime owner is required")
	}
	return o.executeFunc(ctx, req)
}

func (o selectedRunForkRuntimeOwner) plan(ctx context.Context, req runfork.RunForkPlanRequest) (runfork.RunForkPlan, error) {
	if o.planFunc == nil {
		return runfork.RunForkPlan{}, fmt.Errorf("selected run.fork runtime owner is required")
	}
	return o.planFunc(ctx, req)
}
