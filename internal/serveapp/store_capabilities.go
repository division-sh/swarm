package serveapp

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	runtimerunforkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
)

type selectedAPICapabilities struct {
	Database                  apiv1.Pinger
	Runs                      apiv1.RunReadStore
	Entities                  apiv1.EntityReadStore
	Agents                    apiv1.AgentReadStore
	Conversations             apiv1.ConversationReadStore
	Observability             apiv1.ObservabilityReadStore
	RunBundleContext          apiv1.RunBundleContextStore
	TestSetup                 apiv1.TestSetupStore
	Data                      apiv1.DurableDataStore
	ConversationForks         apiv1.ConversationForkReadStore
	ConversationForkLifecycle apiv1.ConversationForkLifecycleStore
	RunForkAvailability       apiv1.RunForkAvailabilityStore
	RunFork                   apiv1.RunForkExecutor
	SelectedForkControls      apiv1.SelectedForkControlAdmission
	SelectedForkRetirement    selectedForkContextRetirement
	SelectedForkProcess       selectedForkProcessOwner
	RuntimeContexts           *runtime.RuntimeContextManager
	ResetCoordinator          apiv1.DestructiveResetCoordinator
}

type selectedAPICapabilityRequest struct {
	RepoRoot                string
	PlatformSpecPath        string
	RunningPlatformSpecPath string
	LoadedBundle            serveRuntimeBundle
	RuntimeContextManager   *runtime.RuntimeContextManager
	RuntimeSupervisor       *processLifecycleSupervisor
	Source                  semanticview.Source
	Config                  *config.Config
	Workspaces              cliapp.ServeWorkspaceLifecycle
	Credentials             runtimecredentials.Store
	ManagedCredentials      runtimemanagedcredentials.Store
	ProviderCredentials     runtimecredentials.Store
	ExecutionPosture        executionposture.Posture
	ProcessCapability       runtimestartupownership.ProcessCapability
	PlatformPackBases       *packartifact.PlatformPackBaseGenerationOwner
	NoticePresentation      runtimetools.InformationalNoticePresentationSink
}

var buildSelectedAPICapabilities = constructSelectedAPICapabilities

func constructSelectedAPICapabilities(owner *storeselected.Owner, req selectedAPICapabilityRequest) (selectedAPICapabilities, error) {
	caps := selectedAPICapabilities{
		Database: owner.Pinger(), Runs: owner.Runs(), Entities: owner.Entities(), Agents: owner.Agents(),
		Conversations: owner.Conversations(), Observability: owner.Observability(),
		RunBundleContext: owner.RunBundleContext(), TestSetup: owner.TestSetup(), Data: owner.Data(),
		RuntimeContexts: req.RuntimeContextManager,
	}
	if family, available := owner.ConversationFork(); available {
		caps.ConversationForks = family.Reader()
		caps.ConversationForkLifecycle = family.Lifecycle()
	}
	if family, available := owner.DestructiveReset(); available {
		planner := runtimedestructivereset.InventoryPlanner{Reader: runtimedestructivereset.CompositeInventoryReader{
			Reader: family.Inventory(), Containers: req.Workspaces,
		}}
		caps.ResetCoordinator = &runtimedestructivereset.Coordinator{
			Planner: planner, Locks: family.Locks(), Quiescer: runtimedestructivereset.Quiescer{Store: family.Quiescence()},
			Cleaner:    runtimedestructivereset.Cleaner{Store: processOwnedDestructiveResetStore{capability: req.ProcessCapability}},
			Containers: runtimedestructivereset.ManagedContainerStopper{Runtime: req.Workspaces}, RuntimeContexts: req.RuntimeContextManager,
		}
	}
	if family, available := owner.RunFork(); available {
		artifactStore := owner.SourceArtifactStore()
		if artifactStore == nil {
			return selectedAPICapabilities{}, fmt.Errorf("run.fork requires selected source artifact reader")
		}
		loader := runtimerunforkexecution.SourceArtifactSelectedContractSourceLoader{
			RepoRoot: req.RepoRoot, PlatformSpecPath: req.RunningPlatformSpecPath,
			PlatformPackBases: req.PlatformPackBases, Store: artifactStore,
		}
		deps := owner.RuntimeDeps()
		executor := apiv1.SelectedContractRunForkExecutor{
			ExecuteSelectedContractRunFork: family.Execute,
			SourceLoader:                   loader,
			AgentRuntime: runtimerunforkexecution.SelectedContractAgentRuntimeOptions{
				Config: req.Config, ExecutionPosture: req.ExecutionPosture,
				EntityStore: deps.ToolEntityStore, HumanTaskStore: deps.HumanTaskStore,
				SessionRegistry: deps.SessionRegistry, ConversationStore: deps.ConversationStore,
				MailboxStore: deps.MailboxStore, Workspace: req.Workspaces,
				NoticePresentation: req.NoticePresentation,
				Credentials:        req.Credentials, ManagedCredentials: req.ManagedCredentials,
				ProviderCredentials: req.ProviderCredentials, ProcessCapability: req.ProcessCapability,
			},
		}
		caps.RunForkAvailability = family.Availability()
		caps.RunFork = executor
		caps.SelectedForkControls = family
		caps.SelectedForkRetirement = family
		caps.SelectedForkProcess = family
		if reset, ok := caps.ResetCoordinator.(*runtimedestructivereset.Coordinator); ok {
			reset.RuntimeContexts = selectedAndNormalContextRetirement{selected: family, normal: req.RuntimeContextManager}
		}
	}
	return caps, nil
}

type selectedForkProcessOwner interface {
	apiv1.SelectedForkStopOwner
	BindSelectedProcess(context.Context, *worklifetime.Process, runtimestartupownership.ProcessCapability) error
}

type selectedAndNormalContextRetirement struct {
	selected selectedForkContextRetirement
	normal   *runtime.RuntimeContextManager
}

func (q selectedAndNormalContextRetirement) QuiesceAllRuntimeContexts(ctx context.Context) error {
	if err := q.selected.RetireSelectedContexts(ctx); err != nil {
		return err
	}
	if q.normal != nil {
		return q.normal.QuiesceAllRuntimeContexts(ctx)
	}
	return nil
}
