package runforkexecution

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	rootruntime "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebudgetspend "github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	managedcapabilities "github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

// SelectedContractForkLifecycle owns planning, materialization, activation,
// binding, and cleanup for one selected-contract fork.
type SelectedContractForkLifecycle interface {
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
	PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	MaterializeRunForkForSelectedContractExecution(context.Context, runfork.RunForkSelectedContractExecutionMaterializeRequest) (runfork.RunForkMaterialization, error)
	DiscardMaterializedSelectedContractExecutionFork(context.Context, string) error
	ActivateRunForkForSelectedContractExecution(context.Context, runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error)
	LoadRunForkSelectedContractBinding(context.Context, string) (runfork.RunForkSelectedContractBinding, bool, error)
	RequireRunForkSelectedContractBinding(context.Context, string) (runfork.RunForkSelectedContractBinding, error)
	LoadRunBundleAvailability(context.Context, string) (runbundle.Availability, error)
	LoadRunForkSelectedContractRouteRecovery(context.Context, string) (runfork.RunForkSelectedContractRouteRecovery, bool, error)
	ActivateRunFork(context.Context, runfork.RunForkActivateRequest) (runfork.RunForkActivation, error)
}

// SelectedContractRuntimeExecutionLifecycle owns the selected runtime lease.
type SelectedContractRuntimeExecutionLifecycle interface {
	IssueRunForkSelectedContractRuntimeExecution(context.Context, runfork.SelectedContractRuntimeExecutionIssueRequest) (runfork.SelectedContractRuntimeExecution, error)
	ClaimRunForkSelectedContractRuntimeExecution(context.Context, runfork.SelectedContractRuntimeExecution, string, time.Duration) (runtimeeffects.Authority, error)
	HeartbeatRunForkSelectedContractRuntimeExecution(context.Context, runtimeeffects.Authority, time.Duration) error
	QuiesceRunForkSelectedContractRuntimeExecution(context.Context, runtimeeffects.Authority) error
	CloseRunForkSelectedContractRuntimeExecution(context.Context, string) error
	FailRunForkSelectedContractRuntimeExecution(context.Context, runtimeeffects.Authority, json.RawMessage) error
}

// SelectedContractReplayPersistence owns source replay admission and exact
// selected-fork event commit.
type SelectedContractReplayPersistence interface {
	EnsureRunForkNoPostForkCommittedReplayScopeMarkers(context.Context, string, string) error
	LoadRunForkSelectedContractSourceEventModes(context.Context, string, []string) ([]executionmode.Mode, error)
	LoadRunForkSelectedContractSourceEvents(context.Context, string, string, []string, []runfork.RunForkSelectedContractWorkflowState) ([]runfork.RunForkSelectedContractSourceEvent, error)
	CommitSelectedForkEvent(context.Context, runtimebus.CommitSelectedForkEventRequest) (runtimebus.CommittedSelectedForkEvent, error)
}

// SelectedContractExecutionOwner is an opaque construction value. Only this
// package can project its immutable roles, so constructed subsystems cannot use
// it as a service locator.
type SelectedContractExecutionOwner struct {
	ports *selectedContractExecutionPorts
}

type selectedContractExecutionPorts struct {
	workflow                runtimepipeline.WorkflowPersistence
	fork                    SelectedContractForkLifecycle
	runtimeExecution        SelectedContractRuntimeExecutionLifecycle
	replay                  SelectedContractReplayPersistence
	events                  runtimebus.EventStore
	busDurable              runtimebus.DurableDependencies
	pipelineObligations     runtimepipelineobligation.Store
	manager                 runtimemanager.ManagerPersistence
	managerRoles            runtimemanager.PersistenceRoles
	effects                 runtimeeffects.Store
	completion              runtimeeffects.CompletionStore
	completionHeartbeat     runtimeeffects.CompletionHeartbeatStore
	liveSessions            runtimellm.LiveSessionAcquirer
	managedCapabilities     managedcapabilities.Persistence
	budget                  runtimebudgetspend.Store
	logs                    rootruntime.RuntimeLogPersistence
	mailbox                 runtimepipeline.MailboxWriteMaterializationStore
	decisionCards           decisioncard.Store
	proposedEffects         decisioncard.ProposedEffectStore
	humanTasks              decisioncard.HumanTaskStore
	decisionCardDraftExpiry runtimepipeline.DecisionCardDraftExpiry
	humanTaskExpiry         runtimepipeline.HumanTaskExpiry
}

func NewSelectedContractExecutionOwner(
	workflow runtimepipeline.WorkflowPersistence,
	fork SelectedContractForkLifecycle,
	runtimeExecution SelectedContractRuntimeExecutionLifecycle,
	replay SelectedContractReplayPersistence,
	events runtimebus.EventStore,
	busDurable runtimebus.DurableDependencies,
	pipelineObligations runtimepipelineobligation.Store,
	manager runtimemanager.ManagerPersistence,
	managerRoles runtimemanager.PersistenceRoles,
	effects runtimeeffects.Store,
	completion runtimeeffects.CompletionStore,
	completionHeartbeat runtimeeffects.CompletionHeartbeatStore,
	liveSessions runtimellm.LiveSessionAcquirer,
	managedCapabilities managedcapabilities.Persistence,
	budget runtimebudgetspend.Store,
	logs rootruntime.RuntimeLogPersistence,
	mailbox runtimepipeline.MailboxWriteMaterializationStore,
	decisionCards decisioncard.Store,
	proposedEffects decisioncard.ProposedEffectStore,
	humanTasks decisioncard.HumanTaskStore,
	decisionCardDraftExpiry runtimepipeline.DecisionCardDraftExpiry,
	humanTaskExpiry runtimepipeline.HumanTaskExpiry,
) (SelectedContractExecutionOwner, error) {
	required := []struct {
		name  string
		value any
	}{
		{"fork lifecycle", fork}, {"runtime execution lifecycle", runtimeExecution}, {"replay persistence", replay},
		{"event store", events},
		{"event run lifecycle", busDurable.RunLifecycle}, {"event delivery lifecycle", busDurable.DeliveryLifecycle},
		{"event flow routes", busDurable.FlowRoutes}, {"event route records", busDurable.FlowRouteRecords},
		{"event route sets", busDurable.FlowRouteSets}, {"event route topology", busDurable.FlowRouteTopology},
		{"event route rollback", busDurable.FlowRouteRollback}, {"event active agents", busDurable.ActiveAgents},
		{"event active flows", busDurable.ActiveFlows}, {"event target owners", busDurable.TargetOwners},
		{"event delivery route sets", busDurable.DeliveryRouteSets}, {"event target failure recorder", busDurable.TargetFailureRecorder},
		{"event run origins", busDurable.RunOrigins}, {"pipeline obligations", pipelineObligations}, {"manager persistence", manager},
		{"manager lifecycle state", managerRoles.LifecycleState}, {"manager lifecycle effects", managerRoles.LifecycleEffects},
		{"manager lifecycle diagnostics", managerRoles.LifecycleDiagnostics}, {"manager effects recovery", managerRoles.EffectsRecovery},
		{"manager delivery quiescence", managerRoles.DeliveryQuiescence}, {"manager event existence", managerRoles.EventExistence},
		{"manager directive operations", managerRoles.DirectiveOperations}, {"manager directive targets", managerRoles.DirectiveTargets},
		{"manager flow routes", managerRoles.FlowRoutes}, {"effects", effects}, {"completion", completion},
		{"completion heartbeat", completionHeartbeat}, {"live sessions", liveSessions},
		{"managed capabilities", managedCapabilities}, {"budget", budget}, {"runtime logs", logs},
		{"mailbox materialization", mailbox}, {"decision cards", decisionCards}, {"proposed effects", proposedEffects},
		{"human tasks", humanTasks}, {"decision-card draft expiry", decisionCardDraftExpiry}, {"human-task expiry", humanTaskExpiry},
	}
	for _, role := range required {
		if role.value == nil {
			return SelectedContractExecutionOwner{}, errors.New("selected-contract execution requires " + role.name)
		}
	}
	if !workflow.Valid() {
		return SelectedContractExecutionOwner{}, errors.New("selected-contract execution requires valid workflow persistence")
	}
	return SelectedContractExecutionOwner{ports: &selectedContractExecutionPorts{
		workflow: workflow, fork: fork, runtimeExecution: runtimeExecution, replay: replay,
		events: events, busDurable: busDurable, pipelineObligations: pipelineObligations,
		manager: manager, managerRoles: managerRoles, effects: effects, completion: completion,
		completionHeartbeat: completionHeartbeat, liveSessions: liveSessions, managedCapabilities: managedCapabilities,
		budget: budget, logs: logs, mailbox: mailbox, decisionCards: decisionCards,
		proposedEffects: proposedEffects, humanTasks: humanTasks,
		decisionCardDraftExpiry: decisionCardDraftExpiry, humanTaskExpiry: humanTaskExpiry,
	}}, nil
}

func (o SelectedContractExecutionOwner) require() (*selectedContractExecutionPorts, error) {
	if o.ports == nil {
		return nil, errors.New("selected-contract execution owner is required")
	}
	return o.ports, nil
}
