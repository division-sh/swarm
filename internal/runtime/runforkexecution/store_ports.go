package runforkexecution

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	rootruntime "github.com/division-sh/swarm/internal/runtime"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebudgetspend "github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	managedcapabilities "github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

// SelectedContractExecutionStore is the exact durable capability group used by
// the selected-fork runtime. It contains no backend identity or concrete store
// shape; composition supplies one implementation explicitly.
type SelectedContractExecutionStore interface {
	runtimebus.EventStore
	runtimereplycontext.Store
	runtimerunlifecycle.OperationOwner
	runtimedelivery.Store
	runtimebus.FlowInstanceRoutePersistence
	runtimebus.FlowInstanceRouteRecordReader
	runtimebus.FlowInstanceRouteSetPersistence
	runtimebus.FlowInstanceRouteTopologyPersistence
	runtimebus.FlowInstanceRouteRollbackPersistence
	runtimebus.ActiveAgentDescriptorLister
	runtimebus.ActiveFlowInstanceDescriptorLister
	runtimebus.EventDeliveryTargetReader
	runtimebus.EventDeliveryRouteSetReader
	runtimebus.TargetFailureDeadLetterRecorder
	runtimebus.RunOriginReader
	runtimemanager.ManagerPersistence
	runtimemanager.AgentLifecyclePersistence
	runtimemanager.AgentLifecycleStateReader
	runtimemanager.AgentLifecycleDiagnosticPersistence
	runtimeeffects.Store
	runtimeeffects.CompletionStore
	runtimeeffects.CompletionHeartbeatStore
	runtimeeffects.RecoveryStore
	runtimellm.LiveSessionAcquirer
	runtimemanager.ActiveRunDeliveryQuiescenceReader
	runtimemanager.EventExistenceReader
	runtimemanager.AgentDirectiveRunTargetResolver
	runtimeagentcontrol.DirectiveOperationStore
	managedcapabilities.Persistence
	runtimebudgetspend.Store
	rootruntime.RuntimeLogPersistence
	runtimepipeline.MailboxWriteMaterializationStore
	decisioncard.Store
	decisioncard.ProposedEffectStore
	decisioncard.HumanTaskStore
	runtimepipeline.DecisionCardDraftExpiry
	runtimepipeline.HumanTaskExpiry

	RuntimeSQLDB() *sql.DB
	PipelineObligations() runtimepipelineobligation.Store
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
	IssueRunForkSelectedContractRuntimeExecution(context.Context, runfork.SelectedContractRuntimeExecutionIssueRequest) (runfork.SelectedContractRuntimeExecution, error)
	ClaimRunForkSelectedContractRuntimeExecution(context.Context, runfork.SelectedContractRuntimeExecution, string, time.Duration) (runtimeeffects.Authority, error)
	EnsureRunForkNoPostForkCommittedReplayScopeMarkers(context.Context, string, string) error
	LoadRunForkSelectedContractSourceEvents(context.Context, string, string, []string) ([]runfork.RunForkSelectedContractSourceEvent, error)
	HeartbeatRunForkSelectedContractRuntimeExecution(context.Context, runtimeeffects.Authority, time.Duration) error
	CommitSelectedForkEvent(context.Context, runtimebus.CommitSelectedForkEventRequest) (runtimebus.EventAppendOutcome, error)
	QuiesceRunForkSelectedContractRuntimeExecution(context.Context, runtimeeffects.Authority) error
	CloseRunForkSelectedContractRuntimeExecution(context.Context, string) error
	FailRunForkSelectedContractRuntimeExecution(context.Context, runtimeeffects.Authority, json.RawMessage) error
}
