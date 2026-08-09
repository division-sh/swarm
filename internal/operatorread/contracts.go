package operatorread

import (
	"context"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

type RunReader interface {
	LoadRunHeader(context.Context, string) (RunHeader, error)
	ListRunHeaders(context.Context, RunHeaderListOptions) ([]RunHeader, string, error)
	LoadRunDebugReport(context.Context, string, RunDebugQueryOptions) (RunDebugReport, error)
}

type EntityReader interface {
	ListOperatorEntities(context.Context, OperatorEntityListOptions) (OperatorEntityListResult, error)
	LoadOperatorEntity(context.Context, string, string) (OperatorEntityFull, error)
	AggregateOperatorEntities(context.Context, OperatorEntityAggregateOptions) (OperatorEntityAggregateResult, error)
}

type AgentReader interface {
	ResolveOperatorAgentIdentity(context.Context, string, string) (agentidentity.Identity, error)
	ListOperatorAgents(context.Context, OperatorAgentListOptions) (OperatorAgentListResult, error)
	LoadOperatorAgent(context.Context, agentidentity.Identity) (OperatorAgentDetail, error)
	LoadOperatorAgentDiagnosis(context.Context, agentidentity.Identity, OperatorAgentDiagnosisOptions) (OperatorAgentDiagnosis, error)
	LoadOperatorAgentDeliveryDiagnostics(context.Context, agentidentity.Identity, OperatorAgentDeliveryDiagnosticsOptions) (OperatorAgentDeliveryDiagnostics, error)
	LoadOperatorAgentDeliveryLifecycle(context.Context, agentidentity.Identity, OperatorAgentDeliveryLifecycleOptions) (OperatorAgentDeliveryLifecycleList, error)
	LoadOperatorAgentUsage(context.Context, agentidentity.Identity, OperatorAgentUsageOptions) (OperatorAgentUsage, error)
}

type ConversationReader interface {
	ListOperatorConversations(context.Context, OperatorConversationListOptions) (OperatorConversationListResult, error)
	ListOperatorConversationTurns(context.Context, OperatorConversationTurnListOptions) (OperatorConversationTurnListResult, error)
	LoadOperatorPublicConversationTurn(context.Context, string, string) (OperatorPublicConversationTurnDetail, error)
}

type ObservabilityReader interface {
	LoadRunDebugTracePage(context.Context, string, RunDebugTraceQueryOptions) ([]RunDebugTraceRow, string, error)
	ListOperatorEvents(context.Context, OperatorEventListOptions) (OperatorEventListResult, error)
	LoadOperatorEvent(context.Context, string) (OperatorEventFull, error)
	ListOperatorRuntimeLogs(context.Context, OperatorRuntimeLogListOptions) (OperatorRuntimeLogListResult, error)
	ListOperatorRuntimeIncidents(context.Context, OperatorRuntimeIncidentListOptions) (OperatorRuntimeIncidentListResult, error)
}

type RunLifecycleReader interface {
	LoadLatestRunFlowInstance(context.Context, string) (string, error)
	LoadLatestRunNonEscalationProgressAt(context.Context, string, string) (time.Time, error)
}
