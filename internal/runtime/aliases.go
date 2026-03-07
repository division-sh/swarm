package runtime

import (
	"database/sql"
	"encoding/json"

	"empireai/internal/config"
	runtimebus "empireai/internal/runtime/bus"
	runtimellm "empireai/internal/runtime/llm"
	runtimemanager "empireai/internal/runtime/manager"
	runtimemcp "empireai/internal/runtime/mcp"
	runtimepipeline "empireai/internal/runtime/pipeline"
	runtimerterr "empireai/internal/runtime/rterrors"
	"empireai/internal/runtime/sessions"
	runtimetools "empireai/internal/runtime/tools"
	workspace "empireai/internal/runtime/workspace"
)

type EventStore = runtimebus.EventStore
type InMemoryEventStore = runtimebus.InMemoryEventStore
type EmittedEventsRecorder = runtimebus.EmittedEventsRecorder
type RoutingTable = runtimebus.RoutingTable
type Route = runtimebus.Route

type AgentTurnRecord = runtimellm.AgentTurnRecord
type UsageTokens = runtimellm.UsageTokens
type ClaudeCLIRuntime = runtimellm.ClaudeCLIRuntime
type AnthropicAPIRuntime = runtimellm.AnthropicAPIRuntime
type RuntimeFactory = runtimellm.RuntimeFactory
type NoopRuntime = runtimellm.NoopRuntime
type TurnPersistence = runtimellm.TurnPersistence
type ConversationRecord = runtimellm.ConversationRecord
type ConversationPersistence = runtimellm.ConversationPersistence

type Agent = runtimemanager.Agent
type BoardInteractiveAgent = runtimemanager.BoardInteractiveAgent
type AgentFactory = runtimemanager.AgentFactory
type PersistedAgent = runtimemanager.PersistedAgent
type PersistedRoutingRule = runtimemanager.PersistedRoutingRule
type VerticalInfoReader = runtimemanager.VerticalInfoReader
type VerticalInfo = runtimemanager.VerticalInfo
type EventReceipt = runtimemanager.EventReceipt
type PromptOverrideRecord = runtimemanager.PromptOverrideRecord
type PromptOverridePersistence = runtimemanager.PromptOverridePersistence
type OrgTemplateRecord = runtimemanager.OrgTemplateRecord
type ManagerPersistence = runtimemanager.ManagerPersistence

type FactoryPipelineCoordinator = runtimepipeline.FactoryPipelineCoordinator
type SchedulePersistence = runtimepipeline.SchedulePersistence
type ScanCampaign = runtimepipeline.ScanCampaign
type CreateScanCampaignInput = runtimepipeline.CreateScanCampaignInput
type ScanCampaignFilter = runtimepipeline.ScanCampaignFilter
type ScanCampaignPersistence = runtimepipeline.ScanCampaignPersistence
type Schedule = runtimepipeline.Schedule
type Scheduler = runtimepipeline.Scheduler
type ScoringNode = runtimepipeline.ScoringNode
type ShardDispatcher = runtimepipeline.ShardDispatcher

type MailboxItem = runtimetools.MailboxItem
type MailboxPersistence = runtimetools.MailboxPersistence
type RuntimeError = runtimerterr.RuntimeError
type MCPStallDiagnosticConfig = runtimemcp.StallDiagnosticConfig

var ErrClaudeAuthRequired = runtimellm.ErrClaudeAuthRequired

const (
	scoringNodeID         = runtimepipeline.ScoringNodeID
	scoringNodeRetryLimit = runtimepipeline.DefaultScoringNodeRetryLimit
	runtimeSpecVersion    = "v2.0.49"
)

func NewScheduler(callbacks ...func(Schedule)) *runtimepipeline.Scheduler {
	return runtimepipeline.NewScheduler(callbacks...)
}

func NewAnthropicAPIRuntime(cfg *config.Config, reg sessions.Registry, lockOwner string, turns TurnPersistence, conversations ConversationPersistence, budget runtimellm.BudgetGuard) *runtimellm.AnthropicAPIRuntime {
	return runtimellm.NewAnthropicAPIRuntime(cfg, reg, lockOwner, turns, conversations, budget)
}

func NewClaudeCLIRuntime(cfg *config.Config, reg sessions.Registry, lockOwner string, turns TurnPersistence, budget runtimellm.BudgetGuard, workspaces workspace.Resolver, conversations ConversationPersistence) *runtimellm.ClaudeCLIRuntime {
	return runtimellm.NewClaudeCLIRuntime(cfg, reg, lockOwner, turns, budget, workspaces, conversations)
}

func NewFactoryPipelineCoordinator(bus *EventBus, db *sql.DB) *runtimepipeline.FactoryPipelineCoordinator {
	if bus == nil {
		return nil
	}
	return runtimepipeline.NewFactoryPipelineCoordinator(bus, db)
}

func NewScoringNode(bus *EventBus, pc *runtimepipeline.FactoryPipelineCoordinator, db *sql.DB) *runtimepipeline.ScoringNode {
	if bus == nil || pc == nil {
		return nil
	}
	return runtimepipeline.NewScoringNode(bus, pc, db)
}

func NewShardDispatcher(db *sql.DB, bus *EventBus, manager *runtimemanager.AgentManager, cfg config.ShardingConfig) *runtimepipeline.ShardDispatcher {
	if bus == nil || manager == nil {
		return nil
	}
	return runtimepipeline.NewShardDispatcher(db, bus, manager, cfg)
}

func withSystemPrompt(raw json.RawMessage, prompt string) json.RawMessage {
	return runtimemanager.WithSystemPrompt(raw, prompt)
}

func defaultOpCoRoster(verticalID string) []PersistedAgent {
	return runtimemanager.DefaultOpCoRoster(verticalID)
}

func defaultOpCoRoutes(verticalID string) []PersistedRoutingRule {
	return runtimemanager.DefaultOpCoRoutes(verticalID)
}

func opCoAgentID(role, verticalID string) string {
	return runtimemanager.OpCoAgentID(role, verticalID)
}

func orderAgentsByParent(in []PersistedAgent) ([]PersistedAgent, error) {
	return runtimemanager.OrderAgentsByParent(in)
}
