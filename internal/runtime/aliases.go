package runtime

import (
	"database/sql"
	"time"

	"empireai/internal/config"
	runtimebus "empireai/internal/runtime/bus"
	runtimemanager "empireai/internal/runtime/manager"
	runtimemcp "empireai/internal/runtime/mcp"
	runtimepipeline "empireai/internal/runtime/pipeline"
	runtimerterr "empireai/internal/runtime/rterrors"
	runtimetools "empireai/internal/runtime/tools"
)

type EventStore = runtimebus.EventStore
type ActiveAgentLister = runtimebus.ActiveAgentLister
type InMemoryEventStore = runtimebus.InMemoryEventStore
type RoutingTable = runtimebus.RoutingTable
type Route = runtimebus.Route
type OpCoCycleTracker = runtimebus.OpCoCycleTracker
type EmittedEventsRecorder = runtimebus.EmittedEventsRecorder

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
type ShardPlanner = runtimepipeline.ShardPlanner

type MailboxItem = runtimetools.MailboxItem
type MailboxPersistence = runtimetools.MailboxPersistence

type RuntimeError = runtimerterr.RuntimeError
type MCPStallDiagnosticConfig = runtimemcp.StallDiagnosticConfig

const (
	ShardStageMarketResearch = runtimepipeline.ShardStageMarketResearch
	ShardStageTrendResearch  = runtimepipeline.ShardStageTrendResearch

	scoringNodeID         = runtimepipeline.ScoringNodeID
	scoringNodeRetryLimit = runtimepipeline.DefaultScoringNodeRetryLimit

	defaultOpCoCycleLimit      = 5
	defaultOpCoCycleWindow     = 4 * time.Hour
	spendNeededCycleLimit      = 3
	spendNeededCycleWindow     = 1 * time.Hour
	defaultCycleEscalationRole = "opco_cto"
)

func NewFactoryPipelineCoordinator(bus *EventBus, db *sql.DB) *FactoryPipelineCoordinator {
	if bus == nil {
		return nil
	}
	return runtimepipeline.NewFactoryPipelineCoordinator(bus, db)
}

func NewScoringNode(bus *EventBus, pc *FactoryPipelineCoordinator, db *sql.DB) *ScoringNode {
	if bus == nil || pc == nil {
		return nil
	}
	return runtimepipeline.NewScoringNode(bus, pc, db)
}

func NewScheduler(callbacks ...func(Schedule)) *runtimepipeline.Scheduler {
	return runtimepipeline.NewScheduler(callbacks...)
}

func NewRecoveryManager() *runtimepipeline.RecoveryManager {
	return runtimepipeline.NewRecoveryManager()
}

func NewRecoveryManagerWith(store EventStore, bus *EventBus) *runtimepipeline.RecoveryManager {
	return runtimepipeline.NewRecoveryManagerWith(store, bus)
}

func NewOpCoCycleTracker(db *sql.DB) *OpCoCycleTracker {
	return runtimebus.NewOpCoCycleTracker(db)
}

func NewShardPlanner(cfg config.ShardingConfig) *ShardPlanner {
	return runtimepipeline.NewShardPlanner(cfg)
}
