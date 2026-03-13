package pipeline

import "strings"

const (
	ScoringNodeID            = "scoring-node"
	ShardStageMarketResearch = "market_research"
	ShardStageTrendResearch  = "trend_research"
)

const runtimeWorkflowID = "workflow-runtime"

func isRuntimeWorkflowSource(source string) bool {
	return strings.TrimSpace(source) == runtimeWorkflowID
}
