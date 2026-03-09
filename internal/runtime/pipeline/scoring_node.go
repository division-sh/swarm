package pipeline

import "database/sql"

const ScoringNodeID = "scoring-node"

func NewScoringNode(bus systemNodeBus, runtime scoringWorkflowRuntime, db *sql.DB) *backgroundWorkflowNode {
	if bus == nil || runtime == nil {
		return nil
	}
	executor := newScoringBackgroundExecutor(runtime)
	return newBackgroundWorkflowNode(executor, bus, db)
}
