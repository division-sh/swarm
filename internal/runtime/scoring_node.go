package runtime

import (
	"database/sql"
	runtimepipeline "empireai/internal/runtime/pipeline"
)

type ScoringNode = runtimepipeline.ScoringNode

func NewScoringNode(bus *EventBus, pc *FactoryPipelineCoordinator, db *sql.DB) *ScoringNode {
	if bus == nil || pc == nil {
		return nil
	}
	return runtimepipeline.NewScoringNode(bus, pc, db)
}

const (
	scoringNodeID         = runtimepipeline.ScoringNodeID
	scoringNodeRetryLimit = runtimepipeline.DefaultScoringNodeRetryLimit
)
