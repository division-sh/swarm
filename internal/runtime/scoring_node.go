package runtime

import (
	"context"
	"database/sql"

	"empireai/internal/events"
	runtimepipeline "empireai/internal/runtime/pipeline"
)

type ScoringNode = runtimepipeline.ScoringNode

func NewScoringNode(bus *EventBus, pc *FactoryPipelineCoordinator, db *sql.DB) *ScoringNode {
	if bus == nil || pc == nil {
		return nil
	}
	return runtimepipeline.NewScoringNode(bus, scoringCoordinatorAdapter{pc: pc}, db)
}

type scoringCoordinatorAdapter struct {
	pc *FactoryPipelineCoordinator
}

func (a scoringCoordinatorAdapter) OnVerticalDiscovered(ctx context.Context, evt events.Event) {
	a.pc.handleScoringRequested(withPipelineSourceAgent(ctx, runtimepipeline.ScoringNodeID), evt)
}

func (a scoringCoordinatorAdapter) OnVerticalDerived(ctx context.Context, evt events.Event) {
	a.pc.handleVerticalDerived(withPipelineSourceAgent(ctx, runtimepipeline.ScoringNodeID), evt)
}

func (a scoringCoordinatorAdapter) OnScoreDimensionComplete(ctx context.Context, evt events.Event) {
	a.pc.handleScoreDimensionComplete(withPipelineSourceAgent(ctx, runtimepipeline.ScoringNodeID), evt)
}

func (a scoringCoordinatorAdapter) OnScoringContestResolved(ctx context.Context, evt events.Event) {
	a.pc.handleScoringContestResolved(withPipelineSourceAgent(ctx, runtimepipeline.ScoringNodeID), evt)
}

const (
	scoringNodeID         = runtimepipeline.ScoringNodeID
	scoringNodeRetryLimit = runtimepipeline.DefaultScoringNodeRetryLimit
)
