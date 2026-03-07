package pipeline

import (
	"context"
	"log"
	"strings"
	"time"

	"empireai/internal/events"
)

func (pc *FactoryPipelineCoordinator) handleScoreDimensionComplete(ctx context.Context, evt events.Event) {
	pc.scoringState.handleScoreDimensionComplete(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) handleScoringContestResolved(ctx context.Context, evt events.Event) {
	pc.scoringState.handleScoringContestResolved(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) computeComposite(acc *scoringAccumulator, partial bool) scoringComposite {
	return pc.scoringState.computeComposite(acc, partial)
}

func (pc *FactoryPipelineCoordinator) finalizeScoringAccumulator(ctx context.Context, verticalID string, partial bool) {
	pc.scoringState.finalizeScoringAccumulator(ctx, verticalID, partial)
}

func (pc *FactoryPipelineCoordinator) checkScoringTimeouts(ctx context.Context, now time.Time) {
	pc.scoringState.checkTimeouts(ctx, now)
}

func (pc *FactoryPipelineCoordinator) updateScoredVerticalState(ctx context.Context, verticalID, stage string, scoredPayloadMap map[string]any, reason string) {
	if pc == nil || pc.db == nil {
		return
	}
	if _, err := dbExecContext(ctx, pc.db, `
		UPDATE verticals
		SET stage = $2,
		    scores = $3::jsonb,
		    parked_at = CASE
				WHEN $2 = 'marginal_review' THEN COALESCE(parked_at, now())
				ELSE NULL
			END,
		    kill_reason = CASE WHEN $2 = 'killed' THEN NULLIF($4,'') ELSE kill_reason END,
		    updated_at = now()
		WHERE id = $1::uuid
	`, verticalID, stage, string(mustJSON(scoredPayloadMap)), strings.TrimSpace(reason)); err != nil {
		log.Printf("pipeline: update vertical score state failed vertical=%s err=%v", verticalID, err)
		return
	}
	pc.notifyTestVerticalStageUpdated(verticalID, stage)
}
