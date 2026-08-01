package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func recordActivityAttemptStory(ctx context.Context, rec ActivityAttemptRecord, transition string) error {
	return runtimeauthoractivity.Record(ctx, ActivityAttemptStoryDraft(rec, transition))
}

func ActivityAttemptStoryDraft(rec ActivityAttemptRecord, transition string) runtimeauthoractivity.Draft {
	retry := rec.Attempt
	eventType := rec.ResultEventType
	failure := rec.Failure
	if transition == ActivityAttemptStatusStarted {
		eventType = ""
		failure = nil
	}
	return runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindActivityLifecycle, Transition: transition,
		SourceOwner: "activity_attempts", SourceIdentity: rec.RequestEventID,
		DedupKey:   "activity:" + rec.RequestEventID + ":" + transition,
		OccurredAt: activityOccurrenceTime(rec, transition), RunID: rec.RunID, EntityID: rec.EntityID, FlowID: rec.FlowInstance,
		Projection: runtimeauthoractivity.Projection{
			SubjectType: "activity", SubjectID: rec.ActivityID, NodeID: rec.NodeID, Activity: rec.ActivityID,
			Tool: rec.Tool, EffectClass: rec.EffectClass, Attempt: intPointer(retry), EventType: eventType, ExecutionMode: string(rec.ExecutionMode),
		},
		Failure: failure,
	}
}

func activityOccurrenceTime(rec ActivityAttemptRecord, transition string) time.Time {
	if transition == ActivityAttemptStatusStarted && !rec.StartedAt.IsZero() {
		return rec.StartedAt.UTC()
	}
	if transition != "started" && rec.CompletedAt != nil && !rec.CompletedAt.IsZero() {
		return rec.CompletedAt.UTC()
	}
	if !rec.UpdatedAt.IsZero() {
		return rec.UpdatedAt.UTC()
	}
	if !rec.StartedAt.IsZero() {
		return rec.StartedAt.UTC()
	}
	return time.Now().UTC()
}

func intPointer(value int) *int { return &value }

func pipelineFailureJSON(failure *runtimefailures.Envelope) (string, error) {
	if failure == nil {
		return "", nil
	}
	encoded, err := json.Marshal(failure)
	if err != nil {
		return "", fmt.Errorf("marshal pipeline failure: %w", err)
	}
	return string(encoded), nil
}
