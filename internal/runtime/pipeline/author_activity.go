package pipeline

import (
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
)

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
