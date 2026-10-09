package runforkpersistence

import (
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func decodeRunForkTimerSnapshot(raw []byte) (runforkrevision.TimerSnapshot, error) {
	snapshot, err := runforkrevision.DecodeTimerSnapshot(raw)
	if err != nil {
		return runforkrevision.TimerSnapshot{}, err
	}
	if snapshot.TaskType == "workflow_timer" {
		if _, err := workflowTimerActivationFromSnapshot(snapshot); err != nil {
			return runforkrevision.TimerSnapshot{}, fmt.Errorf("historical workflow timer: %w", err)
		}
	}
	return snapshot, nil
}

func workflowTimerActivationFromSnapshot(s runforkrevision.TimerSnapshot) (pipeline.WorkflowTimerActivation, error) {
	var routingSource events.RoutingSource
	if err := json.Unmarshal(s.RoutingSource, &routingSource); err != nil {
		return pipeline.WorkflowTimerActivation{}, fmt.Errorf("decode historical timer routing source: %w", err)
	}
	record := pipeline.WorkflowTimerActivationPersistenceRecord{
		ActivationID: s.TimerID, TaskID: s.TimerName, RunID: s.RunID, EntityID: s.EntityID,
		Route: flowidentity.Route{ScopeKey: s.FlowScopeKey, InstanceID: s.FlowInstanceID, InstancePath: s.FlowInstance}, RoutingSource: routingSource,
		EventType: s.FireEvent, ExecutionMode: executionmode.Mode(s.ExecutionMode), Payload: s.FirePayload,
		FireAt: s.FireAt, Recurring: s.Recurring, RecurrenceInterval: s.RecurrenceInterval,
		OwnerNode: s.OwnerNode, OwnerAgent: s.OwnerAgent, TaskType: s.TaskType, Status: s.Status, CreatedAt: s.CreatedAt,
		CancelCause:   pipeline.WorkflowTimerCancelCause(s.CancelCause),
		SourceTimerID: s.SourceTimerID, ForkedFromRunID: s.ForkedFromRunID, ForkedFromEventID: s.ForkedFromEventID,
		ReconstructionOwner: s.ReconstructionOwner, ForkedFromPointKind: s.ForkedFromPointKind,
		ForkedFromPointRevision: s.ForkedFromPointRevision,
	}
	if s.SourceArmedAt != nil {
		record.SourceArmedAt = *s.SourceArmedAt
	}
	if s.FiredAt != nil {
		record.FiredAt = *s.FiredAt
	}
	if s.CancelledAt != nil {
		record.CancelledAt = *s.CancelledAt
	}
	return pipeline.DecodeWorkflowTimerActivationPersistenceRecord(record)
}
