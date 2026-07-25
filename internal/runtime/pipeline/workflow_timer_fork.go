package pipeline

import (
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
)

type WorkflowTimerForkLineage struct {
	ForkRunID           string
	ForkEventID         string
	ReconstructionOwner string
}

type PersistedWorkflowTimerForkSource struct {
	ActivationID         string
	SerializedActivation string
	RunID                string
	EntityID             string
	FlowInstance         string
	OwnerNode            string
	OwnerAgent           string
	EventType            string
	Payload              []byte
	FireAt               time.Time
	Recurring            bool
	RecurrenceCron       string
	RecurrenceInterval   string
	Status               string
	FiredAt              time.Time
	CreatedAt            time.Time
}

// RemintPersistedWorkflowTimerForFork is the decode boundary for a
// structurally selected workflow_timer row.
func RemintPersistedWorkflowTimerForFork(source PersistedWorkflowTimerForkSource, lineage WorkflowTimerForkLineage) (WorkflowTimerActivation, error) {
	ref, ok := timeridentity.ParseWorkflowTimerActivationTaskID(source.SerializedActivation)
	if !ok || ref.ActivationID != strings.TrimSpace(source.ActivationID) {
		return WorkflowTimerActivation{}, fmt.Errorf("fork workflow timer activation identity is invalid")
	}
	if strings.TrimSpace(source.OwnerNode) != "" {
		return WorkflowTimerActivation{}, fmt.Errorf("fork workflow timer cannot carry a node owner")
	}
	if strings.TrimSpace(source.RecurrenceCron) != "" {
		return WorkflowTimerActivation{}, fmt.Errorf("fork workflow timer recurrence must use a persisted interval")
	}
	var interval time.Duration
	if strings.TrimSpace(source.RecurrenceInterval) != "" {
		var parsed bool
		interval, parsed = timeridentity.ParseDelayDuration(source.RecurrenceInterval)
		if !parsed {
			return WorkflowTimerActivation{}, fmt.Errorf("fork workflow timer recurrence interval %q is invalid", source.RecurrenceInterval)
		}
	}
	return RemintWorkflowTimerActivationForFork(WorkflowTimerActivation{
		Ref: ref, RunID: source.RunID, EntityID: source.EntityID, FlowInstance: source.FlowInstance,
		OwnerAgent: source.OwnerAgent, EventType: source.EventType, Payload: append([]byte(nil), source.Payload...),
		FireAt: source.FireAt, Recurring: source.Recurring, RecurrenceInterval: interval,
		Status: source.Status, FiredAt: source.FiredAt, CreatedAt: source.CreatedAt,
	}, lineage)
}

// RemintWorkflowTimerActivationForFork is the workflow-timer domain operation
// for selected-contract forks. Stores provide lineage and persist its result;
// they do not reinterpret activation, generation, or recurrence semantics.
func RemintWorkflowTimerActivationForFork(source WorkflowTimerActivation, lineage WorkflowTimerForkLineage) (WorkflowTimerActivation, error) {
	source = source.normalized()
	lineage.ForkRunID = strings.TrimSpace(lineage.ForkRunID)
	lineage.ForkEventID = strings.TrimSpace(lineage.ForkEventID)
	lineage.ReconstructionOwner = strings.TrimSpace(lineage.ReconstructionOwner)
	if err := source.validate(); err != nil {
		return WorkflowTimerActivation{}, fmt.Errorf("fork source workflow timer: %w", err)
	}
	if source.Status != workflowTimerStatusActive {
		return WorkflowTimerActivation{}, fmt.Errorf("fork source workflow timer must be active")
	}
	if !source.Recurring && !source.FiredAt.IsZero() {
		return WorkflowTimerActivation{}, fmt.Errorf("fork source one-shot workflow timer cannot have a fired occurrence")
	}
	if lineage.ForkRunID == "" || lineage.ForkEventID == "" || lineage.ReconstructionOwner == "" {
		return WorkflowTimerActivation{}, fmt.Errorf("workflow timer fork requires run, event, and reconstruction owner lineage")
	}

	ref := source.Ref
	if ref.Generation.Valid() {
		generation, err := loopruntime.ForkGeneration(ref.Generation, lineage.ForkRunID, source.EntityID)
		if err != nil {
			return WorkflowTimerActivation{}, fmt.Errorf("fork workflow timer loop generation: %w", err)
		}
		ref.Generation = generation
	}
	ref.ActivationID = timeridentity.WorkflowTimerForkActivationID(source.Ref.ActivationID, lineage.ForkRunID, lineage.ForkEventID)

	forked := source
	forked.Ref = ref
	forked.RunID = lineage.ForkRunID
	forked.Status = workflowTimerStatusActive
	forked.FiredAt = time.Time{}
	forked.SourceTimerID = source.Ref.ActivationID
	forked.ForkedFromRunID = source.RunID
	forked.ForkedFromEventID = lineage.ForkEventID
	forked.ReconstructionOwner = lineage.ReconstructionOwner
	forked = forked.normalized()
	if err := forked.validate(); err != nil {
		return WorkflowTimerActivation{}, fmt.Errorf("forked workflow timer: %w", err)
	}
	return forked, nil
}
