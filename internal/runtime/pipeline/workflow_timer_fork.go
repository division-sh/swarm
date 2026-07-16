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
