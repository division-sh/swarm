package runforkpersistence

import (
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

// The caller admits raw at point and supplies the selected declaration's
// canonical activation, or nil for an admitted rule removal. Projection grants
// no execution authority. A removed result must be canceled atomically by the
// lifecycle owner before any wakeup is installed.
func projectRunForkWorkflowTimer(
	raw []byte,
	sourceRunID, forkRunID string,
	point runfork.RunForkPoint,
	selected *pipeline.WorkflowTimerActivation,
	correspondence *loopruntime.ForkCorrespondence,
	bornAt time.Time,
) (pipeline.WorkflowTimerActivation, bool, error) {
	source, bornAt, err := decodeRunForkWorkflowTimerSource(raw, sourceRunID, bornAt)
	if err != nil {
		return pipeline.WorkflowTimerActivation{}, false, err
	}
	record, err := runfork.ProjectWorkflowTimerRecord(source.PersistenceRecord(), source.Ref, forkRunID, point, correspondence, bornAt)
	if err != nil {
		return pipeline.WorkflowTimerActivation{}, false, err
	}
	child, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(record)
	if err != nil {
		return pipeline.WorkflowTimerActivation{}, false, fmt.Errorf("projected fork workflow timer: %w", err)
	}
	if selected != nil {
		if err := validateRunForkWorkflowTimerSelection(*selected, child); err != nil {
			return pipeline.WorkflowTimerActivation{}, false, err
		}
		child.Ref.DeclarationRevision = selected.Ref.DeclarationRevision
		child.OwnerAgent, child.EventType = selected.OwnerAgent, selected.EventType
	}
	child = child.Canonical()
	if err := child.Validate(); err != nil {
		return pipeline.WorkflowTimerActivation{}, false, fmt.Errorf("projected fork workflow timer: %w", err)
	}
	return child, selected == nil, nil
}

func decodeRunForkWorkflowTimerSource(raw []byte, sourceRunID string, bornAt time.Time) (pipeline.WorkflowTimerActivation, time.Time, error) {
	snapshot, err := decodeRunForkTimerSnapshot(raw)
	if err != nil {
		return pipeline.WorkflowTimerActivation{}, time.Time{}, err
	}
	if snapshot.RunID != sourceRunID || snapshot.TaskType != "workflow_timer" {
		return pipeline.WorkflowTimerActivation{}, time.Time{}, fmt.Errorf("fork workflow timer requires an exact source-run workflow fact")
	}
	route := flowidentity.Route{ScopeKey: snapshot.FlowScopeKey, InstanceID: snapshot.FlowInstanceID, InstancePath: snapshot.FlowInstance}
	if !route.Valid() || route != flowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath) {
		return pipeline.WorkflowTimerActivation{}, time.Time{}, fmt.Errorf("fork workflow timer requires a complete canonical source route")
	}
	source, err := workflowTimerActivationFromSnapshot(snapshot)
	if err != nil {
		return pipeline.WorkflowTimerActivation{}, time.Time{}, err
	}
	if source.Status != "active" {
		return pipeline.WorkflowTimerActivation{}, time.Time{}, fmt.Errorf("only an active fixed-cut workflow timer is an inherited obligation")
	}
	bornAt = bornAt.UTC().Truncate(time.Microsecond)
	if bornAt.IsZero() || bornAt.Before(source.CreatedAt) {
		return pipeline.WorkflowTimerActivation{}, time.Time{}, fmt.Errorf("fork workflow timer birth must follow its source row birth")
	}
	return source, bornAt, nil
}

func validateRunForkWorkflowTimerSelection(selected, child pipeline.WorkflowTimerActivation) error {
	if !selected.Route.Valid() || selected.Route != child.Route || selected.RunID != child.RunID || selected.EntityID != child.EntityID {
		return fmt.Errorf("selected workflow timer requires the exact projected child owner")
	}
	if err := selected.Validate(); err != nil {
		return fmt.Errorf("selected workflow timer declaration activation: %w", err)
	}
	if selected.Status != "active" || selected.Ref.DeclarationKey != child.Ref.DeclarationKey ||
		selected.Ref.Cause != child.Ref.Cause || selected.Ref.Generation != child.Ref.Generation ||
		selected.ExecutionMode != child.ExecutionMode || selected.RoutingSource != child.RoutingSource ||
		selected.Ref.DeclarationRevision == "" || selected.Ref.DeclarationRevision != strings.TrimSpace(selected.Ref.DeclarationRevision) {
		return fmt.Errorf("selected workflow timer contradicts its exact declaration, cause, generation, mode, or routing source")
	}
	return nil
}
