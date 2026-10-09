package runforkpersistence

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

const runForkWorkflowTimerReconstructionOwner = "run_fork_workflow_timer"

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
	if sourceRunID == "" || forkRunID == "" || sourceRunID == forkRunID ||
		sourceRunID != strings.TrimSpace(sourceRunID) || forkRunID != strings.TrimSpace(forkRunID) {
		return pipeline.WorkflowTimerActivation{}, false, fmt.Errorf("fork workflow timer requires exact distinct source and child runs")
	}
	if err := point.Validate(); err != nil {
		return pipeline.WorkflowTimerActivation{}, false, fmt.Errorf("fork workflow timer point: %w", err)
	}
	snapshot, err := decodeRunForkTimerSnapshot(raw)
	if err != nil {
		return pipeline.WorkflowTimerActivation{}, false, err
	}
	if snapshot.RunID != sourceRunID || snapshot.TaskType != "workflow_timer" {
		return pipeline.WorkflowTimerActivation{}, false, fmt.Errorf("fork workflow timer requires an exact source-run workflow fact")
	}
	route := flowidentity.Route{ScopeKey: snapshot.FlowScopeKey, InstanceID: snapshot.FlowInstanceID, InstancePath: snapshot.FlowInstance}
	if !route.Valid() || route != flowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath) {
		return pipeline.WorkflowTimerActivation{}, false, fmt.Errorf("fork workflow timer requires a complete canonical source route")
	}
	source, err := workflowTimerActivationFromSnapshot(snapshot)
	if err != nil {
		return pipeline.WorkflowTimerActivation{}, false, err
	}
	if source.Status != "active" {
		return pipeline.WorkflowTimerActivation{}, false, fmt.Errorf("only an active fixed-cut workflow timer is an inherited obligation")
	}
	bornAt = bornAt.UTC().Truncate(time.Microsecond)
	if bornAt.IsZero() || bornAt.Before(source.CreatedAt) {
		return pipeline.WorkflowTimerActivation{}, false, fmt.Errorf("fork workflow timer birth must follow its source row birth")
	}
	ownership, err := projectRunForkEntityOwnership(sourceRunID, forkRunID, source.EntityID, source.Route.InstancePath)
	if err != nil {
		return pipeline.WorkflowTimerActivation{}, false, err
	}
	root := ownership.Source == (runfork.EntityIdentity{EntityID: sourceRunID, FlowInstance: sourceRunID})
	childRoute := route
	if root {
		if route != (flowidentity.Route{ScopeKey: ".", InstanceID: sourceRunID, InstancePath: sourceRunID}) || source.RoutingSource.Kind() != events.RoutingSourceRoot {
			return pipeline.WorkflowTimerActivation{}, false, fmt.Errorf("fork root workflow timer contradicts exact root ownership")
		}
		childRoute.InstanceID, childRoute.InstancePath = forkRunID, forkRunID
	} else if source.RoutingSource.Kind() != events.RoutingSourceFlowOwnedControl || source.RoutingSource.Route().FlowID != route.ScopeKey {
		return pipeline.WorkflowTimerActivation{}, false, fmt.Errorf("fork workflow timer declaration scope contradicts its recorded route")
	}
	childSource, err := runfork.ProjectProducerOwnership(sourceRunID, forkRunID, source.RoutingSource)
	if err != nil {
		return pipeline.WorkflowTimerActivation{}, false, err
	}
	child := source.Canonical()
	child.RunID, child.EntityID, child.Route, child.RoutingSource = forkRunID, ownership.Fork.EntityID, childRoute, childSource
	child.Ref.ActivationID = timeridentity.WorkflowTimerActivationID(
		"fork", sourceRunID, source.Ref.ActivationID, forkRunID,
		string(point.Kind), strconv.FormatInt(point.Revision, 10), point.EventID,
	)
	if correspondence != nil {
		if err := correspondence.RequireDestination(forkRunID, child.EntityID); err != nil {
			return pipeline.WorkflowTimerActivation{}, false, err
		}
	}
	if source.Ref.Generation != (attemptgeneration.Generation{}) {
		admitted, err := correspondence.AdmitSource(source.Ref.Generation)
		if err != nil {
			return pipeline.WorkflowTimerActivation{}, false, fmt.Errorf("fork workflow timer source generation: %w", err)
		}
		bound, err := correspondence.Bind(admitted)
		if err != nil {
			return pipeline.WorkflowTimerActivation{}, false, err
		}
		if err := bound.RequireDestination(forkRunID, child.EntityID); err != nil {
			return pipeline.WorkflowTimerActivation{}, false, err
		}
		child.Ref.Generation = bound.Generation()
	}
	if selected != nil {
		if err := validateRunForkWorkflowTimerSelection(*selected, child); err != nil {
			return pipeline.WorkflowTimerActivation{}, false, err
		}
		child.Ref.DeclarationRevision = selected.Ref.DeclarationRevision
		child.OwnerAgent, child.EventType = selected.OwnerAgent, selected.EventType
	}
	child.CreatedAt, child.FiredAt = bornAt, time.Time{}
	child.SourceTimerID, child.ForkedFromRunID = source.Ref.ActivationID, sourceRunID
	child.ForkedFromPointKind, child.ForkedFromPointRevision, child.ForkedFromEventID = point.Kind, point.Revision, point.EventID
	child.SourceArmedAt = source.CreatedAt
	if !source.SourceArmedAt.IsZero() {
		child.SourceArmedAt = source.SourceArmedAt
	}
	child.ReconstructionOwner = runForkWorkflowTimerReconstructionOwner
	child = child.Canonical()
	if err := child.Validate(); err != nil {
		return pipeline.WorkflowTimerActivation{}, false, fmt.Errorf("projected fork workflow timer: %w", err)
	}
	return child, selected == nil, nil
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
