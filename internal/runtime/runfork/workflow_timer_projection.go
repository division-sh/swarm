package runfork

import (
	"fmt"
	"strconv"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/timerobligation"
	"github.com/google/uuid"
)

const WorkflowTimerForkReconstructionOwner = "run_fork_workflow_timer"

// ProjectWorkflowTimerRecord projects a pipeline-admitted record and its exact
// typed ref. It grants neither activation admission nor permission to execute.
func ProjectWorkflowTimerRecord(
	source timerobligation.WorkflowTimerActivationRecord,
	ref timeridentity.WorkflowTimerActivationRef,
	childRunID string,
	point RunForkPoint,
	correspondence *loopruntime.ForkCorrespondence,
	bornAt time.Time,
) (timerobligation.WorkflowTimerActivationRecord, error) {
	if err := validateWorkflowTimerRecordProjection(source, ref, childRunID, point, bornAt); err != nil {
		return timerobligation.WorkflowTimerActivationRecord{}, err
	}
	child, err := projectWorkflowTimerRecordOwnership(source, childRunID)
	if err != nil {
		return timerobligation.WorkflowTimerActivationRecord{}, err
	}
	ref.Generation, err = projectWorkflowTimerRecordGeneration(ref.Generation, correspondence, childRunID, child.EntityID)
	if err != nil {
		return timerobligation.WorkflowTimerActivationRecord{}, err
	}
	ref.ActivationID = timeridentity.WorkflowTimerActivationID(
		"fork", source.RunID, source.ActivationID, childRunID,
		string(point.Kind), strconv.FormatInt(point.Revision, 10), point.EventID,
	)
	child.ActivationID, child.TaskID = ref.ActivationID, ref.TaskID()
	child.CreatedAt, child.FiredAt = bornAt, time.Time{}
	child.SourceTimerID, child.ForkedFromRunID = source.ActivationID, source.RunID
	child.ForkedFromPointKind, child.ForkedFromPointRevision, child.ForkedFromEventID = point.Kind, point.Revision, point.EventID
	child.SourceArmedAt = source.CreatedAt
	if !source.SourceArmedAt.IsZero() {
		child.SourceArmedAt = source.SourceArmedAt
	}
	child.ReconstructionOwner = WorkflowTimerForkReconstructionOwner
	child.Payload = append([]byte(nil), source.Payload...)
	return child, nil
}

func validateWorkflowTimerRecordProjection(
	source timerobligation.WorkflowTimerActivationRecord,
	ref timeridentity.WorkflowTimerActivationRef,
	childRunID string,
	point RunForkPoint,
	bornAt time.Time,
) error {
	sourceUUID, sourceErr := uuid.Parse(source.RunID)
	childUUID, childErr := uuid.Parse(childRunID)
	if sourceErr != nil || childErr != nil || sourceUUID.String() != source.RunID ||
		childUUID.String() != childRunID || sourceUUID == uuid.Nil || childUUID == uuid.Nil || sourceUUID == childUUID {
		return fmt.Errorf("fork workflow timer requires exact distinct source and child run UUIDs")
	}
	if source.TaskType != string(timerobligation.FamilyWorkflowTimer) || source.Status != "active" {
		return fmt.Errorf("fork workflow timer requires an active source workflow record")
	}
	if !ref.Valid() || ref != ref.Normalize() || ref.ActivationID != source.ActivationID || ref.TaskID() != source.TaskID {
		return fmt.Errorf("fork workflow timer record disagrees with its exact typed activation ref")
	}
	if err := point.Validate(); err != nil {
		return fmt.Errorf("fork workflow timer point: %w", err)
	}
	if bornAt.IsZero() || bornAt != bornAt.UTC().Truncate(time.Microsecond) ||
		source.CreatedAt.IsZero() || bornAt.Before(source.CreatedAt) {
		return fmt.Errorf("fork workflow timer requires canonical birth at or after its source row birth")
	}
	if !source.Route.Valid() || source.Route != flowidentity.StoredRoute(source.Route.ScopeKey, source.Route.InstanceID, source.Route.InstancePath) {
		return fmt.Errorf("fork workflow timer requires a complete canonical source route")
	}
	return nil
}

func projectWorkflowTimerRecordOwnership(source timerobligation.WorkflowTimerActivationRecord, childRunID string) (timerobligation.WorkflowTimerActivationRecord, error) {
	ownership, err := ProjectEntityOwnership(source.RunID, childRunID, source.EntityID, source.Route.InstancePath)
	if err != nil {
		return timerobligation.WorkflowTimerActivationRecord{}, err
	}
	root := ownership.Source == (EntityIdentity{EntityID: source.RunID, FlowInstance: source.RunID})
	if root {
		if source.Route != (flowidentity.Route{ScopeKey: ".", InstanceID: source.RunID, InstancePath: source.RunID}) ||
			source.RoutingSource.Kind() != events.RoutingSourceRoot || source.RoutingSource.Route() != (events.RouteIdentity{EntityID: source.EntityID}) {
			return timerobligation.WorkflowTimerActivationRecord{}, fmt.Errorf("fork root workflow timer contradicts exact root ownership")
		}
	} else if source.RoutingSource.Kind() != events.RoutingSourceFlowOwnedControl ||
		source.RoutingSource.Route() != (events.RouteIdentity{FlowID: source.Route.ScopeKey, FlowInstance: source.Route.InstancePath, EntityID: source.EntityID}) {
		return timerobligation.WorkflowTimerActivationRecord{}, fmt.Errorf("fork workflow timer declaration scope contradicts its recorded route")
	}
	route, err := ProjectExecutionRoute(source.RunID, childRunID, source.Route.ScopeKey, source.Route)
	if err != nil {
		return timerobligation.WorkflowTimerActivationRecord{}, err
	}
	routingSource, err := ProjectProducerOwnership(source.RunID, childRunID, source.RoutingSource)
	if err != nil {
		return timerobligation.WorkflowTimerActivationRecord{}, err
	}
	child := source
	child.RunID, child.EntityID, child.Route, child.RoutingSource = childRunID, ownership.Fork.EntityID, route, routingSource
	return child, nil
}

func projectWorkflowTimerRecordGeneration(
	source attemptgeneration.Generation,
	correspondence *loopruntime.ForkCorrespondence,
	childRunID, childEntityID string,
) (attemptgeneration.Generation, error) {
	if correspondence != nil {
		if err := correspondence.RequireDestination(childRunID, childEntityID); err != nil {
			return attemptgeneration.Generation{}, err
		}
	}
	if source == (attemptgeneration.Generation{}) {
		return source, nil
	}
	admitted, err := correspondence.AdmitSource(source)
	if err != nil {
		return attemptgeneration.Generation{}, fmt.Errorf("fork workflow timer source generation: %w", err)
	}
	child, err := correspondence.Bind(admitted)
	if err != nil {
		return attemptgeneration.Generation{}, err
	}
	if err := child.RequireDestination(childRunID, childEntityID); err != nil {
		return attemptgeneration.Generation{}, err
	}
	return child.Generation(), nil
}
