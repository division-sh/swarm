package runforkreadiness

import (
	"bytes"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type inheritedTimerAdmissionKey struct {
	timerID        string
	declarationKey string
}

type inheritedTimerAdmission struct {
	source       pipeline.WorkflowTimerActivation
	construction flowidentity.Instance
	point        runfork.RunForkPoint
	loops        []loopruntime.Activation
	removed      bool
	revision     string
	ownerAgent   string
	eventType    string
}

func (a Admission) SelectInheritedWorkflowTimerRecord(record pipeline.WorkflowTimerActivationPersistenceRecord) (*pipeline.WorkflowTimerActivationPersistenceRecord, error) {
	activation, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(record)
	if err != nil {
		return nil, err
	}
	selected, err := a.SelectInheritedWorkflowTimer(activation)
	if err != nil || selected == nil {
		return nil, err
	}
	projected := selected.PersistenceRecord()
	return &projected, nil
}

func admitInheritedWorkflowTimers(source semanticview.Source, plan runfork.RunForkPlan) (map[inheritedTimerAdmissionKey]inheritedTimerAdmission, error) {
	out := make(map[inheritedTimerAdmissionKey]inheritedTimerAdmission, len(plan.WorkflowTimers))
	for _, record := range plan.WorkflowTimers {
		timer, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(record)
		if err != nil {
			return nil, err
		}
		if timer.Status != "active" {
			continue
		}
		if err := plan.ForkPoint.Validate(); err != nil {
			return nil, err
		}
		constructed, entity, found, err := runforkadmission.FixedConstructionForRoute(source, plan, timer.Route)
		if err != nil {
			return nil, err
		}
		if !found || timer.RunID != plan.SourceRunID || entity.EntityID != timer.EntityID {
			return nil, fmt.Errorf("inherited workflow timer lacks its exact recorded construction")
		}
		selected, err := pipeline.SelectInheritedWorkflowTimer(source, timer, inheritedTimerHeader(constructed))
		if err != nil {
			return nil, err
		}
		loops, err := admitInheritedTimerLoops(entity, timer.Ref.Generation)
		if err != nil {
			return nil, err
		}
		key := inheritedTimerAdmissionKey{timer.Ref.ActivationID, timer.Ref.DeclarationKey}
		if _, duplicate := out[key]; duplicate {
			return nil, fmt.Errorf("inherited workflow timer repeats sealed source identity")
		}
		sealed := inheritedTimerAdmission{
			source: timer.Canonical(), construction: constructed, point: plan.ForkPoint, loops: loops, removed: selected == nil,
		}
		if selected != nil {
			sealed.revision, sealed.ownerAgent, sealed.eventType = selected.Ref.DeclarationRevision, selected.OwnerAgent, selected.EventType
		}
		out[key] = sealed
	}
	return out, nil
}

// SelectInheritedWorkflowTimer consumes only fixed facts and selected effects
// sealed at Admit. It does not retain or consult a mutable semantic source.
func (a Admission) SelectInheritedWorkflowTimer(projected pipeline.WorkflowTimerActivation) (*pipeline.WorkflowTimerActivation, error) {
	if a.sealed == nil {
		return nil, fmt.Errorf("inherited workflow timer requires selected readiness admission")
	}
	if err := projected.Validate(); err != nil {
		return nil, err
	}
	projected = projected.Canonical()
	key := inheritedTimerAdmissionKey{projected.SourceTimerID, projected.Ref.DeclarationKey}
	sealed, found := a.sealed.timers[key]
	if !found {
		return nil, fmt.Errorf("inherited workflow timer has no sealed source/declaration")
	}
	expected, err := sealed.project(projected.RunID, projected.CreatedAt)
	if err != nil {
		return nil, err
	}
	if err := projected.ValidateCauseReplay(expected); err != nil {
		return nil, err
	}
	if projected.Status != "active" || !projected.FiredAt.IsZero() ||
		projected.RoutingSource != expected.RoutingSource || !projected.FireAt.Equal(expected.FireAt) ||
		!bytes.Equal(projected.Payload, expected.Payload) {
		return nil, fmt.Errorf("inherited workflow timer differs from the exact sealed fixed-cut projection")
	}
	if sealed.removed {
		return nil, nil
	}
	projected.Ref.DeclarationRevision = sealed.revision
	projected.OwnerAgent, projected.EventType = sealed.ownerAgent, sealed.eventType
	if err := projected.Validate(); err != nil {
		return nil, err
	}
	return &projected, nil
}

func (a inheritedTimerAdmission) project(childRunID string, bornAt time.Time) (pipeline.WorkflowTimerActivation, error) {
	constructed, err := runfork.ProjectConstructionIdentity(a.source.RunID, childRunID, a.construction)
	if err != nil {
		return pipeline.WorkflowTimerActivation{}, err
	}
	correspondence, err := loopruntime.NewForkCorrespondence(a.loops, childRunID, constructed.EntityID)
	if err != nil {
		return pipeline.WorkflowTimerActivation{}, err
	}
	record, err := runfork.ProjectWorkflowTimerRecord(a.source.PersistenceRecord(), a.source.Ref, childRunID, a.point, correspondence, bornAt)
	if err != nil {
		return pipeline.WorkflowTimerActivation{}, err
	}
	child, err := pipeline.DecodeWorkflowTimerActivationPersistenceRecord(record)
	if err != nil {
		return pipeline.WorkflowTimerActivation{}, err
	}
	if child.EntityID != constructed.EntityID || child.Route != constructed.Route() {
		return pipeline.WorkflowTimerActivation{}, fmt.Errorf("inherited workflow timer projection contradicts its recorded construction")
	}
	return child, nil
}

func admitInheritedTimerLoops(entity runfork.RunForkEntityState, generation attemptgeneration.Generation) ([]loopruntime.Activation, error) {
	if generation == (attemptgeneration.Generation{}) {
		return nil, nil
	}
	carrier, err := engine.StateCarrierFromPersisted(nil, nil, nil, map[string]any{
		loopruntime.BucketKey: entity.Accumulator[loopruntime.BucketKey],
	})
	if err != nil {
		return nil, err
	}
	activations, err := loopruntime.List(carrier.StateBuckets)
	if err != nil {
		return nil, err
	}
	owners := 0
	for _, activation := range activations {
		if _, exact := carrier.StateBuckets[loopruntime.BucketKey][activation.Key()]; !exact {
			return nil, fmt.Errorf("inherited workflow timer loop has a conflicting recorded key")
		}
		if activation.OwnsGeneration(generation) {
			owners++
		}
	}
	if owners != 1 {
		return nil, fmt.Errorf("inherited workflow timer generation lacks one exact fixed-cut loop owner")
	}
	return activations, nil
}

func inheritedTimerHeader(constructed flowidentity.Instance) pipeline.WorkflowInstance {
	return pipeline.WorkflowInstance{
		InstanceID: constructed.InstanceID, StorageRef: constructed.InstancePath, EntityID: constructed.EntityID,
		WorkflowName: constructed.TemplateID, ParentFlowID: constructed.ParentRoute.FlowID,
		ParentFlowInstance: constructed.ParentRoute.FlowInstance, ParentEntityID: constructed.ParentEntityID,
	}
}
