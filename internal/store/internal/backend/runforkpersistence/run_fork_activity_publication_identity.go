package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/activityresult"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

// Follow only retained request/publication bindings through their own fixed
// cuts. A journal identity, later source event or common payload is not a cut.
func (a runForkSourceStateAdmission) activityPublicationAtCut(ctx context.Context, tx *sql.Tx, requestID string, visiting map[string]bool) (events.Event, bool, error) {
	cut, err := newRunForkReplayCut(a.snapshot)
	if err != nil {
		return events.Event{}, false, err
	}
	key := a.snapshot.RunID + "/" + requestID
	if visiting[key] {
		return events.Event{}, false, fmt.Errorf("activity publication lineage contains a cycle")
	}
	visiting[key] = true
	defer delete(visiting, key)
	request, err := cut.event(requestID)
	if err != nil {
		return events.Event{}, false, err
	}
	outputs, err := pipeline.ActivityRequestResultPublications(request)
	if err != nil {
		return events.Event{}, false, err
	}
	published, candidate, err := directActivityPublicationAtCut(cut, request, outputs)
	if err != nil {
		return events.Event{}, false, err
	}
	if published.ID() != "" {
		return published, true, nil
	}
	if !candidate {
		return events.Event{}, false, nil
	}
	selected, inherited := request.SelectedForkLineage()
	if !inherited {
		return events.Event{}, false, nil
	}
	return a.inheritedActivityPublicationAtCut(ctx, tx, cut, request, selected, visiting)
}

func directActivityPublicationAtCut(cut runForkReplayCut, request events.Event, outputs []activityresult.Record) (events.Event, bool, error) {
	var published events.Event
	candidate := false
	for _, output := range outputs {
		for _, row := range cut.events {
			candidate = candidate || row.EventName == output.EventType
		}
		if _, present := cut.events[output.EventID]; !present {
			continue
		}
		if published.ID() != "" {
			return events.Event{}, false, fmt.Errorf("activity has contradictory published terminal results")
		}
		var err error
		published, err = cut.event(output.EventID)
		if err != nil {
			return events.Event{}, false, err
		}
		if err := pipeline.ValidateRecordedActivityPublication(request, published, published.ID(), published.Payload()); err != nil {
			return events.Event{}, false, err
		}
	}
	return published, candidate, nil
}

func (a runForkSourceStateAdmission) inheritedActivityPublicationAtCut(ctx context.Context, tx *sql.Tx, cut runForkReplayCut, request events.Event, selected events.SelectedForkLineage, visiting map[string]bool) (events.Event, bool, error) {
	if tx == nil {
		return events.Event{}, false, fmt.Errorf("inherited activity publication requires its native retained binding")
	}
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, a.snapshot.RunID)
	if err != nil {
		return events.Event{}, false, err
	}
	if binding.SourceRunID != selected.SourceRunID() {
		return events.Event{}, false, fmt.Errorf("inherited activity has foreign source binding")
	}
	parent, err := loadRunForkPointSnapshot(ctx, tx, binding.SourceRunID, binding.ForkPoint)
	if err != nil {
		return events.Event{}, false, err
	}
	parentAdmission := runForkSourceStateAdmission{snapshot: parent, forkRunID: binding.ForkRunID, postgres: a.postgres}
	previous, present, err := parentAdmission.activityPublicationAtCut(ctx, tx, selected.SourceEventID(), visiting)
	if err != nil || !present {
		return events.Event{}, false, err
	}
	id := activityidentity.ForkLineageEventID(a.snapshot.RunID, previous.ID())
	if _, present := cut.events[id]; !present {
		return events.Event{}, false, nil
	}
	physical, err := cut.event(id)
	if err != nil {
		return events.Event{}, false, err
	}
	parentCut, err := newRunForkReplayCut(parent)
	if err != nil {
		return events.Event{}, false, err
	}
	generation, matched, err := parentAdmission.publishedActivityGenerationAtCut(ctx, tx, parentCut, previous, make(map[string]bool))
	if err != nil {
		return events.Event{}, false, err
	}
	if !matched {
		return events.Event{}, false, fmt.Errorf("inherited activity publication lacks exact original result")
	}
	reference, err := parentAdmission.activityPublicationGenerationProjection(previous, generation)
	if err != nil {
		return events.Event{}, false, err
	}
	if err := requireSelectedPublicationCutLineage(ctx, tx, binding, physical, previous, reference); err != nil {
		return events.Event{}, false, err
	}
	if err := pipeline.ValidateSelectedActivityResultPayload(request, physical); err != nil {
		return events.Event{}, false, err
	}
	return physical, true, nil
}

func requireSelectedPublicationCutLineage(ctx context.Context, tx *sql.Tx, binding runfork.RunForkSelectedContractBinding, physical, parent events.Event, reference *loopruntime.ForkChildReference) error {
	lineage, selected := physical.SelectedForkLineage()
	if !selected || lineage.SourceRunID() != binding.SourceRunID || lineage.SourceEventID() != parent.ID() ||
		physical.RunID() != binding.ForkRunID || parent.RunID() != binding.SourceRunID || physical.Type() != parent.Type() || physical.ExecutionMode() != parent.ExecutionMode() {
		return fmt.Errorf("activity publication has crossed physical selected lineage")
	}
	stored, found, err := readSelectedForkInputLineageTx(ctx, tx, physical.RunID(), parent.ID(), physical.ID())
	if err != nil || !found {
		return fmt.Errorf("activity publication lacks exact retained input lineage: %v", err)
	}
	source := runfork.RunForkSelectedContractSourceEvent{SourceEventID: parent.ID(), EventName: string(parent.Type()), ExecutionMode: parent.ExecutionMode()}
	if err := validateSelectedForkPublishedInputIdentity(binding, stored, physical, source); err != nil {
		return err
	}
	if err := validateSelectedForkPublishedInputFrame(binding, stored, physical, source); err != nil {
		return err
	}
	projected, err := runfork.ProjectSelectedContractSourceEvent(binding.SourceRunID, binding.ForkRunID,
		runfork.RunForkSelectedContractSourceEvent{SourceEventID: parent.ID(), EventName: string(parent.Type()), ExecutionMode: parent.ExecutionMode(),
			RoutingSource: parent.RoutingSource(), Payload: parent.Payload()})
	if err != nil {
		return err
	}
	if reference != nil {
		if err := reference.RequireDestination(binding.ForkRunID, projected.RoutingSource.Route().EntityID); err != nil {
			return err
		}
	}
	projected.Payload, err = projectForkRevisionPayload(projected.Payload, reference)
	if err != nil {
		return err
	}
	if physical.RoutingSource() != projected.RoutingSource || !workflowCommitJSONEqual(physical.Payload(), projected.Payload) {
		return fmt.Errorf("inherited activity publication changed its immediate-source payload or routing projection")
	}
	return nil
}
