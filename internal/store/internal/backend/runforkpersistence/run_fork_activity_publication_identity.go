package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/activityresult"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Request origins use the same retained-cut and projection owners as result
// publications. A child-produced request stops here; inherited requests walk
// only their committed immediate-parent bindings.
func (a runForkSourceStateAdmission) activityRequestOriginAtCut(ctx context.Context, tx *sql.Tx, requestID string, visiting map[string]bool) (semanticview.ActivityRequestOrigin, error) {
	if err := ctx.Err(); err != nil {
		return semanticview.ActivityRequestOrigin{}, err
	}
	if tx == nil || a.sourceAt == nil {
		return semanticview.ActivityRequestOrigin{}, fmt.Errorf("activity origin requires the native run-bound source owner")
	}
	cut, err := newRunForkReplayCut(a.snapshot)
	if err != nil {
		return semanticview.ActivityRequestOrigin{}, err
	}
	key := cut.runID + "/" + requestID
	if visiting[key] {
		return semanticview.ActivityRequestOrigin{}, fmt.Errorf("activity request origin contains a cycle")
	}
	visiting[key] = true
	defer delete(visiting, key)
	request, err := cut.event(requestID)
	if err != nil {
		return semanticview.ActivityRequestOrigin{}, err
	}
	if request.Type() != runForkActivityRequestEvent {
		return semanticview.ActivityRequestOrigin{}, fmt.Errorf("activity origin is not an activity request")
	}
	if _, err := pipeline.ActivityRequestResultPublications(request); err != nil {
		return semanticview.ActivityRequestOrigin{}, err
	}
	var payload runForkActivityRequestPayload
	if err := json.Unmarshal(request.Payload(), &payload); err != nil {
		return semanticview.ActivityRequestOrigin{}, err
	}
	fact, err := runForkActivityFact(payload)
	if err != nil {
		return semanticview.ActivityRequestOrigin{}, err
	}
	if fact.RunID != cut.runID {
		return semanticview.ActivityRequestOrigin{}, fmt.Errorf("activity origin has foreign request coordinates")
	}
	artifact, err := a.sourceAt(ctx, tx, cut.runID)
	if err != nil {
		return semanticview.ActivityRequestOrigin{}, err
	}
	step := semanticview.ActivityRequestCut{RunID: cut.runID, RequestID: request.ID(), Revision: a.snapshot.Revision, Source: artifact}
	if activityidentity.RequestEventID(fact) == request.ID() {
		origin := semanticview.ActivityRequestOrigin{Request: request, Cuts: []semanticview.ActivityRequestCut{step}}
		return origin, origin.Validate()
	}
	if _, err := pipeline.SelectedActivityRequestLineage(request); err != nil {
		return semanticview.ActivityRequestOrigin{}, err
	}
	selected, inherited := request.SelectedForkLineage()
	if !inherited {
		return semanticview.ActivityRequestOrigin{}, fmt.Errorf("activity origin lacks exact retained request lineage")
	}
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, cut.runID)
	if err != nil {
		return semanticview.ActivityRequestOrigin{}, err
	}
	if binding.SourceRunID != selected.SourceRunID() || binding.ForkRunID != cut.runID {
		return semanticview.ActivityRequestOrigin{}, fmt.Errorf("activity request origin has crossed retained binding")
	}
	parent, err := loadRunForkPointSnapshot(ctx, tx, binding.SourceRunID, binding.ForkPoint)
	if err != nil {
		return semanticview.ActivityRequestOrigin{}, err
	}
	parentAdmission := runForkSourceStateAdmission{snapshot: parent, forkRunID: binding.ForkRunID, postgres: a.postgres, sourceAt: a.sourceAt}
	parentCut, err := newRunForkReplayCut(parent)
	if err != nil {
		return semanticview.ActivityRequestOrigin{}, err
	}
	previous, err := parentCut.event(selected.SourceEventID())
	if err != nil {
		return semanticview.ActivityRequestOrigin{}, err
	}
	var previousPayload runForkActivityRequestPayload
	if err := json.Unmarshal(previous.Payload(), &previousPayload); err != nil {
		return semanticview.ActivityRequestOrigin{}, err
	}
	reference, err := parentAdmission.activityPublicationGenerationProjection(previous, previousPayload.Generation)
	if err != nil {
		return semanticview.ActivityRequestOrigin{}, err
	}
	projected, err := requireSelectedPublicationCutFrame(ctx, tx, binding, request, previous, reference)
	if err != nil {
		return semanticview.ActivityRequestOrigin{}, err
	}
	projected.Payload, err = bindRunForkActivitySourceEvent(projected.Payload, binding.ForkRunID, previous.ID(), projected.RoutingSource.Route().EntityID, reference)
	if err != nil {
		return semanticview.ActivityRequestOrigin{}, err
	}
	if request.RoutingSource() != projected.RoutingSource || !workflowCommitJSONEqual(request.Payload(), projected.Payload) {
		return semanticview.ActivityRequestOrigin{}, fmt.Errorf("inherited activity request changed immediate-source payload or routing")
	}
	origin, err := parentAdmission.activityRequestOriginAtCut(ctx, tx, previous.ID(), visiting)
	if err != nil {
		return semanticview.ActivityRequestOrigin{}, err
	}
	step.ParentPointKind, step.ParentPointEventID = string(binding.ForkPoint.Kind), binding.ForkPoint.EventID
	step.ParentPointInput, step.ParentPointRevision = binding.ForkPoint.Input, binding.ForkPoint.Revision
	origin.Cuts = append([]semanticview.ActivityRequestCut{step}, origin.Cuts...)
	return origin, origin.Validate()
}

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
	projected, err := requireSelectedPublicationCutFrame(ctx, tx, binding, physical, parent, reference)
	if err != nil {
		return err
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

func requireSelectedPublicationCutFrame(ctx context.Context, tx *sql.Tx, binding runfork.RunForkSelectedContractBinding, physical, parent events.Event, reference *loopruntime.ForkChildReference) (runfork.RunForkSelectedContractSourceEvent, error) {
	lineage, selected := physical.SelectedForkLineage()
	if !selected || lineage.SourceRunID() != binding.SourceRunID || lineage.SourceEventID() != parent.ID() ||
		physical.RunID() != binding.ForkRunID || parent.RunID() != binding.SourceRunID || physical.Type() != parent.Type() || physical.ExecutionMode() != parent.ExecutionMode() {
		return runfork.RunForkSelectedContractSourceEvent{}, fmt.Errorf("activity publication has crossed physical selected lineage")
	}
	stored, found, err := readSelectedForkInputLineageTx(ctx, tx, physical.RunID(), parent.ID(), physical.ID())
	if err != nil || !found {
		return runfork.RunForkSelectedContractSourceEvent{}, fmt.Errorf("activity publication lacks exact retained input lineage: %v", err)
	}
	source := runfork.RunForkSelectedContractSourceEvent{SourceEventID: parent.ID(), EventName: string(parent.Type()), ExecutionMode: parent.ExecutionMode()}
	if err := validateSelectedForkPublishedInputIdentity(binding, stored, physical, source); err != nil {
		return runfork.RunForkSelectedContractSourceEvent{}, err
	}
	if err := validateSelectedForkPublishedInputFrame(binding, stored, physical, source); err != nil {
		return runfork.RunForkSelectedContractSourceEvent{}, err
	}
	projected, err := runfork.ProjectSelectedContractSourceEvent(binding.SourceRunID, binding.ForkRunID,
		runfork.RunForkSelectedContractSourceEvent{SourceEventID: parent.ID(), EventName: string(parent.Type()), ExecutionMode: parent.ExecutionMode(),
			RoutingSource: parent.RoutingSource(), Payload: parent.Payload()})
	if err != nil {
		return runfork.RunForkSelectedContractSourceEvent{}, err
	}
	if reference != nil {
		if err := reference.RequireDestination(binding.ForkRunID, projected.RoutingSource.Route().EntityID); err != nil {
			return runfork.RunForkSelectedContractSourceEvent{}, err
		}
	}
	return projected, nil
}
