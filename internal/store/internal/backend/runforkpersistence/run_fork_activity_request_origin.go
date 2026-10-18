package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Request origins walk only committed immediate-parent bindings at their fixed
// cuts. A child-produced request stops at its own run-bound source artifact.
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
		if err := requireOriginalActivityRequestArtifact(request, artifact); err != nil {
			return semanticview.ActivityRequestOrigin{}, err
		}
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
	// This target supports event/deployment revision cuts, not D1's run-start
	// projection. Keep its existing fixed-revision snapshot owner unchanged.
	if err := binding.ForkPoint.Validate(); err != nil {
		return semanticview.ActivityRequestOrigin{}, err
	}
	parent, err := loadRunForkRevisionSnapshot(ctx, tx, binding.SourceRunID, binding.ForkPoint.Revision)
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

func requireOriginalActivityRequestArtifact(request events.Event, artifact correlation.SourceArtifactFact) error {
	if err := artifact.Validate(); err != nil {
		return err
	}
	admission, present := request.PayloadAdmission()
	if !present || admission.Binding().BundleHash() != artifact.BundleHash() ||
		admission.Binding().SchemaClass() != events.PayloadSchemaPlatform || admission.Binding().EventKey() != runForkActivityRequestEvent {
		return fmt.Errorf("original activity request does not retain the exact run-bound source artifact")
	}
	return nil
}

func (a runForkSourceStateAdmission) activityPublicationGenerationProjection(result events.Event, generation attemptgeneration.Generation) (*loopruntime.ForkChildReference, error) {
	if generation == (attemptgeneration.Generation{}) {
		return nil, nil
	}
	_, state, err := a.project(runfork.RunForkSelectedContractSourceEvent{SourceEventID: result.ID(), EventName: string(result.Type()),
		ExecutionMode: result.ExecutionMode(), RoutingSource: result.RoutingSource(), Payload: result.Payload()})
	if err != nil {
		return nil, err
	}
	if state == nil {
		return nil, fmt.Errorf("loop activity publication requires exact fixed-cut producer state")
	}
	source, err := state.correspondence.AdmitSource(generation)
	if err != nil {
		return nil, err
	}
	child, err := state.correspondence.Bind(source)
	return &child, err
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

func validateSelectedForkPublishedInputIdentity(binding runfork.RunForkSelectedContractBinding, lineage runfork.RunForkSelectedContractExecutionLineage, event events.Event, sourceEvent runfork.RunForkSelectedContractSourceEvent) error {
	eventID := activityidentity.ForkLineageEventID(binding.ForkRunID, sourceEvent.SourceEventID)
	if lineage.ForkRunID != binding.ForkRunID || lineage.SourceRunID != binding.SourceRunID || lineage.SourceEventID != sourceEvent.SourceEventID || lineage.ForkEventID != eventID || lineage.EventName != sourceEvent.EventName {
		return fmt.Errorf("selected input lineage differs from exact source/child/event binding")
	}
	selected, ok := event.SelectedForkLineage()
	if !ok || event.AdmissionClass() != events.EventAdmissionSelectedForkReplay || event.ID() != eventID || event.RunID() != binding.ForkRunID ||
		selected.SourceRunID() != binding.SourceRunID || selected.SourceEventID() != sourceEvent.SourceEventID || selected.AuthorityStamp() != lineage.SelectionAuthority {
		return fmt.Errorf("selected input event differs from committed selected lineage")
	}
	return nil
}

func validateSelectedForkPublishedInputFrame(binding runfork.RunForkSelectedContractBinding, lineage runfork.RunForkSelectedContractExecutionLineage, event events.Event, sourceEvent runfork.RunForkSelectedContractSourceEvent) error {
	if !lineage.CreatedAt.Equal(event.CreatedAt()) {
		return fmt.Errorf("selected input lineage changed its retained publication time")
	}
	if event.Producer().Type() != events.EventProducerPlatform || event.Producer().ID() != lineage.SelectionAuthority || event.TaskID() != "" || event.ParentEventID() != "" || event.ChainDepth() != 0 {
		return fmt.Errorf("selected input publication changed its original publication frame")
	}
	if original, present := sourceEvent.InputPublication.Event(); present && (original.RunID() != binding.SourceRunID || original.ID() != sourceEvent.SourceEventID || string(original.Type()) != sourceEvent.EventName || original.ExecutionMode() != sourceEvent.ExecutionMode) {
		return fmt.Errorf("selected input original publication differs from fixed source identity")
	}
	return nil
}
