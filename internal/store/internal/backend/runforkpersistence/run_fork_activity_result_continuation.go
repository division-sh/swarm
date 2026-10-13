package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/activityresult"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func (a runForkSourceStateAdmission) publishedActivityResult(ctx context.Context, tx *sql.Tx, sourceID string, evidence runForkActivityAttemptEvidence, child runForkActivityRequestPayload, reference *loopruntime.ForkChildReference, replyID string) (activityresult.PublishedContinuation, error) {
	result, present, err := a.activityPublicationAtCut(ctx, tx, sourceID, make(map[string]bool))
	if err != nil {
		return activityresult.PublishedContinuation{}, err
	}
	if !present {
		// A journal or a later source publication is not publication at this cut.
		return activityresult.PublishedContinuation{}, nil
	}
	if string(result.Type()) != evidence.ResultEventType || !workflowCommitJSONEqual(result.Payload(), evidence.ResultPayload) {
		return activityresult.PublishedContinuation{}, fmt.Errorf("physical activity publication differs from its exact journal outcome")
	}
	_, selected := result.SelectedForkLineage()
	if !selected && !result.CreatedAt().Equal(evidence.CompletedAt) {
		return activityresult.PublishedContinuation{}, fmt.Errorf("published activity completion contradicts its journal timestamp")
	}
	fact, err := runForkActivityFact(child)
	if err != nil {
		return activityresult.PublishedContinuation{}, err
	}
	payload, err := projectForkRevisionPayload(evidence.ResultPayload, reference)
	if err != nil {
		return activityresult.PublishedContinuation{}, err
	}
	return activityresult.NewPublishedContinuation(fact, result.ID(), evidence.ResultEventType, payload, evidence.ExecutionMode, contracts.ActivityEffectClassNonIdempotentWrite,
		evidence.InputHash, replyID, evidence.CompletedAt)
}

func (a runForkSourceStateAdmission) publishedReadOnlyActivityResult(ctx context.Context, tx *sql.Tx, sourceID string, child runForkActivityRequestPayload, reference *loopruntime.ForkChildReference) (activityresult.PublishedContinuation, error) {
	result, present, err := a.activityPublicationAtCut(ctx, tx, sourceID, make(map[string]bool))
	if err != nil || !present {
		return activityresult.PublishedContinuation{}, err
	}
	cut, err := newRunForkReplayCut(a.snapshot)
	if err != nil {
		return activityresult.PublishedContinuation{}, err
	}
	request, err := cut.event(sourceID)
	if err != nil {
		return activityresult.PublishedContinuation{}, err
	}
	fact, err := runForkActivityFact(child)
	if err != nil {
		return activityresult.PublishedContinuation{}, err
	}
	hash, err := pipeline.ActivityRequestInputHash(request)
	if err != nil {
		return activityresult.PublishedContinuation{}, err
	}
	payload, err := projectForkRevisionPayload(result.Payload(), reference)
	if err != nil {
		return activityresult.PublishedContinuation{}, err
	}
	return activityresult.NewPublishedContinuation(fact, result.ID(), string(result.Type()), payload, result.ExecutionMode(), contracts.ActivityEffectClassReadOnly,
		hash, "", result.CreatedAt())
}

// The same fixed-cut activity owner recognizes published results when their
// unfinished physical recipients, rather than the request, enter the workset.
func (a runForkSourceStateAdmission) publishedActivityResultRoutes(ctx context.Context, tx *sql.Tx, sourceID string) ([]events.DeliveryRoute, bool, error) {
	cut, err := newRunForkReplayCut(a.snapshot)
	if err != nil {
		return nil, false, err
	}
	if _, present := cut.events[sourceID]; !present {
		return nil, false, nil
	}
	result, err := cut.event(sourceID)
	if err != nil {
		return nil, false, err
	}
	matched, err := a.admitPublishedActivityResultAtCut(ctx, tx, cut, result, make(map[string]bool))
	if err != nil || !matched {
		return nil, false, err
	}
	routes := make([]events.DeliveryRoute, 0)
	for _, row := range a.snapshot.Deliveries {
		delivery := row.Snapshot
		if delivery.EventID != sourceID || delivery.Terminal() {
			continue
		}
		route, err := projectSelectedFanOutRecipient(a.snapshot, result, delivery, a.forkRunID)
		if err != nil {
			return nil, false, err
		}
		routes = append(routes, route)
	}
	return routes, true, nil
}

func (a runForkSourceStateAdmission) admitPublishedActivityResultAtCut(ctx context.Context, tx *sql.Tx, cut runForkReplayCut, result events.Event, visiting map[string]bool) (bool, error) {
	_, found, err := a.publishedActivityGenerationAtCut(ctx, tx, cut, result, visiting)
	return found, err
}

func (a runForkSourceStateAdmission) publishedActivityGenerationAtCut(ctx context.Context, tx *sql.Tx, cut runForkReplayCut, result events.Event, visiting map[string]bool) (attemptgeneration.Generation, bool, error) {
	var generation attemptgeneration.Generation
	key := cut.runID + "/" + result.ID()
	if visiting[key] {
		return generation, false, fmt.Errorf("activity result publication lineage contains a cycle")
	}
	visiting[key] = true
	defer delete(visiting, key)
	matched := false
	for _, row := range a.snapshot.Events {
		if row.EventName != runForkActivityRequestEvent {
			continue
		}
		publication, present, err := a.activityPublicationAtCut(ctx, tx, row.EventID, make(map[string]bool))
		if err != nil {
			return generation, false, err
		}
		if !present || publication.ID() != result.ID() {
			continue
		}
		if matched {
			return generation, false, fmt.Errorf("published activity result has multiple source requests")
		}
		matched = true
		if err := a.requirePublishedActivityRequestJournal(ctx, tx, cut, row, result); err != nil {
			return generation, false, err
		}
		var request runForkActivityRequestPayload
		if err := json.Unmarshal(row.Payload, &request); err != nil {
			return generation, false, err
		}
		generation = request.Generation
	}
	if matched {
		return generation, true, nil
	}
	lineage, inherited := result.SelectedForkLineage()
	if !inherited {
		return generation, false, nil
	}
	return a.admitInheritedActivityResultAtCut(ctx, tx, cut, result, lineage, visiting)
}

func (a runForkSourceStateAdmission) admitInheritedActivityResultAtCut(ctx context.Context, tx *sql.Tx, cut runForkReplayCut, result events.Event, lineage events.SelectedForkLineage, visiting map[string]bool) (attemptgeneration.Generation, bool, error) {
	var generation attemptgeneration.Generation
	if tx == nil {
		return generation, false, fmt.Errorf("inherited activity result requires its native retained binding")
	}
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, cut.runID)
	if err != nil {
		return generation, false, err
	}
	if binding.SourceRunID != lineage.SourceRunID() {
		return generation, false, fmt.Errorf("inherited result has foreign source binding")
	}
	if binding.ForkPoint.Kind == runfork.RunForkPointRunStart {
		// The exclusive creation cut cannot contain an activity result. Its
		// separately admitted first turn is not a retained publication.
		return generation, false, nil
	}
	parent, err := loadRunForkPointSnapshot(ctx, tx, binding.SourceRunID, binding.ForkPoint)
	if err != nil {
		return generation, false, err
	}
	parentCut, err := newRunForkReplayCut(parent)
	if err != nil {
		return generation, false, err
	}
	previous, err := parentCut.event(lineage.SourceEventID())
	if err != nil {
		return generation, false, err
	}
	parentAdmission := runForkSourceStateAdmission{snapshot: parent, forkRunID: binding.ForkRunID, postgres: a.postgres}
	generation, matched, err := parentAdmission.publishedActivityGenerationAtCut(ctx, tx, parentCut, previous, visiting)
	if err != nil || !matched {
		return generation, false, err
	}
	reference, err := parentAdmission.activityPublicationGenerationProjection(previous, generation)
	if err != nil {
		return generation, false, err
	}
	// A result can be committed before the inherited request is published in
	// this child. Its exact earlier binding, never later history, owns identity.
	if err := requireSelectedPublicationCutLineage(ctx, tx, binding, result, previous, reference); err != nil {
		return generation, false, err
	}
	if reference != nil {
		generation = reference.Generation()
	}
	return generation, true, nil
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

func (a runForkSourceStateAdmission) requirePublishedActivityRequestJournal(ctx context.Context, tx *sql.Tx, cut runForkReplayCut, row runForkRevisionEvent, result events.Event) error {
	var request runForkActivityRequestPayload
	if err := json.Unmarshal(row.Payload, &request); err != nil {
		return err
	}
	fact, err := runForkActivityFact(request)
	if err != nil {
		return err
	}
	original, err := cut.event(row.EventID)
	if err != nil {
		return err
	}
	source := runfork.RunForkSelectedContractSourceEvent{SourceEventID: row.EventID, EventName: row.EventName,
		ExecutionMode: original.ExecutionMode(), RoutingSource: row.RoutingSource, Payload: row.Payload}
	requestID, _, err := admitRunForkActivitySourceIdentity(ctx, tx, a, source, fact)
	if err != nil {
		return err
	}
	if request.EffectClass == string(contracts.ActivityEffectClassReadOnly) {
		return nil
	}
	evidence, err := loadRunForkActivityAttemptEvidence(ctx, tx, requestID)
	if err != nil {
		return err
	}
	_, state, err := a.project(source)
	if err != nil {
		return err
	}
	if err := validateSourceActivityEvidence(evidence, request, state, requestID); err != nil {
		return err
	}
	if string(result.Type()) != evidence.ResultEventType || !workflowCommitJSONEqual(result.Payload(), evidence.ResultPayload) {
		return fmt.Errorf("physical activity result differs from its exact journal outcome")
	}
	_, selected := result.SelectedForkLineage()
	if !selected && !result.CreatedAt().Equal(evidence.CompletedAt) {
		return fmt.Errorf("published activity completion contradicts its journal timestamp")
	}
	return nil
}

func requireSelectedActivityResultRecipientsTx(ctx context.Context, tx *sql.Tx, binding runfork.RunForkSelectedContractBinding, sourceID string, routes []events.DeliveryRoute, postgres bool) error {
	// The exclusive creation cut cannot contain a physical activity result.
	if binding.ForkPoint.Kind == runfork.RunForkPointRunStart {
		return nil
	}
	snapshot, err := loadRunForkPointSnapshot(ctx, tx, binding.SourceRunID, binding.ForkPoint)
	if err != nil {
		return err
	}
	admission := runForkSourceStateAdmission{snapshot: snapshot, forkRunID: binding.ForkRunID, postgres: postgres}
	expected, found, err := admission.publishedActivityResultRoutes(ctx, tx, sourceID)
	if err != nil || !found {
		return err
	}
	if err := events.ValidateDeliveryRoutes(routes); err != nil {
		return err
	}
	if len(routes) != len(expected) {
		return fmt.Errorf("published activity continuation changed its unfinished recipient partition")
	}
	for _, want := range expected {
		matched := false
		for _, actual := range routes {
			matched = matched || events.SameDeliveryRouteIdentity(want, actual)
		}
		if !matched {
			return fmt.Errorf("published activity continuation substituted a recipient or return binding")
		}
	}
	return nil
}

func requireSelectedActivityResultPublishedTx(ctx context.Context, tx *sql.Tx, binding runfork.RunForkSelectedContractBinding, sourceID string, postgres bool) error {
	if binding.ForkPoint.Kind == runfork.RunForkPointRunStart {
		return nil
	}
	adapter := sqliteDeliveryAdapter
	if postgres {
		adapter = postgresDeliveryAdapter
	}
	childID := activityidentity.ForkLineageEventID(binding.ForkRunID, sourceID)
	deliveries, err := adapter.SnapshotsForEvent(ctx, tx, childID)
	if err != nil {
		return err
	}
	routes := make([]events.DeliveryRoute, 0, len(deliveries))
	for _, delivery := range deliveries {
		if delivery.RunID != binding.ForkRunID || delivery.EventID != childID {
			return fmt.Errorf("published activity readback has foreign child recipients")
		}
		routes = append(routes, delivery.Route)
	}
	return requireSelectedActivityResultRecipientsTx(ctx, tx, binding, sourceID, routes, postgres)
}

func requireSelectedActivityResultPublicationTx(ctx context.Context, tx *sql.Tx, lineage runfork.RunForkSelectedContractExecutionLineage, routes []events.DeliveryRoute, postgres bool) error {
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, lineage.ForkRunID)
	if err != nil {
		return err
	}
	if binding.SourceRunID != lineage.SourceRunID {
		return fmt.Errorf("published activity continuation has foreign source binding")
	}
	return requireSelectedActivityResultRecipientsTx(ctx, tx, binding, lineage.SourceEventID, routes, postgres)
}
