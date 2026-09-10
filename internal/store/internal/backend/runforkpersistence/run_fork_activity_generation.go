package runforkpersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const runForkActivityRequestEvent = "platform.activity_requested"

const RunForkActivityRequestEvent = runForkActivityRequestEvent

type runForkActivityRequestPayload struct {
	ActivityID      string                       `json:"activity_id"`
	Tool            string                       `json:"tool"`
	EffectClass     string                       `json:"effect_class"`
	SuccessEvent    string                       `json:"success_event"`
	FailureEvent    string                       `json:"failure_event"`
	ForkPolicy      string                       `json:"fork_policy"`
	EntityID        string                       `json:"entity_id"`
	NodeID          string                       `json:"node_id"`
	FlowID          string                       `json:"flow_id"`
	HandlerEventKey string                       `json:"handler_event_key"`
	SourceEventID   string                       `json:"source_event_id"`
	SourceRunID     string                       `json:"source_run_id"`
	ParentEventID   string                       `json:"parent_event_id"`
	Attempt         int                          `json:"attempt"`
	Generation      attemptgeneration.Generation `json:"loop_generation,omitempty"`
	LoopStage       string                       `json:"loop_stage,omitempty"`
}

type RunForkActivityRequestPayload = runForkActivityRequestPayload

type runForkActivityAttemptEvidence struct {
	SourceRequest   runForkActivityRequestPayload
	FlowInstance    string
	ResultEventID   string
	GenerationJSON  json.RawMessage
	Status          string
	ExecutionMode   executionmode.Mode
	ResultEventType string
	ResultPayload   json.RawMessage
	Failure         json.RawMessage
	InputHash       string
	StartedAt       time.Time
	CompletedAt     time.Time
	UpdatedAt       time.Time
}

type runForkSourceStateAdmission struct {
	carriage  semanticview.OriginalLoopCarriage
	forkRunID string
	snapshot  *runForkRevisionSnapshot
	postgres  bool
}

type runForkProjectedSourceState struct {
	runfork.EntityProjection
	entityType     string
	correspondence *loopruntime.ForkCorrespondence
}

func loadRunForkSourceStateAdmission(ctx context.Context, tx *sql.Tx, forkRunID string, validate runForkRevisionValidator, resolve runForkRevisionPointResolver, postgres bool) (runForkSourceStateAdmission, error) {
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, forkRunID)
	if err != nil {
		return runForkSourceStateAdmission{}, fmt.Errorf("load source-state fork binding: %w", err)
	}
	if err := validate(ctx, tx, binding.SourceRunID); err != nil {
		return runForkSourceStateAdmission{}, err
	}
	point, err := resolve(ctx, tx, binding.SourceRunID, binding.ForkEventID)
	if err != nil {
		return runForkSourceStateAdmission{}, err
	}
	snapshot, err := loadRunForkRevisionSnapshot(ctx, tx, binding.SourceRunID, point.Revision)
	if err != nil {
		return runForkSourceStateAdmission{}, err
	}
	if err := validateRunForkEntityMetadataOwners(snapshot); err != nil {
		return runForkSourceStateAdmission{}, err
	}
	return runForkSourceStateAdmission{forkRunID: forkRunID, snapshot: snapshot, postgres: postgres}, nil
}

// Presence comes only from the bound source snapshot. Neither a producer root
// coordinate nor a later child row can establish source-owned state at R.
func (a runForkSourceStateAdmission) project(event runfork.RunForkSelectedContractSourceEvent) (runfork.RunForkSelectedContractSourceEvent, *runForkProjectedSourceState, error) {
	if a.snapshot == nil || a.snapshot.Revision <= 0 || a.forkRunID == "" {
		return event, nil, fmt.Errorf("source event preparation requires fixed-revision source-state admission")
	}
	found := false
	for _, historical := range a.snapshot.Events {
		if historical.EventID == event.SourceEventID {
			if historical.RoutingSource != event.RoutingSource || historical.EventName != event.EventName {
				return event, nil, fmt.Errorf("source event %s disagrees with bound revision producer evidence", event.SourceEventID)
			}
			if !bytes.Equal(historical.Payload, event.Payload) {
				return event, nil, fmt.Errorf("source event %s payload disagrees with bound revision evidence", event.SourceEventID)
			}
			found = true
			break
		}
	}
	if !found {
		return event, nil, fmt.Errorf("source event %s is outside the bound fork revision", event.SourceEventID)
	}
	projected, err := runfork.ProjectSelectedContractSourceEvent(a.snapshot.RunID, a.forkRunID, event)
	if err != nil {
		return event, nil, err
	}
	entityID := event.RoutingSource.Route().EntityID
	for _, meta := range a.snapshot.EntityMetadata {
		if meta.EntityID != entityID {
			continue
		}
		metadata, message, ok := loadRunForkMaterializedEntitySnapshotMetadata(a.snapshot, runfork.RunForkEntityState{EntityID: entityID})
		if !ok {
			return event, nil, fmt.Errorf("source event state metadata: %s", message)
		}
		projection, err := runfork.ProjectEntityOwnership(a.snapshot.RunID, a.forkRunID, entityID, metadata.FlowInstance)
		if err != nil {
			return event, nil, err
		}
		route := projected.RoutingSource.Route()
		flowInstance := route.FlowInstance
		if projection.Fork.EntityID == a.forkRunID && flowInstance == "" {
			flowInstance = projection.Fork.FlowInstance
		}
		if route.EntityID != projection.Fork.EntityID || flowInstance != projection.Fork.FlowInstance {
			return event, nil, fmt.Errorf("source event %s producer disagrees with fixed-revision state owner", event.SourceEventID)
		}
		states, err := loadRunForkEntityStates(a.snapshot)
		if err != nil {
			return event, nil, err
		}
		var accumulator map[string]any
		for _, state := range states {
			if state.EntityID == entityID {
				accumulator = state.Accumulator
				break
			}
		}
		_, correspondence, err := projectRunForkAttemptGenerationState(accumulator, a.forkRunID, projection.Fork.EntityID)
		if err != nil {
			return event, nil, err
		}
		return projected, &runForkProjectedSourceState{EntityProjection: projection, entityType: metadata.EntityType, correspondence: correspondence}, nil
	}
	for _, mutation := range a.snapshot.EntityMutations {
		if mutation.EntityID == entityID {
			return event, nil, fmt.Errorf("source event %s has state mutations without fixed-revision metadata", event.SourceEventID)
		}
	}
	if event.EventName == runForkActivityRequestEvent {
		return event, nil, fmt.Errorf("activity request %s requires fixed-revision producer state", event.SourceEventID)
	}
	return projected, nil, nil
}

func prepareRunForkSelectedContractSourceEvent(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, admission runForkSourceStateAdmission, event runfork.RunForkSelectedContractSourceEvent) (runfork.RunForkSelectedContractSourceEvent, error) {
	if tx == nil {
		return event, fmt.Errorf("selected-contract fork source preparation requires transaction")
	}
	sourceEvent := event
	event, state, err := admission.project(event)
	if err != nil {
		return event, err
	}
	forkRunID := admission.forkRunID
	var actual []loopruntime.Activation
	var flowInstance string
	if state != nil {
		flowInstance = state.Fork.FlowInstance
		if err := requireSelectedContractWorkflowEntity(ctx, tx, admission.postgres, selectedContractWorkflowState{
			RunID: forkRunID, EntityID: state.Fork.EntityID, Route: state.Fork.FlowInstance, EntityType: state.entityType,
		}); err != nil {
			return event, fmt.Errorf("load fork-local loop state for entity %s: %w", state.Fork.EntityID, err)
		}
		actual, err = loadRunForkEntityActivations(ctx, tx, forkRunID, state.Fork.EntityID)
		if err != nil {
			return event, err
		}
	}
	if strings.TrimSpace(event.EventName) != runForkActivityRequestEvent {
		role, found, err := admission.eventRole(sourceEvent.SourceEventID)
		if err != nil {
			return event, err
		}
		if !found {
			event.Payload = append(json.RawMessage(nil), sourceEvent.Payload...)
			return event, nil
		}
		if state == nil {
			return event, fmt.Errorf("declared revision event requires exact source state")
		}
		payload, err := projectDeclaredForkPayload(sourceEvent.Payload, role, state.correspondence, actual)
		if err != nil {
			return event, fmt.Errorf("project original event %s: %w", event.SourceEventID, err)
		}
		event.Payload = payload
		return event, nil
	}
	var sourceRequest runForkActivityRequestPayload
	if err := json.Unmarshal(sourceEvent.Payload, &sourceRequest); err != nil {
		return event, err
	}
	if state == nil {
		return event, fmt.Errorf("activity request requires admitted source ownership")
	}
	owner, err := activityidentity.ParseOwnerKey(sourceRequest.NodeID)
	if err != nil {
		return event, err
	}
	sourceFact, err := runForkActivityFact(sourceRequest)
	if err != nil {
		return event, err
	}
	if activityidentity.RequestEventID(sourceFact) != event.SourceEventID {
		return event, fmt.Errorf("activity request identity contradicts its exact source coordinates")
	}
	flowID, err := admission.carriage.ExecutionFlow(sourceEvent.RoutingSource)
	if err != nil {
		return event, err
	}
	if sourceRequest.SourceRunID != admission.snapshot.RunID || sourceRequest.EntityID != state.Source.EntityID || sourceRequest.FlowID != flowID {
		return event, fmt.Errorf("activity source request disagrees with fixed-revision ownership")
	}
	var role semanticview.LoopRevisionRole
	var declared bool
	if node, isNode := owner.Node(); isNode {
		role, declared, err = admission.carriage.ResolveActivity(node, sourceRequest.HandlerEventKey, sourceRequest.ActivityID, sourceRequest.Tool)
	} else {
		role, declared, err = admission.carriage.ResolveEvent(sourceEvent.RoutingSource, sourceRequest.HandlerEventKey, identity.ExecutableNode{}, "")
	}
	if err != nil {
		return event, err
	}
	var reference *loopruntime.ForkChildReference
	if sourceRequest.Generation != (attemptgeneration.Generation{}) {
		if !declared || sourceRequest.Generation.FlowID != role.FlowID() || sourceRequest.Generation.LoopID != role.LoopID() || sourceRequest.Generation.RevisionField != role.RevisionField() {
			return event, fmt.Errorf("activity generation disagrees with its original declaration")
		}
		sourceRef, err := state.correspondence.AdmitSource(sourceRequest.Generation)
		if err != nil {
			return event, err
		}
		child, err := state.correspondence.Bind(sourceRef)
		if err != nil {
			return event, err
		}
		if err := state.correspondence.ValidateChild(child, actual); err != nil {
			return event, err
		}
		reference = &child
	} else if declared {
		return event, fmt.Errorf("loop activity request is missing its declared generation")
	}
	payload, err := bindRunForkActivitySourceEvent(event.Payload, forkRunID, event.SourceEventID, state.Fork.EntityID, reference)
	if err != nil {
		return event, fmt.Errorf("bind selected-contract activity request %s to fork-local frontier: %w", event.SourceEventID, err)
	}
	event.Payload = payload
	var request runForkActivityRequestPayload
	if err := json.Unmarshal(event.Payload, &request); err != nil {
		return event, fmt.Errorf("decode selected-contract activity request %s: %w", event.SourceEventID, err)
	}
	policy := runtimecontracts.ActivityForkPolicy(strings.TrimSpace(request.ForkPolicy))
	proposed, err := loadRunForkProposedEffectAuthority(ctx, tx, event.SourceEventID)
	if err != nil {
		return event, err
	}
	if proposed {
		if policy != runtimecontracts.ActivityForkRequireConfirmation {
			return event, fmt.Errorf("approved activity request %s must retain require_manual_confirmation fork policy", event.SourceEventID)
		}
		evidence, err := loadRunForkActivityAttemptEvidence(ctx, tx, event.SourceEventID)
		if err != nil {
			return event, fmt.Errorf("approved proposed effect %s cannot authorize a fork-local call: %w", event.SourceEventID, err)
		}
		if evidence.Status == "uncertain" {
			return event, fmt.Errorf("approved proposed effect %s has ambiguous dispatch evidence and cannot authorize a fork-local call", event.SourceEventID)
		}
		if evidence.ExecutionMode != event.ExecutionMode {
			return event, fmt.Errorf("approved proposed effect %s execution mode %q conflicts with source event mode %q", event.SourceEventID, evidence.ExecutionMode, event.ExecutionMode)
		}
		if err := validateSourceActivityEvidence(evidence, sourceRequest, state, event.SourceEventID); err != nil {
			return event, err
		}
		if err := copyRunForkActivityAttemptEvidence(ctx, tx, story, forkRunID, flowInstance, request, reference, evidence); err != nil {
			return event, err
		}
		return event, nil
	}
	switch policy {
	case runtimecontracts.ActivityForkReexecuteRead:
		if runtimecontracts.NormalizeActivityEffectClass(request.EffectClass) != runtimecontracts.ActivityEffectClassReadOnly {
			return event, fmt.Errorf("activity request %s declares reexecute_read for effect class %s", event.SourceEventID, request.EffectClass)
		}
		return event, nil
	case runtimecontracts.ActivityForkReuseRecordedResult:
		if runtimecontracts.NormalizeActivityEffectClass(request.EffectClass) != runtimecontracts.ActivityEffectClassNonIdempotentWrite {
			return event, fmt.Errorf("activity request %s declares reuse_recorded_result for effect class %s", event.SourceEventID, request.EffectClass)
		}
	default:
		return event, fmt.Errorf("activity request %s fork policy %q is not executable", event.SourceEventID, policy)
	}
	evidence, err := loadRunForkActivityAttemptEvidence(ctx, tx, event.SourceEventID)
	if err != nil {
		return event, err
	}
	if evidence.ExecutionMode != event.ExecutionMode {
		return event, fmt.Errorf("activity request %s execution mode %q conflicts with source event mode %q", event.SourceEventID, evidence.ExecutionMode, event.ExecutionMode)
	}
	if err := validateSourceActivityEvidence(evidence, sourceRequest, state, event.SourceEventID); err != nil {
		return event, err
	}
	if err := copyRunForkActivityAttemptEvidence(ctx, tx, story, forkRunID, flowInstance, request, reference, evidence); err != nil {
		return event, err
	}
	return event, nil
}

func loadRunForkProposedEffectAuthority(ctx context.Context, tx *sql.Tx, requestEventID string) (bool, error) {
	var status, verdict, state string
	err := tx.QueryRowContext(ctx, `
		SELECT c.status, COALESCE(c.verdict, ''), p.state
		FROM proposed_effect_continuations p
		JOIN decision_cards c ON c.card_id = p.card_id
		WHERE p.request_event_id = $1
	`, strings.TrimSpace(requestEventID)).Scan(&status, &verdict, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load proposed-effect fork authority for request %s: %w", requestEventID, err)
	}
	if status != decisioncard.StatusDecided || verdict != "approve" || state != decisioncard.ProposedEffectRequestReleased {
		return false, fmt.Errorf("approved proposed effect %s is not terminal fork evidence: card=%s verdict=%s continuation=%s", requestEventID, status, verdict, state)
	}
	return true, nil
}

func bindRunForkActivitySourceEvent(raw json.RawMessage, forkRunID, sourceRequestEventID, entityID string, reference *loopruntime.ForkChildReference) (json.RawMessage, error) {
	payload := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, fmt.Errorf("activity source payload must be an object")
	}
	if encoded, ok := payload["loop_generation"]; ok {
		var source, child attemptgeneration.Generation
		if reference != nil {
			source, child = reference.Source().Generation(), reference.Generation()
		}
		expected, err := json.Marshal(source)
		if err != nil {
			return nil, err
		}
		if !workflowCommitJSONEqual(encoded, expected) {
			return nil, fmt.Errorf("activity request generation shape contradicts admitted source")
		}
		payload["loop_generation"], err = json.Marshal(child)
		if err != nil {
			return nil, err
		}
	} else if reference != nil {
		return nil, fmt.Errorf("activity request omits admitted generation")
	}
	payload["source_run_id"], _ = json.Marshal(forkRunID)
	payload["entity_id"], _ = json.Marshal(entityID)
	var parentID string
	if json.Unmarshal(payload["parent_event_id"], &parentID) == nil && strings.TrimSpace(parentID) != "" {
		payload["parent_event_id"], _ = json.Marshal(activityidentity.ForkLineageEventID(forkRunID, parentID))
	}
	payload["source_event_id"], _ = json.Marshal(activityidentity.ForkLineageEventID(forkRunID, sourceRequestEventID))
	return json.Marshal(payload)
}
func loadRunForkEntityActivations(ctx context.Context, tx *sql.Tx, forkRunID, entityID string) ([]loopruntime.Activation, error) {
	if strings.TrimSpace(entityID) == "" {
		return nil, nil
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT accumulator FROM entity_state WHERE run_id = $1 AND entity_id = $2`, forkRunID, entityID).Scan(&raw); err != nil {
		return nil, fmt.Errorf("load fork-local loop state for entity %s: %w", entityID, err)
	}
	state := map[string]any{}
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, fmt.Errorf("decode fork-local loop state for entity %s: %w", entityID, err)
	}
	if state == nil {
		return nil, fmt.Errorf("fork-local loop state for entity %s must be an object", entityID)
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(nil, nil, nil, state)
	if err != nil {
		return nil, err
	}
	return loopruntime.List(carrier.StateBuckets)
}

func projectForkRevisionPayload(raw json.RawMessage, reference *loopruntime.ForkChildReference) (json.RawMessage, error) {
	if reference == nil {
		return append(json.RawMessage(nil), raw...), nil
	}
	source, child := reference.Source().Generation(), reference.Generation()
	if !source.Valid() || !child.Valid() {
		return nil, fmt.Errorf("revision projection requires admitted source and child references")
	}
	payload := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	var revision string
	if json.Unmarshal(payload[source.RevisionField], &revision) != nil || revision != source.RevisionID {
		return nil, fmt.Errorf("declared payload revision disagrees with admitted original generation")
	}
	payload[source.RevisionField], _ = json.Marshal(child.RevisionID)
	return json.Marshal(payload)
}

func projectDeclaredForkPayload(raw json.RawMessage, role semanticview.LoopRevisionRole, correspondence *loopruntime.ForkCorrespondence, actual []loopruntime.Activation) (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, err
	}
	var revision string
	if err := json.Unmarshal(fields[role.RevisionField()], &revision); err != nil {
		return nil, fmt.Errorf("declared revision field must be present text: %w", err)
	}
	source, err := role.AdmitRevision(correspondence, revision)
	if err != nil {
		return nil, err
	}
	child, err := correspondence.Bind(source)
	if err != nil {
		return nil, err
	}
	if err := correspondence.ValidateChild(child, actual); err != nil {
		return nil, err
	}
	return projectForkRevisionPayload(raw, &child)
}

func (a runForkSourceStateAdmission) eventRole(eventID string) (semanticview.LoopRevisionRole, bool, error) {
	for _, event := range a.snapshot.Events {
		if event.EventID != eventID {
			continue
		}
		var node identity.ExecutableNode
		var handler string
		if event.ProducedByType == string(events.EventProducerNode) {
			var err error
			node, err = identity.ParseExecutableNodeKey(event.ProducedBy)
			if err != nil {
				return semanticview.LoopRevisionRole{}, false, err
			}
			for _, parent := range a.snapshot.Events {
				if parent.EventID == event.SourceEventID {
					handler = parent.EventName
					break
				}
			}
			if handler == "" {
				return semanticview.LoopRevisionRole{}, false, fmt.Errorf("original producer lacks its fixed-revision input event")
			}
		}
		return a.carriage.ResolveEvent(event.RoutingSource, event.EventName, node, handler)
	}
	return semanticview.LoopRevisionRole{}, false, fmt.Errorf("original event is absent at the fork revision")
}

func loadRunForkActivityAttemptEvidence(ctx context.Context, tx *sql.Tx, requestEventID string) (runForkActivityAttemptEvidence, error) {
	var evidence runForkActivityAttemptEvidence
	var resultPayload, failure, generation []byte
	var startedRaw, completedRaw, updatedRaw any
	err := tx.QueryRowContext(ctx, `
		SELECT status, execution_mode, COALESCE(result_event_type, ''), COALESCE(result_payload, '{}'),
		       COALESCE(failure, 'null'), input_hash, started_at, completed_at, updated_at,
		       CAST(run_id AS TEXT), COALESCE(CAST(source_event_id AS TEXT), ''), COALESCE(CAST(parent_event_id AS TEXT), ''),
		       COALESCE(CAST(entity_id AS TEXT), ''), COALESCE(flow_instance, ''), node_id, handler_event_key,
		       activity_id, tool, effect_class, attempt, success_event, failure_event,
		       COALESCE(CAST(result_event_id AS TEXT), ''), loop_generation, COALESCE(loop_stage, '')
		FROM activity_attempts WHERE request_event_id = $1
	`, requestEventID).Scan(&evidence.Status, &evidence.ExecutionMode, &evidence.ResultEventType, &resultPayload, &failure, &evidence.InputHash, &startedRaw, &completedRaw, &updatedRaw,
		&evidence.SourceRequest.SourceRunID, &evidence.SourceRequest.SourceEventID, &evidence.SourceRequest.ParentEventID,
		&evidence.SourceRequest.EntityID, &evidence.FlowInstance, &evidence.SourceRequest.NodeID, &evidence.SourceRequest.HandlerEventKey,
		&evidence.SourceRequest.ActivityID, &evidence.SourceRequest.Tool, &evidence.SourceRequest.EffectClass, &evidence.SourceRequest.Attempt,
		&evidence.SourceRequest.SuccessEvent, &evidence.SourceRequest.FailureEvent, &evidence.ResultEventID, &generation, &evidence.SourceRequest.LoopStage)
	if err == sql.ErrNoRows {
		return evidence, fmt.Errorf("activity request %s has no recorded attempt evidence for fork reuse", requestEventID)
	}
	if err != nil {
		return evidence, fmt.Errorf("load activity request %s fork evidence: %w", requestEventID, err)
	}
	if evidence.Status != "succeeded" && evidence.Status != "failed" && evidence.Status != "uncertain" {
		return evidence, fmt.Errorf("activity request %s recorded evidence is not terminal: %s", requestEventID, evidence.Status)
	}
	if !evidence.ExecutionMode.Valid() {
		return evidence, fmt.Errorf("activity request %s recorded evidence has invalid execution mode %q", requestEventID, evidence.ExecutionMode)
	}
	if strings.TrimSpace(evidence.ResultEventType) == "" {
		return evidence, fmt.Errorf("activity request %s recorded evidence is incomplete", requestEventID)
	}
	for _, field := range []struct {
		name string
		raw  any
		dest *time.Time
	}{
		{"started_at", startedRaw, &evidence.StartedAt},
		{"completed_at", completedRaw, &evidence.CompletedAt},
		{"updated_at", updatedRaw, &evidence.UpdatedAt},
	} {
		value, present, err := sqliteTimeValue(field.raw)
		if err != nil {
			return evidence, fmt.Errorf("decode activity request %s recorded %s: %w", requestEventID, field.name, err)
		}
		if !present || value.IsZero() {
			return evidence, fmt.Errorf("activity request %s recorded evidence is incomplete: %s is required", requestEventID, field.name)
		}
		*field.dest = value
	}
	evidence.ResultPayload = append(json.RawMessage(nil), resultPayload...)
	evidence.GenerationJSON = append(json.RawMessage(nil), generation...)
	evidence.Failure = append(json.RawMessage(nil), failure...)
	return evidence, nil
}

func validateSourceActivityEvidence(e runForkActivityAttemptEvidence, request runForkActivityRequestPayload, state *runForkProjectedSourceState, requestID string) error {
	if state == nil {
		return fmt.Errorf("activity journal requires exact source state ownership")
	}
	want := request
	// These coordinates are not columns in the attempt journal. Their owners
	// remain the admitted request declaration and source entity projection.
	want.FlowID, want.ForkPolicy, want.Generation = "", "", attemptgeneration.Generation{}
	if e.SourceRequest != want || e.FlowInstance != state.Source.FlowInstance {
		return fmt.Errorf("source activity journal contradicts exact request ownership")
	}
	generationJSON, err := json.Marshal(request.Generation)
	if err != nil {
		return err
	}
	if !workflowCommitJSONEqual(e.GenerationJSON, generationJSON) {
		return fmt.Errorf("source activity journal contradicts exact request generation")
	}
	fact, err := runForkActivityFact(request)
	if err != nil {
		return err
	}
	if activityidentity.RequestEventID(fact) != requestID || activityidentity.ResultEventID(fact, e.ResultEventType) != e.ResultEventID {
		return fmt.Errorf("source activity journal request/result identity contradicts its ownership")
	}
	wantResult := request.FailureEvent
	if e.Status == "succeeded" {
		wantResult = request.SuccessEvent
	}
	if e.ResultEventType != wantResult {
		return fmt.Errorf("source activity journal result contradicts declared outcome")
	}
	return nil
}

func runForkActivityFact(request runForkActivityRequestPayload) (activityidentity.Fact, error) {
	owner, err := activityidentity.ParseOwnerKey(request.NodeID)
	if err != nil {
		return activityidentity.Fact{}, err
	}
	if request.Attempt <= 0 {
		return activityidentity.Fact{}, fmt.Errorf("activity request attempt must be positive")
	}
	return activityidentity.Fact{RunID: request.SourceRunID, SourceEventID: request.SourceEventID, ParentEventID: request.ParentEventID,
		EntityID: request.EntityID, Owner: owner, ExecutionFlowID: request.FlowID, HandlerEventKey: request.HandlerEventKey,
		ActivityID: request.ActivityID, Tool: request.Tool, Attempt: request.Attempt, RevisionID: request.Generation.RevisionID}, nil
}

func copyRunForkActivityAttemptEvidence(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, forkRunID, flowInstance string, request runForkActivityRequestPayload, reference *loopruntime.ForkChildReference, evidence runForkActivityAttemptEvidence) error {
	if story == nil {
		return fmt.Errorf("run fork activity evidence requires private story ownership")
	}
	generation := request.Generation
	var expected attemptgeneration.Generation
	if reference != nil {
		expected = reference.Generation()
	}
	if generation != expected {
		return fmt.Errorf("copied activity generation disagrees with admitted correspondence")
	}
	fact, err := runForkActivityFact(request)
	if err != nil {
		return fmt.Errorf("fork activity %s owner identity: %w", request.ActivityID, err)
	}
	if fact.RunID != forkRunID {
		return fmt.Errorf("copied activity request does not belong to the fork")
	}
	requestEventID := activityidentity.RequestEventID(fact)
	resultEventID := activityidentity.ResultEventID(fact, evidence.ResultEventType)
	resultPayload, err := projectForkRevisionPayload(evidence.ResultPayload, reference)
	if err != nil {
		return fmt.Errorf("remint activity %s recorded result: %w", request.ActivityID, err)
	}
	generationJSON, err := json.Marshal(generation)
	if err != nil {
		return err
	}
	var failure any
	if raw := strings.TrimSpace(string(evidence.Failure)); raw != "" && raw != "null" {
		failure = raw
	}
	result, err := tx.ExecContext(ctx, `
		INSERT INTO activity_attempts (
			request_event_id, run_id, execution_mode, source_event_id, parent_event_id, entity_id, flow_instance,
			node_id, handler_event_key, activity_id, tool, effect_class, attempt, status,
			success_event, failure_event, result_event_id, result_event_type, result_payload,
			failure, input_hash, loop_generation, loop_stage, reply_context_id,
			started_at, completed_at, updated_at
		) VALUES (
			$1, $2, $25, $3, $4, $5, $24,
			$6, $7, $8, $9, $10, 1, $11,
			$12, $13, $14, $15, $16,
			$17, $18, $19, $20, NULL,
			$21, $22, $23
		) ON CONFLICT (request_event_id) DO NOTHING
	`, requestEventID, forkRunID, nullableRunForkString(request.SourceEventID), nullableRunForkString(request.ParentEventID), nullableRunForkString(request.EntityID),
		request.NodeID, request.HandlerEventKey, request.ActivityID, request.Tool, request.EffectClass, evidence.Status,
		request.SuccessEvent, request.FailureEvent, resultEventID, evidence.ResultEventType, string(resultPayload),
		failure, evidence.InputHash, string(generationJSON), nullableRunForkString(request.LoopStage),
		evidence.StartedAt.UTC().Format(time.RFC3339Nano), evidence.CompletedAt.UTC().Format(time.RFC3339Nano), evidence.UpdatedAt.UTC().Format(time.RFC3339Nano), nullableRunForkString(flowInstance), evidence.ExecutionMode)
	if err != nil {
		return fmt.Errorf("copy fork-local activity evidence %s: %w", request.ActivityID, err)
	}
	inserted, err := rowsAffected(result)
	if err != nil {
		return err
	}
	var gotRunID, gotStatus, gotResultID string
	var gotExecutionMode executionmode.Mode
	var gotRequest runForkActivityRequestPayload
	var gotFlowInstance, gotResultType, gotInputHash, gotReplyContext string
	var gotGeneration, gotResultPayload, gotFailure []byte
	var gotStarted, gotCompleted, gotUpdated any
	if err := tx.QueryRowContext(ctx, `
		SELECT CAST(run_id AS TEXT), execution_mode, status, CAST(result_event_id AS TEXT), loop_generation,
		       COALESCE(CAST(source_event_id AS TEXT), ''), COALESCE(CAST(parent_event_id AS TEXT), ''),
		       COALESCE(CAST(entity_id AS TEXT), ''), COALESCE(flow_instance, ''), node_id, handler_event_key,
		       activity_id, tool, effect_class, attempt, success_event, failure_event, result_event_type,
		       result_payload, COALESCE(failure, 'null'), input_hash, COALESCE(loop_stage, ''),
		       COALESCE(CAST(reply_context_id AS TEXT), ''), started_at, completed_at, updated_at
		FROM activity_attempts WHERE request_event_id = $1`, requestEventID).
		Scan(&gotRunID, &gotExecutionMode, &gotStatus, &gotResultID, &gotGeneration,
			&gotRequest.SourceEventID, &gotRequest.ParentEventID, &gotRequest.EntityID, &gotFlowInstance,
			&gotRequest.NodeID, &gotRequest.HandlerEventKey, &gotRequest.ActivityID, &gotRequest.Tool,
			&gotRequest.EffectClass, &gotRequest.Attempt, &gotRequest.SuccessEvent, &gotRequest.FailureEvent,
			&gotResultType, &gotResultPayload, &gotFailure, &gotInputHash, &gotRequest.LoopStage,
			&gotReplyContext, &gotStarted, &gotCompleted, &gotUpdated); err != nil {
		return fmt.Errorf("confirm fork-local activity evidence %s: %w", request.ActivityID, err)
	}
	var got attemptgeneration.Generation
	if err := json.Unmarshal(gotGeneration, &got); err != nil {
		return err
	}
	// An idempotency key proves which row to inspect, not that its evidence agrees.
	// Compare raw generation coordinates; malformed partial evidence is not no-loop.
	if gotRunID != forkRunID || gotExecutionMode != evidence.ExecutionMode || gotStatus != evidence.Status || gotResultID != resultEventID || got != generation ||
		gotRequest.SourceEventID != request.SourceEventID || gotRequest.ParentEventID != request.ParentEventID ||
		gotRequest.EntityID != request.EntityID || gotFlowInstance != flowInstance || gotRequest.NodeID != request.NodeID ||
		gotRequest.HandlerEventKey != request.HandlerEventKey || gotRequest.ActivityID != request.ActivityID ||
		gotRequest.Tool != request.Tool || gotRequest.EffectClass != request.EffectClass || gotRequest.Attempt != 1 ||
		gotRequest.SuccessEvent != request.SuccessEvent || gotRequest.FailureEvent != request.FailureEvent ||
		gotResultType != evidence.ResultEventType || gotInputHash != evidence.InputHash || gotRequest.LoopStage != request.LoopStage ||
		gotReplyContext != "" || !workflowCommitJSONEqual(gotGeneration, generationJSON) || !workflowCommitJSONEqual(gotResultPayload, resultPayload) {
		return fmt.Errorf("fork-local activity evidence %s conflicts with canonical fork identity", request.ActivityID)
	}
	expectedFailure := evidence.Failure
	if len(strings.TrimSpace(string(expectedFailure))) == 0 {
		expectedFailure = json.RawMessage("null")
	}
	if !workflowCommitJSONEqual(gotFailure, expectedFailure) {
		return fmt.Errorf("fork-local activity evidence %s conflicts with canonical fork failure", request.ActivityID)
	}
	for _, stamp := range []struct {
		raw  any
		want time.Time
	}{
		{gotStarted, evidence.StartedAt}, {gotCompleted, evidence.CompletedAt}, {gotUpdated, evidence.UpdatedAt},
	} {
		actual, present, err := sqliteTimeValue(stamp.raw)
		if err != nil || !present || !actual.Equal(stamp.want) {
			return fmt.Errorf("fork-local activity evidence %s conflicts with canonical fork timestamps", request.ActivityID)
		}
	}
	if !inserted {
		return nil
	}
	var canonicalFailure *runtimefailures.Envelope
	if raw := strings.TrimSpace(string(evidence.Failure)); raw != "" && raw != "null" {
		var decoded runtimefailures.Envelope
		if err := json.Unmarshal(evidence.Failure, &decoded); err != nil {
			return fmt.Errorf("decode fork-local activity failure %s: %w", request.ActivityID, err)
		}
		canonicalFailure = &decoded
	}
	attempt := 1
	return story.Record(ctx, runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindActivityLifecycle, Transition: evidence.Status,
		SourceOwner: "activity_attempts", SourceIdentity: requestEventID + ":" + evidence.Status,
		DedupKey:   "activity:" + requestEventID + ":" + evidence.Status,
		OccurredAt: evidence.CompletedAt.UTC(), RunID: forkRunID, EntityID: request.EntityID, FlowID: strings.TrimSpace(flowInstance),
		Projection: runtimeauthoractivity.Projection{
			SubjectType: "activity", SubjectID: request.ActivityID, NodeID: request.NodeID, Activity: request.ActivityID,
			Tool: request.Tool, EffectClass: request.EffectClass, Attempt: &attempt, EventType: evidence.ResultEventType, ExecutionMode: string(evidence.ExecutionMode),
		},
		Failure: canonicalFailure,
	})
}

func nullableRunForkString(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}
