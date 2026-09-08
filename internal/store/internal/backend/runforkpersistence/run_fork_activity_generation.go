package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
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
	forkRunID string
	snapshot  *runForkRevisionSnapshot
	postgres  bool
}

type runForkProjectedSourceState struct {
	runfork.EntityProjection
	entityType string
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
		return projected, &runForkProjectedSourceState{EntityProjection: projection, entityType: metadata.EntityType}, nil
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
	event, state, err := admission.project(event)
	if err != nil {
		return event, err
	}
	forkRunID := admission.forkRunID
	var generations []attemptgeneration.Generation
	var flowInstance string
	if state != nil {
		flowInstance = state.Fork.FlowInstance
		if err := requireSelectedContractWorkflowEntity(ctx, tx, admission.postgres, selectedContractWorkflowState{
			RunID: forkRunID, EntityID: state.Fork.EntityID, Route: state.Fork.FlowInstance, EntityType: state.entityType,
		}); err != nil {
			return event, fmt.Errorf("load fork-local loop state for entity %s: %w", state.Fork.EntityID, err)
		}
		generations, err = loadRunForkEntityGenerations(ctx, tx, forkRunID, state.Fork.EntityID)
		if err != nil {
			return event, err
		}
	}
	payload, err := remintRunForkPayload(event.Payload, generations)
	if err != nil {
		return event, fmt.Errorf("remint selected-contract source event %s loop generation: %w", event.SourceEventID, err)
	}
	event.Payload = payload
	if strings.TrimSpace(event.EventName) != runForkActivityRequestEvent {
		return event, nil
	}
	payload, err = bindRunForkActivitySourceEvent(payload, forkRunID, event.SourceEventID, generations)
	if err != nil {
		return event, fmt.Errorf("bind selected-contract activity request %s to fork-local frontier: %w", event.SourceEventID, err)
	}
	event.Payload = payload
	var request runForkActivityRequestPayload
	if err := json.Unmarshal(payload, &request); err != nil {
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
		if err := copyRunForkActivityAttemptEvidence(ctx, tx, story, forkRunID, flowInstance, request, generations, evidence); err != nil {
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
	if err := copyRunForkActivityAttemptEvidence(ctx, tx, story, forkRunID, flowInstance, request, generations, evidence); err != nil {
		return event, err
	}
	return event, nil
}

func PrepareRunForkSelectedContractSourceEvent(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, forkRunID string, event runfork.RunForkSelectedContractSourceEvent) (runfork.RunForkSelectedContractSourceEvent, error) {
	admission, err := loadRunForkSourceStateAdmission(ctx, tx, forkRunID, runforkrevision.ValidateCompletePostgres, resolveRunForkRevisionPoint, true)
	if err != nil {
		return event, err
	}
	return prepareRunForkSelectedContractSourceEvent(ctx, tx, story, admission, event)
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

func bindRunForkActivitySourceEvent(raw json.RawMessage, forkRunID, sourceRequestEventID string, generations []attemptgeneration.Generation) (json.RawMessage, error) {
	payload := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	if payload == nil {
		return nil, fmt.Errorf("activity source payload must be an object")
	}
	if encoded, ok := payload["loop_generation"]; ok {
		var source attemptgeneration.Generation
		if err := json.Unmarshal(encoded, &source); err != nil {
			return nil, err
		}
		for _, generation := range generations {
			if strings.TrimSpace(generation.LoopID) == strings.TrimSpace(source.LoopID) {
				if normalized := generation.Normalize(); source.Normalize() != normalized {
					payload["loop_generation"], _ = json.Marshal(normalized)
				}
				break
			}
		}
	}
	if _, ok := payload["source_run_id"]; ok {
		payload["source_run_id"], _ = json.Marshal(strings.TrimSpace(forkRunID))
	}
	var parentID string
	if json.Unmarshal(payload["parent_event_id"], &parentID) == nil && strings.TrimSpace(parentID) != "" {
		payload["parent_event_id"], _ = json.Marshal(activityidentity.ForkLineageEventID(forkRunID, parentID))
	}
	payload["source_event_id"], _ = json.Marshal(activityidentity.ForkLineageEventID(forkRunID, sourceRequestEventID))
	return json.Marshal(payload)
}

func loadRunForkEntityGenerations(ctx context.Context, tx *sql.Tx, forkRunID, entityID string) ([]attemptgeneration.Generation, error) {
	activations, err := loadRunForkEntityActivations(ctx, tx, forkRunID, entityID)
	if err != nil {
		return nil, err
	}
	out := make([]attemptgeneration.Generation, 0, len(activations))
	for _, activation := range activations {
		out = append(out, activation.Generation())
	}
	return out, nil
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

func remintRunForkPayload(raw json.RawMessage, generations []attemptgeneration.Generation) (json.RawMessage, error) {
	payload := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	changed := false
	for _, generation := range generations {
		generation = generation.Normalize()
		if generation.Valid() {
			if encoded, ok := payload[generation.RevisionField]; ok {
				var current string
				if json.Unmarshal(encoded, &current) != nil || current != generation.RevisionID {
					payload[generation.RevisionField], _ = json.Marshal(generation.RevisionID)
					changed = true
				}
			}
		}
	}
	if !changed {
		return append(json.RawMessage(nil), raw...), nil
	}
	return json.Marshal(payload)
}

func loadRunForkActivityAttemptEvidence(ctx context.Context, tx *sql.Tx, requestEventID string) (runForkActivityAttemptEvidence, error) {
	var evidence runForkActivityAttemptEvidence
	var resultPayload, failure []byte
	var startedRaw, completedRaw, updatedRaw any
	err := tx.QueryRowContext(ctx, `
		SELECT status, execution_mode, COALESCE(result_event_type, ''), COALESCE(result_payload, '{}'),
		       COALESCE(failure, 'null'), input_hash, started_at, completed_at, updated_at
		FROM activity_attempts WHERE request_event_id = $1
	`, requestEventID).Scan(&evidence.Status, &evidence.ExecutionMode, &evidence.ResultEventType, &resultPayload, &failure, &evidence.InputHash, &startedRaw, &completedRaw, &updatedRaw)
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
	evidence.Failure = append(json.RawMessage(nil), failure...)
	return evidence, nil
}

func copyRunForkActivityAttemptEvidence(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, forkRunID, flowInstance string, request runForkActivityRequestPayload, generations []attemptgeneration.Generation, evidence runForkActivityAttemptEvidence) error {
	if story == nil {
		return fmt.Errorf("run fork activity evidence requires private story ownership")
	}
	generation := request.Generation.Normalize()
	for _, candidate := range generations {
		if strings.TrimSpace(candidate.LoopID) == strings.TrimSpace(generation.LoopID) {
			generation = candidate.Normalize()
			break
		}
	}
	if request.Attempt <= 0 {
		request.Attempt = 1
	}
	owner, err := activityidentity.ParseOwnerKey(request.NodeID)
	if err != nil {
		return fmt.Errorf("fork activity %s owner identity: %w", request.ActivityID, err)
	}
	fact := activityidentity.Fact{
		RunID: forkRunID, SourceEventID: request.SourceEventID, ParentEventID: request.ParentEventID,
		EntityID: request.EntityID, Owner: owner, ExecutionFlowID: request.FlowID,
		HandlerEventKey: request.HandlerEventKey, ActivityID: request.ActivityID, Tool: request.Tool,
		Attempt: request.Attempt, RevisionID: generation.RevisionID,
	}
	requestEventID := activityidentity.RequestEventID(fact)
	resultEventID := activityidentity.ResultEventID(fact, evidence.ResultEventType)
	resultPayload, err := remintRunForkPayload(evidence.ResultPayload, generationsForRunForkActivity(generation))
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
		evidence.StartedAt, evidence.CompletedAt, evidence.UpdatedAt, nullableRunForkString(flowInstance), evidence.ExecutionMode)
	if err != nil {
		return fmt.Errorf("copy fork-local activity evidence %s: %w", request.ActivityID, err)
	}
	inserted, err := rowsAffected(result)
	if err != nil {
		return err
	}
	var gotRunID, gotStatus, gotResultID string
	var gotExecutionMode executionmode.Mode
	var gotGeneration []byte
	if err := tx.QueryRowContext(ctx, `SELECT CAST(run_id AS TEXT), execution_mode, status, CAST(result_event_id AS TEXT), loop_generation FROM activity_attempts WHERE request_event_id = $1`, requestEventID).
		Scan(&gotRunID, &gotExecutionMode, &gotStatus, &gotResultID, &gotGeneration); err != nil {
		return fmt.Errorf("confirm fork-local activity evidence %s: %w", request.ActivityID, err)
	}
	var got attemptgeneration.Generation
	if err := json.Unmarshal(gotGeneration, &got); err != nil {
		return err
	}
	generationMatches := (!got.Valid() && !generation.Valid()) || got.Equal(generation)
	if gotRunID != forkRunID || gotExecutionMode != evidence.ExecutionMode || gotStatus != evidence.Status || gotResultID != resultEventID || !generationMatches {
		return fmt.Errorf("fork-local activity evidence %s conflicts with canonical fork identity", request.ActivityID)
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

func generationsForRunForkActivity(generation attemptgeneration.Generation) []attemptgeneration.Generation {
	if !generation.Valid() {
		return nil
	}
	return []attemptgeneration.Generation{generation}
}
