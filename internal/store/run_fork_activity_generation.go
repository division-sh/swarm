package store

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
)

const runForkActivityRequestEvent = "platform.activity_requested"

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

func prepareRunForkSelectedContractSourceEvent(ctx context.Context, tx *sql.Tx, forkRunID string, event RunForkSelectedContractSourceEvent) (RunForkSelectedContractSourceEvent, error) {
	if tx == nil {
		return event, fmt.Errorf("selected-contract fork source preparation requires transaction")
	}
	generations, err := loadRunForkEntityGenerations(ctx, tx, forkRunID, event.EntityID)
	if err != nil {
		return event, err
	}
	payload, err := remintRunForkPayload(event.Payload, forkRunID, generations)
	if err != nil {
		return event, fmt.Errorf("remint selected-contract source event %s loop generation: %w", event.SourceEventID, err)
	}
	event.Payload = payload
	if strings.TrimSpace(event.EventName) != runForkActivityRequestEvent {
		return event, nil
	}
	payload, err = bindRunForkActivitySourceEvent(payload, forkRunID, event.SourceEventID)
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
		if err := copyRunForkActivityAttemptEvidence(ctx, tx, forkRunID, event.FlowInstance, request, generations, evidence); err != nil {
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
	if err := copyRunForkActivityAttemptEvidence(ctx, tx, forkRunID, event.FlowInstance, request, generations, evidence); err != nil {
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
		WHERE p.request_event_id = $1::uuid
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

func bindRunForkActivitySourceEvent(raw json.RawMessage, forkRunID, sourceRequestEventID string) (json.RawMessage, error) {
	payload := map[string]any{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	payload["source_event_id"] = activityidentity.ForkLineageEventID(forkRunID, sourceRequestEventID)
	return json.Marshal(payload)
}

func loadRunForkEntityGenerations(ctx context.Context, tx *sql.Tx, forkRunID, entityID string) ([]attemptgeneration.Generation, error) {
	if strings.TrimSpace(entityID) == "" {
		return nil, nil
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, `SELECT accumulator FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`, forkRunID, entityID).Scan(&raw); err != nil {
		return nil, fmt.Errorf("load fork-local loop state for entity %s: %w", entityID, err)
	}
	state := map[string]any{}
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, fmt.Errorf("decode fork-local loop state for entity %s: %w", entityID, err)
	}
	structured := make(map[string]any, len(state))
	for key, value := range state {
		if _, ok := value.(map[string]any); ok {
			structured[key] = value
		}
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(nil, structured)
	if err != nil {
		return nil, err
	}
	activations, err := loopruntime.List(carrier.StateBuckets)
	if err != nil {
		return nil, err
	}
	out := make([]attemptgeneration.Generation, 0, len(activations))
	for _, activation := range activations {
		out = append(out, activation.Generation())
	}
	return out, nil
}

func remintRunForkPayload(raw json.RawMessage, forkRunID string, generations []attemptgeneration.Generation) (json.RawMessage, error) {
	payload := map[string]any{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	for _, generation := range generations {
		generation = generation.Normalize()
		if generation.Valid() {
			if _, ok := payload[generation.RevisionField]; ok {
				payload[generation.RevisionField] = generation.RevisionID
			}
		}
	}
	if encoded, ok := payload["loop_generation"]; ok {
		rawGeneration, err := json.Marshal(encoded)
		if err != nil {
			return nil, err
		}
		var source attemptgeneration.Generation
		if err := json.Unmarshal(rawGeneration, &source); err != nil {
			return nil, err
		}
		for _, generation := range generations {
			if strings.TrimSpace(generation.LoopID) == strings.TrimSpace(source.LoopID) {
				payload["loop_generation"] = generation.Normalize()
				break
			}
		}
	}
	if _, ok := payload["source_run_id"]; ok {
		payload["source_run_id"] = strings.TrimSpace(forkRunID)
	}
	for _, field := range []string{"source_event_id", "parent_event_id"} {
		if sourceID, ok := payload[field].(string); ok && strings.TrimSpace(sourceID) != "" {
			payload[field] = activityidentity.ForkLineageEventID(forkRunID, sourceID)
		}
	}
	return json.Marshal(payload)
}

func loadRunForkActivityAttemptEvidence(ctx context.Context, tx *sql.Tx, requestEventID string) (runForkActivityAttemptEvidence, error) {
	var evidence runForkActivityAttemptEvidence
	var resultPayload, failure []byte
	var completedAt sql.NullTime
	err := tx.QueryRowContext(ctx, `
		SELECT status, execution_mode, COALESCE(result_event_type, ''), COALESCE(result_payload, '{}'::jsonb),
		       COALESCE(failure, 'null'::jsonb), input_hash, started_at, completed_at, updated_at
		FROM activity_attempts WHERE request_event_id = $1::uuid
	`, requestEventID).Scan(&evidence.Status, &evidence.ExecutionMode, &evidence.ResultEventType, &resultPayload, &failure, &evidence.InputHash, &evidence.StartedAt, &completedAt, &evidence.UpdatedAt)
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
	if !completedAt.Valid || strings.TrimSpace(evidence.ResultEventType) == "" {
		return evidence, fmt.Errorf("activity request %s recorded evidence is incomplete", requestEventID)
	}
	evidence.CompletedAt = completedAt.Time
	evidence.ResultPayload = append(json.RawMessage(nil), resultPayload...)
	evidence.Failure = append(json.RawMessage(nil), failure...)
	return evidence, nil
}

func copyRunForkActivityAttemptEvidence(ctx context.Context, tx *sql.Tx, forkRunID, flowInstance string, request runForkActivityRequestPayload, generations []attemptgeneration.Generation, evidence runForkActivityAttemptEvidence) error {
	if err := runtimeauthoractivity.Require(ctx); err != nil {
		return err
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
	fact := activityidentity.Fact{
		RunID: forkRunID, SourceEventID: request.SourceEventID, ParentEventID: request.ParentEventID,
		EntityID: request.EntityID, FlowID: request.FlowID, NodeID: request.NodeID,
		HandlerEventKey: request.HandlerEventKey, ActivityID: request.ActivityID, Tool: request.Tool,
		Attempt: request.Attempt, RevisionID: generation.RevisionID,
	}
	requestEventID := activityidentity.RequestEventID(fact)
	resultEventID := activityidentity.ResultEventID(fact, evidence.ResultEventType)
	resultPayload, err := remintRunForkPayload(evidence.ResultPayload, forkRunID, generationsForRunForkActivity(generation))
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
			$1::uuid, $2::uuid, $25, NULLIF($3, '')::uuid, NULLIF($4, '')::uuid, NULLIF($5, '')::uuid, NULLIF($24, ''),
			$6, $7, $8, $9, $10, 1, $11,
			$12, $13, $14::uuid, $15, $16::jsonb,
			$17::jsonb, $18, $19::jsonb, NULLIF($20, ''), NULL,
			$21, $22, $23
		) ON CONFLICT (request_event_id) DO NOTHING
	`, requestEventID, forkRunID, request.SourceEventID, request.ParentEventID, request.EntityID,
		request.NodeID, request.HandlerEventKey, request.ActivityID, request.Tool, request.EffectClass, evidence.Status,
		request.SuccessEvent, request.FailureEvent, resultEventID, evidence.ResultEventType, string(resultPayload),
		failure, evidence.InputHash, string(generationJSON), request.LoopStage,
		evidence.StartedAt, evidence.CompletedAt, evidence.UpdatedAt, strings.TrimSpace(flowInstance), evidence.ExecutionMode)
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
	if err := tx.QueryRowContext(ctx, `SELECT run_id::text, execution_mode, status, result_event_id::text, loop_generation FROM activity_attempts WHERE request_event_id = $1::uuid`, requestEventID).
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
	return runtimeauthoractivity.Record(ctx, runtimeauthoractivity.Draft{
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

func generationsForRunForkActivity(generation attemptgeneration.Generation) []attemptgeneration.Generation {
	if !generation.Valid() {
		return nil
	}
	return []attemptgeneration.Generation{generation}
}
