package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
)

var _ decisioncard.ProposedEffectStore = (*PostgresStore)(nil)
var _ decisioncard.ProposedEffectStore = (*SQLiteRuntimeStore)(nil)

func (s *PostgresStore) CreateProposedEffectCard(ctx context.Context, card decisioncard.Card, continuation decisioncard.ProposedEffectContinuation) error {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return insertProposedEffectCard(ctx, tx, card, continuation, true)
	}
	return runPostgresDecisionCardMutation(ctx, s.DB, func(txctx context.Context, tx *sql.Tx) error {
		return insertProposedEffectCard(txctx, tx, card, continuation, true)
	})
}

func (s *SQLiteRuntimeStore) CreateProposedEffectCard(ctx context.Context, card decisioncard.Card, continuation decisioncard.ProposedEffectContinuation) error {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return insertProposedEffectCard(ctx, tx, card, continuation, false)
	}
	return s.runDecisionCardMutation(ctx, "sqlite create proposed-effect card", func(txctx context.Context, tx *sql.Tx) error {
		return insertProposedEffectCard(txctx, tx, card, continuation, false)
	})
}

func insertProposedEffectCard(ctx context.Context, tx *sql.Tx, card decisioncard.Card, continuation decisioncard.ProposedEffectContinuation, postgres bool) error {
	continuation = continuation.Canonical()
	if err := continuation.Validate(card); err != nil {
		return err
	}
	if err := insertDecisionCard(ctx, tx, card, postgres); err != nil {
		return err
	}
	effect, err := continuation.EffectValue()
	if err != nil {
		return err
	}
	rawEffect, err := canonicaljson.Encode(effect)
	if err != nil {
		return err
	}
	query := `INSERT INTO proposed_effect_continuations (
		card_id, run_id, request_event_id, effect, effect_content_hash, state,
		created_at, updated_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (card_id) DO NOTHING`
	if postgres {
		query = `INSERT INTO proposed_effect_continuations (
			card_id, run_id, request_event_id, effect, effect_content_hash, state,
			created_at, updated_at
		) VALUES ($1, $2::uuid, $3::uuid, $4::jsonb, $5, $6, $7, $8) ON CONFLICT (card_id) DO NOTHING`
	}
	result, err := tx.ExecContext(ctx, query, continuation.CardID, continuation.RunID, continuation.RequestEventID,
		string(rawEffect), continuation.EffectContentHash, continuation.State, continuation.CreatedAt, continuation.UpdatedAt)
	if err != nil {
		return fmt.Errorf("create proposed-effect continuation: %w", err)
	}
	if rows, _ := result.RowsAffected(); rows == 0 {
		existing, loadErr := loadProposedEffectContinuation(ctx, tx, card.CardID, postgres, false)
		if loadErr != nil {
			return loadErr
		}
		if existing.RequestEventID != continuation.RequestEventID || existing.BundleHash != continuation.BundleHash ||
			existing.WorkflowVersion != continuation.WorkflowVersion || existing.EffectContentHash != continuation.EffectContentHash || !existing.Input.Equal(continuation.Input) {
			return runtimefailures.New(runtimefailures.ClassConflictingDuplicate, "proposed_effect_identity_conflict", "proposed-effect-store", "create", map[string]any{
				"card_id": card.CardID, "request_event_id": continuation.RequestEventID,
			})
		}
	}
	return nil
}

func (s *PostgresStore) LoadProposedEffectContinuation(ctx context.Context, cardID string) (decisioncard.ProposedEffectContinuation, error) {
	db := decisionCardSQL(s.DB)
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		db = tx
	}
	return loadProposedEffectContinuation(ctx, db, cardID, true, false)
}

func (s *SQLiteRuntimeStore) LoadProposedEffectContinuation(ctx context.Context, cardID string) (decisioncard.ProposedEffectContinuation, error) {
	db := decisionCardSQL(s.DB)
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		db = tx
	}
	return loadProposedEffectContinuation(ctx, db, cardID, false, false)
}

func (s *PostgresStore) ProposedEffectReadback(ctx context.Context, cardID string) (decisioncard.ProposedEffectReadback, error) {
	db := decisionCardSQL(s.DB)
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		db = tx
	}
	return proposedEffectReadback(ctx, db, cardID, true)
}

func (s *SQLiteRuntimeStore) ProposedEffectReadback(ctx context.Context, cardID string) (decisioncard.ProposedEffectReadback, error) {
	db := decisionCardSQL(s.DB)
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		db = tx
	}
	return proposedEffectReadback(ctx, db, cardID, false)
}

func proposedEffectReadback(ctx context.Context, db decisionCardSQL, cardID string, postgres bool) (decisioncard.ProposedEffectReadback, error) {
	continuation, err := loadProposedEffectContinuation(ctx, db, cardID, postgres, false)
	if err != nil {
		return decisioncard.ProposedEffectReadback{}, err
	}
	dispatchState := "held"
	switch continuation.State {
	case decisioncard.ProposedEffectDecisionCommitted:
		dispatchState = "release_pending"
	case decisioncard.ProposedEffectRequestReleased:
		dispatchState = "released"
	case decisioncard.ProposedEffectOutcomeDispatched:
		dispatchState = "not_dispatched"
	case decisioncard.ProposedEffectSuperseded:
		dispatchState = "superseded"
	}
	query := `SELECT status FROM activity_attempts WHERE request_event_id = ?`
	if postgres {
		query = `SELECT status FROM activity_attempts WHERE request_event_id = $1::uuid`
	}
	var attemptState string
	if err := db.QueryRowContext(ctx, query, continuation.RequestEventID).Scan(&attemptState); err == nil {
		dispatchState = strings.TrimSpace(attemptState)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return decisioncard.ProposedEffectReadback{}, err
	}
	return decisioncard.ProposedEffectReadback{
		ContinuationState: continuation.State, DispatchState: dispatchState,
		RequestEventID: continuation.RequestEventID, ActivityID: continuation.ActivityID,
	}, nil
}

func loadProposedEffectContinuation(ctx context.Context, db decisionCardSQL, cardID string, postgres, forUpdate bool) (decisioncard.ProposedEffectContinuation, error) {
	query := `SELECT card_id, run_id, request_event_id, effect, effect_content_hash, state,
		COALESCE(verdict, ''), COALESCE(CAST(decision_event_id AS TEXT), ''), COALESCE(CAST(route_event_id AS TEXT), ''),
		COALESCE(superseded_reason, ''), created_at, updated_at
		FROM proposed_effect_continuations WHERE card_id = ?`
	if postgres {
		query = strings.Replace(query, "?", "$1", 1)
		if forUpdate {
			query += ` FOR UPDATE`
		}
	}
	var out decisioncard.ProposedEffectContinuation
	var effect []byte
	var created, updated any
	err := db.QueryRowContext(ctx, query, strings.TrimSpace(cardID)).Scan(
		&out.CardID, &out.RunID, &out.RequestEventID, &effect, &out.EffectContentHash, &out.State,
		&out.Verdict, &out.DecisionEventID, &out.RouteEventID, &out.SupersededReason, &created, &updated,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return decisioncard.ProposedEffectContinuation{}, decisioncard.ErrNotFound
	}
	if err != nil {
		return decisioncard.ProposedEffectContinuation{}, err
	}
	if at, ok, parseErr := sqliteTimeValue(created); parseErr != nil || !ok {
		return decisioncard.ProposedEffectContinuation{}, fmt.Errorf("decode proposed-effect created_at: %w", parseErr)
	} else {
		out.CreatedAt = at
	}
	if at, ok, parseErr := sqliteTimeValue(updated); parseErr != nil || !ok {
		return decisioncard.ProposedEffectContinuation{}, fmt.Errorf("decode proposed-effect updated_at: %w", parseErr)
	} else {
		out.UpdatedAt = at
	}
	value, err := canonicaljson.Decode(effect)
	if err != nil {
		return decisioncard.ProposedEffectContinuation{}, fmt.Errorf("decode proposed effect: %w", err)
	}
	if err := projectProposedEffect(value, &out); err != nil {
		return decisioncard.ProposedEffectContinuation{}, err
	}
	return out.Canonical(), nil
}

type proposedEffectProjection struct {
	RequestEventID   string                       `json:"request_event_id"`
	ActivityID       string                       `json:"activity_id"`
	Tool             string                       `json:"tool"`
	BundleHash       string                       `json:"bundle_hash"`
	WorkflowVersion  string                       `json:"workflow_version"`
	EffectClass      string                       `json:"effect_class"`
	SuccessEvent     string                       `json:"success_event"`
	FailureEvent     string                       `json:"failure_event"`
	RevisionEvent    string                       `json:"revision_event"`
	RejectedEvent    string                       `json:"rejected_event"`
	RetryMaxAttempts int                          `json:"retry_max_attempts"`
	RetryBackoff     string                       `json:"retry_backoff"`
	ForkPolicy       string                       `json:"fork_policy"`
	EntityID         string                       `json:"entity_id"`
	NodeID           string                       `json:"node_id"`
	FlowID           string                       `json:"flow_id"`
	FlowInstance     string                       `json:"flow_instance"`
	HandlerEventKey  string                       `json:"handler_event_key"`
	SourceEventID    string                       `json:"source_event_id"`
	SourceRunID      string                       `json:"source_run_id"`
	SourceTaskID     string                       `json:"source_task_id"`
	ParentEventID    string                       `json:"parent_event_id"`
	ChainDepth       int                          `json:"chain_depth"`
	Attempt          int                          `json:"attempt"`
	Generation       attemptgeneration.Generation `json:"loop_generation"`
	LoopStage        string                       `json:"loop_stage"`
	ExecutionMode    executionmode.Mode           `json:"execution_mode"`
	ReplyContextID   string                       `json:"reply_context_id"`
}

func projectProposedEffect(value semanticvalue.Value, out *decisioncard.ProposedEffectContinuation) error {
	if out == nil {
		return fmt.Errorf("proposed-effect projection destination is nil")
	}
	input, ok := value.Lookup("input")
	if !ok || input.Kind() != semanticvalue.KindObject {
		return fmt.Errorf("proposed-effect input must be a semantic object")
	}
	var dto proposedEffectProjection
	if err := canonicaljson.ValueInto(value, &dto); err != nil {
		return fmt.Errorf("project proposed effect: %w", err)
	}
	out.RequestEventID = dto.RequestEventID
	out.ActivityID = dto.ActivityID
	out.Tool = dto.Tool
	out.BundleHash = dto.BundleHash
	out.WorkflowVersion = dto.WorkflowVersion
	out.Input = input
	out.EffectClass = runtimecontracts.NormalizeActivityEffectClass(dto.EffectClass)
	out.SuccessEvent = dto.SuccessEvent
	out.FailureEvent = dto.FailureEvent
	out.RevisionEvent = dto.RevisionEvent
	out.RejectedEvent = dto.RejectedEvent
	out.RetryMaxAttempts = dto.RetryMaxAttempts
	out.RetryBackoff = dto.RetryBackoff
	out.ForkPolicy = runtimecontracts.ActivityForkPolicy(dto.ForkPolicy)
	out.EntityID = dto.EntityID
	out.NodeID = dto.NodeID
	out.FlowID = dto.FlowID
	out.FlowInstance = dto.FlowInstance
	out.HandlerEventKey = dto.HandlerEventKey
	out.SourceEventID = dto.SourceEventID
	out.SourceRunID = dto.SourceRunID
	out.SourceTaskID = dto.SourceTaskID
	out.ParentEventID = dto.ParentEventID
	out.ChainDepth = dto.ChainDepth
	out.Attempt = dto.Attempt
	out.Generation = dto.Generation
	out.LoopStage = dto.LoopStage
	out.ExecutionMode = dto.ExecutionMode
	out.ReplyContextID = dto.ReplyContextID
	return nil
}

func commitProposedEffectDecision(ctx context.Context, tx *sql.Tx, card decisioncard.Card, eventID string, now time.Time, postgres bool) error {
	continuation, err := loadProposedEffectContinuation(ctx, tx, card.CardID, postgres, true)
	if err != nil {
		return err
	}
	if continuation.State != decisioncard.ProposedEffectPending {
		return decisioncard.ErrAlreadyTerminal
	}
	query := `UPDATE proposed_effect_continuations SET state = 'decision_committed', verdict = ?, decision_event_id = ?, updated_at = ? WHERE card_id = ? AND state = 'pending'`
	if postgres {
		query = `UPDATE proposed_effect_continuations SET state = 'decision_committed', verdict = $1, decision_event_id = $2::uuid, updated_at = $3 WHERE card_id = $4 AND state = 'pending'`
	}
	result, err := tx.ExecContext(ctx, query, strings.TrimSpace(card.Verdict), strings.TrimSpace(eventID), now, card.CardID)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return decisioncard.ErrAlreadyTerminal
	}
	return nil
}

func (s *PostgresStore) CompleteProposedEffectRoute(ctx context.Context, cardID, routeEventID string, at time.Time) (decisioncard.ProposedEffectContinuation, error) {
	tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx)
	if !ok || tx == nil {
		return decisioncard.ProposedEffectContinuation{}, fmt.Errorf("proposed-effect route completion requires an active pipeline transaction")
	}
	return completeProposedEffectRoute(ctx, tx, cardID, routeEventID, at, true)
}

func (s *SQLiteRuntimeStore) CompleteProposedEffectRoute(ctx context.Context, cardID, routeEventID string, at time.Time) (decisioncard.ProposedEffectContinuation, error) {
	tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx)
	if !ok || tx == nil {
		return decisioncard.ProposedEffectContinuation{}, fmt.Errorf("proposed-effect route completion requires an active pipeline transaction")
	}
	return completeProposedEffectRoute(ctx, tx, cardID, routeEventID, at, false)
}

func completeProposedEffectRoute(ctx context.Context, tx *sql.Tx, cardID, routeEventID string, at time.Time, postgres bool) (decisioncard.ProposedEffectContinuation, error) {
	at = decisioncard.CanonicalTimestamp(at)
	if at.IsZero() {
		return decisioncard.ProposedEffectContinuation{}, fmt.Errorf("proposed-effect route completion requires an authoritative timestamp")
	}
	card, err := loadDecisionCard(ctx, tx, cardID, postgres, true)
	if err != nil {
		return decisioncard.ProposedEffectContinuation{}, err
	}
	current, err := loadProposedEffectContinuation(ctx, tx, cardID, postgres, true)
	if err != nil {
		return decisioncard.ProposedEffectContinuation{}, err
	}
	routeEventID = strings.TrimSpace(routeEventID)
	if routeEventID == "" || routeEventID != current.DecisionEventID || card.Status != decisioncard.StatusDecided || card.DecisionEventID != current.DecisionEventID || card.Verdict != current.Verdict {
		return decisioncard.ProposedEffectContinuation{}, fmt.Errorf("proposed-effect continuation does not authorize route %s", routeEventID)
	}
	wantState := decisioncard.ProposedEffectOutcomeDispatched
	expected := decisionCardOutcomeEvent{runID: current.RunID}
	if current.Verdict == "approve" {
		wantState = decisioncard.ProposedEffectRequestReleased
		expected.eventID = current.RequestEventID
		expected.eventName = "platform.activity_requested"
		expected.sourceEventID = current.SourceEventID
	} else {
		expected.eventID = decisioncard.ProposedEffectOutcomeEventID(card.CardID, current.DecisionEventID, current.Verdict)
		expected.sourceEventID = current.DecisionEventID
		switch current.Verdict {
		case "revise":
			expected.eventName = current.RevisionEvent
		case "reject":
			expected.eventName = current.RejectedEvent
		default:
			return decisioncard.ProposedEffectContinuation{}, fmt.Errorf("proposed-effect verdict %q has no route operation", current.Verdict)
		}
	}
	if current.RouteEventID != "" && current.RouteEventID != current.DecisionEventID {
		return decisioncard.ProposedEffectContinuation{}, fmt.Errorf("proposed-effect continuation has inconsistent route identity")
	}
	if err := requireDecisionCardOutcomeEvent(ctx, tx, expected, postgres); err != nil {
		return decisioncard.ProposedEffectContinuation{}, err
	}
	if current.State == wantState && current.RouteEventID == routeEventID {
		return current, nil
	}
	if current.State != decisioncard.ProposedEffectDecisionCommitted || current.DecisionEventID == "" {
		return decisioncard.ProposedEffectContinuation{}, fmt.Errorf("proposed-effect continuation does not authorize route %s", routeEventID)
	}
	query := `UPDATE proposed_effect_continuations SET state = ?, route_event_id = ?, updated_at = ? WHERE card_id = ? AND state = 'decision_committed' AND decision_event_id = ?`
	if postgres {
		query = `UPDATE proposed_effect_continuations SET state = $1, route_event_id = $2::uuid, updated_at = $3 WHERE card_id = $4 AND state = 'decision_committed' AND decision_event_id = $5::uuid`
	}
	result, err := tx.ExecContext(ctx, query, wantState, routeEventID, at, current.CardID, current.DecisionEventID)
	if err != nil {
		return decisioncard.ProposedEffectContinuation{}, err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return decisioncard.ProposedEffectContinuation{}, fmt.Errorf("proposed-effect route lost authority")
	}
	current.State = wantState
	current.RouteEventID = routeEventID
	current.UpdatedAt = at
	return current, nil
}

func (s *PostgresStore) SupersedeProposedEffectsForLoopGenerations(ctx context.Context, runID, entityID string, current []attemptgeneration.Generation, reason string, at time.Time) error {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return supersedeProposedEffectsForLoopGenerations(ctx, tx, runID, entityID, current, reason, at, true)
	}
	return runPostgresDecisionCardMutation(ctx, s.DB, func(txctx context.Context, tx *sql.Tx) error {
		return supersedeProposedEffectsForLoopGenerations(txctx, tx, runID, entityID, current, reason, at, true)
	})
}

func (s *SQLiteRuntimeStore) SupersedeProposedEffectsForLoopGenerations(ctx context.Context, runID, entityID string, current []attemptgeneration.Generation, reason string, at time.Time) error {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return supersedeProposedEffectsForLoopGenerations(ctx, tx, runID, entityID, current, reason, at, false)
	}
	return s.runDecisionCardMutation(ctx, "sqlite supersede proposed effects for loop generation", func(txctx context.Context, tx *sql.Tx) error {
		return supersedeProposedEffectsForLoopGenerations(txctx, tx, runID, entityID, current, reason, at, false)
	})
}

func supersedeProposedEffectsForLoopGenerations(ctx context.Context, tx *sql.Tx, runID, entityID string, current []attemptgeneration.Generation, reason string, at time.Time, postgres bool) error {
	runID = strings.TrimSpace(runID)
	entityID = strings.TrimSpace(entityID)
	reason = strings.TrimSpace(reason)
	at = decisioncard.CanonicalTimestamp(at)
	if runID == "" || entityID == "" || reason == "" || at.IsZero() {
		return fmt.Errorf("loop-generation proposed-effect supersession identity is incomplete")
	}
	query := `SELECT p.card_id FROM proposed_effect_continuations p JOIN decision_cards c ON c.card_id = p.card_id
		WHERE c.run_id = ? AND c.status = 'pending' AND p.state = 'pending' ORDER BY p.card_id`
	if postgres {
		query = `SELECT p.card_id FROM proposed_effect_continuations p JOIN decision_cards c ON c.card_id = p.card_id
			WHERE c.run_id = $1::uuid AND c.status = 'pending' AND p.state = 'pending' ORDER BY p.card_id FOR UPDATE OF p, c`
	}
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, strings.TrimSpace(id))
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, cardID := range ids {
		continuation, err := loadProposedEffectContinuation(ctx, tx, cardID, postgres, false)
		if err != nil {
			return err
		}
		if continuation.EntityID != entityID || !continuation.Generation.Valid() || generationStillCurrent(continuation.Generation, current) {
			continue
		}
		if err := supersedeProposedEffectContinuation(ctx, tx, cardID, reason, at, postgres); err != nil {
			return err
		}
		update := `UPDATE decision_cards SET status = ?, superseded_reason = ?, updated_at = ? WHERE card_id = ? AND status = 'pending'`
		if postgres {
			update = `UPDATE decision_cards SET status = $1, superseded_reason = $2, updated_at = $3 WHERE card_id = $4 AND status = 'pending'`
		}
		result, err := tx.ExecContext(ctx, update, decisioncard.StatusSuperseded, reason, at, cardID)
		if err != nil {
			return err
		}
		if affected, _ := result.RowsAffected(); affected != 1 {
			return decisioncard.ErrAlreadyTerminal
		}
		if _, err := appendDecisionCardChangeDTO(ctx, tx, runID, cardID, decisioncard.ChangeSuperseded, map[string]any{"reason": reason}, at, postgres); err != nil {
			return err
		}
	}
	return nil
}

func generationStillCurrent(candidate attemptgeneration.Generation, current []attemptgeneration.Generation) bool {
	for _, generation := range current {
		if candidate.Equal(generation) {
			return true
		}
	}
	return false
}

func supersedeProposedEffectContinuation(ctx context.Context, tx *sql.Tx, cardID, reason string, at time.Time, postgres bool) error {
	query := `UPDATE proposed_effect_continuations SET state = 'superseded', superseded_reason = ?, updated_at = ? WHERE card_id = ? AND state = 'pending'`
	if postgres {
		query = `UPDATE proposed_effect_continuations SET state = 'superseded', superseded_reason = $1, updated_at = $2 WHERE card_id = $3 AND state = 'pending'`
	}
	result, err := tx.ExecContext(ctx, query, strings.TrimSpace(reason), decisioncard.CanonicalTimestamp(at), strings.TrimSpace(cardID))
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return decisioncard.ErrAlreadyTerminal
	}
	return nil
}
