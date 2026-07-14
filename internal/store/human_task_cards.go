package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

var _ decisioncard.HumanTaskStore = (*PostgresStore)(nil)
var _ decisioncard.HumanTaskStore = (*SQLiteRuntimeStore)(nil)

func (s *PostgresStore) CreateHumanTaskCard(ctx context.Context, card decisioncard.Card, continuation decisioncard.HumanTaskContinuation) error {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return insertHumanTaskCard(ctx, tx, card, continuation, true)
	}
	return runPostgresDecisionCardMutation(ctx, s.DB, func(txctx context.Context, tx *sql.Tx) error {
		return insertHumanTaskCard(txctx, tx, card, continuation, true)
	})
}

func (s *SQLiteRuntimeStore) CreateHumanTaskCard(ctx context.Context, card decisioncard.Card, continuation decisioncard.HumanTaskContinuation) error {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return insertHumanTaskCard(ctx, tx, card, continuation, false)
	}
	return s.runDecisionCardMutation(ctx, "sqlite create human-task card", func(txctx context.Context, tx *sql.Tx) error {
		return insertHumanTaskCard(txctx, tx, card, continuation, false)
	})
}

func insertHumanTaskCard(ctx context.Context, tx *sql.Tx, card decisioncard.Card, continuation decisioncard.HumanTaskContinuation, postgres bool) error {
	continuation = continuation.Canonical()
	if err := continuation.Validate(card); err != nil {
		return err
	}
	if err := insertDecisionCard(ctx, tx, card, postgres); err != nil {
		return err
	}
	query := `INSERT INTO human_task_continuations (
		card_id, run_id, requester_flow_instance, requester_entity_id, reply_context_id, source_event_id, deadline_at,
		budget_bundle_hash, budget_limit, budget_window_start, budget_window_end,
		requeue_count, defer_cause, deferred_until, state, outcome_event_id, created_at, updated_at
	) VALUES (?, ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, NULLIF(?, ''), ?, ?)
	ON CONFLICT (card_id) DO NOTHING`
	if postgres {
		query = `INSERT INTO human_task_continuations (
			card_id, run_id, requester_flow_instance, requester_entity_id, reply_context_id, source_event_id, deadline_at,
			budget_bundle_hash, budget_limit, budget_window_start, budget_window_end,
			requeue_count, defer_cause, deferred_until, state, outcome_event_id, created_at, updated_at
		) VALUES ($1, $2::uuid, NULLIF($3, ''), NULLIF($4, '')::uuid, NULLIF($5, ''), $6::uuid, $7, $8, $9, $10, $11, $12, NULLIF($13, ''), $14, $15, NULLIF($16, '')::uuid, $17, $18)
		ON CONFLICT (card_id) DO NOTHING`
	}
	res, err := tx.ExecContext(ctx, query,
		continuation.CardID, continuation.RunID, continuation.RequesterRoute.FlowInstance, continuation.RequesterRoute.EntityID,
		continuation.ReplyContextID, continuation.SourceEventID,
		continuation.DeadlineAt.UTC(), continuation.BudgetBundleHash, continuation.BudgetLimit,
		continuation.BudgetWindowStart.UTC(), continuation.BudgetWindowEnd.UTC(), continuation.RequeueCount,
		continuation.DeferCause, sqliteNullTime(continuation.DeferredUntil), continuation.State,
		continuation.OutcomeEventID, continuation.CreatedAt.UTC(), continuation.UpdatedAt.UTC(),
	)
	if err != nil {
		return fmt.Errorf("create human-task continuation: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		existing, loadErr := loadHumanTaskContinuation(ctx, tx, card.CardID, postgres, false)
		if loadErr != nil {
			return loadErr
		}
		if !sameHumanTaskCreationIdentity(existing, continuation) {
			return fmt.Errorf("human-task continuation identity collision: %s", card.CardID)
		}
	}
	return nil
}

func sameHumanTaskCreationIdentity(existing, requested decisioncard.HumanTaskContinuation) bool {
	return existing.CardID == requested.CardID &&
		existing.RunID == requested.RunID &&
		existing.RequesterRoute.Normalized() == requested.RequesterRoute.Normalized() &&
		existing.ReplyContextID == requested.ReplyContextID &&
		existing.SourceEventID == requested.SourceEventID &&
		existing.DeadlineAt.Equal(requested.DeadlineAt) &&
		existing.BudgetBundleHash == requested.BudgetBundleHash &&
		existing.BudgetLimit == requested.BudgetLimit &&
		existing.BudgetWindowStart.Equal(requested.BudgetWindowStart) &&
		existing.BudgetWindowEnd.Equal(requested.BudgetWindowEnd)
}

func (s *PostgresStore) LoadHumanTaskContinuation(ctx context.Context, cardID string) (decisioncard.HumanTaskContinuation, error) {
	return loadHumanTaskContinuation(ctx, s.DB, cardID, true, false)
}

func (s *SQLiteRuntimeStore) LoadHumanTaskContinuation(ctx context.Context, cardID string) (decisioncard.HumanTaskContinuation, error) {
	return loadHumanTaskContinuation(ctx, s.DB, cardID, false, false)
}

func (s *PostgresStore) CompleteHumanTaskOutcome(ctx context.Context, cardID, eventID string, at time.Time) (decisioncard.HumanTaskContinuation, error) {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return completeHumanTaskOutcome(ctx, tx, cardID, eventID, at, true)
	}
	var out decisioncard.HumanTaskContinuation
	err := runPostgresDecisionCardMutation(ctx, s.DB, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = completeHumanTaskOutcome(txctx, tx, cardID, eventID, at, true)
		return err
	})
	return out, err
}

func (s *SQLiteRuntimeStore) CompleteHumanTaskOutcome(ctx context.Context, cardID, eventID string, at time.Time) (decisioncard.HumanTaskContinuation, error) {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return completeHumanTaskOutcome(ctx, tx, cardID, eventID, at, false)
	}
	var out decisioncard.HumanTaskContinuation
	err := s.runRuntimeMutation(ctx, "sqlite complete human-task outcome", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = completeHumanTaskOutcome(txctx, tx, cardID, eventID, at, false)
		return err
	})
	return out, err
}

func completeHumanTaskOutcome(ctx context.Context, tx *sql.Tx, cardID, eventID string, at time.Time, postgres bool) (decisioncard.HumanTaskContinuation, error) {
	at = decisioncard.CanonicalTimestamp(at)
	if at.IsZero() {
		return decisioncard.HumanTaskContinuation{}, fmt.Errorf("human-task outcome completion requires an authoritative timestamp")
	}
	current, err := loadHumanTaskContinuation(ctx, tx, cardID, postgres, true)
	if err != nil {
		return decisioncard.HumanTaskContinuation{}, err
	}
	if current.State == decisioncard.HumanTaskContinuationOutcomeDispatched && current.OutcomeEventID == strings.TrimSpace(eventID) {
		return current, nil
	}
	if (current.State != decisioncard.HumanTaskContinuationDecisionCommitted && current.State != decisioncard.HumanTaskContinuationExpired) || current.OutcomeEventID != strings.TrimSpace(eventID) {
		return decisioncard.HumanTaskContinuation{}, fmt.Errorf("human-task continuation does not authorize outcome %s", eventID)
	}
	query := `UPDATE human_task_continuations SET state = 'outcome_dispatched', updated_at = ? WHERE card_id = ? AND state IN ('decision_committed', 'expired') AND outcome_event_id = ?`
	if postgres {
		query = `UPDATE human_task_continuations SET state = 'outcome_dispatched', updated_at = $1 WHERE card_id = $2 AND state IN ('decision_committed', 'expired') AND outcome_event_id = $3::uuid`
	}
	result, err := tx.ExecContext(ctx, query, at, strings.TrimSpace(cardID), strings.TrimSpace(eventID))
	if err != nil {
		return decisioncard.HumanTaskContinuation{}, err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return decisioncard.HumanTaskContinuation{}, fmt.Errorf("human-task outcome dispatch lost authority")
	}
	current.State = decisioncard.HumanTaskContinuationOutcomeDispatched
	current.UpdatedAt = at
	return current, nil
}

func (s *PostgresStore) ExpireHumanTaskCards(ctx context.Context, now time.Time, limit int) (int, error) {
	count := 0
	err := runPostgresDecisionCardMutation(ctx, s.DB, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = expireHumanTaskCards(txctx, tx, now, limit, true)
		return err
	})
	return count, err
}

func (s *SQLiteRuntimeStore) ExpireHumanTaskCards(ctx context.Context, now time.Time, limit int) (int, error) {
	count := 0
	err := s.runDecisionCardMutation(ctx, "sqlite expire human-task cards", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = expireHumanTaskCards(txctx, tx, now, limit, false)
		return err
	})
	return count, err
}

func expireHumanTaskCards(ctx context.Context, tx *sql.Tx, now time.Time, limit int, postgres bool) (int, error) {
	now = decisioncard.CanonicalTimestamp(now)
	if now.IsZero() {
		return 0, fmt.Errorf("human-task expiry requires an authoritative timestamp")
	}
	if limit <= 0 || limit > 200 {
		limit = 200
	}
	query := `SELECT h.card_id FROM human_task_continuations h JOIN decision_cards c ON c.card_id = h.card_id
		WHERE h.state = 'pending' AND c.status = 'pending' AND h.deadline_at <= ? ORDER BY h.deadline_at, h.card_id LIMIT ?`
	if postgres {
		query = `SELECT h.card_id FROM human_task_continuations h JOIN decision_cards c ON c.card_id = h.card_id
			WHERE h.state = 'pending' AND c.status = 'pending' AND h.deadline_at <= $1 ORDER BY h.deadline_at, h.card_id LIMIT $2 FOR UPDATE OF h, c SKIP LOCKED`
	}
	rows, err := tx.QueryContext(ctx, query, now, limit)
	if err != nil {
		return 0, err
	}
	var cardIDs []string
	for rows.Next() {
		var cardID string
		if err := rows.Scan(&cardID); err != nil {
			rows.Close()
			return 0, err
		}
		cardIDs = append(cardIDs, strings.TrimSpace(cardID))
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	expired := 0
	for _, cardID := range cardIDs {
		card, err := loadDecisionCard(ctx, tx, cardID, postgres, true)
		if err != nil {
			return 0, err
		}
		continuation, err := loadHumanTaskContinuation(ctx, tx, cardID, postgres, true)
		if err != nil {
			return 0, err
		}
		if card.Status != decisioncard.StatusPending || continuation.State != decisioncard.HumanTaskContinuationPending || continuation.DeadlineAt.After(now) {
			continue
		}
		eventID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm.human-task.expired.v1\x00"+card.CardID+"\x00"+continuation.DeadlineAt.UTC().Format(time.RFC3339Nano))).String()
		if _, err := transitionDecisionCardDrafts(ctx, tx, draftTransitionFilter{cardID: card.CardID}, now, false, postgres); err != nil {
			return 0, err
		}
		cardUpdate := `UPDATE decision_cards SET status = 'expired', decided_at = ?, deferred_until = NULL, updated_at = ? WHERE card_id = ? AND status = 'pending'`
		continuationUpdate := `UPDATE human_task_continuations SET state = 'expired', outcome_event_id = ?, deferred_until = NULL, defer_cause = 'deadline_elapsed', updated_at = ? WHERE card_id = ? AND state = 'pending'`
		if postgres {
			cardUpdate = `UPDATE decision_cards SET status = 'expired', decided_at = $1, deferred_until = NULL, updated_at = $2 WHERE card_id = $3 AND status = 'pending'`
			continuationUpdate = `UPDATE human_task_continuations SET state = 'expired', outcome_event_id = $1, deferred_until = NULL, defer_cause = 'deadline_elapsed', updated_at = $2 WHERE card_id = $3 AND state = 'pending'`
		}
		if result, err := tx.ExecContext(ctx, cardUpdate, now, now, card.CardID); err != nil {
			return 0, err
		} else if affected, _ := result.RowsAffected(); affected != 1 {
			return 0, fmt.Errorf("human-task card expiry lost card authority")
		}
		if result, err := tx.ExecContext(ctx, continuationUpdate, eventID, now, card.CardID); err != nil {
			return 0, err
		} else if affected, _ := result.RowsAffected(); affected != 1 {
			return 0, fmt.Errorf("human-task card expiry lost continuation authority")
		}
		if _, err := appendDecisionCardChangeDTO(ctx, tx, card.RunID, card.CardID, decisioncard.ChangeExpired, map[string]any{
			"cause": "deadline_elapsed", "deadline_at": continuation.DeadlineAt.UTC().Format(time.RFC3339Nano),
		}, now, postgres); err != nil {
			return 0, err
		}
		payload, err := canonicaljson.Bytes(map[string]any{
			"card_id": card.CardID, "anchor_kind": card.Anchor.Kind(), "cause": "deadline_elapsed",
			"deadline_at": continuation.DeadlineAt.UTC().Format(time.RFC3339Nano),
		})
		if err != nil {
			return 0, err
		}
		scope, err := card.Anchor.Scope()
		if err != nil {
			return 0, err
		}
		evt := events.NewRuntimeControlEvent(eventID, events.EventType("mailbox.card_expired"), "platform", "", payload, 0, card.RunID, "",
			events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, scope.EntityID), scope.FlowInstance), now)
		if err := insertDecisionCardLifecycleOutbox(ctx, tx, card, evt, postgres); err != nil {
			return 0, err
		}
		expired++
	}
	return expired, nil
}

func loadHumanTaskContinuation(ctx context.Context, db decisionCardSQL, cardID string, postgres, forUpdate bool) (decisioncard.HumanTaskContinuation, error) {
	query := `SELECT card_id, run_id, COALESCE(requester_flow_instance, ''), COALESCE(CAST(requester_entity_id AS TEXT), ''),
		COALESCE(reply_context_id, ''), COALESCE(CAST(source_event_id AS TEXT), ''),
		deadline_at, budget_bundle_hash, budget_limit, budget_window_start, budget_window_end,
		requeue_count, COALESCE(defer_cause, ''), deferred_until, state,
		COALESCE(CAST(outcome_event_id AS TEXT), ''), created_at, updated_at
		FROM human_task_continuations WHERE card_id = ?`
	if postgres {
		query = strings.Replace(query, "?", "$1", 1)
		if forUpdate {
			query += ` FOR UPDATE`
		}
	}
	var out decisioncard.HumanTaskContinuation
	var deadline, windowStart, windowEnd, deferred, created, updated any
	err := db.QueryRowContext(ctx, query, strings.TrimSpace(cardID)).Scan(
		&out.CardID, &out.RunID, &out.RequesterRoute.FlowInstance, &out.RequesterRoute.EntityID,
		&out.ReplyContextID, &out.SourceEventID, &deadline,
		&out.BudgetBundleHash, &out.BudgetLimit, &windowStart, &windowEnd, &out.RequeueCount,
		&out.DeferCause, &deferred, &out.State, &out.OutcomeEventID, &created, &updated,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return decisioncard.HumanTaskContinuation{}, decisioncard.ErrNotFound
	}
	if err != nil {
		return decisioncard.HumanTaskContinuation{}, err
	}
	for _, item := range []struct {
		raw    any
		target *time.Time
	}{
		{deadline, &out.DeadlineAt}, {windowStart, &out.BudgetWindowStart}, {windowEnd, &out.BudgetWindowEnd},
		{created, &out.CreatedAt}, {updated, &out.UpdatedAt},
	} {
		at, ok, err := sqliteTimeValue(item.raw)
		if err != nil || !ok {
			return decisioncard.HumanTaskContinuation{}, fmt.Errorf("decode human-task continuation timestamp: %w", err)
		}
		*item.target = at
	}
	if at, ok, err := sqliteTimeValue(deferred); err != nil {
		return decisioncard.HumanTaskContinuation{}, err
	} else if ok {
		out.DeferredUntil = at
	}
	return out.Canonical(), nil
}

func commitHumanTaskContinuation(ctx context.Context, tx *sql.Tx, card decisioncard.Card, eventID string, now time.Time, postgres bool) (bool, error) {
	now = decisioncard.CanonicalTimestamp(now)
	if now.IsZero() {
		return false, fmt.Errorf("human-task decision requires an authoritative timestamp")
	}
	continuation, err := loadHumanTaskContinuation(ctx, tx, card.CardID, postgres, true)
	if err != nil {
		return false, err
	}
	if continuation.State != decisioncard.HumanTaskContinuationPending {
		return false, decisioncard.ErrAlreadyTerminal
	}
	if continuation.BudgetLimit > 0 && card.Verdict == "approve" {
		if err := lockHumanTaskBudgetAdmission(ctx, tx, continuation, postgres); err != nil {
			return false, err
		}
		query := `SELECT COUNT(*) FROM human_task_continuations h JOIN decision_cards c ON c.card_id = h.card_id
			WHERE h.budget_bundle_hash = ? AND h.budget_window_start = ? AND c.verdict = 'approve'
			AND h.state IN ('decision_committed', 'outcome_dispatched')`
		if postgres {
			query = numberPostgresPlaceholders(strings.ReplaceAll(query, "?", "$%d"))
		}
		var count int
		if err := tx.QueryRowContext(ctx, query, continuation.BudgetBundleHash, continuation.BudgetWindowStart.UTC()).Scan(&count); err != nil {
			return false, err
		}
		if count >= continuation.BudgetLimit {
			update := `UPDATE human_task_continuations SET requeue_count = requeue_count + 1, defer_cause = 'weekly_budget_exhausted', deferred_until = ?, updated_at = ? WHERE card_id = ? AND state = 'pending'`
			if postgres {
				update = `UPDATE human_task_continuations SET requeue_count = requeue_count + 1, defer_cause = 'weekly_budget_exhausted', deferred_until = $1, updated_at = $2 WHERE card_id = $3 AND state = 'pending'`
			}
			if _, err := tx.ExecContext(ctx, update, continuation.BudgetWindowEnd, now, card.CardID); err != nil {
				return false, err
			}
			cardUpdate := `UPDATE decision_cards SET deferred_until = ?, updated_at = ? WHERE card_id = ? AND status = 'pending'`
			if postgres {
				cardUpdate = `UPDATE decision_cards SET deferred_until = $1, updated_at = $2 WHERE card_id = $3 AND status = 'pending'`
			}
			if _, err := tx.ExecContext(ctx, cardUpdate, continuation.BudgetWindowEnd, now, card.CardID); err != nil {
				return false, err
			}
			return true, nil
		}
	}
	query := `UPDATE human_task_continuations SET state = 'decision_committed', outcome_event_id = ?, deferred_until = NULL, defer_cause = NULL, updated_at = ? WHERE card_id = ? AND state = 'pending'`
	if postgres {
		query = `UPDATE human_task_continuations SET state = 'decision_committed', outcome_event_id = $1, deferred_until = NULL, defer_cause = NULL, updated_at = $2 WHERE card_id = $3 AND state = 'pending'`
	}
	result, err := tx.ExecContext(ctx, query, strings.TrimSpace(eventID), now, card.CardID)
	if err != nil {
		return false, err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return false, decisioncard.ErrAlreadyTerminal
	}
	return false, nil
}

func lockHumanTaskBudgetAdmission(ctx context.Context, tx *sql.Tx, continuation decisioncard.HumanTaskContinuation, postgres bool) error {
	if !postgres {
		return nil
	}
	key := fmt.Sprintf("swarm:human-task-weekly-budget:v1:%d:%s:%s", len(continuation.BudgetBundleHash), continuation.BudgetBundleHash, continuation.BudgetWindowStart.Format(time.RFC3339Nano))
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, key); err != nil {
		return fmt.Errorf("serialize human-task weekly budget admission: %w", err)
	}
	return nil
}

func deferHumanTaskContinuation(ctx context.Context, tx *sql.Tx, cardID string, until, now time.Time, postgres bool) error {
	until = decisioncard.CanonicalTimestamp(until)
	now = decisioncard.CanonicalTimestamp(now)
	query := `UPDATE human_task_continuations SET requeue_count = requeue_count + 1, defer_cause = 'operator_deferred', deferred_until = ?, updated_at = ? WHERE card_id = ? AND state = 'pending'`
	if postgres {
		query = `UPDATE human_task_continuations SET requeue_count = requeue_count + 1, defer_cause = 'operator_deferred', deferred_until = $1, updated_at = $2 WHERE card_id = $3 AND state = 'pending'`
	}
	result, err := tx.ExecContext(ctx, query, until, now, cardID)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return decisioncard.ErrAlreadyTerminal
	}
	return nil
}

func supersedeHumanTaskContinuation(ctx context.Context, tx *sql.Tx, cardID string, now time.Time, postgres bool) error {
	now = decisioncard.CanonicalTimestamp(now)
	query := `UPDATE human_task_continuations SET state = 'superseded', deferred_until = NULL, updated_at = ? WHERE card_id = ? AND state = 'pending'`
	if postgres {
		query = `UPDATE human_task_continuations SET state = 'superseded', deferred_until = NULL, updated_at = $1 WHERE card_id = $2 AND state = 'pending'`
	}
	result, err := tx.ExecContext(ctx, query, now, strings.TrimSpace(cardID))
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return fmt.Errorf("human-task continuation lost run-supersession authority")
	}
	return nil
}
