package decisionpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/google/uuid"
)

var _ decisioncard.HumanTaskStore = (*DecisionPostgresOwner)(nil)
var _ decisioncard.HumanTaskStore = (*DecisionSQLiteOwner)(nil)

func (s *DecisionPostgresOwner) ListDueHumanTaskExpiryEvents(ctx context.Context, now time.Time, limit int) ([]events.Event, error) {
	return listDueHumanTaskExpiryEvents(ctx, s.backend, now, limit, true)
}

func (s *DecisionSQLiteOwner) ListDueHumanTaskExpiryEvents(ctx context.Context, now time.Time, limit int) ([]events.Event, error) {
	return listDueHumanTaskExpiryEvents(ctx, s.backend, now, limit, false)
}

func listDueHumanTaskExpiryEvents(ctx context.Context, db decisionCardSQL, now time.Time, limit int, postgres bool) ([]events.Event, error) {
	now = decisioncard.CanonicalTimestamp(now)
	if now.IsZero() {
		return nil, fmt.Errorf("human-task expiry requires an authoritative timestamp")
	}
	if limit <= 0 || limit > 200 {
		return nil, fmt.Errorf("human-task expiry limit must be between 1 and 200")
	}
	query := `SELECT h.card_id FROM human_task_continuations h JOIN decision_cards c ON c.card_id = h.card_id JOIN runs run ON run.run_id = h.run_id
		WHERE h.state = 'pending' AND c.status = 'pending' AND run.status IN (` + runLifecycleActiveStateSQLValues + `) AND h.deadline_at <= ? ORDER BY h.deadline_at, h.card_id LIMIT ?`
	if postgres {
		query = `SELECT h.card_id FROM human_task_continuations h JOIN decision_cards c ON c.card_id = h.card_id JOIN runs run ON run.run_id = h.run_id
			WHERE h.state = 'pending' AND c.status = 'pending' AND run.status IN (` + runLifecycleActiveStateSQLValues + `) AND h.deadline_at <= $1 ORDER BY h.deadline_at, h.card_id LIMIT $2`
	}
	rows, err := db.QueryContext(ctx, query, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cardIDs []string
	for rows.Next() {
		var cardID string
		if err := rows.Scan(&cardID); err != nil {
			return nil, err
		}
		cardIDs = append(cardIDs, strings.TrimSpace(cardID))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	eventsOut := make([]events.Event, 0, len(cardIDs))
	for _, cardID := range cardIDs {
		card, err := loadDecisionCard(ctx, db, cardID, postgres, false)
		if err != nil {
			return nil, err
		}
		continuation, err := loadHumanTaskContinuation(ctx, db, cardID, postgres, false)
		if err != nil {
			return nil, err
		}
		if card.Status != decisioncard.StatusPending || continuation.State != decisioncard.HumanTaskContinuationPending || continuation.DeadlineAt.After(now) {
			continue
		}
		event, err := humanTaskExpiredEvent(card, continuation, now)
		if err != nil {
			return nil, err
		}
		eventsOut = append(eventsOut, event)
	}
	return eventsOut, nil
}

func (s *DecisionPostgresOwner) CreateHumanTaskCard(ctx context.Context, card decisioncard.Card, continuation decisioncard.HumanTaskContinuation) error {
	return runPostgresDecisionCardMutation(ctx, s, func(txctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation) error {
		if err := requireActiveDecisionRun(txctx, tx, card.RunID, true); err != nil {
			return err
		}
		return insertHumanTaskCardWithStory(txctx, story, tx, card, continuation, true)
	})
}

func (s *DecisionSQLiteOwner) CreateHumanTaskCard(ctx context.Context, card decisioncard.Card, continuation decisioncard.HumanTaskContinuation) error {
	return s.runDecisionCardMutation(ctx, "sqlite create human-task card", func(txctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation) error {
		if err := requireActiveDecisionRun(txctx, tx, card.RunID, false); err != nil {
			return err
		}
		return insertHumanTaskCardWithStory(txctx, story, tx, card, continuation, false)
	})
}

func insertHumanTaskCardWithStory(ctx context.Context, story runtimeauthoractivity.Mutation, tx *sql.Tx, card decisioncard.Card, continuation decisioncard.HumanTaskContinuation, postgres bool) error {
	continuation = continuation.Canonical()
	if err := continuation.Validate(card); err != nil {
		return err
	}
	if err := insertDecisionCardWithStory(ctx, story, tx, card, postgres); err != nil {
		return err
	}
	query := `INSERT INTO human_task_continuations (
		card_id, run_id, requester_flow_id, requester_flow_instance, requester_entity_id, reply_context_id, source_event_id, deadline_at,
		budget_bundle_hash, budget_limit, budget_window_start, budget_window_end,
		requeue_count, defer_cause, deferred_until, state, outcome_event_id, created_at, updated_at
	) VALUES (?, ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, NULLIF(?, ''), ?, ?)
	ON CONFLICT (card_id) DO NOTHING`
	if postgres {
		query = `INSERT INTO human_task_continuations (
			card_id, run_id, requester_flow_id, requester_flow_instance, requester_entity_id, reply_context_id, source_event_id, deadline_at,
			budget_bundle_hash, budget_limit, budget_window_start, budget_window_end,
			requeue_count, defer_cause, deferred_until, state, outcome_event_id, created_at, updated_at
		) VALUES ($1, $2::uuid, NULLIF($3, ''), NULLIF($4, ''), NULLIF($5, '')::uuid, NULLIF($6, ''), $7::uuid, $8, $9, $10, $11, $12, $13, NULLIF($14, ''), $15, $16, NULLIF($17, '')::uuid, $18, $19)
		ON CONFLICT (card_id) DO NOTHING`
	}
	res, err := tx.ExecContext(ctx, query,
		continuation.CardID, continuation.RunID, continuation.RequesterRoute.FlowID, continuation.RequesterRoute.FlowInstance, continuation.RequesterRoute.EntityID,
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

func (s *DecisionPostgresOwner) LoadHumanTaskContinuation(ctx context.Context, cardID string) (decisioncard.HumanTaskContinuation, error) {
	return loadHumanTaskContinuation(ctx, s.backend, cardID, true, false)
}

func (s *DecisionSQLiteOwner) LoadHumanTaskContinuation(ctx context.Context, cardID string) (decisioncard.HumanTaskContinuation, error) {
	return loadHumanTaskContinuation(ctx, s.backend, cardID, false, false)
}

func (s *DecisionPostgresOwner) CompleteHumanTaskOutcome(ctx context.Context, cardID, eventID string, at time.Time) (decisioncard.HumanTaskContinuation, error) {
	handoff, err := reserveRunLifecycleCandidateHandoff(ctx)
	if err != nil {
		return decisioncard.HumanTaskContinuation{}, err
	}
	defer handoff.Rollback()
	var continuation decisioncard.HumanTaskContinuation
	err = runPostgresDecisionCardMutation(ctx, s, func(txctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation) error {
		var changed bool
		continuation, changed, err = completeHumanTaskOutcome(txctx, tx, cardID, eventID, at, true)
		if err != nil || !changed {
			return err
		}
		_, err = s.requestCompletionCandidateTx(txctx, tx, continuation.RunID, nil, handoff)
		return err
	})
	if err != nil {
		return decisioncard.HumanTaskContinuation{}, err
	}
	return continuation, handoff.Commit()
}

func (s *DecisionSQLiteOwner) CompleteHumanTaskOutcome(ctx context.Context, cardID, eventID string, at time.Time) (decisioncard.HumanTaskContinuation, error) {
	handoff, err := reserveRunLifecycleCandidateHandoff(ctx)
	if err != nil {
		return decisioncard.HumanTaskContinuation{}, err
	}
	defer handoff.Rollback()
	var continuation decisioncard.HumanTaskContinuation
	err = s.runDecisionCardMutation(ctx, "sqlite complete human-task outcome", func(txctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation) error {
		var changed bool
		continuation, changed, err = completeHumanTaskOutcome(txctx, tx, cardID, eventID, at, false)
		if err != nil || !changed {
			return err
		}
		_, err = s.requestCompletionCandidateTx(txctx, tx, continuation.RunID, nil, handoff)
		return err
	})
	if err != nil {
		return decisioncard.HumanTaskContinuation{}, err
	}
	return continuation, handoff.Commit()
}

func (s *DecisionPostgresOwner) CompleteHumanTaskOutcomeTx(ctx context.Context, tx *sql.Tx, cardID, eventID string, at time.Time) (decisioncard.HumanTaskContinuation, bool, error) {
	return completeHumanTaskOutcome(ctx, tx, cardID, eventID, at, true)
}

func (s *DecisionSQLiteOwner) CompleteHumanTaskOutcomeTx(ctx context.Context, tx *sql.Tx, cardID, eventID string, at time.Time) (decisioncard.HumanTaskContinuation, bool, error) {
	return completeHumanTaskOutcome(ctx, tx, cardID, eventID, at, false)
}

func completeHumanTaskOutcome(ctx context.Context, tx *sql.Tx, cardID, eventID string, at time.Time, postgres bool) (decisioncard.HumanTaskContinuation, bool, error) {
	at = decisioncard.CanonicalTimestamp(at)
	if at.IsZero() {
		return decisioncard.HumanTaskContinuation{}, false, fmt.Errorf("human-task outcome completion requires an authoritative timestamp")
	}
	if err := requireActiveDecisionCardRun(ctx, tx, cardID, postgres); err != nil {
		return decisioncard.HumanTaskContinuation{}, false, err
	}
	card, err := loadDecisionCard(ctx, tx, cardID, postgres, true)
	if err != nil {
		return decisioncard.HumanTaskContinuation{}, false, err
	}
	current, err := loadHumanTaskContinuation(ctx, tx, cardID, postgres, true)
	if err != nil {
		return decisioncard.HumanTaskContinuation{}, false, err
	}
	if err := current.Validate(card); err != nil {
		return decisioncard.HumanTaskContinuation{}, false, err
	}
	eventID = strings.TrimSpace(eventID)
	if current.OutcomeEventID != eventID {
		return decisioncard.HumanTaskContinuation{}, false, fmt.Errorf("human-task continuation does not authorize outcome %s", eventID)
	}
	eventName := ""
	switch {
	case card.Status == decisioncard.StatusDecided && card.DecisionEventID == eventID && card.Verdict == "approve":
		eventName = "human_task.approved"
	case card.Status == decisioncard.StatusDecided && card.DecisionEventID == eventID && card.Verdict == "reject":
		eventName = "human_task.rejected"
	case card.Status == decisioncard.StatusExpired && current.State != decisioncard.HumanTaskContinuationDecisionCommitted:
		eventName = "human_task.expired"
	default:
		return decisioncard.HumanTaskContinuation{}, false, fmt.Errorf("human-task card does not authorize outcome %s", eventID)
	}
	if err := requireDecisionCardOutcomeEvent(ctx, tx, decisionCardOutcomeEvent{
		eventID: decisioncard.HumanTaskOutcomeEventID(card.CardID, eventID), runID: current.RunID,
		eventName: eventName, sourceEventID: eventID,
	}, postgres); err != nil {
		return decisioncard.HumanTaskContinuation{}, false, err
	}
	if current.State == decisioncard.HumanTaskContinuationOutcomeDispatched {
		return current, false, nil
	}
	if current.State != decisioncard.HumanTaskContinuationDecisionCommitted && current.State != decisioncard.HumanTaskContinuationExpired {
		return decisioncard.HumanTaskContinuation{}, false, fmt.Errorf("human-task continuation does not authorize outcome %s", eventID)
	}
	query := `UPDATE human_task_continuations SET state = 'outcome_dispatched', updated_at = ? WHERE card_id = ? AND state IN ('decision_committed', 'expired') AND outcome_event_id = ?`
	if postgres {
		query = `UPDATE human_task_continuations SET state = 'outcome_dispatched', updated_at = $1 WHERE card_id = $2 AND state IN ('decision_committed', 'expired') AND outcome_event_id = $3::uuid`
	}
	result, err := tx.ExecContext(ctx, query, at, strings.TrimSpace(cardID), eventID)
	if err != nil {
		return decisioncard.HumanTaskContinuation{}, false, err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return decisioncard.HumanTaskContinuation{}, false, fmt.Errorf("human-task outcome dispatch lost authority")
	}
	current.State = decisioncard.HumanTaskContinuationOutcomeDispatched
	current.UpdatedAt = at
	return current, true, nil
}

func expireHumanTaskCards(ctx context.Context, story runtimeauthoractivity.Mutation, tx *sql.Tx, now time.Time, limit int, postgres bool) ([]events.Event, error) {
	now = decisioncard.CanonicalTimestamp(now)
	if now.IsZero() {
		return nil, fmt.Errorf("human-task expiry requires an authoritative timestamp")
	}
	if limit <= 0 || limit > 200 {
		limit = 200
	}
	query := `SELECT h.card_id FROM human_task_continuations h JOIN decision_cards c ON c.card_id = h.card_id JOIN runs run ON run.run_id = h.run_id
		WHERE h.state = 'pending' AND c.status = 'pending' AND run.status IN (` + runLifecycleActiveStateSQLValues + `) AND h.deadline_at <= ? ORDER BY h.deadline_at, h.card_id LIMIT ?`
	if postgres {
		query = `SELECT h.card_id FROM human_task_continuations h JOIN decision_cards c ON c.card_id = h.card_id JOIN runs run ON run.run_id = h.run_id
			WHERE h.state = 'pending' AND c.status = 'pending' AND run.status IN (` + runLifecycleActiveStateSQLValues + `) AND h.deadline_at <= $1 ORDER BY h.deadline_at, h.card_id LIMIT $2 FOR UPDATE OF h, c, run SKIP LOCKED`
	}
	rows, err := tx.QueryContext(ctx, query, now, limit)
	if err != nil {
		return nil, err
	}
	var cardIDs []string
	for rows.Next() {
		var cardID string
		if err := rows.Scan(&cardID); err != nil {
			rows.Close()
			return nil, err
		}
		cardIDs = append(cardIDs, strings.TrimSpace(cardID))
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	expiredEvents := make([]events.Event, 0, len(cardIDs))
	for _, cardID := range cardIDs {
		card, err := loadDecisionCard(ctx, tx, cardID, postgres, true)
		if err != nil {
			return nil, err
		}
		continuation, err := loadHumanTaskContinuation(ctx, tx, cardID, postgres, true)
		if err != nil {
			return nil, err
		}
		if err := continuation.Validate(card); err != nil {
			return nil, err
		}
		if card.Status != decisioncard.StatusPending || continuation.State != decisioncard.HumanTaskContinuationPending || continuation.DeadlineAt.After(now) {
			continue
		}
		eventID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm.human-task.expired.v1\x00"+card.CardID+"\x00"+continuation.DeadlineAt.UTC().Format(time.RFC3339Nano))).String()
		if _, err := transitionDecisionCardDrafts(ctx, tx, draftTransitionFilter{cardID: card.CardID}, now, false, postgres); err != nil {
			return nil, err
		}
		cardUpdate := `UPDATE decision_cards SET status = 'expired', decided_at = ?, deferred_until = NULL, updated_at = ? WHERE card_id = ? AND status = 'pending'`
		continuationUpdate := `UPDATE human_task_continuations SET state = 'expired', outcome_event_id = ?, deferred_until = NULL, defer_cause = 'deadline_elapsed', updated_at = ? WHERE card_id = ? AND state = 'pending'`
		if postgres {
			cardUpdate = `UPDATE decision_cards SET status = 'expired', decided_at = $1, deferred_until = NULL, updated_at = $2 WHERE card_id = $3 AND status = 'pending'`
			continuationUpdate = `UPDATE human_task_continuations SET state = 'expired', outcome_event_id = $1, deferred_until = NULL, defer_cause = 'deadline_elapsed', updated_at = $2 WHERE card_id = $3 AND state = 'pending'`
		}
		if result, err := tx.ExecContext(ctx, cardUpdate, now, now, card.CardID); err != nil {
			return nil, err
		} else if affected, _ := result.RowsAffected(); affected != 1 {
			return nil, fmt.Errorf("human-task card expiry lost card authority")
		}
		if result, err := tx.ExecContext(ctx, continuationUpdate, eventID, now, card.CardID); err != nil {
			return nil, err
		} else if affected, _ := result.RowsAffected(); affected != 1 {
			return nil, fmt.Errorf("human-task card expiry lost continuation authority")
		}
		if _, err := appendDecisionCardChangeDTOWithStory(ctx, story, tx, card.RunID, card.CardID, decisioncard.ChangeExpired, map[string]any{
			"cause": "deadline_elapsed", "deadline_at": continuation.DeadlineAt.UTC().Format(time.RFC3339Nano),
		}, now, postgres); err != nil {
			return nil, err
		}
		evt, err := humanTaskExpiredEvent(card, continuation, now)
		if err != nil {
			return nil, err
		}
		expiredEvents = append(expiredEvents, evt)
	}
	return expiredEvents, nil
}

func ExpireHumanTaskCards(ctx context.Context, story runtimeauthoractivity.Mutation, tx *sql.Tx, now time.Time, limit int, postgres bool) ([]events.Event, error) {
	return expireHumanTaskCards(ctx, story, tx, now, limit, postgres)
}

func (s *DecisionPostgresOwner) ExpireHumanTasksTx(ctx context.Context, story runtimeauthoractivity.Mutation, tx *sql.Tx, now time.Time, limit int) ([]events.Event, error) {
	return expireHumanTaskCards(ctx, story, tx, now, limit, true)
}

func (s *DecisionSQLiteOwner) ExpireHumanTasksTx(ctx context.Context, story runtimeauthoractivity.Mutation, tx *sql.Tx, now time.Time, limit int) ([]events.Event, error) {
	return expireHumanTaskCards(ctx, story, tx, now, limit, false)
}

func humanTaskExpiredEvent(card decisioncard.Card, continuation decisioncard.HumanTaskContinuation, now time.Time) (events.Event, error) {
	var noEvent events.Event
	eventID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm.human-task.expired.v1\x00"+card.CardID+"\x00"+continuation.DeadlineAt.UTC().Format(time.RFC3339Nano))).String()
	payload, err := canonicaljson.Bytes(map[string]any{
		"card_id": card.CardID, "anchor_kind": card.Anchor.Kind(), "cause": "deadline_elapsed",
		"deadline_at": continuation.DeadlineAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return noEvent, err
	}
	scope, err := card.Anchor.Scope()
	if err != nil {
		return noEvent, err
	}
	routingSource, err := card.Anchor.ControlRoutingSource()
	if err != nil {
		return noEvent, err
	}
	return events.NewRunScopedRuntimeControlEvent(events.RunScopedRuntimeEventInput{
		Facts: events.EventFacts{
			ID: eventID, Type: events.EventType("mailbox.card_expired"),
			Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "platform"},
			Payload:  payload,
			Envelope: events.EnvelopeForFlowInstance(
				events.EnvelopeForEntityID(events.EventEnvelope{}, scope.EntityID), scope.FlowInstance,
			),
			RoutingSource: routingSource, CreatedAt: now, ExecutionMode: card.ExecutionMode,
		},
		RunID: card.RunID,
	})
}

func loadHumanTaskContinuation(ctx context.Context, db decisionCardSQL, cardID string, postgres, forUpdate bool) (decisioncard.HumanTaskContinuation, error) {
	query := `SELECT card_id, run_id, COALESCE(requester_flow_id, ''), COALESCE(requester_flow_instance, ''), COALESCE(CAST(requester_entity_id AS TEXT), ''),
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
		&out.CardID, &out.RunID, &out.RequesterRoute.FlowID, &out.RequesterRoute.FlowInstance, &out.RequesterRoute.EntityID,
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
	if err := continuation.Validate(card); err != nil {
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

func deferHumanTaskContinuation(ctx context.Context, tx *sql.Tx, card decisioncard.Card, until, now time.Time, postgres bool) error {
	until = decisioncard.CanonicalTimestamp(until)
	now = decisioncard.CanonicalTimestamp(now)
	continuation, err := loadHumanTaskContinuation(ctx, tx, card.CardID, postgres, true)
	if err != nil {
		return err
	}
	if err := continuation.Validate(card); err != nil {
		return err
	}
	query := `UPDATE human_task_continuations SET requeue_count = requeue_count + 1, defer_cause = 'operator_deferred', deferred_until = ?, updated_at = ? WHERE card_id = ? AND state = 'pending'`
	if postgres {
		query = `UPDATE human_task_continuations SET requeue_count = requeue_count + 1, defer_cause = 'operator_deferred', deferred_until = $1, updated_at = $2 WHERE card_id = $3 AND state = 'pending'`
	}
	result, err := tx.ExecContext(ctx, query, until, now, card.CardID)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows != 1 {
		return decisioncard.ErrAlreadyTerminal
	}
	return nil
}

func supersedeHumanTaskContinuation(ctx context.Context, tx *sql.Tx, card decisioncard.Card, now time.Time, includeCommitted bool, postgres bool) error {
	now = decisioncard.CanonicalTimestamp(now)
	continuation, err := loadHumanTaskContinuation(ctx, tx, card.CardID, postgres, true)
	if err != nil {
		return err
	}
	if err := continuation.Validate(card); err != nil {
		return err
	}
	states := `('pending')`
	if includeCommitted {
		states = `('pending', 'decision_committed', 'expired')`
	}
	query := `UPDATE human_task_continuations SET state = 'superseded', deferred_until = NULL, updated_at = ? WHERE card_id = ? AND state IN ` + states
	if postgres {
		query = `UPDATE human_task_continuations SET state = 'superseded', deferred_until = NULL, updated_at = $1 WHERE card_id = $2 AND state IN ` + states
	}
	result, err := tx.ExecContext(ctx, query, now, card.CardID)
	if err != nil {
		return err
	}
	if affected, _ := result.RowsAffected(); affected != 1 {
		return fmt.Errorf("human-task continuation lost run-supersession authority")
	}
	return nil
}
