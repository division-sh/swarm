package delivery

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

// InstallUnretainedCleanupHistoryTx writes historical physical evidence only.
// It never calls ClaimDelivery, captures a profile or returns a claim capability.
func InstallUnretainedCleanupHistoryTx(ctx context.Context, tx *sql.Tx, dialect Dialect, source correlation.SourceArtifactFact, history [2]events.Event, route events.DeliveryRoute) ([2]string, error) {
	var ids [2]string
	adapter, err := NewAdapter(dialect)
	if err != nil {
		return ids, err
	}
	// Keep the original descriptive history coordinate. It is deliberately not
	// issued/current, and a paused fork cannot execute this physical fixture.
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(source, "delivery-fixture:"+history[0].RunID(), 1)
	if err != nil {
		return ids, err
	}
	for i, event := range history {
		obligation, err := deliverylifecycle.NewObligation(event.ID(), event.RunID(), route, authority)
		if err != nil {
			return [2]string{}, err
		}
		_, err = adapter.InsertUnrevisionedFixtureObligationTx(ctx, tx, obligation)
		if err != nil {
			return [2]string{}, err
		}
		ids[i] = obligation.DeliveryID()
		if err := adapter.installUnretainedCleanupAttempt(ctx, tx, ids[i], i == 1); err != nil {
			return [2]string{}, err
		}
	}
	return ids, nil
}

type UnretainedCleanupHistoryObservation struct {
	Deliveries, Claimed, Delivered, Attempts, OpenAttempts, Settlements, RuleSelections, Profiles int
}

func (a *Adapter) installUnretainedCleanupAttempt(ctx context.Context, tx *sql.Tx, id string, settled bool) error {
	at, err := a.databaseNow(ctx, tx)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE event_deliveries SET status='in_progress',
		claim_version=1,current_attempt_version=1,current_attempt_open=TRUE,
		next_eligible_at=NULL,started_at=$1,updated_at=$1 WHERE delivery_id=$2 AND claim_version=0`, at, id)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return fmt.Errorf("unretained history requires one fresh obligation: rows=%d err=%v", n, err)
	}
	if err := a.insertAttempt(ctx, tx, id, 1, uuid.NewString(), at, at.Add(5*time.Minute), nil); err != nil {
		return err
	}
	if !settled {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE event_delivery_attempts SET open_marker=FALSE,
		current_delivery_id=NULL,closure_kind='settled',outcome='delivered',side_effects='null',
		duration_ms=0,completed_at=$1 WHERE delivery_id=$2 AND claim_version=1 AND open_marker=TRUE`, at, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE event_deliveries SET status='delivered',
		current_attempt_version=NULL,current_attempt_open=NULL,settled_at=$1,updated_at=$1
		WHERE delivery_id=$2 AND claim_version=1`, at, id); err != nil {
		return err
	}
	return a.persistHandlerRuleSelectionSQL(ctx, tx, id, deliverylifecycle.NotApplicableHandlerRuleSelection())
}

func RequireEmptyUnretainedCleanupHistoryTx(ctx context.Context, tx *sql.Tx, runID string) error {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1`, runID).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("unretained cleanup history target is not empty")
	}
	return nil
}

func ReadUnretainedCleanupHistoryTx(ctx context.Context, tx *sql.Tx, runID string) (UnretainedCleanupHistoryObservation, error) {
	var out UnretainedCleanupHistoryObservation
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*),
		COALESCE(SUM(CASE WHEN d.status='in_progress' AND d.claim_version=1 THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN d.status='delivered' AND d.claim_version=1 THEN 1 ELSE 0 END),0),
		(SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries h ON h.delivery_id=a.delivery_id WHERE h.run_id=$1),
		(SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries h ON h.delivery_id=a.delivery_id WHERE h.run_id=$1 AND a.open_marker=TRUE),
		(SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries h ON h.delivery_id=a.delivery_id WHERE h.run_id=$1 AND a.closure_kind='settled' AND a.outcome='delivered'),
		(SELECT COUNT(*) FROM event_delivery_handler_rule_selections s JOIN event_deliveries h ON h.delivery_id=s.delivery_id WHERE h.run_id=$1),
		(SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries h ON h.delivery_id=a.delivery_id WHERE h.run_id=$1 AND a.agent_claim_profile IS NOT NULL)
		FROM event_deliveries d WHERE d.run_id=$1`, runID).Scan(&out.Deliveries, &out.Claimed, &out.Delivered,
		&out.Attempts, &out.OpenAttempts, &out.Settlements, &out.RuleSelections, &out.Profiles)
	return out, err
}
