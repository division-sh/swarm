package decisionpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func CountGateRunProposedEffectContinuationsTx(ctx context.Context, tx *sql.Tx, run string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM proposed_effect_continuations WHERE run_id = $1`, run).Scan(&count)
	return count, err
}

func InstallGateProposedEffectCreationFailureTx(ctx context.Context, tx *sql.Tx, postgres bool) error {
	statement := `CREATE TRIGGER fail_proposed_effect_create BEFORE INSERT ON proposed_effect_continuations BEGIN SELECT RAISE(ABORT, 'injected proposed-effect persistence failure'); END`
	if postgres {
		statement = `CREATE FUNCTION fail_proposed_effect_create_fn() RETURNS trigger AS $$
			BEGIN RAISE EXCEPTION 'injected proposed-effect persistence failure'; END;
			$$ LANGUAGE plpgsql;
			CREATE TRIGGER fail_proposed_effect_create BEFORE INSERT ON proposed_effect_continuations
			FOR EACH ROW EXECUTE FUNCTION fail_proposed_effect_create_fn()`
	}
	_, err := tx.ExecContext(ctx, statement)
	return err
}

func SetGateDecisionRouteAttemptFaultTx(ctx context.Context, tx *sql.Tx, event string, attempt int, due time.Time) error {
	result, err := tx.ExecContext(ctx, `UPDATE decision_card_route_obligations SET attempt_count = $1, next_attempt_at = $2 WHERE event_id = $3`, attempt, due.UTC(), event)
	return requireGateRouteFaultRow(result, err)
}

func SetGateDecisionRouteDueFaultTx(ctx context.Context, tx *sql.Tx, event string, due time.Time) error {
	result, err := tx.ExecContext(ctx, `UPDATE decision_card_route_obligations SET next_attempt_at = $1 WHERE event_id = $2`, due.UTC(), event)
	return requireGateRouteFaultRow(result, err)
}

func requireGateRouteFaultRow(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("gate route fault requires exactly one existing obligation, got %d", count)
	}
	return nil
}
