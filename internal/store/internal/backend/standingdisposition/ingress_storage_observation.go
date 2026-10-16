package standingdisposition

import (
	"context"
	"database/sql"
)

type StandingCurrentStorage struct {
	RunID, State string
	Generation   int64
}

type StandingServiceRunStorage struct {
	StandingCurrentStorage
	RunStatus string
}

func ReadFirstStandingServiceStorageTx(ctx context.Context, tx *sql.Tx, postgres bool) (StandingFlowStorage, error) {
	query := `SELECT service_id, current_run_id, current_generation FROM standing_services ORDER BY service_id LIMIT 1`
	if postgres {
		query = `SELECT service_id::text, current_run_id::text, current_generation FROM standing_services ORDER BY service_id LIMIT 1`
	}
	var out StandingFlowStorage
	err := tx.QueryRowContext(ctx, query).Scan(&out.ServiceID, &out.RunID, &out.Generation)
	return out, err
}

func ReadUnfilteredStandingCurrentStorageTx(ctx context.Context, tx *sql.Tx) (StandingCurrentStorage, error) {
	var out StandingCurrentStorage
	err := tx.QueryRowContext(ctx, `SELECT current_run_id, current_generation, effective_state FROM standing_services`).Scan(&out.RunID, &out.Generation, &out.State)
	return out, err
}

func ReadUnfilteredStandingServiceIDStorageTx(ctx context.Context, tx *sql.Tx) (string, error) {
	var id string
	err := tx.QueryRowContext(ctx, `SELECT service_id FROM standing_services`).Scan(&id)
	return id, err
}

func ReadStandingServiceRunStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, serviceID string) (StandingServiceRunStorage, error) {
	query := `SELECT ss.current_run_id, ss.current_generation, ss.effective_state, r.status
		FROM standing_services ss JOIN runs r ON r.run_id = ss.current_run_id WHERE ss.service_id = ?`
	if postgres {
		query = `SELECT ss.current_run_id::text, ss.current_generation, ss.effective_state, r.status
			FROM standing_services ss JOIN runs r ON r.run_id = ss.current_run_id WHERE ss.service_id = $1::uuid`
	}
	var out StandingServiceRunStorage
	err := tx.QueryRowContext(ctx, query, serviceID).Scan(&out.RunID, &out.Generation, &out.State, &out.RunStatus)
	return out, err
}

func ReadActiveDeclaredStandingFlowRunStorageTx(ctx context.Context, tx *sql.Tx, postgres bool, flow string) (string, error) {
	query := `SELECT current_run_id FROM standing_services WHERE flow_path = $1 AND declaration_present = TRUE AND effective_state = 'active'`
	if postgres {
		query = `SELECT current_run_id::text FROM standing_services WHERE flow_path = $1 AND declaration_present = TRUE AND effective_state = 'active'`
	}
	var id string
	err := tx.QueryRowContext(ctx, query, flow).Scan(&id)
	return id, err
}

func CountStandingFlowAuthoritiesStorageTx(ctx context.Context, tx *sql.Tx, flow string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM standing_services WHERE flow_path = $1`, flow).Scan(&count)
	return count, err
}

func CountStandingServicesStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM standing_services`).Scan(&count)
	return count, err
}

func CountStandingGenerationsStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM standing_service_generations`).Scan(&count)
	return count, err
}

func CountStandingJournalStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM standing_service_journal`).Scan(&count)
	return count, err
}
