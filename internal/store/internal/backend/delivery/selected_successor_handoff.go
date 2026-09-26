package delivery

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

// TransferSelectedSuccessorAuthority moves only one fenced selected fork's
// unsettled delivery continuations to its freshly issued execution generation.
func (a *Adapter) TransferSelectedSuccessorAuthority(ctx context.Context, attempt *mutationprotocol.Attempt, predecessorID string, successor runtimedelivery.ExecutionAuthority) error {
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := a.transferSelectedSuccessorAuthorityTx(ctx, tx, predecessorID, successor); err != nil {
			return err
		}
		return declareAuthorityDeliveryRuns(ctx, tx, successor, attempt)
	})
}

func (a *Adapter) transferSelectedSuccessorAuthorityTx(ctx context.Context, tx *sql.Tx, predecessorID string, successor runtimedelivery.ExecutionAuthority) error {
	if tx == nil || successor.Kind() != runtimedelivery.ExecutionAuthoritySelectedContractFork {
		return fmt.Errorf("selected delivery successor requires transaction and selected authority")
	}
	if err := successor.Validate(); err != nil {
		return err
	}
	if id, err := uuid.Parse(predecessorID); err != nil || id == uuid.Nil || id.String() != predecessorID || predecessorID == successor.ExecutionID() {
		return fmt.Errorf("selected delivery successor requires distinct canonical predecessor")
	}
	var priorRunID, nextRunID, priorBindingID, nextBindingID, priorState, nextState, bundleHash string
	var priorGeneration, nextGeneration uint64
	query := `SELECT CAST(previous.fork_run_id AS TEXT),previous.generation,CAST(previous.binding_id AS TEXT),previous.state,
		CAST(next.fork_run_id AS TEXT),next.generation,CAST(next.binding_id AS TEXT),next.state,run.bundle_hash
		FROM run_fork_selected_contract_runtime_executions previous
		JOIN run_fork_selected_contract_runtime_executions next ON next.fork_run_id=previous.fork_run_id
		JOIN runs run ON run.run_id=next.fork_run_id
		WHERE previous.execution_id=$1 AND next.execution_id=$2`
	if a.dialect == DialectPostgres {
		query += ` FOR SHARE OF previous, next`
	}
	if err := tx.QueryRowContext(ctx, query, predecessorID, successor.ExecutionID()).Scan(
		&priorRunID, &priorGeneration, &priorBindingID, &priorState,
		&nextRunID, &nextGeneration, &nextBindingID, &nextState, &bundleHash,
	); err != nil {
		return fmt.Errorf("load selected delivery successor lineage: %w", err)
	}
	if priorRunID != successor.ForkRunID() || nextRunID != successor.ForkRunID() ||
		priorBindingID != nextBindingID || priorState != "closed" || (nextState != "prepared" && nextState != "running") ||
		priorGeneration+1 != nextGeneration || nextGeneration != successor.Generation() ||
		bundleHash != successor.SourceArtifact().BundleHash() {
		return fmt.Errorf("selected delivery successor conflicts with fenced predecessor")
	}
	var foreign int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1
		AND execution_authority_kind='selected_contract_fork' AND status IN ('pending','failed','in_progress')
		AND (authority_bundle_hash<>$4 OR NOT (
			(COALESCE(CAST(selected_execution_id AS TEXT),'')=$2 AND COALESCE(selected_execution_generation,-1)=$3) OR
			(COALESCE(CAST(selected_execution_id AS TEXT),'')=$5 AND COALESCE(selected_execution_generation,-1)=$6)))`,
		successor.ForkRunID(), predecessorID, priorGeneration, bundleHash, successor.ExecutionID(), successor.Generation()).Scan(&foreign); err != nil {
		return fmt.Errorf("inspect selected predecessor deliveries: %w", err)
	}
	if foreign != 0 {
		return fmt.Errorf("selected delivery successor found %d foreign nonterminal continuations", foreign)
	}
	now, err := a.databaseNow(ctx, tx)
	if err != nil {
		return err
	}
	reclaimAt := now.Add(time.Millisecond)
	// A predecessor may have claimed a receiver before its publication
	// committed the continuation handoff. Fence that claim now, but transfer
	// executable delivery authority only after the canonical handoff stamp.
	query = `UPDATE event_delivery_attempts AS attempt SET lease_expires_at=$1
		FROM event_deliveries AS delivery
		WHERE attempt.delivery_id=delivery.delivery_id AND attempt.claim_version=delivery.current_attempt_version
		AND attempt.open_marker=TRUE AND delivery.current_attempt_open=TRUE AND delivery.status='in_progress'
		AND delivery.run_id=$2 AND delivery.execution_authority_kind='selected_contract_fork'
		AND delivery.selected_execution_id=$3 AND delivery.selected_execution_generation=$4
		AND attempt.lease_expires_at>$1`
	args := []any{reclaimAt, successor.ForkRunID(), predecessorID, priorGeneration}
	if a.dialect == DialectSQLite {
		query = `UPDATE event_delivery_attempts SET lease_expires_at=$1
			WHERE open_marker=TRUE AND lease_expires_at>$1 AND EXISTS (
			SELECT 1 FROM event_deliveries delivery WHERE delivery.delivery_id=event_delivery_attempts.delivery_id
			AND delivery.current_attempt_version=event_delivery_attempts.claim_version
			AND delivery.current_attempt_open=TRUE AND delivery.status='in_progress'
			AND delivery.run_id=$2 AND delivery.execution_authority_kind='selected_contract_fork'
			AND delivery.selected_execution_id=$3 AND delivery.selected_execution_generation=$4)`
	}
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("fence predecessor selected delivery attempts: %w", err)
	}
	query = `UPDATE event_deliveries SET execution_authority_id=$1::text,execution_authority_generation=$2,
		selected_execution_id=$1::uuid,selected_execution_generation=$2,updated_at=GREATEST(updated_at,CURRENT_TIMESTAMP)
		WHERE run_id=$3 AND execution_authority_kind='selected_contract_fork'
		AND selected_execution_id=$4 AND selected_execution_generation=$5
		AND authority_bundle_hash=$6 AND continuation_handoff_at IS NOT NULL
		AND status IN ('pending','failed','in_progress')`
	if a.dialect == DialectSQLite {
		query = `UPDATE event_deliveries SET execution_authority_id=$1,execution_authority_generation=$2,
			selected_execution_id=$1,selected_execution_generation=$2,
			updated_at=CASE WHEN CURRENT_TIMESTAMP>updated_at THEN CURRENT_TIMESTAMP ELSE updated_at END
			WHERE run_id=$3 AND execution_authority_kind='selected_contract_fork'
			AND selected_execution_id=$4 AND selected_execution_generation=$5
			AND authority_bundle_hash=$6 AND continuation_handoff_at IS NOT NULL
			AND status IN ('pending','failed','in_progress')`
	}
	if _, err := tx.ExecContext(ctx, query, successor.ExecutionID(), successor.Generation(), successor.ForkRunID(), predecessorID, priorGeneration, bundleHash); err != nil {
		return fmt.Errorf("transfer selected delivery successor authority: %w", err)
	}
	return nil
}
