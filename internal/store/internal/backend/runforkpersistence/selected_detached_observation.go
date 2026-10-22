package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/store/internal/adminpersistence"
)

// Only a positive, acknowledged reset disposition selects this historical arm.
// Callers retain one read transaction; absence never substitutes for live proof.
func readDetachedSelectedHistoryTx(ctx context.Context, tx *sql.Tx, child string) (bool, error) {
	var present bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM run_fork_selected_contract_execution_detachments WHERE fork_run_id=$1)`, child).Scan(&present); err != nil || !present {
		return false, err
	}
	facts, err := readClosedSelectedGenerationRows(ctx, tx, child)
	if err != nil {
		return true, err
	}
	var conflicting bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM run_fork_selected_contract_execution_live_parents WHERE fork_run_id=$1
		UNION ALL SELECT 1 FROM run_fork_operation_live_parents WHERE fork_run_id=$1
		UNION ALL SELECT 1 FROM activity_attempt_live_parents WHERE run_id=$1
		UNION ALL SELECT 1 FROM agent_turn_live_parents WHERE run_id=$1
		UNION ALL SELECT 1 FROM run_fork_selected_contract_bindings WHERE fork_run_id=$1
		UNION ALL SELECT 1 FROM runs WHERE run_id=$1)`, child).Scan(&conflicting); err != nil {
		return true, err
	}
	if conflicting {
		return true, errors.New("selected detachment conflicts with live parents or mutable child")
	}
	actual, err := readSelectedRetainedManifestTx(ctx, tx, child)
	if err != nil {
		return true, err
	}
	if err := readDetachedSelectedMembershipsTx(ctx, tx, child, &actual); err != nil {
		return true, err
	}
	actualHash, err := canonicaljson.Hash(actual)
	if err != nil {
		return true, err
	}
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_execution_detachments WHERE fork_run_id=$1`, child).Scan(&count); err != nil {
		return true, err
	}
	if count != len(facts) {
		return true, errors.New("selected detached generation membership is incomplete or crossed")
	}
	for _, fact := range facts {
		e := fact.Execution
		var resetID, planned, fingerprint, manifestHash string
		var revision int64
		var raw []byte
		err := tx.QueryRowContext(ctx, `SELECT CAST(reset_operation_id AS TEXT),reset_revision,planned_runs_fingerprint,
			execution_fingerprint,manifest_fingerprint,manifest FROM run_fork_selected_contract_execution_detachments
			WHERE execution_id=$1 AND binding_id=$2 AND fork_run_id=$3 AND source_run_id=$4
			AND fork_point_kind=$5 AND fork_revision=$6 AND generation=$7 AND execution_state='closed'
			AND COALESCE(CAST(fork_event_id AS TEXT),'')=$8`, e.ExecutionID, fact.BindingID, child, e.SourceRunID,
			e.ForkPoint.Kind, e.ForkPoint.Revision, e.Generation, e.ForkEventID).
			Scan(&resetID, &revision, &planned, &fingerprint, &manifestHash, &raw)
		if err != nil {
			return true, fmt.Errorf("read exact selected detachment receipt: %w", err)
		}
		expected, err := actual.rowFingerprint("run_fork_selected_contract_runtime_executions", e.ExecutionID)
		if err != nil {
			return true, err
		}
		storedHash, err := canonicaljson.HashRaw(raw)
		if err != nil || storedHash != actualHash || manifestHash != actualHash || fingerprint != expected {
			return true, errors.Join(err, errors.New("selected detached canonical inventory differs from its acknowledged manifest"))
		}
		op, err := adminpersistence.ReadResetOperationTx(ctx, tx, resetID)
		if err != nil {
			return true, err
		}
		if op.Cleanup == nil || op.Plan == nil || op.Cleanup.SelectedHistory.Projected || op.Revision < revision ||
			(op.Phase != destructivereset.PhaseCleanupCommitted && op.Phase != destructivereset.PhaseContainersSettled && op.Phase != destructivereset.PhaseCompleted) {
			return true, errors.New("selected detachment has no acknowledged original cleanup disposition")
		}
		runs := slices.Clone(op.Cleanup.RunIDs)
		slices.Sort(runs)
		plannedHash, err := canonicaljson.Hash(runs)
		if err != nil || plannedHash != planned || !slices.Contains(runs, child) {
			return true, errors.Join(err, errors.New("selected detachment differs from its acknowledged planned run set"))
		}
		receipt := destructivereset.SelectedHistoryReceipt{ExecutionID: e.ExecutionID, ForkRunID: child, ResetRevision: revision,
			ExecutionFingerprint: fingerprint, ManifestFingerprint: manifestHash}
		matches := 0
		for _, retained := range op.Cleanup.SelectedHistory.Receipts {
			if retained.ExecutionID == e.ExecutionID {
				if retained != receipt {
					return true, errors.New("selected receipt conflicts with the immutable cleanup acknowledgment")
				}
				matches++
			}
		}
		if matches != 1 {
			return true, errors.New("selected receipt is absent or repeated in the cleanup acknowledgment")
		}
	}
	return true, ctx.Err()
}

func readDetachedSelectedMembershipsTx(ctx context.Context, tx *sql.Tx, child string, manifest *selectedRetainedManifest) error {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(execution_id AS TEXT),CAST(request_event_id AS TEXT),origin_kind,lineage,row_fingerprint
		FROM run_fork_selected_contract_activity_detachments WHERE fork_run_id=$1 ORDER BY request_event_id`, child)
	if err != nil {
		return err
	}
	for rows.Next() {
		var member selectedRetainedActivity
		var fingerprint string
		if err := rows.Scan(&member.ExecutionID, &member.RequestID, &member.OriginKind, &member.Lineage, &fingerprint); err != nil {
			return errors.Join(err, rows.Close())
		}
		expected, err := manifest.rowFingerprint("activity_attempts", member.RequestID)
		if err != nil || expected != fingerprint {
			return errors.Join(err, rows.Close(), errors.New("selected detached journal changed its canonical row"))
		}
		manifest.Activities = append(manifest.Activities, member)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	rows, err = tx.QueryContext(ctx, `SELECT CAST(execution_id AS TEXT),CAST(turn_id AS TEXT),CAST(completion_attempt_id AS TEXT),
		CAST(capability_surface_id AS TEXT),row_fingerprint FROM run_fork_selected_contract_turn_detachments WHERE fork_run_id=$1 ORDER BY turn_id`, child)
	if err != nil {
		return err
	}
	for rows.Next() {
		var member selectedRetainedTurn
		var fingerprint string
		if err := rows.Scan(&member.ExecutionID, &member.TurnID, &member.AttemptID, &member.SurfaceID, &fingerprint); err != nil {
			return errors.Join(err, rows.Close())
		}
		expected, err := manifest.rowFingerprint("agent_turns", member.TurnID)
		if err != nil || expected != fingerprint {
			return errors.Join(err, rows.Close(), errors.New("selected detached turn changed its canonical row"))
		}
		manifest.Turns = append(manifest.Turns, member)
	}
	return errors.Join(rows.Err(), rows.Close())
}
