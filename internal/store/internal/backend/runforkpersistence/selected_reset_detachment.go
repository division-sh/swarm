package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/store/internal/adminpersistence"
)

type selectedRetainedActivity struct {
	ExecutionID string          `json:"execution_id"`
	RequestID   string          `json:"request_id"`
	OriginKind  string          `json:"origin_kind"`
	Lineage     json.RawMessage `json:"lineage"`
}

type selectedRetainedTurn struct {
	ExecutionID string `json:"execution_id"`
	TurnID      string `json:"turn_id"`
	AttemptID   string `json:"attempt_id"`
	SurfaceID   string `json:"surface_id"`
}

// The caller holds the existing process/reset and generation fences and commits
// this disposition with parent deletion and CommitResetCleanupTx, or rolls all back.
func ApplySelectedResetHistoryTx(ctx context.Context, tx *sql.Tx, req destructivereset.CleanupRequest, sqlite bool) (destructivereset.SelectedHistoryDisposition, error) {
	out := destructivereset.SelectedHistoryDisposition{Projected: req.Result.DryRun}
	cohorts, err := captureSelectedResetHistoryTx(ctx, tx, req, sqlite)
	if err != nil || len(cohorts) == 0 {
		return out, err
	}
	operation, err := adminpersistence.ReadResetOperationTx(ctx, tx, req.OperationID)
	if err != nil {
		return out, err
	}
	if operation.Phase != destructivereset.PhaseQuiesced || operation.Cleanup != nil || operation.Request.ActorTokenID != req.ActorTokenID {
		return out, errors.New("selected detachment requires the original durably quiesced reset")
	}
	runs, err := adminpersistence.ValidateDestructiveResetCleanupRequest(req, req.RequestedAt)
	if err != nil {
		return out, err
	}
	slices.Sort(runs)
	planned, err := canonicaljson.Hash(runs)
	if err != nil {
		return out, err
	}
	counts := make(map[string]*destructivereset.SelectedHistoryTable)
	counted := make(map[string]bool)
	count := func(table string) *destructivereset.SelectedHistoryTable {
		if counts[table] == nil {
			counts[table] = &destructivereset.SelectedHistoryTable{Table: table}
		}
		return counts[table]
	}
	// Every cohort has been recaptured before the first receipt or pair removal.
	for _, cohort := range cohorts {
		manifest := cohort.Manifest
		raw, err := canonicaljson.Bytes(manifest)
		if err != nil {
			return out, err
		}
		manifestFingerprint := canonicaljson.HashBytes(raw)
		countSelectedRetainedRows(manifest, counted, count)
		receipts, err := appendSelectedResetCohortTx(ctx, tx, cohort, req.OperationID, operation.Revision+1, planned, manifestFingerprint, raw, out.Projected, count)
		if err != nil {
			return out, err
		}
		out.Receipts = append(out.Receipts, receipts...)
	}
	for _, cohort := range cohorts {
		if err := removeSelectedResetLiveParentsTx(ctx, tx, cohort, out.Projected, count); err != nil {
			return out, err
		}
	}
	for _, item := range counts {
		out.Tables = append(out.Tables, *item)
	}
	slices.SortFunc(out.Tables, func(a, b destructivereset.SelectedHistoryTable) int { return strings.Compare(a.Table, b.Table) })
	return out, ctx.Err()
}

func countSelectedRetainedRows(manifest selectedRetainedManifest, counted map[string]bool, count func(string) *destructivereset.SelectedHistoryTable) {
	for _, table := range manifest.Tables {
		count(table.Table)
		for _, row := range table.Rows {
			key := table.Table + "/" + row.Identity
			if counted[key] {
				continue
			}
			counted[key] = true
			count(table.Table).PreservedRows++
			if row.Detached {
				count(table.Table).DetachedRows++
			}
		}
	}
}

func appendSelectedResetCohortTx(ctx context.Context, tx *sql.Tx, cohort selectedResetCohort, operationID string, revision int64, planned, manifestFingerprint string, raw []byte, projected bool, count func(string) *destructivereset.SelectedHistoryTable) ([]destructivereset.SelectedHistoryReceipt, error) {
	manifest := cohort.Manifest
	var receipts []destructivereset.SelectedHistoryReceipt
	for _, generation := range cohort.Permanent.Generations {
		e := generation.Execution
		fingerprint, err := manifest.rowFingerprint("run_fork_selected_contract_runtime_executions", e.ExecutionID)
		if err != nil {
			return nil, err
		}
		receipt := destructivereset.SelectedHistoryReceipt{ExecutionID: e.ExecutionID, ForkRunID: e.ForkRunID,
			ResetRevision: revision, ExecutionFingerprint: fingerprint, ManifestFingerprint: manifestFingerprint}
		receipts = append(receipts, receipt)
		count("run_fork_selected_contract_execution_detachments").AddedReceipts++
		if !projected {
			result, err := tx.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_execution_detachments
					(execution_id,binding_id,fork_run_id,source_run_id,fork_point_kind,fork_revision,generation,fork_event_id,execution_state,
					reset_operation_id,reset_revision,planned_runs_fingerprint,execution_fingerprint,manifest_fingerprint,manifest)
					SELECT execution_id,binding_id,fork_run_id,source_run_id,fork_point_kind,fork_revision,generation,fork_event_id,'closed',
					$2,$3,$4,$5,$6,$7 FROM run_fork_selected_contract_runtime_executions WHERE execution_id=$1 AND state='closed'`,
				e.ExecutionID, operationID, receipt.ResetRevision, planned, fingerprint, manifestFingerprint, string(raw))
			if err := requireExactlyOneMutation(result, err, "append selected reset execution receipt"); err != nil {
				return nil, err
			}
		}
	}
	for _, member := range manifest.Activities {
		fingerprint, err := manifest.rowFingerprint("activity_attempts", member.RequestID)
		if err != nil {
			return nil, err
		}
		count("run_fork_selected_contract_activity_detachments").AddedReceipts++
		if !projected {
			result, err := tx.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_activity_detachments
					(execution_id,request_event_id,fork_run_id,origin_kind,lineage,row_fingerprint) VALUES ($1,$2,$3,$4,$5,$6)`,
				member.ExecutionID, member.RequestID, cohort.Permanent.Binding.ForkRunID, member.OriginKind, string(member.Lineage), fingerprint)
			if err := requireExactlyOneMutation(result, err, "append selected reset journal membership"); err != nil {
				return nil, err
			}
		}
	}
	for _, member := range manifest.Turns {
		fingerprint, err := manifest.rowFingerprint("agent_turns", member.TurnID)
		if err != nil {
			return nil, err
		}
		count("run_fork_selected_contract_turn_detachments").AddedReceipts++
		if !projected {
			result, err := tx.ExecContext(ctx, `INSERT INTO run_fork_selected_contract_turn_detachments
					(execution_id,turn_id,fork_run_id,completion_attempt_id,capability_surface_id,row_fingerprint) VALUES ($1,$2,$3,$4,$5,$6)`,
				member.ExecutionID, member.TurnID, cohort.Permanent.Binding.ForkRunID, member.AttemptID, member.SurfaceID, fingerprint)
			if err := requireExactlyOneMutation(result, err, "append selected reset turn membership"); err != nil {
				return nil, err
			}
		}
	}
	return receipts, nil
}

func removeSelectedResetLiveParentsTx(ctx context.Context, tx *sql.Tx, cohort selectedResetCohort, projected bool, count func(string) *destructivereset.SelectedHistoryTable) error {
	pairs := []struct{ table, key, id string }{{"run_fork_operation_live_parents", "operation_id", cohort.Permanent.Operation.Request.OperationID}}
	for _, generation := range cohort.Permanent.Generations {
		pairs = append(pairs, struct{ table, key, id string }{"run_fork_selected_contract_execution_live_parents", "execution_id", generation.Execution.ExecutionID})
	}
	for _, member := range cohort.Manifest.Activities {
		pairs = append(pairs, struct{ table, key, id string }{"activity_attempt_live_parents", "request_event_id", member.RequestID})
	}
	for _, member := range cohort.Manifest.Turns {
		pairs = append(pairs, struct{ table, key, id string }{"agent_turn_live_parents", "turn_id", member.TurnID})
	}
	for _, pair := range pairs {
		count(pair.table).DeletedAssociations++
		if projected {
			continue
		}
		result, err := tx.ExecContext(ctx, `DELETE FROM `+pair.table+` WHERE `+pair.key+`=$1`, pair.id)
		if err := requireExactlyOneMutation(result, err, "remove exact selected reset live parent"); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (m selectedRetainedManifest) rowFingerprint(table, id string) (string, error) {
	for _, entry := range m.Tables {
		if entry.Table == table {
			for _, row := range entry.Rows {
				if row.Identity == id {
					return row.Fingerprint, nil
				}
			}
		}
	}
	return "", fmt.Errorf("selected retained manifest lacks %s/%s", table, id)
}
