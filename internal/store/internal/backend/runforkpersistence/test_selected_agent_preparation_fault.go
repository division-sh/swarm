package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/google/uuid"
)

type SelectedAgentPreparationFault string

const (
	SelectedAgentPreparationMissing SelectedAgentPreparationFault = "missing"
	SelectedAgentPreparationCorrupt SelectedAgentPreparationFault = "corrupt"
	SelectedAgentPreparationForeign SelectedAgentPreparationFault = "foreign_actor"
)

// The receipt is opaque: callers cannot supply JSON, alter the CAS identity or
// use restoration to acquire execution authority.
type SelectedAgentPreparationFaultReceipt struct {
	expected               deliverylifecycle.Snapshot
	original, replacement  string
	fingerprint, bindingID string
	coordinate, state      string
	fenceGeneration        uint64
	sourceRun, pointKind   string
	forkRevision           int64
	forkEvent, declaration string
}

func ApplySelectedAgentPreparationFaultTxForTest(ctx context.Context, tx *sql.Tx, expected deliverylifecycle.Snapshot, run runlifecycle.Snapshot, fault SelectedAgentPreparationFault, sqlite bool) (*SelectedAgentPreparationFaultReceipt, error) {
	if tx == nil || ctx == nil {
		return nil, fmt.Errorf("selected preparation fault requires its native transaction")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if fault != SelectedAgentPreparationMissing && fault != SelectedAgentPreparationCorrupt && fault != SelectedAgentPreparationForeign {
		return nil, fmt.Errorf("selected preparation fault has no named variant")
	}
	dialect := delivery.DialectPostgres
	if sqlite {
		dialect = delivery.DialectSQLite
	}
	actual, err := delivery.ReadExactClaimedAgentSnapshotTx(ctx, tx, dialect, expected)
	if err != nil {
		return nil, err
	}
	if actual.Authority.Kind() != deliverylifecycle.ExecutionAuthoritySelectedContractFork || actual.RunID != run.RunID {
		return nil, fmt.Errorf("selected preparation fault refuses foreign delivery/run authority")
	}
	receipt := &SelectedAgentPreparationFaultReceipt{expected: actual}
	query := `SELECT preparation_binding,preparation_fingerprint,binding_id,fence_generation,executable_coordinate_fingerprint,state,
		source_run_id,fork_point_kind,fork_revision,COALESCE(CAST(fork_event_id AS TEXT),''),declaration_plan_fingerprint
		FROM run_fork_selected_contract_runtime_executions WHERE execution_id=$1 AND fork_run_id=$2 AND generation=$3`
	if !sqlite {
		query += ` FOR UPDATE`
	}
	err = tx.QueryRowContext(ctx, query, actual.Authority.ExecutionID(), actual.RunID, actual.Authority.Generation()).Scan(
		&receipt.original, &receipt.fingerprint, &receipt.bindingID, &receipt.fenceGeneration, &receipt.coordinate, &receipt.state,
		&receipt.sourceRun, &receipt.pointKind, &receipt.forkRevision, &receipt.forkEvent, &receipt.declaration)
	if err != nil {
		return nil, err
	}
	if _, err := readSelectedAgentPreparationTx(ctx, tx, actual, run, sqlite); err != nil {
		return nil, err
	}
	receipt.replacement, err = selectedAgentPreparationFaultReplacement(receipt.original, fault)
	if err != nil {
		return nil, err
	}
	if err := receipt.replaceTx(ctx, tx, receipt.original, receipt.replacement, sqlite); err != nil {
		return nil, err
	}
	// PostgreSQL stores canonical JSONB text, not the submitted JSON spelling.
	if err := tx.QueryRowContext(ctx, `SELECT preparation_binding FROM run_fork_selected_contract_runtime_executions
		WHERE execution_id=$1 AND fork_run_id=$2 AND generation=$3`, actual.Authority.ExecutionID(), actual.RunID, actual.Authority.Generation()).Scan(&receipt.replacement); err != nil {
		return nil, err
	}
	return receipt, nil
}

func RestoreSelectedAgentPreparationFaultTxForTest(ctx context.Context, tx *sql.Tx, receipt *SelectedAgentPreparationFaultReceipt, sqlite bool) error {
	if ctx == nil || tx == nil || receipt == nil || receipt.expected.Authority.Validate() != nil || receipt.original == "" || receipt.replacement == "" {
		return fmt.Errorf("selected preparation restoration requires its captured receipt")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Do not decode the deliberately corrupt replacement. Its exact persisted
	// spelling and execution/fence identity are the restoration precondition.
	return receipt.replaceTx(ctx, tx, receipt.replacement, receipt.original, sqlite)
}

func (r *SelectedAgentPreparationFaultReceipt) replaceTx(ctx context.Context, tx *sql.Tx, prior, replacement string, sqlite bool) error {
	column := "preparation_binding"
	if !sqlite {
		column = "CAST(preparation_binding AS TEXT)"
	}
	query := `UPDATE run_fork_selected_contract_runtime_executions SET preparation_binding=$1
		WHERE ` + column + `=$2 AND execution_id=$3 AND fork_run_id=$4 AND generation=$5
		AND preparation_fingerprint=$6 AND binding_id=$7 AND fence_generation=$8
		AND executable_coordinate_fingerprint=$9 AND state=$10 AND source_run_id=$11
		AND fork_point_kind=$12 AND fork_revision=$13 AND COALESCE(CAST(fork_event_id AS TEXT),'')=$14
		AND declaration_plan_fingerprint=$15`
	authority := r.expected.Authority
	result, err := tx.ExecContext(ctx, query, replacement, prior, authority.ExecutionID(), r.expected.RunID, authority.Generation(),
		r.fingerprint, r.bindingID, r.fenceGeneration, r.coordinate, r.state, r.sourceRun,
		r.pointKind, r.forkRevision, r.forkEvent, r.declaration)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		return fmt.Errorf("selected preparation fault lost exact one-row CAS: rows=%d err=%v", rows, err)
	}
	return nil
}

func selectedAgentPreparationFaultReplacement(original string, fault SelectedAgentPreparationFault) (string, error) {
	marker := uuid.NewString()
	switch fault {
	case SelectedAgentPreparationMissing:
		raw, err := json.Marshal(map[string]string{"s6_missing_preparation": marker})
		return string(raw), err
	case SelectedAgentPreparationCorrupt:
		raw, err := json.Marshal(map[string]any{"s6_corrupt_preparation": marker, "actors": "not-an-actor-census"})
		return string(raw), err
	case SelectedAgentPreparationForeign:
		var binding runfork.SelectedForkPreparationBinding
		if err := canonicaljson.DecodeInto([]byte(original), &binding); err != nil {
			return "", err
		}
		if len(binding.Actors) != 1 {
			return "", fmt.Errorf("foreign-actor fault requires its single native prepared actor")
		}
		binding.Actors[0].Plan.Name.AgentID = "s6-foreign-" + marker
		raw, err := canonicaljson.MarshalPreservingNumberKinds(binding)
		return string(raw), err
	default:
		return "", fmt.Errorf("selected preparation fault has no named variant")
	}
}
