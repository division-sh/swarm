package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

const selectedContractRuntimeExecutionLease = 2 * time.Minute

func (s *RunForkPostgresOwner) IssueRunForkSelectedContractRuntimeExecution(ctx context.Context, req runfork.SelectedContractRuntimeExecutionIssueRequest) (runfork.SelectedContractRuntimeExecution, error) {
	if s == nil || s.backend == nil {
		return runfork.SelectedContractRuntimeExecution{}, fmt.Errorf("postgres store is required")
	}
	tx, err := s.backend.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return runfork.SelectedContractRuntimeExecution{}, fmt.Errorf("begin selected-contract runtime issuance: %w", err)
	}
	defer tx.Rollback()
	issued, err := issueSelectedContractRuntimeExecution(ctx, tx, postgresDialect{}, req)
	if err != nil {
		return runfork.SelectedContractRuntimeExecution{}, err
	}
	if err := tx.Commit(); err != nil {
		return runfork.SelectedContractRuntimeExecution{}, fmt.Errorf("commit selected-contract runtime issuance: %w", err)
	}
	return issued, nil
}

func (s *RunForkSQLiteOwner) IssueRunForkSelectedContractRuntimeExecution(ctx context.Context, req runfork.SelectedContractRuntimeExecutionIssueRequest) (issued runfork.SelectedContractRuntimeExecution, err error) {
	err = s.runRuntimeMutation(ctx, "sqlite selected-contract runtime issuance", func(txctx context.Context, tx *sql.Tx) error {
		var issueErr error
		issued, issueErr = issueSelectedContractRuntimeExecution(txctx, tx, sqliteDialect{}, req)
		return issueErr
	})
	return issued, err
}

type selectedRuntimeDialect interface {
	placeholder(int) string
	uuid(string) string
	lockBindingSQL() string
	currentSQL() string
	maxGenerationSQL() string
	insertSQL() string
}

type postgresDialect struct{}

func (postgresDialect) placeholder(n int) string { return fmt.Sprintf("$%d", n) }
func (postgresDialect) uuid(v string) string     { return v }
func (postgresDialect) lockBindingSQL() string {
	return `SELECT binding_id::text, source_run_id::text, fork_event_id::text, mode, COALESCE(contracts_root,''), COALESCE(bundle_hash,''), workflow_name, workflow_version FROM run_fork_selected_contract_bindings WHERE fork_run_id=$1::uuid FOR UPDATE`
}
func (postgresDialect) currentSQL() string {
	return `SELECT execution_id::text FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1::uuid AND state <> 'closed'`
}
func (postgresDialect) maxGenerationSQL() string {
	return `SELECT COALESCE(MAX(generation),0) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1::uuid`
}
func (postgresDialect) insertSQL() string {
	return `INSERT INTO run_fork_selected_contract_runtime_executions (execution_id,fork_run_id,source_run_id,binding_id,fork_event_id,generation,executable_coordinate_fingerprint,admission_fingerprint,container_plan_fingerprint,actor_census_fingerprint,effective_config_fingerprint,state,execution_owner,lease_expires_at,fence_generation,evidence,created_at,updated_at) VALUES ($1::uuid,$2::uuid,$3::uuid,$4::uuid,$5::uuid,$6,$7,$8,$9,$10,$11,'prepared',$12,$13,1,'{}'::jsonb,$14,$14)`
}

type sqliteDialect struct{}

func (sqliteDialect) placeholder(n int) string { return "?" }
func (sqliteDialect) uuid(v string) string     { return v }
func (sqliteDialect) lockBindingSQL() string {
	return `SELECT binding_id, source_run_id, fork_event_id, mode, COALESCE(contracts_root,''), COALESCE(bundle_hash,''), workflow_name, workflow_version FROM run_fork_selected_contract_bindings WHERE fork_run_id=?`
}
func (sqliteDialect) currentSQL() string {
	return `SELECT execution_id FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=? AND state <> 'closed'`
}
func (sqliteDialect) maxGenerationSQL() string {
	return `SELECT COALESCE(MAX(generation),0) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=?`
}
func (sqliteDialect) insertSQL() string {
	return `INSERT INTO run_fork_selected_contract_runtime_executions (execution_id,fork_run_id,source_run_id,binding_id,fork_event_id,generation,executable_coordinate_fingerprint,admission_fingerprint,container_plan_fingerprint,actor_census_fingerprint,effective_config_fingerprint,state,execution_owner,lease_expires_at,fence_generation,evidence,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,'prepared',?,?,1,'{}',?,?)`
}

func issueSelectedContractRuntimeExecution(ctx context.Context, tx *sql.Tx, dialect selectedRuntimeDialect, req runfork.SelectedContractRuntimeExecutionIssueRequest) (runfork.SelectedContractRuntimeExecution, error) {
	if tx == nil {
		return runfork.SelectedContractRuntimeExecution{}, fmt.Errorf("selected-contract runtime issuance transaction is required")
	}
	admission := req.Admission
	if err := validateSelectedRuntimeAdmission(admission); err != nil {
		return runfork.SelectedContractRuntimeExecution{}, err
	}
	if err := requireSelectedRuntimeRunActive(ctx, tx, admission.ForkRunID, dialect); err != nil {
		return runfork.SelectedContractRuntimeExecution{}, err
	}
	if !nonEmptyStrings(req.ContainerPlanFingerprint, req.ActorCensusFingerprint, req.EffectiveConfigFingerprint) {
		return runfork.SelectedContractRuntimeExecution{}, fmt.Errorf("selected-contract runtime issuance requires container, actor, and config fingerprints")
	}
	if !req.ExecutionMode.Valid() {
		return runfork.SelectedContractRuntimeExecution{}, fmt.Errorf("selected-contract runtime issuance requires an exact execution mode")
	}
	var bindingID, sourceRunID, forkEventID, mode, contractsRoot, bundleHash, workflowName, workflowVersion string
	if err := tx.QueryRowContext(ctx, dialect.lockBindingSQL(), dialect.uuid(admission.ForkRunID)).Scan(
		&bindingID, &sourceRunID, &forkEventID, &mode, &contractsRoot, &bundleHash, &workflowName, &workflowVersion,
	); err != nil {
		return runfork.SelectedContractRuntimeExecution{}, fmt.Errorf("lock selected-contract runtime binding: %w", err)
	}
	if sourceRunID != admission.SourceRunID || forkEventID != admission.ForkEventID ||
		mode != admission.ContractSelection.Mode || strings.TrimSpace(contractsRoot) != strings.TrimSpace(admission.ContractSelection.ContractsRoot) ||
		strings.TrimSpace(bundleHash) != strings.TrimSpace(admission.ContractSelection.BundleHash) || workflowName != admission.ContractSelection.WorkflowName || workflowVersion != admission.ContractSelection.WorkflowVersion {
		return runfork.SelectedContractRuntimeExecution{}, fmt.Errorf("selected-contract runtime admission does not match durable binding")
	}
	var current string
	if err := tx.QueryRowContext(ctx, dialect.currentSQL(), dialect.uuid(admission.ForkRunID)).Scan(&current); err == nil {
		return runfork.SelectedContractRuntimeExecution{}, fmt.Errorf("selected-contract runtime fork %s already has current execution %s", admission.ForkRunID, current)
	} else if err != sql.ErrNoRows {
		return runfork.SelectedContractRuntimeExecution{}, fmt.Errorf("check selected-contract current runtime: %w", err)
	}
	var generation uint64
	if err := tx.QueryRowContext(ctx, dialect.maxGenerationSQL(), dialect.uuid(admission.ForkRunID)).Scan(&generation); err != nil {
		return runfork.SelectedContractRuntimeExecution{}, fmt.Errorf("load selected-contract runtime generation: %w", err)
	}
	generation++
	admissionFingerprint, err := runfork.RunForkSelectedContractRuntimeFingerprint(admission)
	if err != nil {
		return runfork.SelectedContractRuntimeExecution{}, err
	}
	executableFingerprint, err := runfork.RunForkSelectedContractRuntimeFingerprint(struct {
		ForkRunID, Admission, Container, Actors, Config string
		Generation                                      uint64
	}{admission.ForkRunID, admissionFingerprint, req.ContainerPlanFingerprint, req.ActorCensusFingerprint, req.EffectiveConfigFingerprint, generation})
	if err != nil {
		return runfork.SelectedContractRuntimeExecution{}, err
	}
	now := req.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	executionID := uuid.NewString()
	issued := runfork.SelectedContractRuntimeExecution{
		ExecutionID: executionID, ForkRunID: admission.ForkRunID, SourceRunID: admission.SourceRunID, ForkEventID: admission.ForkEventID,
		Generation: generation, ExecutableCoordinateFingerprint: executableFingerprint, AdmissionFingerprint: admissionFingerprint,
		ContainerPlanFingerprint: req.ContainerPlanFingerprint, ActorCensusFingerprint: req.ActorCensusFingerprint,
		EffectiveConfigFingerprint: req.EffectiveConfigFingerprint, State: "prepared",
		ExecutionOwner: "selected-issue:" + executionID + ":" + uuid.NewString(), LeaseExpiresAt: now.Add(selectedContractRuntimeExecutionLease), FenceGeneration: 1,
		ExecutionMode: req.ExecutionMode,
	}
	args := []any{issued.ExecutionID, issued.ForkRunID, issued.SourceRunID, bindingID, issued.ForkEventID, issued.Generation,
		issued.ExecutableCoordinateFingerprint, issued.AdmissionFingerprint, issued.ContainerPlanFingerprint, issued.ActorCensusFingerprint,
		issued.EffectiveConfigFingerprint, issued.ExecutionOwner, issued.LeaseExpiresAt, now}
	if _, ok := dialect.(sqliteDialect); ok {
		args = append(args, now)
	}
	if _, err := tx.ExecContext(ctx, dialect.insertSQL(), args...); err != nil {
		return runfork.SelectedContractRuntimeExecution{}, fmt.Errorf("insert selected-contract runtime execution: %w", err)
	}
	return issued, nil
}

func validateSelectedRuntimeAdmission(admission runfork.RunForkSelectedContractExecutionAdmission) error {
	if admission.Owner != runfork.RunForkSelectedContractExecutionAdmissionOwner || admission.FutureExecutionOwner != runfork.RunForkSelectedContractExecutionOwner ||
		!admission.NonMutating || admission.ExecutionSupported || admission.ContractBindingOwner != runfork.RunForkSelectedContractBindingOwner ||
		admission.AdmissionUse != runfork.RunForkSelectedContractExecutionAdmissionUseDurableBinding ||
		admission.DeferredWorkAdmissionOwner != runfork.RunForkSelectedContractDeferredWorkAdmissionOwner {
		return fmt.Errorf("selected-contract runtime issuance requires exact non-mutating execution admission")
	}
	if !validUUIDStrings(admission.ForkRunID, admission.SourceRunID, admission.ForkEventID) {
		return fmt.Errorf("selected-contract runtime admission coordinates are invalid")
	}
	return nil
}

func (s *RunForkPostgresOwner) ClaimRunForkSelectedContractRuntimeExecution(ctx context.Context, issued runfork.SelectedContractRuntimeExecution, owner string, lease time.Duration) (runtimeeffects.Authority, error) {
	if s == nil || s.backend == nil {
		return runtimeeffects.Authority{}, fmt.Errorf("postgres store is required")
	}
	return claimSelectedContractRuntimeExecutionPostgres(ctx, s.backend, issued, owner, lease)
}

func (s *RunForkSQLiteOwner) ClaimRunForkSelectedContractRuntimeExecution(ctx context.Context, issued runfork.SelectedContractRuntimeExecution, owner string, lease time.Duration) (authority runtimeeffects.Authority, err error) {
	err = s.runRuntimeMutation(ctx, "sqlite selected-contract runtime claim", func(txctx context.Context, tx *sql.Tx) error {
		var claimErr error
		authority, claimErr = claimSelectedContractRuntimeExecutionTx(txctx, tx, true, issued, owner, lease)
		return claimErr
	})
	return authority, err
}

func claimSelectedContractRuntimeExecutionPostgres(ctx context.Context, db interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}, issued runfork.SelectedContractRuntimeExecution, owner string, lease time.Duration) (runtimeeffects.Authority, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return runtimeeffects.Authority{}, err
	}
	defer tx.Rollback()
	authority, err := claimSelectedContractRuntimeExecutionTx(ctx, tx, false, issued, owner, lease)
	if err != nil {
		return runtimeeffects.Authority{}, err
	}
	if err := tx.Commit(); err != nil {
		return runtimeeffects.Authority{}, err
	}
	return authority, nil
}

func claimSelectedContractRuntimeExecutionTx(ctx context.Context, tx *sql.Tx, sqlite bool, issued runfork.SelectedContractRuntimeExecution, owner string, lease time.Duration) (runtimeeffects.Authority, error) {
	owner = strings.TrimSpace(owner)
	if owner == "" || strings.TrimSpace(issued.ExecutionOwner) == "" || issued.LeaseExpiresAt.IsZero() || !issued.ExecutionMode.Valid() || !validUUIDStrings(issued.ExecutionID, issued.ForkRunID) {
		return runtimeeffects.Authority{}, fmt.Errorf("selected-contract runtime claim requires execution identity and owner")
	}
	if lease <= 0 {
		lease = selectedContractRuntimeExecutionLease
	}
	dialect := selectedRuntimeDialect(postgresDialect{})
	if sqlite {
		dialect = sqliteDialect{}
	}
	if err := requireSelectedRuntimeRunActive(ctx, tx, issued.ForkRunID, dialect); err != nil {
		return runtimeeffects.Authority{}, err
	}
	now := time.Now().UTC()
	expires := now.Add(lease)
	query := `UPDATE run_fork_selected_contract_runtime_executions SET state='running',execution_owner=$2,lease_expires_at=$3,updated_at=$4 WHERE execution_id=$1::uuid AND fork_run_id=$5::uuid AND generation=$6 AND state='prepared' AND admission_fingerprint=$7 AND container_plan_fingerprint=$8 AND actor_census_fingerprint=$9 AND effective_config_fingerprint=$10 AND execution_owner=$11 AND lease_expires_at=$12 AND lease_expires_at>$4`
	args := []any{issued.ExecutionID, owner, expires, now, issued.ForkRunID, issued.Generation, issued.AdmissionFingerprint, issued.ContainerPlanFingerprint, issued.ActorCensusFingerprint, issued.EffectiveConfigFingerprint, issued.ExecutionOwner, issued.LeaseExpiresAt.UTC()}
	if sqlite {
		query = `UPDATE run_fork_selected_contract_runtime_executions SET state='running',execution_owner=?,lease_expires_at=?,updated_at=? WHERE execution_id=? AND fork_run_id=? AND generation=? AND state='prepared' AND admission_fingerprint=? AND container_plan_fingerprint=? AND actor_census_fingerprint=? AND effective_config_fingerprint=? AND execution_owner=? AND lease_expires_at=? AND lease_expires_at>?`
		args = []any{owner, expires, now, issued.ExecutionID, issued.ForkRunID, issued.Generation, issued.AdmissionFingerprint, issued.ContainerPlanFingerprint, issued.ActorCensusFingerprint, issued.EffectiveConfigFingerprint, issued.ExecutionOwner, issued.LeaseExpiresAt.UTC(), now}
	}
	res, err := tx.ExecContext(ctx, query, args...)
	if err := requireExactlyOneMutation(res, err, "claim selected-contract runtime execution"); err != nil {
		return runtimeeffects.Authority{}, err
	}
	authority := runtimeeffects.Authority{
		Kind: runtimeeffects.AuthoritySelectedContractFork, ID: issued.ExecutionID, ExecutionOwner: owner, LeaseExpiresAt: expires,
		FenceGeneration: issued.FenceGeneration, ExecutionMode: issued.ExecutionMode,
		SelectedFork: runtimeeffects.SelectedContractForkAuthority{ExecutionID: issued.ExecutionID, ForkRunID: issued.ForkRunID, Generation: issued.Generation,
			AdmissionFingerprint: issued.AdmissionFingerprint, ContainerPlanFingerprint: issued.ContainerPlanFingerprint,
			ActorCensusFingerprint: issued.ActorCensusFingerprint, EffectiveConfigFingerprint: issued.EffectiveConfigFingerprint},
	}
	return authority, nil
}

func requireExactlyOneMutation(res sql.Result, err error, operation string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	rows, err := res.RowsAffected()
	if err != nil || rows != 1 {
		return fmt.Errorf("%s rejected stale or conflicting authority", operation)
	}
	return nil
}

func validUUIDStrings(values ...string) bool {
	for _, value := range values {
		if _, err := uuid.Parse(strings.TrimSpace(value)); err != nil {
			return false
		}
	}
	return true
}

func nonEmptyStrings(values ...string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return false
		}
	}
	return true
}

func nullableJSON(raw []byte) any {
	if len(raw) == 0 {
		return nil
	}
	return string(raw)
}

func sqliteNullableJSON(raw []byte) any { return nullableJSON(raw) }

func (s *RunForkPostgresOwner) HeartbeatRunForkSelectedContractRuntimeExecution(ctx context.Context, authority runtimeeffects.Authority, lease time.Duration) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres store is required")
	}
	if lease <= 0 {
		lease = selectedContractRuntimeExecutionLease
	}
	tx, err := s.backend.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("heartbeat selected-contract runtime begin: %w", err)
	}
	defer tx.Rollback()
	if err := s.EffectPostgresOwner.RequireExternalEffectAuthorityTx(ctx, tx, authority, false); err != nil {
		return err
	}
	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `
		UPDATE run_fork_selected_contract_runtime_executions
		SET lease_expires_at=$2,updated_at=$3
		WHERE execution_id=$1::uuid AND state='running'
	`, authority.ID, now.Add(lease), now)
	if err := requireExactlyOneMutation(res, err, "heartbeat selected-contract runtime execution"); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *RunForkSQLiteOwner) HeartbeatRunForkSelectedContractRuntimeExecution(ctx context.Context, authority runtimeeffects.Authority, lease time.Duration) error {
	if lease <= 0 {
		lease = selectedContractRuntimeExecutionLease
	}
	return s.runRuntimeMutation(ctx, "sqlite selected-contract runtime heartbeat", func(txctx context.Context, tx *sql.Tx) error {
		if err := s.EffectSQLiteOwner.RequireExternalEffectAuthorityTx(txctx, tx, authority, false); err != nil {
			return err
		}
		now := time.Now().UTC()
		res, err := tx.ExecContext(txctx, `
			UPDATE run_fork_selected_contract_runtime_executions
			SET lease_expires_at=?,updated_at=?
			WHERE execution_id=? AND state='running'
		`, now.Add(lease), now, authority.ID)
		return requireExactlyOneMutation(res, err, "heartbeat sqlite selected-contract runtime execution")
	})
}

func (s *RunForkPostgresOwner) QuiesceRunForkSelectedContractRuntimeExecution(ctx context.Context, authority runtimeeffects.Authority) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres store is required")
	}
	tx, err := s.backend.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.EffectPostgresOwner.RequireCurrentExternalEffectAuthorityTx(ctx, tx, authority); err != nil {
		return err
	}
	if err := requireSelectedRuntimeNoLiveAttempts(ctx, tx, s.EffectPostgresOwner, authority.ID); err != nil {
		return err
	}
	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET state='quiesced',lease_expires_at=NULL,terminal_at=$2,updated_at=$2 WHERE execution_id=$1::uuid AND state='running'`, authority.ID, now)
	if err := requireExactlyOneMutation(res, err, "quiesce selected-contract runtime execution"); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *RunForkSQLiteOwner) QuiesceRunForkSelectedContractRuntimeExecution(ctx context.Context, authority runtimeeffects.Authority) error {
	return s.runRuntimeMutation(ctx, "sqlite selected-contract runtime quiesce", func(txctx context.Context, tx *sql.Tx) error {
		if err := s.EffectSQLiteOwner.RequireCurrentExternalEffectAuthorityTx(txctx, tx, authority); err != nil {
			return err
		}
		if err := requireSelectedRuntimeNoLiveAttempts(txctx, tx, s.EffectSQLiteOwner, authority.ID); err != nil {
			return err
		}
		now := time.Now().UTC()
		res, err := tx.ExecContext(txctx, `UPDATE run_fork_selected_contract_runtime_executions SET state='quiesced',lease_expires_at=NULL,terminal_at=?,updated_at=? WHERE execution_id=? AND state='running'`, now, now, authority.ID)
		return requireExactlyOneMutation(res, err, "quiesce sqlite selected-contract runtime execution")
	})
}

func requireSelectedRuntimeNoLiveAttempts(ctx context.Context, tx *sql.Tx, owner interface {
	RequireCompletionAuthorityNoLiveAttemptsTx(context.Context, *sql.Tx, runtimeeffects.Authority) error
}, executionID string) error {
	return owner.RequireCompletionAuthorityNoLiveAttemptsTx(ctx, tx, runtimeeffects.Authority{
		Kind:         runtimeeffects.AuthoritySelectedContractFork,
		SelectedFork: runtimeeffects.SelectedContractForkAuthority{ExecutionID: executionID},
	})
}

func (s *RunForkPostgresOwner) CloseRunForkSelectedContractRuntimeExecution(ctx context.Context, executionID string) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres store is required")
	}
	tx, err := s.backend.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("close selected-contract runtime begin: %w", err)
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET state='closed',lease_expires_at=NULL,terminal_at=COALESCE(terminal_at,$2),updated_at=$2 WHERE execution_id=$1::uuid AND state IN ('quiesced','failed')`, executionID, now)
	if err := requireExactlyOneMutation(res, err, "close selected-contract runtime execution"); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *RunForkSQLiteOwner) CloseRunForkSelectedContractRuntimeExecution(ctx context.Context, executionID string) error {
	return s.runRuntimeMutation(ctx, "sqlite selected-contract runtime close", func(txctx context.Context, tx *sql.Tx) error {
		now := time.Now().UTC()
		res, err := tx.ExecContext(txctx, `UPDATE run_fork_selected_contract_runtime_executions SET state='closed',lease_expires_at=NULL,terminal_at=COALESCE(terminal_at,?),updated_at=? WHERE execution_id=? AND state IN ('quiesced','failed')`, now, now, executionID)
		return requireExactlyOneMutation(res, err, "close sqlite selected-contract runtime execution")
	})
}

func (s *RunForkPostgresOwner) FailRunForkSelectedContractRuntimeExecution(ctx context.Context, authority runtimeeffects.Authority, failure json.RawMessage) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres store is required")
	}
	tx, err := s.backend.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.EffectPostgresOwner.RequireCurrentExternalEffectAuthorityTx(ctx, tx, authority); err != nil {
		return err
	}
	if err := requireSelectedRuntimeNoLiveAttempts(ctx, tx, s.EffectPostgresOwner, authority.ID); err != nil {
		return err
	}
	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET state='failed',lease_expires_at=NULL,failure=$2::jsonb,terminal_at=$3,updated_at=$3 WHERE execution_id=$1::uuid AND state IN ('prepared','running') AND fence_generation=$4`, authority.ID, nullableJSON(failure), now, authority.FenceGeneration)
	if err := requireExactlyOneMutation(res, err, "fail selected-contract runtime execution"); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *RunForkSQLiteOwner) FailRunForkSelectedContractRuntimeExecution(ctx context.Context, authority runtimeeffects.Authority, failure json.RawMessage) error {
	return s.runRuntimeMutation(ctx, "sqlite selected-contract runtime fail", func(txctx context.Context, tx *sql.Tx) error {
		if err := s.EffectSQLiteOwner.RequireCurrentExternalEffectAuthorityTx(txctx, tx, authority); err != nil {
			return err
		}
		if err := requireSelectedRuntimeNoLiveAttempts(txctx, tx, s.EffectSQLiteOwner, authority.ID); err != nil {
			return err
		}
		now := time.Now().UTC()
		res, err := tx.ExecContext(txctx, `UPDATE run_fork_selected_contract_runtime_executions SET state='failed',lease_expires_at=NULL,failure=?,terminal_at=?,updated_at=? WHERE execution_id=? AND state IN ('prepared','running') AND fence_generation=?`, sqliteNullableJSON(failure), now, now, authority.ID, authority.FenceGeneration)
		return requireExactlyOneMutation(res, err, "fail sqlite selected-contract runtime execution")
	})
}

func requireSelectedRuntimeRunActive(ctx context.Context, tx *sql.Tx, runID string, dialect selectedRuntimeDialect) error {
	var err error
	if _, ok := dialect.(sqliteDialect); ok {
		err = requireSQLiteRunActive(ctx, tx, runID)
	} else {
		err = requirePostgresRunActive(ctx, tx, runID)
	}
	if err != nil {
		return fmt.Errorf("admit selected-contract runtime mutation: %w", err)
	}
	return nil
}
