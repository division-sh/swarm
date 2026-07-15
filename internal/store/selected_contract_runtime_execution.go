package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
)

const selectedContractRuntimeExecutionLease = 2 * time.Minute

type SelectedContractRuntimeExecutionIssueRequest struct {
	Admission                  RunForkSelectedContractExecutionAdmission
	ContainerPlanFingerprint   string
	ActorCensusFingerprint     string
	EffectiveConfigFingerprint string
	Now                        time.Time
}

type SelectedContractRuntimeExecution struct {
	ExecutionID                     string
	ForkRunID                       string
	SourceRunID                     string
	ForkEventID                     string
	Generation                      uint64
	ExecutableCoordinateFingerprint string
	AdmissionFingerprint            string
	ContainerPlanFingerprint        string
	ActorCensusFingerprint          string
	EffectiveConfigFingerprint      string
	State                           string
	ExecutionOwner                  string
	LeaseExpiresAt                  time.Time
	FenceGeneration                 uint64
}

func RunForkSelectedContractRuntimeFingerprint(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal selected-contract runtime fingerprint: %w", err)
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func (s *PostgresStore) IssueRunForkSelectedContractRuntimeExecution(ctx context.Context, req SelectedContractRuntimeExecutionIssueRequest) (SelectedContractRuntimeExecution, error) {
	if s == nil || s.DB == nil {
		return SelectedContractRuntimeExecution{}, fmt.Errorf("postgres store is required")
	}
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return SelectedContractRuntimeExecution{}, fmt.Errorf("begin selected-contract runtime issuance: %w", err)
	}
	defer tx.Rollback()
	issued, err := issueSelectedContractRuntimeExecution(ctx, tx, postgresDialect{}, req)
	if err != nil {
		return SelectedContractRuntimeExecution{}, err
	}
	if err := tx.Commit(); err != nil {
		return SelectedContractRuntimeExecution{}, fmt.Errorf("commit selected-contract runtime issuance: %w", err)
	}
	return issued, nil
}

func (s *SQLiteRuntimeStore) IssueRunForkSelectedContractRuntimeExecution(ctx context.Context, req SelectedContractRuntimeExecutionIssueRequest) (issued SelectedContractRuntimeExecution, err error) {
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

func issueSelectedContractRuntimeExecution(ctx context.Context, tx *sql.Tx, dialect selectedRuntimeDialect, req SelectedContractRuntimeExecutionIssueRequest) (SelectedContractRuntimeExecution, error) {
	if tx == nil {
		return SelectedContractRuntimeExecution{}, fmt.Errorf("selected-contract runtime issuance transaction is required")
	}
	admission := req.Admission
	if err := validateSelectedRuntimeAdmission(admission); err != nil {
		return SelectedContractRuntimeExecution{}, err
	}
	if !nonEmptyStrings(req.ContainerPlanFingerprint, req.ActorCensusFingerprint, req.EffectiveConfigFingerprint) {
		return SelectedContractRuntimeExecution{}, fmt.Errorf("selected-contract runtime issuance requires container, actor, and config fingerprints")
	}
	var bindingID, sourceRunID, forkEventID, mode, contractsRoot, bundleHash, workflowName, workflowVersion string
	if err := tx.QueryRowContext(ctx, dialect.lockBindingSQL(), dialect.uuid(admission.ForkRunID)).Scan(
		&bindingID, &sourceRunID, &forkEventID, &mode, &contractsRoot, &bundleHash, &workflowName, &workflowVersion,
	); err != nil {
		return SelectedContractRuntimeExecution{}, fmt.Errorf("lock selected-contract runtime binding: %w", err)
	}
	if sourceRunID != admission.SourceRunID || forkEventID != admission.ForkEventID ||
		mode != admission.ContractSelection.Mode || strings.TrimSpace(contractsRoot) != strings.TrimSpace(admission.ContractSelection.ContractsRoot) ||
		strings.TrimSpace(bundleHash) != strings.TrimSpace(admission.ContractSelection.BundleHash) || workflowName != admission.ContractSelection.WorkflowName || workflowVersion != admission.ContractSelection.WorkflowVersion {
		return SelectedContractRuntimeExecution{}, fmt.Errorf("selected-contract runtime admission does not match durable binding")
	}
	var current string
	if err := tx.QueryRowContext(ctx, dialect.currentSQL(), dialect.uuid(admission.ForkRunID)).Scan(&current); err == nil {
		return SelectedContractRuntimeExecution{}, fmt.Errorf("selected-contract runtime fork %s already has current execution %s", admission.ForkRunID, current)
	} else if err != sql.ErrNoRows {
		return SelectedContractRuntimeExecution{}, fmt.Errorf("check selected-contract current runtime: %w", err)
	}
	var generation uint64
	if err := tx.QueryRowContext(ctx, dialect.maxGenerationSQL(), dialect.uuid(admission.ForkRunID)).Scan(&generation); err != nil {
		return SelectedContractRuntimeExecution{}, fmt.Errorf("load selected-contract runtime generation: %w", err)
	}
	generation++
	admissionFingerprint, err := RunForkSelectedContractRuntimeFingerprint(admission)
	if err != nil {
		return SelectedContractRuntimeExecution{}, err
	}
	executableFingerprint, err := RunForkSelectedContractRuntimeFingerprint(struct {
		ForkRunID, Admission, Container, Actors, Config string
		Generation                                      uint64
	}{admission.ForkRunID, admissionFingerprint, req.ContainerPlanFingerprint, req.ActorCensusFingerprint, req.EffectiveConfigFingerprint, generation})
	if err != nil {
		return SelectedContractRuntimeExecution{}, err
	}
	now := req.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	executionID := uuid.NewString()
	issued := SelectedContractRuntimeExecution{
		ExecutionID: executionID, ForkRunID: admission.ForkRunID, SourceRunID: admission.SourceRunID, ForkEventID: admission.ForkEventID,
		Generation: generation, ExecutableCoordinateFingerprint: executableFingerprint, AdmissionFingerprint: admissionFingerprint,
		ContainerPlanFingerprint: req.ContainerPlanFingerprint, ActorCensusFingerprint: req.ActorCensusFingerprint,
		EffectiveConfigFingerprint: req.EffectiveConfigFingerprint, State: "prepared",
		ExecutionOwner: "selected-issue:" + executionID + ":" + uuid.NewString(), LeaseExpiresAt: now.Add(selectedContractRuntimeExecutionLease), FenceGeneration: 1,
	}
	args := []any{issued.ExecutionID, issued.ForkRunID, issued.SourceRunID, bindingID, issued.ForkEventID, issued.Generation,
		issued.ExecutableCoordinateFingerprint, issued.AdmissionFingerprint, issued.ContainerPlanFingerprint, issued.ActorCensusFingerprint,
		issued.EffectiveConfigFingerprint, issued.ExecutionOwner, issued.LeaseExpiresAt, now}
	if _, ok := dialect.(sqliteDialect); ok {
		args = append(args, now)
	}
	if _, err := tx.ExecContext(ctx, dialect.insertSQL(), args...); err != nil {
		return SelectedContractRuntimeExecution{}, fmt.Errorf("insert selected-contract runtime execution: %w", err)
	}
	return issued, nil
}

func validateSelectedRuntimeAdmission(admission RunForkSelectedContractExecutionAdmission) error {
	if admission.Owner != RunForkSelectedContractExecutionAdmissionOwner || admission.FutureExecutionOwner != RunForkSelectedContractExecutionOwner ||
		!admission.NonMutating || admission.ExecutionSupported || admission.ContractBindingOwner != RunForkSelectedContractBindingOwner ||
		admission.AdmissionUse != RunForkSelectedContractExecutionAdmissionUseDurableBinding {
		return fmt.Errorf("selected-contract runtime issuance requires exact non-mutating execution admission")
	}
	if !validUUIDStrings(admission.ForkRunID, admission.SourceRunID, admission.ForkEventID) {
		return fmt.Errorf("selected-contract runtime admission coordinates are invalid")
	}
	return nil
}

func (s *PostgresStore) ClaimRunForkSelectedContractRuntimeExecution(ctx context.Context, issued SelectedContractRuntimeExecution, owner string, lease time.Duration) (runtimeeffects.Authority, error) {
	if s == nil || s.DB == nil {
		return runtimeeffects.Authority{}, fmt.Errorf("postgres store is required")
	}
	return claimSelectedContractRuntimeExecutionPostgres(ctx, s.DB, issued, owner, lease)
}

func (s *SQLiteRuntimeStore) ClaimRunForkSelectedContractRuntimeExecution(ctx context.Context, issued SelectedContractRuntimeExecution, owner string, lease time.Duration) (authority runtimeeffects.Authority, err error) {
	err = s.runRuntimeMutation(ctx, "sqlite selected-contract runtime claim", func(txctx context.Context, tx *sql.Tx) error {
		var claimErr error
		authority, claimErr = claimSelectedContractRuntimeExecutionTx(txctx, tx, true, issued, owner, lease)
		return claimErr
	})
	return authority, err
}

func claimSelectedContractRuntimeExecutionPostgres(ctx context.Context, db *sql.DB, issued SelectedContractRuntimeExecution, owner string, lease time.Duration) (runtimeeffects.Authority, error) {
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

func claimSelectedContractRuntimeExecutionTx(ctx context.Context, tx *sql.Tx, sqlite bool, issued SelectedContractRuntimeExecution, owner string, lease time.Duration) (runtimeeffects.Authority, error) {
	owner = strings.TrimSpace(owner)
	if owner == "" || strings.TrimSpace(issued.ExecutionOwner) == "" || issued.LeaseExpiresAt.IsZero() || !validUUIDStrings(issued.ExecutionID, issued.ForkRunID) {
		return runtimeeffects.Authority{}, fmt.Errorf("selected-contract runtime claim requires execution identity and owner")
	}
	if lease <= 0 {
		lease = selectedContractRuntimeExecutionLease
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
		FenceGeneration: issued.FenceGeneration, ExecutionMode: runtimeeffects.ExecutionModeLive,
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

func (s *PostgresStore) HeartbeatRunForkSelectedContractRuntimeExecution(ctx context.Context, authority runtimeeffects.Authority, lease time.Duration) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	if lease <= 0 {
		lease = selectedContractRuntimeExecutionLease
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("heartbeat selected-contract runtime begin: %w", err)
	}
	defer tx.Rollback()
	if err := requireCurrentExternalEffectAuthorityPostgres(ctx, tx, authority); err != nil {
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

func (s *SQLiteRuntimeStore) HeartbeatRunForkSelectedContractRuntimeExecution(ctx context.Context, authority runtimeeffects.Authority, lease time.Duration) error {
	if lease <= 0 {
		lease = selectedContractRuntimeExecutionLease
	}
	return s.runRuntimeMutation(ctx, "sqlite selected-contract runtime heartbeat", func(txctx context.Context, tx *sql.Tx) error {
		if err := requireCurrentExternalEffectAuthoritySQLite(txctx, tx, authority); err != nil {
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

func (s *PostgresStore) QuiesceRunForkSelectedContractRuntimeExecution(ctx context.Context, authority runtimeeffects.Authority) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := requireCurrentExternalEffectAuthorityPostgres(ctx, tx, authority); err != nil {
		return err
	}
	if err := requireSelectedRuntimeNoLiveAttempts(ctx, tx, false, authority.ID); err != nil {
		return err
	}
	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET state='quiesced',lease_expires_at=NULL,terminal_at=$2,updated_at=$2 WHERE execution_id=$1::uuid AND state='running'`, authority.ID, now)
	if err := requireExactlyOneMutation(res, err, "quiesce selected-contract runtime execution"); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteRuntimeStore) QuiesceRunForkSelectedContractRuntimeExecution(ctx context.Context, authority runtimeeffects.Authority) error {
	return s.runRuntimeMutation(ctx, "sqlite selected-contract runtime quiesce", func(txctx context.Context, tx *sql.Tx) error {
		if err := requireCurrentExternalEffectAuthoritySQLite(txctx, tx, authority); err != nil {
			return err
		}
		if err := requireSelectedRuntimeNoLiveAttempts(txctx, tx, true, authority.ID); err != nil {
			return err
		}
		now := time.Now().UTC()
		res, err := tx.ExecContext(txctx, `UPDATE run_fork_selected_contract_runtime_executions SET state='quiesced',lease_expires_at=NULL,terminal_at=?,updated_at=? WHERE execution_id=? AND state='running'`, now, now, authority.ID)
		return requireExactlyOneMutation(res, err, "quiesce sqlite selected-contract runtime execution")
	})
}

func requireSelectedRuntimeNoLiveAttempts(ctx context.Context, tx *sql.Tx, sqlite bool, executionID string) error {
	return requireCompletionAuthorityNoLiveAttempts(ctx, tx, sqlite, runtimeeffects.Authority{
		Kind:         runtimeeffects.AuthoritySelectedContractFork,
		SelectedFork: runtimeeffects.SelectedContractForkAuthority{ExecutionID: executionID},
	})
}

func (s *PostgresStore) CloseRunForkSelectedContractRuntimeExecution(ctx context.Context, executionID string) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	now := time.Now().UTC()
	res, err := s.DB.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET state='closed',lease_expires_at=NULL,terminal_at=COALESCE(terminal_at,$2),updated_at=$2 WHERE execution_id=$1::uuid AND state IN ('quiesced','failed')`, executionID, now)
	return requireExactlyOneMutation(res, err, "close selected-contract runtime execution")
}

func (s *SQLiteRuntimeStore) CloseRunForkSelectedContractRuntimeExecution(ctx context.Context, executionID string) error {
	return s.runRuntimeMutation(ctx, "sqlite selected-contract runtime close", func(txctx context.Context, tx *sql.Tx) error {
		now := time.Now().UTC()
		res, err := tx.ExecContext(txctx, `UPDATE run_fork_selected_contract_runtime_executions SET state='closed',lease_expires_at=NULL,terminal_at=COALESCE(terminal_at,?),updated_at=? WHERE execution_id=? AND state IN ('quiesced','failed')`, now, now, executionID)
		return requireExactlyOneMutation(res, err, "close sqlite selected-contract runtime execution")
	})
}

func (s *PostgresStore) FailRunForkSelectedContractRuntimeExecution(ctx context.Context, authority runtimeeffects.Authority, failure json.RawMessage) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := requireSelectedRuntimeNoLiveAttempts(ctx, tx, false, authority.ID); err != nil {
		return err
	}
	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET state='failed',lease_expires_at=NULL,failure=$2::jsonb,terminal_at=$3,updated_at=$3 WHERE execution_id=$1::uuid AND state IN ('prepared','running') AND fence_generation=$4`, authority.ID, nullableJSON(failure), now, authority.FenceGeneration)
	if err := requireExactlyOneMutation(res, err, "fail selected-contract runtime execution"); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteRuntimeStore) FailRunForkSelectedContractRuntimeExecution(ctx context.Context, authority runtimeeffects.Authority, failure json.RawMessage) error {
	return s.runRuntimeMutation(ctx, "sqlite selected-contract runtime fail", func(txctx context.Context, tx *sql.Tx) error {
		if err := requireSelectedRuntimeNoLiveAttempts(txctx, tx, true, authority.ID); err != nil {
			return err
		}
		now := time.Now().UTC()
		res, err := tx.ExecContext(txctx, `UPDATE run_fork_selected_contract_runtime_executions SET state='failed',lease_expires_at=NULL,failure=?,terminal_at=?,updated_at=? WHERE execution_id=? AND state IN ('prepared','running') AND fence_generation=?`, sqliteNullableJSON(failure), now, now, authority.ID, authority.FenceGeneration)
		return requireExactlyOneMutation(res, err, "fail sqlite selected-contract runtime execution")
	})
}
