package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/google/uuid"
)

const standingRestartAbandonReason = "server_restart_abandon"

var ErrStandingServiceNotFound = errors.New("standing service not found")

type StandingServiceError struct {
	ServiceID string
	Err       error
	Detail    string
}

func (e *StandingServiceError) Error() string {
	if e == nil {
		return ""
	}
	if strings.TrimSpace(e.Detail) == "" {
		return fmt.Sprintf("%s: %s", e.Err, e.ServiceID)
	}
	return fmt.Sprintf("%s: %s: %s", e.Err, e.ServiceID, e.Detail)
}

func (e *StandingServiceError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type StandingServiceCandidate struct {
	ServiceID  string
	PackageKey string
	FlowID     string
	InstanceID string
	EntityID   string
	Source     runtimecorrelation.BundleSourceFact
}

func (c StandingServiceCandidate) Normalized() StandingServiceCandidate {
	c.ServiceID = strings.TrimSpace(c.ServiceID)
	c.PackageKey = strings.TrimSpace(c.PackageKey)
	c.FlowID = strings.TrimSpace(c.FlowID)
	c.InstanceID = strings.TrimSpace(c.InstanceID)
	c.EntityID = strings.TrimSpace(c.EntityID)
	c.Source = c.Source.Normalized()
	return c
}

func (c StandingServiceCandidate) Validate() error {
	c = c.Normalized()
	for field, value := range map[string]string{
		"service_id": c.ServiceID, "package_key": c.PackageKey, "flow_id": c.FlowID,
		"instance_id": c.InstanceID, "entity_id": c.EntityID,
		"bundle_hash": c.Source.BundleHash, "bundle_source": c.Source.BundleSource,
	} {
		if value == "" {
			return fmt.Errorf("standing service %s is required", field)
		}
	}
	for field, value := range map[string]string{"service_id": c.ServiceID, "entity_id": c.EntityID} {
		if _, err := uuid.Parse(value); err != nil {
			return fmt.Errorf("standing service %s must be a UUID: %w", field, err)
		}
	}
	wantServiceID := runtimeflowidentity.StandingServiceID(c.PackageKey, c.FlowID)
	if c.ServiceID != wantServiceID {
		return fmt.Errorf("standing service_id %s does not match package_key/flow_id owner %s", c.ServiceID, wantServiceID)
	}
	return nil
}

type StandingServiceReconciliation struct {
	ServiceID           string
	PackageKey          string
	FlowID              string
	InstanceID          string
	EntityID            string
	RunID               string
	Generation          int64
	PublicationSequence int64
	Transition          string
	EffectiveState      string
	BundleHash          string
	BundleSource        string
	Reason              string
}

type StandingServiceOperation struct {
	ServiceID string
	Actor     string
	Reason    string
}

type StandingServiceStatus struct {
	StandingServiceReconciliation
	DeclarationPresent bool
	OperatorOverride   string
	PublicationState   string
	OverrideActor      string
	OverrideReason     string
	OverrideAt         time.Time
}

func (o StandingServiceOperation) normalized() StandingServiceOperation {
	o.ServiceID = strings.TrimSpace(o.ServiceID)
	o.Actor = strings.TrimSpace(o.Actor)
	o.Reason = strings.TrimSpace(o.Reason)
	if o.Actor == "" {
		o.Actor = "operator"
	}
	return o
}

type standingServiceRow struct {
	StandingServiceReconciliation
	DeclarationPresent bool
	OperatorOverride   string
	RevisionSequence   int64
	PublicationState   string
	RunStatus          string
	RunControlReason   string
}

func (s *WorkflowInstanceStore) ReconcileStandingService(ctx context.Context, candidate StandingServiceCandidate) (StandingServiceReconciliation, error) {
	if s == nil || s.db == nil {
		return StandingServiceReconciliation{}, fmt.Errorf("workflow instance store is required")
	}
	candidate = candidate.Normalized()
	if err := candidate.Validate(); err != nil {
		return StandingServiceReconciliation{}, err
	}
	var result StandingServiceReconciliation
	err := s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		result, err = s.reconcileStandingServiceTx(txctx, tx, candidate)
		return err
	})
	return result, err
}

func (s *WorkflowInstanceStore) LoadReconciledStandingService(ctx context.Context, candidate StandingServiceCandidate) (StandingServiceReconciliation, bool, error) {
	candidate = candidate.Normalized()
	if err := candidate.Validate(); err != nil {
		return StandingServiceReconciliation{}, false, err
	}
	var result StandingServiceReconciliation
	var found bool
	err := s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		current, exists, err := s.loadStandingServiceTx(txctx, tx, candidate.ServiceID)
		if err != nil || !exists {
			return err
		}
		if current.PackageKey != candidate.PackageKey || current.FlowID != candidate.FlowID || current.InstanceID != candidate.InstanceID || current.EntityID != candidate.EntityID {
			return fmt.Errorf("standing service identity conflict for %s", candidate.ServiceID)
		}
		if !current.DeclarationPresent || current.BundleHash != candidate.Source.BundleHash || current.BundleSource != candidate.Source.BundleSource {
			return nil
		}
		transition := "resumed"
		reason := ""
		var journalErr error
		if s.isSQLite() {
			journalErr = tx.QueryRowContext(txctx, `SELECT transition, COALESCE(reason, '') FROM standing_service_journal WHERE service_id = ? ORDER BY sequence DESC LIMIT 1`, candidate.ServiceID).Scan(&transition, &reason)
		} else {
			journalErr = tx.QueryRowContext(txctx, `SELECT transition, COALESCE(reason, '') FROM standing_service_journal WHERE service_id = $1::uuid ORDER BY sequence DESC LIMIT 1`, candidate.ServiceID).Scan(&transition, &reason)
		}
		if journalErr != nil {
			return fmt.Errorf("load standing service %s latest journal entry: %w", candidate.ServiceID, journalErr)
		}
		result = standingResult(candidate, current.RunID, current.Generation, current.PublicationSequence, transition, current.EffectiveState, reason)
		found = true
		return nil
	})
	return result, found, err
}

// ReconcileStandingServiceSet is the startup desired-state owner. It validates
// the full declaration set before mutation and orphans persisted services that
// are no longer declared in that same selected-store transaction.
func (s *WorkflowInstanceStore) ReconcileStandingServiceSet(ctx context.Context, candidates []StandingServiceCandidate) ([]StandingServiceReconciliation, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("workflow instance store is required")
	}
	normalized, err := normalizeStandingServiceCandidates(candidates)
	if err != nil {
		return nil, err
	}

	results := make([]StandingServiceReconciliation, 0, len(normalized))
	err = s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		persisted, err := s.loadAllStandingServicesTx(txctx, tx)
		if err != nil {
			return err
		}
		declared := make(map[string]struct{}, len(normalized))
		for _, candidate := range normalized {
			declared[candidate.ServiceID] = struct{}{}
			result, err := s.reconcileStandingServiceTx(txctx, tx, candidate)
			if err != nil {
				return err
			}
			results = append(results, result)
		}
		for _, current := range persisted {
			if _, ok := declared[current.ServiceID]; ok || !current.DeclarationPresent {
				continue
			}
			result, err := s.orphanStandingServiceTx(txctx, tx, current)
			if err != nil {
				return err
			}
			results = append(results, result)
		}
		return nil
	})
	return results, err
}

// ReconcileStandingServiceReplacement is the hot-reload desired-state owner.
// It mutates only the predecessor declaration set so independently loaded
// runtime contexts remain outside the replacement transaction.
func (s *WorkflowInstanceStore) ReconcileStandingServiceReplacement(ctx context.Context, previous, candidates []StandingServiceCandidate) ([]StandingServiceReconciliation, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("workflow instance store is required")
	}
	previous, err := normalizeStandingServiceCandidates(previous)
	if err != nil {
		return nil, fmt.Errorf("validate predecessor standing declarations: %w", err)
	}
	candidates, err = normalizeStandingServiceCandidates(candidates)
	if err != nil {
		return nil, fmt.Errorf("validate replacement standing declarations: %w", err)
	}

	retained := make(map[string]struct{}, len(candidates))
	results := make([]StandingServiceReconciliation, 0, len(previous)+len(candidates))
	err = s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		for _, candidate := range candidates {
			retained[candidate.ServiceID] = struct{}{}
			result, err := s.reconcileStandingServiceTx(txctx, tx, candidate)
			if err != nil {
				return err
			}
			results = append(results, result)
		}
		for _, predecessor := range previous {
			if _, ok := retained[predecessor.ServiceID]; ok {
				continue
			}
			current, found, err := s.loadStandingServiceTx(txctx, tx, predecessor.ServiceID)
			if err != nil {
				return err
			}
			if !found {
				return fmt.Errorf("predecessor standing service %s is not persisted", predecessor.ServiceID)
			}
			if !current.DeclarationPresent {
				continue
			}
			result, err := s.orphanStandingServiceTx(txctx, tx, current)
			if err != nil {
				return err
			}
			results = append(results, result)
		}
		return nil
	})
	return results, err
}

func normalizeStandingServiceCandidates(candidates []StandingServiceCandidate) ([]StandingServiceCandidate, error) {
	normalized := make([]StandingServiceCandidate, 0, len(candidates))
	seenService := map[string]struct{}{}
	seenOwner := map[string]struct{}{}
	for _, raw := range candidates {
		candidate := raw.Normalized()
		if err := candidate.Validate(); err != nil {
			return nil, err
		}
		owner := candidate.PackageKey + "\x00" + candidate.FlowID
		if _, exists := seenService[candidate.ServiceID]; exists {
			return nil, fmt.Errorf("standing service %s is declared by more than one runtime context", candidate.ServiceID)
		}
		if _, exists := seenOwner[owner]; exists {
			return nil, fmt.Errorf("standing owner %s/%s is declared by more than one runtime context", candidate.PackageKey, candidate.FlowID)
		}
		seenService[candidate.ServiceID] = struct{}{}
		seenOwner[owner] = struct{}{}
		normalized = append(normalized, candidate)
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].ServiceID < normalized[j].ServiceID })
	return normalized, nil
}

func (s *WorkflowInstanceStore) SuspendStandingService(ctx context.Context, operation StandingServiceOperation) (StandingServiceReconciliation, error) {
	operation = operation.normalized()
	if operation.ServiceID == "" {
		return StandingServiceReconciliation{}, fmt.Errorf("standing service_id is required")
	}
	if operation.Reason == "" {
		operation.Reason = "operator_suspend"
	}
	var result StandingServiceReconciliation
	err := s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		current, found, err := s.loadStandingServiceTx(txctx, tx, operation.ServiceID)
		if err != nil {
			return err
		}
		if !found {
			return &StandingServiceError{ServiceID: operation.ServiceID, Err: ErrStandingServiceNotFound}
		}
		if !current.DeclarationPresent {
			return fmt.Errorf("standing service %s is orphaned; restore its declaration before suspending it", operation.ServiceID)
		}
		if current.OperatorOverride == "suspended" && current.EffectiveState == "suspended" {
			result = current.StandingServiceReconciliation
			result.Transition = "suspended"
			return nil
		}
		now := time.Now().UTC()
		if err := s.quiesceStandingRunTx(txctx, tx, current.RunID, "standing_suspended", "cancelled", now); err != nil {
			return err
		}
		if err := s.setStandingRunPausedTx(txctx, tx, current.RunID, operation.Reason, operation.Actor, now); err != nil {
			return err
		}
		if s.isSQLite() {
			_, err = tx.ExecContext(txctx, `UPDATE standing_services SET operator_override = 'suspended', effective_state = 'suspended', override_actor = ?, override_reason = NULLIF(?, ''), override_at = ?, publication_state = 'pending', updated_at = ? WHERE service_id = ?`, operation.Actor, operation.Reason, now, now, current.ServiceID)
		} else {
			_, err = tx.ExecContext(txctx, `UPDATE standing_services SET operator_override = 'suspended', effective_state = 'suspended', override_actor = $2, override_reason = NULLIF($3, ''), override_at = $4, publication_state = 'pending', updated_at = $4 WHERE service_id = $1::uuid`, current.ServiceID, operation.Actor, operation.Reason, now)
		}
		if err != nil {
			return fmt.Errorf("suspend standing service: %w", err)
		}
		result = current.StandingServiceReconciliation
		result.Transition = "suspended"
		result.EffectiveState = "suspended"
		result.Reason = operation.Reason
		return s.insertStandingJournalTx(txctx, tx, result, current.EffectiveState, current.BundleHash, current.BundleSource, operation.Actor, now)
	})
	return result, err
}

func (s *WorkflowInstanceStore) ResumeStandingService(ctx context.Context, operation StandingServiceOperation) (StandingServiceReconciliation, error) {
	operation = operation.normalized()
	if operation.ServiceID == "" {
		return StandingServiceReconciliation{}, fmt.Errorf("standing service_id is required")
	}
	if operation.Reason == "" {
		operation.Reason = "operator_resume"
	}
	var result StandingServiceReconciliation
	err := s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		current, found, err := s.loadStandingServiceTx(txctx, tx, operation.ServiceID)
		if err != nil {
			return err
		}
		if !found {
			return &StandingServiceError{ServiceID: operation.ServiceID, Err: ErrStandingServiceNotFound}
		}
		if !current.DeclarationPresent {
			return fmt.Errorf("standing service %s is orphaned; restore its declaration before running `swarm standing resume %s`", operation.ServiceID, operation.ServiceID)
		}
		if current.OperatorOverride == "none" && current.EffectiveState == "active" {
			result = current.StandingServiceReconciliation
			result.Transition = "operator_resumed"
			return nil
		}
		now := time.Now().UTC()
		if err := s.setStandingRunRunningTx(txctx, tx, current.RunID, operation.Reason, operation.Actor, now); err != nil {
			return err
		}
		if s.isSQLite() {
			_, err = tx.ExecContext(txctx, `UPDATE standing_services SET operator_override = 'none', effective_state = 'active', override_actor = NULL, override_reason = NULL, override_at = NULL, publication_state = 'pending', updated_at = ? WHERE service_id = ?`, now, current.ServiceID)
		} else {
			_, err = tx.ExecContext(txctx, `UPDATE standing_services SET operator_override = 'none', effective_state = 'active', override_actor = NULL, override_reason = NULL, override_at = NULL, publication_state = 'pending', updated_at = $2 WHERE service_id = $1::uuid`, current.ServiceID, now)
		}
		if err != nil {
			return fmt.Errorf("resume standing service: %w", err)
		}
		result = current.StandingServiceReconciliation
		result.Transition = "operator_resumed"
		result.EffectiveState = "active"
		result.Reason = operation.Reason
		return s.insertStandingJournalTx(txctx, tx, result, current.EffectiveState, current.BundleHash, current.BundleSource, operation.Actor, now)
	})
	return result, err
}

func (s *WorkflowInstanceStore) ResetStandingService(ctx context.Context, operation StandingServiceOperation) (StandingServiceReconciliation, error) {
	operation = operation.normalized()
	if operation.ServiceID == "" {
		return StandingServiceReconciliation{}, fmt.Errorf("standing service_id is required")
	}
	if operation.Reason == "" {
		operation.Reason = "standing_reset"
	}
	var result StandingServiceReconciliation
	err := s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		current, found, err := s.loadStandingServiceTx(txctx, tx, operation.ServiceID)
		if err != nil {
			return err
		}
		if !found {
			return &StandingServiceError{ServiceID: operation.ServiceID, Err: ErrStandingServiceNotFound}
		}
		if !current.DeclarationPresent {
			return fmt.Errorf("standing service %s is orphaned; restore its declaration before resetting it", operation.ServiceID)
		}
		now := time.Now().UTC()
		if current.RunStatus == "running" || current.RunStatus == "paused" {
			if err := s.quiesceStandingRunTx(txctx, tx, current.RunID, "standing_reset", "cancelled", now); err != nil {
				return err
			}
			if err := s.setStandingRunCancelledTx(txctx, tx, current.RunID, "standing_reset", operation.Actor, now); err != nil {
				return err
			}
		}
		nextGeneration := current.Generation + 1
		nextRunID := runtimeflowidentity.StandingGenerationRunID(current.ServiceID, nextGeneration)
		source := runtimecorrelation.BundleSourceFact{BundleHash: current.BundleHash, BundleSource: current.BundleSource}
		if err := s.insertStandingRunTx(txctx, tx, nextRunID, source, now); err != nil {
			return err
		}
		effectiveState := "active"
		if current.OperatorOverride == "suspended" {
			effectiveState = "suspended"
			if err := s.setStandingRunPausedTx(txctx, tx, nextRunID, "standing_reset_preserved_suspend", operation.Actor, now); err != nil {
				return err
			}
		}
		if s.isSQLite() {
			if _, err := tx.ExecContext(txctx, `UPDATE standing_service_generations SET retired_at = ?, retired_reason = 'standing_reset', retired_by = ? WHERE service_id = ? AND generation = ? AND retired_at IS NULL`, now, operation.Actor, current.ServiceID, current.Generation); err != nil {
				return err
			}
			if _, err := tx.ExecContext(txctx, `INSERT INTO standing_service_generations (service_id, generation, run_id, created_bundle_hash, created_bundle_source, created_at) VALUES (?, ?, ?, ?, ?, ?)`, current.ServiceID, nextGeneration, nextRunID, current.BundleHash, current.BundleSource, now); err != nil {
				return err
			}
			_, err = tx.ExecContext(txctx, `UPDATE standing_services SET current_generation = ?, current_run_id = ?, effective_state = ?, publication_state = 'pending', updated_at = ? WHERE service_id = ?`, nextGeneration, nextRunID, effectiveState, now, current.ServiceID)
		} else {
			if _, err := tx.ExecContext(txctx, `UPDATE standing_service_generations SET retired_at = $3, retired_reason = 'standing_reset', retired_by = $4 WHERE service_id = $1::uuid AND generation = $2 AND retired_at IS NULL`, current.ServiceID, current.Generation, now, operation.Actor); err != nil {
				return err
			}
			if _, err := tx.ExecContext(txctx, `INSERT INTO standing_service_generations (service_id, generation, run_id, created_bundle_hash, created_bundle_source, created_at) VALUES ($1::uuid, $2, $3::uuid, $4, $5, $6)`, current.ServiceID, nextGeneration, nextRunID, current.BundleHash, current.BundleSource, now); err != nil {
				return err
			}
			_, err = tx.ExecContext(txctx, `UPDATE standing_services SET current_generation = $2, current_run_id = $3::uuid, effective_state = $4, publication_state = 'pending', updated_at = $5 WHERE service_id = $1::uuid`, current.ServiceID, nextGeneration, nextRunID, effectiveState, now)
		}
		if err != nil {
			return fmt.Errorf("reset standing service: %w", err)
		}
		candidate := StandingServiceCandidate{ServiceID: current.ServiceID, PackageKey: current.PackageKey, FlowID: current.FlowID, InstanceID: current.InstanceID, EntityID: current.EntityID, Source: source}
		result = standingResult(candidate, nextRunID, nextGeneration, current.PublicationSequence, "reset", effectiveState, operation.Reason)
		return s.insertStandingJournalTx(txctx, tx, result, current.EffectiveState, current.BundleHash, current.BundleSource, operation.Actor, now)
	})
	return result, err
}

func (s *WorkflowInstanceStore) reconcileStandingServiceTx(ctx context.Context, tx *sql.Tx, candidate StandingServiceCandidate) (StandingServiceReconciliation, error) {
	current, found, err := s.loadStandingServiceTx(ctx, tx, candidate.ServiceID)
	if err != nil {
		return StandingServiceReconciliation{}, err
	}
	if !found {
		return s.createStandingServiceTx(ctx, tx, candidate)
	}
	if current.PackageKey != candidate.PackageKey || current.FlowID != candidate.FlowID || current.InstanceID != candidate.InstanceID || current.EntityID != candidate.EntityID {
		return StandingServiceReconciliation{}, fmt.Errorf("standing service identity conflict for %s", candidate.ServiceID)
	}
	switch current.RunStatus {
	case "running", "paused":
		return s.resumeStandingServiceTx(ctx, tx, current, candidate)
	case "cancelled":
		if current.RunControlReason != standingRestartAbandonReason {
			return StandingServiceReconciliation{}, standingResetRequiredError(current, "cancelled standing generation is not owned by restart abandonment")
		}
		if live, err := s.standingRunHasLiveWorkTx(ctx, tx, current.RunID); err != nil {
			return StandingServiceReconciliation{}, err
		} else if live {
			return StandingServiceReconciliation{}, standingResetRequiredError(current, "restart-abandoned generation still owns live work")
		}
		return s.repairStandingServiceTx(ctx, tx, current, candidate)
	default:
		return StandingServiceReconciliation{}, standingResetRequiredError(current, "standing generation is terminal")
	}
}

func (s *WorkflowInstanceStore) PublishStandingService(ctx context.Context, serviceID, runID string, generation int64) (int64, error) {
	serviceID = strings.TrimSpace(serviceID)
	runID = strings.TrimSpace(runID)
	if serviceID == "" || runID == "" || generation <= 0 {
		return 0, fmt.Errorf("standing publication requires service_id, run_id, and generation")
	}
	var sequence int64
	err := s.runInPipelineTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if s.isSQLite() {
			result, err := tx.ExecContext(txctx, `
				UPDATE standing_services
				SET publication_state = 'published', publication_sequence = publication_sequence + 1, updated_at = ?
				WHERE service_id = ? AND current_run_id = ? AND current_generation = ? AND effective_state = 'active'
			`, time.Now().UTC(), serviceID, runID, generation)
			if err != nil {
				return err
			}
			if err := requireOneStandingRow(result); err != nil {
				return err
			}
			return tx.QueryRowContext(txctx, `SELECT publication_sequence FROM standing_services WHERE service_id = ?`, serviceID).Scan(&sequence)
		}
		return tx.QueryRowContext(txctx, `
			UPDATE standing_services
			SET publication_state = 'published', publication_sequence = publication_sequence + 1, updated_at = now()
			WHERE service_id = $1::uuid AND current_run_id = $2::uuid AND current_generation = $3 AND effective_state = 'active'
			RETURNING publication_sequence
		`, serviceID, runID, generation).Scan(&sequence)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("standing service changed before ingress publication")
	}
	return sequence, err
}

func (s *WorkflowInstanceStore) StandingRunUsesIntrinsicRecovery(ctx context.Context, runID string) (bool, error) {
	runID = strings.TrimSpace(runID)
	if s == nil || s.db == nil || runID == "" {
		return false, nil
	}
	queryer := interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	}(s.db)
	if tx, ok := PipelineSQLTxFromContext(ctx); ok && tx != nil {
		queryer = tx
	}
	var active bool
	if s.isSQLite() {
		err := queryer.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM standing_services
				WHERE current_run_id = ? AND declaration_present = TRUE AND effective_state = 'active'
			)
		`, runID).Scan(&active)
		return active, err
	}
	err := queryer.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM standing_services
			WHERE current_run_id = $1::uuid AND declaration_present = TRUE AND effective_state = 'active'
		)
	`, runID).Scan(&active)
	return active, err
}

func (s *WorkflowInstanceStore) ListStandingServiceStatuses(ctx context.Context) ([]StandingServiceStatus, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("workflow instance store is required")
	}
	query := `
		SELECT ss.service_id, ss.package_key, ss.flow_id, ss.instance_id, ss.entity_id,
		       ss.current_run_id, ss.current_generation, ss.publication_sequence,
		       ss.effective_state, ss.current_bundle_hash, ss.current_bundle_source,
		       ss.declaration_present, ss.operator_override, ss.publication_state,
		       COALESCE(ss.override_actor, ''), COALESCE(ss.override_reason, ''), ss.override_at,
		       COALESCE((SELECT transition FROM standing_service_journal j WHERE j.service_id = ss.service_id ORDER BY sequence DESC LIMIT 1), 'resumed'),
		       COALESCE((SELECT reason FROM standing_service_journal j WHERE j.service_id = ss.service_id ORDER BY sequence DESC LIMIT 1), '')
		FROM standing_services ss
		ORDER BY ss.package_key, ss.flow_id
	`
	if !s.isSQLite() {
		query = `
			SELECT ss.service_id::text, ss.package_key, ss.flow_id, ss.instance_id, ss.entity_id::text,
			       ss.current_run_id::text, ss.current_generation, ss.publication_sequence,
			       ss.effective_state, ss.current_bundle_hash, ss.current_bundle_source,
			       ss.declaration_present, ss.operator_override, ss.publication_state,
			       COALESCE(ss.override_actor, ''), COALESCE(ss.override_reason, ''), ss.override_at,
			       COALESCE((SELECT transition FROM standing_service_journal j WHERE j.service_id = ss.service_id ORDER BY sequence DESC LIMIT 1), 'resumed'),
			       COALESCE((SELECT reason FROM standing_service_journal j WHERE j.service_id = ss.service_id ORDER BY sequence DESC LIMIT 1), '')
			FROM standing_services ss
			ORDER BY ss.package_key, ss.flow_id
		`
	}
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list standing service statuses: %w", err)
	}
	defer rows.Close()
	var out []StandingServiceStatus
	for rows.Next() {
		var status StandingServiceStatus
		var overrideAt any
		if err := rows.Scan(
			&status.ServiceID, &status.PackageKey, &status.FlowID, &status.InstanceID, &status.EntityID,
			&status.RunID, &status.Generation, &status.PublicationSequence,
			&status.EffectiveState, &status.BundleHash, &status.BundleSource,
			&status.DeclarationPresent, &status.OperatorOverride, &status.PublicationState,
			&status.OverrideActor, &status.OverrideReason, &overrideAt, &status.Transition, &status.Reason,
		); err != nil {
			return nil, fmt.Errorf("scan standing service status: %w", err)
		}
		if overrideAt != nil {
			switch value := overrideAt.(type) {
			case time.Time:
				status.OverrideAt = value.UTC()
			case string:
				parsed, err := parseStandingTimestamp(value)
				if err != nil {
					return nil, fmt.Errorf("parse standing override time: %w", err)
				}
				status.OverrideAt = parsed.UTC()
			case []byte:
				parsed, err := parseStandingTimestamp(string(value))
				if err != nil {
					return nil, fmt.Errorf("parse standing override time: %w", err)
				}
				status.OverrideAt = parsed.UTC()
			}
		}
		out = append(out, status)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read standing service statuses: %w", err)
	}
	return out, nil
}

func parseStandingTimestamp(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	var lastErr error
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999 -0700 MST", "2006-01-02 15:04:05.999999999Z07:00"} {
		parsed, err := time.Parse(layout, raw)
		if err == nil {
			return parsed, nil
		}
		lastErr = err
	}
	return time.Time{}, lastErr
}

func (s *WorkflowInstanceStore) loadStandingServiceTx(ctx context.Context, tx *sql.Tx, serviceID string) (standingServiceRow, bool, error) {
	var row standingServiceRow
	var query string
	if s.isSQLite() {
		query = `
			SELECT ss.service_id, ss.package_key, ss.flow_id, ss.instance_id, ss.entity_id,
			       ss.current_run_id, ss.current_generation, ss.publication_sequence,
			       ss.declaration_present, ss.operator_override, ss.effective_state,
			       ss.current_bundle_hash, ss.current_bundle_source, ss.revision_sequence,
			       ss.publication_state, COALESCE(r.status, ''), COALESCE(rc.reason, '')
			FROM standing_services ss
			JOIN runs r ON r.run_id = ss.current_run_id
			LEFT JOIN run_control_state rc ON rc.run_id = ss.current_run_id
			WHERE ss.service_id = ?
		`
	} else {
		query = `
			SELECT ss.service_id::text, ss.package_key, ss.flow_id, ss.instance_id, ss.entity_id::text,
			       ss.current_run_id::text, ss.current_generation, ss.publication_sequence,
			       ss.declaration_present, ss.operator_override, ss.effective_state,
			       ss.current_bundle_hash, ss.current_bundle_source, ss.revision_sequence,
			       ss.publication_state, COALESCE(r.status, ''), COALESCE(rc.reason, '')
			FROM standing_services ss
			JOIN runs r ON r.run_id = ss.current_run_id
			LEFT JOIN run_control_state rc ON rc.run_id = ss.current_run_id
			WHERE ss.service_id = $1::uuid
			FOR UPDATE OF ss, r
		`
	}
	err := tx.QueryRowContext(ctx, query, serviceID).Scan(
		&row.ServiceID, &row.PackageKey, &row.FlowID, &row.InstanceID, &row.EntityID,
		&row.RunID, &row.Generation, &row.PublicationSequence,
		&row.DeclarationPresent, &row.OperatorOverride, &row.EffectiveState,
		&row.BundleHash, &row.BundleSource, &row.RevisionSequence,
		&row.PublicationState, &row.RunStatus, &row.RunControlReason,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return standingServiceRow{}, false, nil
	}
	if err != nil {
		return standingServiceRow{}, false, fmt.Errorf("load standing service %s: %w", serviceID, err)
	}
	return row, true, nil
}

func (s *WorkflowInstanceStore) loadAllStandingServicesTx(ctx context.Context, tx *sql.Tx) ([]standingServiceRow, error) {
	query := `
		SELECT ss.service_id, ss.package_key, ss.flow_id, ss.instance_id, ss.entity_id,
		       ss.current_run_id, ss.current_generation, ss.publication_sequence,
		       ss.declaration_present, ss.operator_override, ss.effective_state,
		       ss.current_bundle_hash, ss.current_bundle_source, ss.revision_sequence,
		       ss.publication_state, COALESCE(r.status, ''), COALESCE(rc.reason, '')
		FROM standing_services ss
		JOIN runs r ON r.run_id = ss.current_run_id
		LEFT JOIN run_control_state rc ON rc.run_id = ss.current_run_id
		ORDER BY ss.service_id
	`
	if !s.isSQLite() {
		query = `
			SELECT ss.service_id::text, ss.package_key, ss.flow_id, ss.instance_id, ss.entity_id::text,
			       ss.current_run_id::text, ss.current_generation, ss.publication_sequence,
			       ss.declaration_present, ss.operator_override, ss.effective_state,
			       ss.current_bundle_hash, ss.current_bundle_source, ss.revision_sequence,
			       ss.publication_state, COALESCE(r.status, ''), COALESCE(rc.reason, '')
			FROM standing_services ss
			JOIN runs r ON r.run_id = ss.current_run_id
			LEFT JOIN run_control_state rc ON rc.run_id = ss.current_run_id
			ORDER BY ss.service_id
			FOR UPDATE OF ss, r
		`
	}
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("load standing service set: %w", err)
	}
	defer rows.Close()
	var out []standingServiceRow
	for rows.Next() {
		var row standingServiceRow
		if err := rows.Scan(
			&row.ServiceID, &row.PackageKey, &row.FlowID, &row.InstanceID, &row.EntityID,
			&row.RunID, &row.Generation, &row.PublicationSequence,
			&row.DeclarationPresent, &row.OperatorOverride, &row.EffectiveState,
			&row.BundleHash, &row.BundleSource, &row.RevisionSequence,
			&row.PublicationState, &row.RunStatus, &row.RunControlReason,
		); err != nil {
			return nil, fmt.Errorf("scan standing service set: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read standing service set: %w", err)
	}
	return out, nil
}

func (s *WorkflowInstanceStore) createStandingServiceTx(ctx context.Context, tx *sql.Tx, candidate StandingServiceCandidate) (StandingServiceReconciliation, error) {
	generation := int64(1)
	runID := runtimeflowidentity.StandingGenerationRunID(candidate.ServiceID, generation)
	now := time.Now().UTC()
	if err := s.insertStandingRunTx(ctx, tx, runID, candidate.Source, now); err != nil {
		return StandingServiceReconciliation{}, err
	}
	if s.isSQLite() {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO standing_services (
				service_id, package_key, flow_id, instance_id, entity_id, declaration_present,
				operator_override, effective_state, current_bundle_hash, current_bundle_source,
				revision_sequence, current_generation, current_run_id, publication_state,
				publication_sequence, created_at, updated_at
			) VALUES (?, ?, ?, ?, ?, TRUE, 'none', 'active', ?, ?, 1, ?, ?, 'pending', 0, ?, ?)
		`, candidate.ServiceID, candidate.PackageKey, candidate.FlowID, candidate.InstanceID, candidate.EntityID,
			candidate.Source.BundleHash, candidate.Source.BundleSource, generation, runID, now, now); err != nil {
			return StandingServiceReconciliation{}, fmt.Errorf("insert standing service: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO standing_service_generations (service_id, generation, run_id, created_bundle_hash, created_bundle_source, created_at)
			VALUES (?, ?, ?, ?, ?, ?)
		`, candidate.ServiceID, generation, runID, candidate.Source.BundleHash, candidate.Source.BundleSource, now); err != nil {
			return StandingServiceReconciliation{}, fmt.Errorf("insert standing generation: %w", err)
		}
	} else {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO standing_services (
				service_id, package_key, flow_id, instance_id, entity_id, declaration_present,
				operator_override, effective_state, current_bundle_hash, current_bundle_source,
				revision_sequence, current_generation, current_run_id, publication_state,
				publication_sequence, created_at, updated_at
			) VALUES ($1::uuid, $2, $3, $4, $5::uuid, TRUE, 'none', 'active', $6, $7, 1, $8, $9::uuid, 'pending', 0, $10, $10)
		`, candidate.ServiceID, candidate.PackageKey, candidate.FlowID, candidate.InstanceID, candidate.EntityID,
			candidate.Source.BundleHash, candidate.Source.BundleSource, generation, runID, now); err != nil {
			return StandingServiceReconciliation{}, fmt.Errorf("insert standing service: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO standing_service_generations (service_id, generation, run_id, created_bundle_hash, created_bundle_source, created_at)
			VALUES ($1::uuid, $2, $3::uuid, $4, $5, $6)
		`, candidate.ServiceID, generation, runID, candidate.Source.BundleHash, candidate.Source.BundleSource, now); err != nil {
			return StandingServiceReconciliation{}, fmt.Errorf("insert standing generation: %w", err)
		}
	}
	result := standingResult(candidate, runID, generation, 0, "created", "active", "")
	if err := s.insertStandingJournalTx(ctx, tx, result, "", "", "", "runtime", now); err != nil {
		return StandingServiceReconciliation{}, err
	}
	return result, nil
}

func (s *WorkflowInstanceStore) resumeStandingServiceTx(ctx context.Context, tx *sql.Tx, current standingServiceRow, candidate StandingServiceCandidate) (StandingServiceReconciliation, error) {
	transition := "resumed"
	if current.BundleHash != candidate.Source.BundleHash || current.BundleSource != candidate.Source.BundleSource {
		transition = "revised"
	}
	effectiveState := "active"
	if current.OperatorOverride == "suspended" {
		effectiveState = "suspended"
	}
	revisionSequence := current.RevisionSequence
	if transition == "revised" {
		revisionSequence++
	}
	now := time.Now().UTC()
	if effectiveState == "active" {
		if err := s.setStandingRunRunningTx(ctx, tx, current.RunID, "standing_reconcile", "runtime", now); err != nil {
			return StandingServiceReconciliation{}, err
		}
	} else if err := s.setStandingRunPausedTx(ctx, tx, current.RunID, "standing_suspended", "runtime", now); err != nil {
		return StandingServiceReconciliation{}, err
	}
	if s.isSQLite() {
		if _, err := tx.ExecContext(ctx, `UPDATE runs SET bundle_hash = ?, bundle_source = ?, bundle_fingerprint = NULLIF(?, '') WHERE run_id = ? AND status IN ('running', 'paused')`, candidate.Source.BundleHash, candidate.Source.BundleSource, candidate.Source.BundleFingerprint, current.RunID); err != nil {
			return StandingServiceReconciliation{}, fmt.Errorf("update standing run revision: %w", err)
		}
		_, err := tx.ExecContext(ctx, `
			UPDATE standing_services
			SET declaration_present = TRUE, effective_state = ?, current_bundle_hash = ?, current_bundle_source = ?,
			    revision_sequence = ?, publication_state = 'pending', updated_at = ?
			WHERE service_id = ?
		`, effectiveState, candidate.Source.BundleHash, candidate.Source.BundleSource, revisionSequence, now, candidate.ServiceID)
		if err != nil {
			return StandingServiceReconciliation{}, err
		}
	} else {
		if _, err := tx.ExecContext(ctx, `UPDATE runs SET bundle_hash = $2, bundle_source = $3, bundle_fingerprint = NULLIF($4, '') WHERE run_id = $1::uuid AND status IN ('running', 'paused')`, current.RunID, candidate.Source.BundleHash, candidate.Source.BundleSource, candidate.Source.BundleFingerprint); err != nil {
			return StandingServiceReconciliation{}, fmt.Errorf("update standing run revision: %w", err)
		}
		_, err := tx.ExecContext(ctx, `
			UPDATE standing_services
			SET declaration_present = TRUE, effective_state = $2, current_bundle_hash = $3, current_bundle_source = $4,
			    revision_sequence = $5, publication_state = 'pending', updated_at = $6
			WHERE service_id = $1::uuid
		`, candidate.ServiceID, effectiveState, candidate.Source.BundleHash, candidate.Source.BundleSource, revisionSequence, now)
		if err != nil {
			return StandingServiceReconciliation{}, err
		}
	}
	result := standingResult(candidate, current.RunID, current.Generation, current.PublicationSequence, transition, effectiveState, "")
	if err := s.insertStandingJournalTx(ctx, tx, result, current.EffectiveState, current.BundleHash, current.BundleSource, "runtime", now); err != nil {
		return StandingServiceReconciliation{}, err
	}
	return result, nil
}

func (s *WorkflowInstanceStore) repairStandingServiceTx(ctx context.Context, tx *sql.Tx, current standingServiceRow, candidate StandingServiceCandidate) (StandingServiceReconciliation, error) {
	nextGeneration := current.Generation + 1
	nextRunID := runtimeflowidentity.StandingGenerationRunID(candidate.ServiceID, nextGeneration)
	now := time.Now().UTC()
	if err := s.insertStandingRunTx(ctx, tx, nextRunID, candidate.Source, now); err != nil {
		return StandingServiceReconciliation{}, err
	}
	if err := s.copyStandingEntityStateTx(ctx, tx, current.RunID, nextRunID, candidate.EntityID); err != nil {
		return StandingServiceReconciliation{}, err
	}
	if s.isSQLite() {
		if _, err := tx.ExecContext(ctx, `UPDATE standing_service_generations SET retired_at = ?, retired_reason = ?, retired_by = 'runtime' WHERE service_id = ? AND generation = ? AND retired_at IS NULL`, now, standingRestartAbandonReason, candidate.ServiceID, current.Generation); err != nil {
			return StandingServiceReconciliation{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO standing_service_generations (service_id, generation, run_id, created_bundle_hash, created_bundle_source, created_at) VALUES (?, ?, ?, ?, ?, ?)`, candidate.ServiceID, nextGeneration, nextRunID, candidate.Source.BundleHash, candidate.Source.BundleSource, now); err != nil {
			return StandingServiceReconciliation{}, err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE standing_services
			SET declaration_present = TRUE, effective_state = CASE WHEN operator_override = 'suspended' THEN 'suspended' ELSE 'active' END,
			    current_bundle_hash = ?, current_bundle_source = ?, revision_sequence = revision_sequence + 1,
			    current_generation = ?, current_run_id = ?, publication_state = 'pending', updated_at = ?
			WHERE service_id = ?
		`, candidate.Source.BundleHash, candidate.Source.BundleSource, nextGeneration, nextRunID, now, candidate.ServiceID); err != nil {
			return StandingServiceReconciliation{}, err
		}
	} else {
		if _, err := tx.ExecContext(ctx, `UPDATE standing_service_generations SET retired_at = $3, retired_reason = $4, retired_by = 'runtime' WHERE service_id = $1::uuid AND generation = $2 AND retired_at IS NULL`, candidate.ServiceID, current.Generation, now, standingRestartAbandonReason); err != nil {
			return StandingServiceReconciliation{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO standing_service_generations (service_id, generation, run_id, created_bundle_hash, created_bundle_source, created_at) VALUES ($1::uuid, $2, $3::uuid, $4, $5, $6)`, candidate.ServiceID, nextGeneration, nextRunID, candidate.Source.BundleHash, candidate.Source.BundleSource, now); err != nil {
			return StandingServiceReconciliation{}, err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE standing_services
			SET declaration_present = TRUE, effective_state = CASE WHEN operator_override = 'suspended' THEN 'suspended' ELSE 'active' END,
			    current_bundle_hash = $2, current_bundle_source = $3, revision_sequence = revision_sequence + 1,
			    current_generation = $4, current_run_id = $5::uuid, publication_state = 'pending', updated_at = $6
			WHERE service_id = $1::uuid
		`, candidate.ServiceID, candidate.Source.BundleHash, candidate.Source.BundleSource, nextGeneration, nextRunID, now); err != nil {
			return StandingServiceReconciliation{}, err
		}
	}
	effectiveState := "active"
	if current.OperatorOverride == "suspended" {
		effectiveState = "suspended"
	}
	result := standingResult(candidate, nextRunID, nextGeneration, current.PublicationSequence, "repaired", effectiveState, standingRestartAbandonReason)
	if err := s.insertStandingJournalTx(ctx, tx, result, current.EffectiveState, current.BundleHash, current.BundleSource, "runtime", now); err != nil {
		return StandingServiceReconciliation{}, err
	}
	return result, nil
}

func (s *WorkflowInstanceStore) orphanStandingServiceTx(ctx context.Context, tx *sql.Tx, current standingServiceRow) (StandingServiceReconciliation, error) {
	if current.RunStatus != "running" && current.RunStatus != "paused" {
		return StandingServiceReconciliation{}, standingResetRequiredError(current, "removed declaration points at a terminal generation")
	}
	now := time.Now().UTC()
	if err := s.quiesceStandingRunTx(ctx, tx, current.RunID, "standing_declaration_removed", "orphaned", now); err != nil {
		return StandingServiceReconciliation{}, err
	}
	if err := s.setStandingRunPausedTx(ctx, tx, current.RunID, "standing_declaration_removed", "runtime", now); err != nil {
		return StandingServiceReconciliation{}, err
	}
	var err error
	if s.isSQLite() {
		_, err = tx.ExecContext(ctx, `UPDATE standing_services SET declaration_present = FALSE, effective_state = 'orphaned', publication_state = 'pending', updated_at = ? WHERE service_id = ?`, now, current.ServiceID)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE standing_services SET declaration_present = FALSE, effective_state = 'orphaned', publication_state = 'pending', updated_at = $2 WHERE service_id = $1::uuid`, current.ServiceID, now)
	}
	if err != nil {
		return StandingServiceReconciliation{}, fmt.Errorf("orphan standing service: %w", err)
	}
	result := current.StandingServiceReconciliation
	result.Transition = "orphaned"
	result.EffectiveState = "orphaned"
	result.Reason = "standing_declaration_removed"
	if err := s.insertStandingJournalTx(ctx, tx, result, current.EffectiveState, current.BundleHash, current.BundleSource, "runtime", now); err != nil {
		return StandingServiceReconciliation{}, err
	}
	return result, nil
}

func (s *WorkflowInstanceStore) standingRunHasLiveWorkTx(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	query := `
		SELECT EXISTS (
			SELECT 1 FROM event_deliveries WHERE run_id = ? AND status IN ('pending', 'in_progress', 'failed')
			UNION ALL SELECT 1 FROM agent_sessions WHERE run_id = ? AND status IN ('active', 'suspended')
			UNION ALL SELECT 1 FROM timers WHERE run_id = ? AND status = 'active'
		)
	`
	args := []any{runID, runID, runID}
	if !s.isSQLite() {
		query = `
			SELECT EXISTS (
				SELECT 1 FROM event_deliveries WHERE run_id = $1::uuid AND status IN ('pending', 'in_progress', 'failed')
				UNION ALL SELECT 1 FROM agent_sessions WHERE run_id = $1::uuid AND status IN ('active', 'suspended')
				UNION ALL SELECT 1 FROM timers WHERE run_id = $1::uuid AND status = 'active'
			)
		`
		args = []any{runID}
	}
	var live bool
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&live); err != nil {
		return false, fmt.Errorf("inspect standing run live work: %w", err)
	}
	return live, nil
}

func (s *WorkflowInstanceStore) quiesceStandingRunTx(ctx context.Context, tx *sql.Tx, runID, reason, sessionReason string, now time.Time) error {
	diagnosticTypes := events.DiagnosticDirectEventTypes()
	if s.isSQLite() {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO event_receipts (event_id, subscriber_type, subscriber_id, outcome, reason_code, processed_at)
			SELECT event_id, subscriber_type, subscriber_id, 'dead_letter', ?, ?
			FROM event_deliveries
			WHERE run_id = ? AND status IN ('pending', 'in_progress', 'failed')
			ON CONFLICT(event_id, subscriber_type, subscriber_id) DO UPDATE SET
				outcome = 'dead_letter', reason_code = excluded.reason_code, failure = NULL, processed_at = excluded.processed_at
		`, reason, now, runID); err != nil {
			return fmt.Errorf("settle sqlite standing delivery receipts: %w", err)
		}
		diagnosticPlaceholders := strings.TrimRight(strings.Repeat("?,", len(diagnosticTypes)), ",")
		pipelineArgs := []any{reason, now, runID}
		for _, eventType := range diagnosticTypes {
			pipelineArgs = append(pipelineArgs, string(eventType))
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO event_receipts (event_id, subscriber_type, subscriber_id, outcome, reason_code, processed_at)
			SELECT e.event_id, 'platform', 'pipeline', 'dead_letter', ?, ?
			FROM events e
			LEFT JOIN event_receipts r
			  ON r.event_id = e.event_id
			 AND r.subscriber_type = 'platform'
			 AND r.subscriber_id = 'pipeline'
			WHERE e.run_id = ?
			  AND e.event_name NOT IN (`+diagnosticPlaceholders+`)
			  AND (r.event_id IS NULL OR COALESCE(r.outcome, '') <> 'success')
			ON CONFLICT(event_id, subscriber_type, subscriber_id) DO UPDATE SET
				outcome = 'dead_letter', reason_code = excluded.reason_code, failure = NULL, processed_at = excluded.processed_at
		`, pipelineArgs...); err != nil {
			return fmt.Errorf("settle sqlite standing pipeline receipts: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE event_deliveries SET status = 'dead_letter', reason_code = ?, failure = NULL, active_session_id = NULL, delivered_at = COALESCE(delivered_at, ?) WHERE run_id = ? AND status IN ('pending', 'in_progress', 'failed')`, reason, now, runID); err != nil {
			return fmt.Errorf("terminalize sqlite standing deliveries: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE agent_sessions SET status = 'terminated', termination_reason = ?, termination_detail = ?, terminated_at = COALESCE(terminated_at, ?), lease_holder = NULL, lease_expires_at = NULL, updated_at = ? WHERE run_id = ? AND status IN ('active', 'suspended')`, sessionReason, reason, now, now, runID); err != nil {
			return fmt.Errorf("terminate sqlite standing sessions: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE timers SET status = 'cancelled' WHERE run_id = ? AND status = 'active'`, runID); err != nil {
			return fmt.Errorf("cancel sqlite standing timers: %w", err)
		}
		return nil
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO event_receipts (event_id, subscriber_type, subscriber_id, outcome, reason_code, processed_at)
		SELECT event_id, subscriber_type, subscriber_id, 'dead_letter', $2, $3::timestamptz
		FROM event_deliveries
		WHERE run_id = $1::uuid AND status IN ('pending', 'in_progress', 'failed')
		ON CONFLICT(event_id, subscriber_type, subscriber_id) DO UPDATE SET
			outcome = 'dead_letter', reason_code = EXCLUDED.reason_code, failure = NULL, processed_at = EXCLUDED.processed_at
	`, runID, reason, now); err != nil {
		return fmt.Errorf("settle standing delivery receipts: %w", err)
	}
	diagnosticPlaceholders := make([]string, 0, len(diagnosticTypes))
	pipelineArgs := []any{runID, reason, now}
	for i, eventType := range diagnosticTypes {
		diagnosticPlaceholders = append(diagnosticPlaceholders, fmt.Sprintf("$%d", i+4))
		pipelineArgs = append(pipelineArgs, string(eventType))
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO event_receipts (event_id, subscriber_type, subscriber_id, outcome, reason_code, processed_at)
		SELECT e.event_id, 'platform', 'pipeline', 'dead_letter', $2, $3::timestamptz
		FROM events e
		LEFT JOIN event_receipts r
		  ON r.event_id = e.event_id
		 AND r.subscriber_type = 'platform'
		 AND r.subscriber_id = 'pipeline'
		WHERE e.run_id = $1::uuid
		  AND e.event_name NOT IN (`+strings.Join(diagnosticPlaceholders, ", ")+`)
		  AND (r.event_id IS NULL OR COALESCE(r.outcome, '') <> 'success')
		ON CONFLICT(event_id, subscriber_type, subscriber_id) DO UPDATE SET
			outcome = 'dead_letter', reason_code = EXCLUDED.reason_code, failure = NULL, processed_at = EXCLUDED.processed_at
	`, pipelineArgs...); err != nil {
		return fmt.Errorf("settle standing pipeline receipts: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE event_deliveries SET status = 'dead_letter', reason_code = $2, failure = NULL, active_session_id = NULL, delivered_at = COALESCE(delivered_at, $3) WHERE run_id = $1::uuid AND status IN ('pending', 'in_progress', 'failed')`, runID, reason, now); err != nil {
		return fmt.Errorf("terminalize standing deliveries: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE agent_sessions SET status = 'terminated', termination_reason = $2, termination_detail = $3, terminated_at = COALESCE(terminated_at, $4), lease_holder = NULL, lease_expires_at = NULL, updated_at = $4 WHERE run_id = $1::uuid AND status IN ('active', 'suspended')`, runID, sessionReason, reason, now); err != nil {
		return fmt.Errorf("terminate standing sessions: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE timers SET status = 'cancelled' WHERE run_id = $1::uuid AND status = 'active'`, runID); err != nil {
		return fmt.Errorf("cancel standing timers: %w", err)
	}
	return nil
}

func (s *WorkflowInstanceStore) setStandingRunPausedTx(ctx context.Context, tx *sql.Tx, runID, reason, actor string, now time.Time) error {
	if s.isSQLite() {
		if _, err := tx.ExecContext(ctx, `UPDATE runs SET status = 'paused', ended_at = NULL, failure = NULL WHERE run_id = ? AND status IN ('running', 'paused')`, runID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at) VALUES (?, 'paused', ?, ?, ?, ?, NULL) ON CONFLICT(run_id) DO UPDATE SET control_status = 'paused', reason = excluded.reason, controlled_by = excluded.controlled_by, updated_at = excluded.updated_at, paused_at = COALESCE(run_control_state.paused_at, excluded.paused_at), stopped_at = NULL`, runID, reason, actor, now, now)
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET status = 'paused', ended_at = NULL, failure = NULL WHERE run_id = $1::uuid AND status IN ('running', 'paused')`, runID); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at) VALUES ($1::uuid, 'paused', $2, $3, $4, $4, NULL) ON CONFLICT(run_id) DO UPDATE SET control_status = 'paused', reason = EXCLUDED.reason, controlled_by = EXCLUDED.controlled_by, updated_at = EXCLUDED.updated_at, paused_at = COALESCE(run_control_state.paused_at, EXCLUDED.paused_at), stopped_at = NULL`, runID, reason, actor, now)
	return err
}

func (s *WorkflowInstanceStore) setStandingRunRunningTx(ctx context.Context, tx *sql.Tx, runID, reason, actor string, now time.Time) error {
	if s.isSQLite() {
		result, err := tx.ExecContext(ctx, `UPDATE runs SET status = 'running', ended_at = NULL, failure = NULL WHERE run_id = ? AND status = 'paused'`, runID)
		if err != nil {
			return err
		}
		if count, _ := result.RowsAffected(); count == 0 {
			var status string
			if err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = ?`, runID).Scan(&status); err != nil || status != "running" {
				return fmt.Errorf("standing run %s cannot resume from status %s", runID, status)
			}
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at) VALUES (?, 'running', ?, ?, ?, NULL, NULL) ON CONFLICT(run_id) DO UPDATE SET control_status = 'running', reason = excluded.reason, controlled_by = excluded.controlled_by, updated_at = excluded.updated_at, paused_at = NULL, stopped_at = NULL`, runID, reason, actor, now)
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE runs SET status = 'running', ended_at = NULL, failure = NULL WHERE run_id = $1::uuid AND status = 'paused'`, runID)
	if err != nil {
		return err
	}
	if count, _ := result.RowsAffected(); count == 0 {
		var status string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, runID).Scan(&status); err != nil || status != "running" {
			return fmt.Errorf("standing run %s cannot resume from status %s", runID, status)
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at) VALUES ($1::uuid, 'running', $2, $3, $4, NULL, NULL) ON CONFLICT(run_id) DO UPDATE SET control_status = 'running', reason = EXCLUDED.reason, controlled_by = EXCLUDED.controlled_by, updated_at = EXCLUDED.updated_at, paused_at = NULL, stopped_at = NULL`, runID, reason, actor, now)
	return err
}

func (s *WorkflowInstanceStore) setStandingRunCancelledTx(ctx context.Context, tx *sql.Tx, runID, reason, actor string, now time.Time) error {
	if s.isSQLite() {
		if _, err := tx.ExecContext(ctx, `UPDATE runs SET status = 'cancelled', failure = NULL, ended_at = COALESCE(ended_at, ?) WHERE run_id = ? AND status IN ('running', 'paused')`, now, runID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at) VALUES (?, 'stopped', ?, ?, ?, NULL, ?) ON CONFLICT(run_id) DO UPDATE SET control_status = 'stopped', reason = excluded.reason, controlled_by = excluded.controlled_by, updated_at = excluded.updated_at, paused_at = NULL, stopped_at = excluded.stopped_at`, runID, reason, actor, now, now)
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET status = 'cancelled', failure = NULL, ended_at = COALESCE(ended_at, $2) WHERE run_id = $1::uuid AND status IN ('running', 'paused')`, runID, now); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at) VALUES ($1::uuid, 'stopped', $2, $3, $4, NULL, $4) ON CONFLICT(run_id) DO UPDATE SET control_status = 'stopped', reason = EXCLUDED.reason, controlled_by = EXCLUDED.controlled_by, updated_at = EXCLUDED.updated_at, paused_at = NULL, stopped_at = EXCLUDED.stopped_at`, runID, reason, actor, now)
	return err
}

func (s *WorkflowInstanceStore) insertStandingRunTx(ctx context.Context, tx *sql.Tx, runID string, source runtimecorrelation.BundleSourceFact, now time.Time) error {
	if s.isSQLite() {
		_, err := tx.ExecContext(ctx, `INSERT INTO runs (run_id, status, bundle_hash, bundle_source, bundle_fingerprint, started_at) VALUES (?, 'running', ?, ?, NULLIF(?, ''), ?)`, runID, source.BundleHash, source.BundleSource, source.BundleFingerprint, now)
		if err != nil {
			return fmt.Errorf("insert standing run: %w", err)
		}
		return nil
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO runs (run_id, status, bundle_hash, bundle_source, bundle_fingerprint, started_at) VALUES ($1::uuid, 'running', $2, $3, NULLIF($4, ''), $5)`, runID, source.BundleHash, source.BundleSource, source.BundleFingerprint, now)
	if err != nil {
		return fmt.Errorf("insert standing run: %w", err)
	}
	return nil
}

func (s *WorkflowInstanceStore) copyStandingEntityStateTx(ctx context.Context, tx *sql.Tx, oldRunID, newRunID, entityID string) error {
	var result sql.Result
	var err error
	if s.isSQLite() {
		result, err = tx.ExecContext(ctx, `
			INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, slug, name, current_state, gates, fields, accumulator, revision, entered_state_at, created_at, updated_at)
			SELECT ?, entity_id, flow_instance, entity_type, slug, name, current_state, gates, fields, accumulator, revision, entered_state_at, created_at, ?
			FROM entity_state WHERE run_id = ? AND entity_id = ?
		`, newRunID, time.Now().UTC(), oldRunID, entityID)
	} else {
		result, err = tx.ExecContext(ctx, `
			INSERT INTO entity_state (run_id, entity_id, flow_instance, entity_type, slug, name, current_state, gates, fields, accumulator, revision, entered_state_at, created_at, updated_at)
			SELECT $1::uuid, entity_id, flow_instance, entity_type, slug, name, current_state, gates, fields, accumulator, revision, entered_state_at, created_at, now()
			FROM entity_state WHERE run_id = $2::uuid AND entity_id = $3::uuid
		`, newRunID, oldRunID, entityID)
	}
	if err != nil {
		return fmt.Errorf("copy standing entity state: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count > 1 {
		return fmt.Errorf("standing repair found multiple current entity rows")
	}
	return nil
}

func (s *WorkflowInstanceStore) insertStandingJournalTx(ctx context.Context, tx *sql.Tx, result StandingServiceReconciliation, previousState, previousHash, previousSource, actor string, now time.Time) error {
	operationID := uuid.NewString()
	if s.isSQLite() {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO standing_service_journal (
				service_id, sequence, operation_id, generation, run_id, transition,
				previous_effective_state, effective_state, previous_bundle_hash, bundle_hash,
				previous_bundle_source, bundle_source, actor, reason, poison_provenance, occurred_at
			) VALUES (?, (SELECT COALESCE(MAX(sequence), 0) + 1 FROM standing_service_journal WHERE service_id = ?), ?, ?, ?, ?, NULLIF(?, ''), ?, NULLIF(?, ''), ?, NULLIF(?, ''), ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?)
		`, result.ServiceID, result.ServiceID, operationID, result.Generation, result.RunID, result.Transition,
			previousState, result.EffectiveState, previousHash, result.BundleHash, previousSource, result.BundleSource,
			actor, result.Reason, result.Reason, now)
		if err != nil {
			return fmt.Errorf("insert standing journal: %w", err)
		}
		return nil
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO standing_service_journal (
			service_id, sequence, operation_id, generation, run_id, transition,
			previous_effective_state, effective_state, previous_bundle_hash, bundle_hash,
			previous_bundle_source, bundle_source, actor, reason, poison_provenance, occurred_at
		) VALUES ($1::uuid, (SELECT COALESCE(MAX(sequence), 0) + 1 FROM standing_service_journal WHERE service_id = $1::uuid), $2::uuid, $3, $4::uuid, $5, NULLIF($6, ''), $7, NULLIF($8, ''), $9, NULLIF($10, ''), $11, $12, NULLIF($13, ''), NULLIF($14, ''), $15)
	`, result.ServiceID, operationID, result.Generation, result.RunID, result.Transition,
		previousState, result.EffectiveState, previousHash, result.BundleHash, previousSource, result.BundleSource,
		actor, result.Reason, result.Reason, now)
	if err != nil {
		return fmt.Errorf("insert standing journal: %w", err)
	}
	return nil
}

func standingResult(candidate StandingServiceCandidate, runID string, generation, publicationSequence int64, transition, effectiveState, reason string) StandingServiceReconciliation {
	return StandingServiceReconciliation{
		ServiceID: candidate.ServiceID, PackageKey: candidate.PackageKey, FlowID: candidate.FlowID,
		InstanceID: candidate.InstanceID, EntityID: candidate.EntityID, RunID: runID,
		Generation: generation, PublicationSequence: publicationSequence, Transition: transition,
		EffectiveState: effectiveState, BundleHash: candidate.Source.BundleHash,
		BundleSource: candidate.Source.BundleSource, Reason: reason,
	}
}

func standingResetRequiredError(current standingServiceRow, reason string) error {
	return fmt.Errorf("standing service %s (%s/%s) cannot reconcile run %s status %s: %s; run `swarm standing reset %s`",
		current.ServiceID, current.PackageKey, current.FlowID, current.RunID, current.RunStatus, reason, current.ServiceID)
}

func requireOneStandingRow(result sql.Result) error {
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("standing service changed during mutation")
	}
	return nil
}
