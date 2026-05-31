package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	"swarm/internal/runtime/bundledelete"
	storerunlifecycle "swarm/internal/store/runlifecycle"
)

func (s *PostgresStore) PlanBundleDelete(ctx context.Context, req bundledelete.Request) (bundledelete.Plan, error) {
	if s == nil || s.DB == nil {
		return bundledelete.Plan{}, fmt.Errorf("postgres store is required")
	}
	bundleHash := strings.TrimSpace(req.BundleHash)
	if bundleHash == "" {
		return bundledelete.Plan{}, fmt.Errorf("%w: bundle_hash is required", bundledelete.ErrInvalidRequest)
	}
	if err := s.requireBundleDeletePlanningCapabilities(ctx); err != nil {
		return bundledelete.Plan{}, err
	}
	var exists bool
	if err := s.DB.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM bundles WHERE bundle_hash = $1)`, bundleHash).Scan(&exists); err != nil {
		return bundledelete.Plan{}, fmt.Errorf("plan bundle delete bundle row: %w", err)
	}
	if !exists {
		return bundledelete.Plan{}, ErrBundleNotFound
	}
	plan := bundledelete.Plan{
		BundleHash: bundleHash,
		PlannedAt:  req.RequestedAt.UTC(),
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			run_id::text,
			COALESCE(status, ''),
			COALESCE(bundle_hash, ''),
			COALESCE(bundle_source, ''),
			COALESCE(bundle_fingerprint, '')
		FROM runs
		WHERE bundle_hash = $1
		  AND bundle_source = $2
		ORDER BY run_id::text
	`, bundleHash, storerunlifecycle.BundleSourcePersisted)
	if err != nil {
		return bundledelete.Plan{}, fmt.Errorf("plan bundle delete runs: %w", err)
	}
	defer rows.Close()
	activeRunIDs := []string{}
	for rows.Next() {
		var run bundledelete.RunRef
		if err := rows.Scan(&run.RunID, &run.Status, &run.BundleHash, &run.BundleSource, &run.BundleFingerprint); err != nil {
			return bundledelete.Plan{}, fmt.Errorf("scan bundle delete run: %w", err)
		}
		normalizeBundleDeleteRunRef(&run)
		plan.AffectedRuns = append(plan.AffectedRuns, run)
		if bundleDeleteRunStatusActive(run.Status) {
			plan.ActiveRuns = append(plan.ActiveRuns, run)
			activeRunIDs = append(activeRunIDs, run.RunID)
		} else {
			plan.NonActiveRuns = append(plan.NonActiveRuns, run)
		}
	}
	if err := rows.Err(); err != nil {
		return bundledelete.Plan{}, fmt.Errorf("read bundle delete runs: %w", err)
	}
	if len(activeRunIDs) > 0 {
		deliveries, err := s.planBundleDeleteDeliveries(ctx, activeRunIDs)
		if err != nil {
			return bundledelete.Plan{}, err
		}
		plan.ActiveDeliveries = deliveries
		sessions, err := s.planBundleDeleteSessions(ctx, activeRunIDs)
		if err != nil {
			return bundledelete.Plan{}, err
		}
		plan.ActiveSessions = sessions
		timers, err := s.planBundleDeleteTimers(ctx, activeRunIDs)
		if err != nil {
			return bundledelete.Plan{}, err
		}
		plan.ActiveTimers = timers
	}
	return plan, nil
}

func (s *PostgresStore) ApplyBundleDeleteFinalMutation(ctx context.Context, req bundledelete.FinalMutationRequest) (bundledelete.FinalMutationResult, error) {
	if s == nil || s.DB == nil {
		return bundledelete.FinalMutationResult{}, fmt.Errorf("postgres store is required")
	}
	bundleHash := strings.TrimSpace(req.BundleHash)
	if bundleHash == "" {
		return bundledelete.FinalMutationResult{}, fmt.Errorf("%w: bundle_hash is required", bundledelete.ErrInvalidRequest)
	}
	if err := s.requireBundleDeleteFinalMutationCapabilities(ctx); err != nil {
		return bundledelete.FinalMutationResult{}, err
	}
	now := req.RequestedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	operationName := strings.TrimSpace(req.OperationName)
	if operationName == "" {
		operationName = bundledelete.DefaultOperationName
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return bundledelete.FinalMutationResult{}, fmt.Errorf("begin bundle delete final mutation tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	var lockedHash string
	err = tx.QueryRowContext(ctx, `
		SELECT bundle_hash
		FROM bundles
		WHERE bundle_hash = $1
		FOR UPDATE
	`, bundleHash).Scan(&lockedHash)
	if errors.Is(err, sql.ErrNoRows) {
		return bundledelete.FinalMutationResult{}, ErrBundleNotFound
	}
	if err != nil {
		return bundledelete.FinalMutationResult{}, fmt.Errorf("lock bundle delete bundle row: %w", err)
	}

	activeRemaining, err := lockBundleDeleteReferencingRunsTx(ctx, tx, bundleHash)
	if err != nil {
		return bundledelete.FinalMutationResult{}, err
	}
	result := bundledelete.FinalMutationResult{
		OperationName:        operationName,
		BundleHash:           bundleHash,
		AppliedAt:            now,
		RemainingActiveRuns:  activeRemaining,
		SourceAuthorityOwner: "store.ApplyBundleDeleteFinalMutation",
		TransactionOrderProof: []string{
			"update_eligible_runs_bundle_source_to_deleted",
			"delete_matching_bundles_row",
		},
	}
	if activeRemaining > 0 {
		return result, bundledelete.ErrActiveRunsRemain
	}
	updateResult, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET bundle_source = $2
		WHERE bundle_hash = $1
		  AND bundle_source = $3
		  AND lower(COALESCE(status, '')) NOT IN ('running', 'paused')
	`, bundleHash, storerunlifecycle.BundleSourceDeleted, storerunlifecycle.BundleSourcePersisted)
	if err != nil {
		return bundledelete.FinalMutationResult{}, fmt.Errorf("mark bundle delete runs deleted: %w", err)
	}
	updated, err := updateResult.RowsAffected()
	if err != nil {
		return bundledelete.FinalMutationResult{}, fmt.Errorf("count bundle delete run source updates: %w", err)
	}
	deleteResult, err := tx.ExecContext(ctx, `DELETE FROM bundles WHERE bundle_hash = $1`, bundleHash)
	if err != nil {
		return bundledelete.FinalMutationResult{}, fmt.Errorf("delete bundle row: %w", err)
	}
	deleted, err := deleteResult.RowsAffected()
	if err != nil {
		return bundledelete.FinalMutationResult{}, fmt.Errorf("count deleted bundle rows: %w", err)
	}
	result.RunsMarkedDeleted = int(updated)
	result.BundleRowsDeleted = int(deleted)
	result.Deleted = deleted > 0
	if err := tx.Commit(); err != nil {
		return bundledelete.FinalMutationResult{}, fmt.Errorf("commit bundle delete final mutation tx: %w", err)
	}
	committed = true
	return result, nil
}

func (s *PostgresStore) requireBundleDeletePlanningCapabilities(ctx context.Context) error {
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return err
	}
	if !catalog.hasColumns("bundles", "bundle_hash") {
		return fmt.Errorf("bundle delete requires bundles.bundle_hash")
	}
	caps := detectStoreSchemaCapabilities(catalog)
	if err := requirePreservationCleanupCapabilities(caps); err != nil {
		return err
	}
	if !caps.Events.RunBundleHash || !caps.Events.RunBundleSource {
		return fmt.Errorf("bundle delete requires runs.bundle_hash and runs.bundle_source")
	}
	return nil
}

func (s *PostgresStore) requireBundleDeleteFinalMutationCapabilities(ctx context.Context) error {
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return err
	}
	if !catalog.hasColumns("bundles", "bundle_hash") {
		return fmt.Errorf("bundle delete requires bundles.bundle_hash")
	}
	caps := detectStoreSchemaCapabilities(catalog)
	if !caps.Events.RunBundleHash || !caps.Events.RunBundleSource {
		return fmt.Errorf("bundle delete requires runs.bundle_hash and runs.bundle_source")
	}
	return nil
}

func (s *PostgresStore) planBundleDeleteDeliveries(ctx context.Context, runIDs []string) ([]bundledelete.DeliveryRef, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			d.delivery_id::text,
			d.run_id::text,
			d.event_id::text,
			COALESCE(d.subscriber_type, ''),
			COALESCE(d.subscriber_id, ''),
			COALESCE(d.status, '')
		FROM event_deliveries d
		WHERE d.run_id = ANY($1::uuid[])
		  AND d.subscriber_type IN ('agent', 'node')
		  AND d.status IN ('pending', 'in_progress')
		ORDER BY d.run_id::text, d.event_id::text, d.subscriber_type, d.subscriber_id
	`, pq.Array(runIDs))
	if err != nil {
		return nil, fmt.Errorf("plan bundle delete deliveries: %w", err)
	}
	defer rows.Close()
	out := []bundledelete.DeliveryRef{}
	for rows.Next() {
		var item bundledelete.DeliveryRef
		if err := rows.Scan(&item.DeliveryID, &item.RunID, &item.EventID, &item.SubscriberType, &item.SubscriberID, &item.Status); err != nil {
			return nil, fmt.Errorf("scan bundle delete delivery: %w", err)
		}
		normalizeBundleDeleteDeliveryRef(&item)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read bundle delete deliveries: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) planBundleDeleteSessions(ctx context.Context, runIDs []string) ([]bundledelete.SessionRef, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT session_id::text, run_id::text, COALESCE(agent_id, ''), COALESCE(status, '')
		FROM agent_sessions
		WHERE run_id = ANY($1::uuid[])
		  AND status IN ('active', 'suspended')
		ORDER BY run_id::text, agent_id, session_id::text
	`, pq.Array(runIDs))
	if err != nil {
		return nil, fmt.Errorf("plan bundle delete sessions: %w", err)
	}
	defer rows.Close()
	out := []bundledelete.SessionRef{}
	for rows.Next() {
		var item bundledelete.SessionRef
		if err := rows.Scan(&item.SessionID, &item.RunID, &item.AgentID, &item.Status); err != nil {
			return nil, fmt.Errorf("scan bundle delete session: %w", err)
		}
		item.SessionID = strings.TrimSpace(item.SessionID)
		item.RunID = strings.TrimSpace(item.RunID)
		item.AgentID = strings.TrimSpace(item.AgentID)
		item.Status = strings.TrimSpace(item.Status)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read bundle delete sessions: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) planBundleDeleteTimers(ctx context.Context, runIDs []string) ([]bundledelete.TimerRef, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT timer_id::text, run_id::text, COALESCE(timer_name, ''), COALESCE(status, '')
		FROM timers
		WHERE run_id = ANY($1::uuid[])
		  AND status = 'active'
		ORDER BY run_id::text, timer_name, timer_id::text
	`, pq.Array(runIDs))
	if err != nil {
		return nil, fmt.Errorf("plan bundle delete timers: %w", err)
	}
	defer rows.Close()
	out := []bundledelete.TimerRef{}
	for rows.Next() {
		var item bundledelete.TimerRef
		if err := rows.Scan(&item.TimerID, &item.RunID, &item.TimerName, &item.Status); err != nil {
			return nil, fmt.Errorf("scan bundle delete timer: %w", err)
		}
		item.TimerID = strings.TrimSpace(item.TimerID)
		item.RunID = strings.TrimSpace(item.RunID)
		item.TimerName = strings.TrimSpace(item.TimerName)
		item.Status = strings.TrimSpace(item.Status)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read bundle delete timers: %w", err)
	}
	return out, nil
}

func lockBundleDeleteReferencingRunsTx(ctx context.Context, tx *sql.Tx, bundleHash string) (int, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT COALESCE(status, '')
		FROM runs
		WHERE bundle_hash = $1
		  AND bundle_source = $2
		FOR UPDATE
	`, bundleHash, storerunlifecycle.BundleSourcePersisted)
	if err != nil {
		return 0, fmt.Errorf("lock bundle delete referencing runs: %w", err)
	}
	defer rows.Close()
	active := 0
	for rows.Next() {
		var status string
		if err := rows.Scan(&status); err != nil {
			return 0, fmt.Errorf("scan bundle delete referencing run: %w", err)
		}
		if bundleDeleteRunStatusActive(status) {
			active++
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read bundle delete referencing runs: %w", err)
	}
	return active, nil
}

func normalizeBundleDeleteRunRef(run *bundledelete.RunRef) {
	run.RunID = strings.TrimSpace(run.RunID)
	run.Status = strings.TrimSpace(run.Status)
	run.BundleHash = strings.TrimSpace(run.BundleHash)
	run.BundleSource = strings.TrimSpace(run.BundleSource)
	run.BundleFingerprint = strings.TrimSpace(run.BundleFingerprint)
}

func normalizeBundleDeleteDeliveryRef(delivery *bundledelete.DeliveryRef) {
	delivery.DeliveryID = strings.TrimSpace(delivery.DeliveryID)
	delivery.RunID = strings.TrimSpace(delivery.RunID)
	delivery.EventID = strings.TrimSpace(delivery.EventID)
	delivery.SubscriberType = strings.TrimSpace(delivery.SubscriberType)
	delivery.SubscriberID = strings.TrimSpace(delivery.SubscriberID)
	delivery.Status = strings.TrimSpace(delivery.Status)
}

func bundleDeleteRunStatusActive(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "running", "paused":
		return true
	default:
		return false
	}
}
