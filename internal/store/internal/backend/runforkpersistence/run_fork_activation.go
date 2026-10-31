package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	privaterunlifecycle "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/workflowheader"
)

type runForkActivationLineage struct {
	ForkRootReference   sourceartifact.RootAdmissionReference
	SourceRootReference sourceartifact.RootAdmissionReference
	ForkRunID           string
	ForkStatus          string
	ForkBundleHash      string
	SourceRunID         string
	SourceBundleHash    string
	ForkPoint           runfork.RunForkPoint
	ForkEventID         string
	ForkEventName       string
	ForkEventTime       time.Time
	ForkEventRevision   int64
	SourceRunStatus     string
	EntityIDs           []string
}

type RunForkActivationLineage = runForkActivationLineage

func loadRunForkActivationLineage(ctx context.Context, lifecycle *privaterunlifecycle.RunLifecyclePostgresOwner, tx *sql.Tx, forkRunID string) (runForkActivationLineage, error) {
	snapshot, err := lifecycle.LoadSnapshotTx(ctx, tx, forkRunID, true)
	if err != nil {
		if errors.Is(err, runtimerunlifecycle.ErrRunNotFound) {
			return runForkActivationLineage{}, fmt.Errorf("fork run %s not found", forkRunID)
		}
		return runForkActivationLineage{}, fmt.Errorf("load fork activation lifecycle: %w", err)
	}
	if snapshot.Origin.Kind() != runtimerunlifecycle.OriginForkMaterialization {
		return runForkActivationLineage{}, fmt.Errorf(
			"fork activation requires fork materialization origin; run %s has %s",
			forkRunID,
			snapshot.Origin.Kind(),
		)
	}
	lineage := runForkActivationLineage{
		ForkRunID:         snapshot.RunID,
		ForkStatus:        string(snapshot.State),
		ForkBundleHash:    snapshot.BundleHash,
		ForkRootReference: snapshot.RootReference,
		SourceRunID:       snapshot.Origin.SourceRunID(),
		ForkEventID:       snapshot.Origin.SourceEventID(),
		ForkPoint: runfork.RunForkPoint{Kind: snapshot.Origin.ForkPointKind(), Revision: snapshot.Origin.ForkRevision(),
			EventID: snapshot.Origin.SourceEventID()},
	}
	if err := lineage.ForkPoint.Validate(); err != nil {
		return runForkActivationLineage{}, fmt.Errorf("fork activation origin point: %w", err)
	}
	lineage.ForkEventRevision = lineage.ForkPoint.Revision
	var forkEventTime sql.NullTime
	err = tx.QueryRowContext(ctx, `
		SELECT
			COALESCE(s.status, ''),
			COALESCE(s.bundle_hash, ''),
			COALESCE(s.source_root_directory, ''),
			COALESCE(e.event_name, ''),
			e.created_at
		FROM runs s
		LEFT JOIN events e ON e.run_id = s.run_id AND e.event_id = NULLIF($2, '')::uuid
		WHERE s.run_id = $1::uuid
		FOR UPDATE OF s
	`, lineage.SourceRunID, lineage.ForkEventID).Scan(
		&lineage.SourceRunStatus,
		&lineage.SourceBundleHash,
		&lineage.SourceRootReference.RootDirectory,
		&lineage.ForkEventName,
		&forkEventTime,
	)
	if err == sql.ErrNoRows {
		return runForkActivationLineage{}, fmt.Errorf("fork activation requires source run")
	}
	if err != nil {
		return runForkActivationLineage{}, fmt.Errorf("load fork activation lineage: %w", err)
	}
	if lineage.SourceRunStatus == "" || (lineage.ForkPoint.Kind == runfork.RunForkPointEvent && !forkEventTime.Valid) {
		return runForkActivationLineage{}, fmt.Errorf("fork activation requires source run and exact fork point")
	}
	if forkEventTime.Valid {
		lineage.ForkEventTime = forkEventTime.Time
	}
	headers, err := workflowheader.InventoryForMutation(ctx, tx, true, lineage.ForkRunID)
	if err != nil {
		return runForkActivationLineage{}, fmt.Errorf("load fork constructed inventory: %w", err)
	}
	for _, header := range headers {
		lineage.EntityIDs = append(lineage.EntityIDs, header.EntityID)
	}
	lineage.ForkBundleHash = strings.TrimSpace(lineage.ForkBundleHash)
	lineage.SourceBundleHash = strings.TrimSpace(lineage.SourceBundleHash)
	lineage.SourceRootReference.BundleHash = lineage.SourceBundleHash
	return lineage, nil
}

func loadSQLiteRunForkActivationLineage(ctx context.Context, lifecycle *privaterunlifecycle.RunLifecycleSQLiteOwner, tx *sql.Tx, forkRunID string) (runForkActivationLineage, error) {
	snapshot, err := lifecycle.LoadSnapshotTx(ctx, tx, forkRunID)
	if err != nil {
		if errors.Is(err, runtimerunlifecycle.ErrRunNotFound) {
			return runForkActivationLineage{}, fmt.Errorf("fork run %s not found", forkRunID)
		}
		return runForkActivationLineage{}, fmt.Errorf("load sqlite fork activation lifecycle: %w", err)
	}
	if snapshot.Origin.Kind() != runtimerunlifecycle.OriginForkMaterialization {
		return runForkActivationLineage{}, fmt.Errorf("fork activation requires fork materialization origin; run %s has %s", forkRunID, snapshot.Origin.Kind())
	}
	lineage := runForkActivationLineage{
		ForkRunID: snapshot.RunID, ForkStatus: string(snapshot.State), ForkBundleHash: snapshot.BundleHash,
		ForkRootReference: snapshot.RootReference,
		SourceRunID:       snapshot.Origin.SourceRunID(), ForkEventID: snapshot.Origin.SourceEventID(),
		ForkPoint: runfork.RunForkPoint{Kind: snapshot.Origin.ForkPointKind(), Revision: snapshot.Origin.ForkRevision(),
			EventID: snapshot.Origin.SourceEventID()},
	}
	if err := lineage.ForkPoint.Validate(); err != nil {
		return runForkActivationLineage{}, fmt.Errorf("fork activation origin point: %w", err)
	}
	lineage.ForkEventRevision = lineage.ForkPoint.Revision
	var forkEventTimeRaw any
	err = tx.QueryRowContext(ctx, `
		SELECT COALESCE(s.status, ''), COALESCE(s.bundle_hash, ''), COALESCE(s.source_root_directory, ''), COALESCE(e.event_name, ''), e.created_at
		FROM runs s
		LEFT JOIN events e ON e.run_id = s.run_id AND e.event_id = $2
		WHERE s.run_id = $1
	`, lineage.SourceRunID, lineage.ForkEventID).Scan(
		&lineage.SourceRunStatus, &lineage.SourceBundleHash, &lineage.SourceRootReference.RootDirectory, &lineage.ForkEventName, &forkEventTimeRaw,
	)
	if err == sql.ErrNoRows {
		return runForkActivationLineage{}, fmt.Errorf("fork activation requires source run")
	}
	if err != nil {
		return runForkActivationLineage{}, fmt.Errorf("load sqlite fork activation lineage: %w", err)
	}
	forkEventTime, present, err := sqliteTimeValue(forkEventTimeRaw)
	if err != nil {
		return runForkActivationLineage{}, fmt.Errorf("decode sqlite fork activation event time: %w", err)
	}
	if lineage.SourceRunStatus == "" || (lineage.ForkPoint.Kind == runfork.RunForkPointEvent && !present) {
		return runForkActivationLineage{}, fmt.Errorf("fork activation requires source run and exact fork point")
	}
	if present {
		lineage.ForkEventTime = forkEventTime
	}
	headers, err := workflowheader.InventoryForMutation(ctx, tx, false, lineage.ForkRunID)
	if err != nil {
		return runForkActivationLineage{}, fmt.Errorf("load sqlite fork constructed inventory: %w", err)
	}
	for _, header := range headers {
		lineage.EntityIDs = append(lineage.EntityIDs, header.EntityID)
	}
	lineage.ForkBundleHash = strings.TrimSpace(lineage.ForkBundleHash)
	lineage.SourceBundleHash = strings.TrimSpace(lineage.SourceBundleHash)
	lineage.SourceRootReference.BundleHash = lineage.SourceBundleHash
	return lineage, nil
}

func lockRunForkSourceRevisionFrontier(ctx context.Context, tx *sql.Tx, lineage *runForkActivationLineage) error {
	if lineage == nil {
		return fmt.Errorf("fork activation requires lineage")
	}
	point, err := resolveFixedRunForkRevisionPoint(ctx, tx, lineage.SourceRunID, lineage.ForkPoint, resolveRunForkRevisionPoint)
	if err != nil {
		return fmt.Errorf("resolve fork activation fixed revision: %w", err)
	}
	lineage.ForkEventRevision = point.Revision
	var currentRevision int64
	if err := tx.QueryRowContext(ctx, `
		SELECT last_revision
		FROM run_fork_revision_heads
		WHERE run_id = $1::uuid
		FOR UPDATE
	`, lineage.SourceRunID).Scan(&currentRevision); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("fork activation source revision frontier is missing; recreate the store and retry")
		}
		return fmt.Errorf("lock fork activation source revision frontier: %w", err)
	}
	if currentRevision < lineage.ForkEventRevision {
		return fmt.Errorf("fork activation source revision frontier is corrupt; recreate the store and retry")
	}
	return nil
}

func lockSQLiteRunForkSourceRevisionFrontier(ctx context.Context, tx *sql.Tx, lineage *runForkActivationLineage) error {
	if lineage == nil {
		return fmt.Errorf("fork activation requires lineage")
	}
	point, err := resolveFixedRunForkRevisionPoint(ctx, tx, lineage.SourceRunID, lineage.ForkPoint, resolveSQLiteRunForkRevisionPoint)
	if err != nil {
		return fmt.Errorf("resolve sqlite fork activation fixed revision: %w", err)
	}
	lineage.ForkEventRevision = point.Revision
	var currentRevision int64
	if err := tx.QueryRowContext(ctx, `
		SELECT last_revision FROM run_fork_revision_heads WHERE run_id = $1
	`, lineage.SourceRunID).Scan(&currentRevision); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("fork activation source revision frontier is missing; recreate the store and retry")
		}
		return fmt.Errorf("load sqlite fork activation source revision frontier: %w", err)
	}
	if currentRevision < lineage.ForkEventRevision {
		return fmt.Errorf("fork activation source revision frontier is corrupt; recreate the store and retry")
	}
	return nil
}

func collectRunForkSourceAdvancedFacts(ctx context.Context, tx *sql.Tx, lineage runForkActivationLineage) ([]string, error) {
	where := "revision > $2"
	args := []any{lineage.SourceRunID, lineage.ForkEventRevision}
	if lineage.ForkPoint.Kind == runfork.RunForkPointRunStart {
		revision, projection, err := privaterunforkrevision.LoadStartProjection(ctx, tx, lineage.SourceRunID)
		if err != nil {
			return nil, err
		}
		if revision != lineage.ForkEventRevision {
			return nil, fmt.Errorf("source advancement requires the original start revision")
		}
		// An exclusive start and its creating ingress share an atomic commit.
		// Facts outside the immutable initial membership are still post-cut work.
		initial := make([]string, 0, len(projection.Facts))
		for _, fact := range projection.Facts {
			args = append(args, string(fact.Family), fact.Key)
			initial = append(initial, fmt.Sprintf("(family=$%d AND fact_key=$%d)", len(args)-1, len(args)))
		}
		where = "revision >= $2"
		if len(initial) != 0 {
			where = "(revision > $2 OR (revision = $2 AND NOT (" + strings.Join(initial, " OR ") + ")))"
		}
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT family
		FROM run_fork_fact_revisions
		WHERE run_id = $1
		  AND `+where+`
		ORDER BY family
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("read source revisions after fork point: %w", err)
	}
	defer rows.Close()
	facts := []string{}
	for rows.Next() {
		var family string
		if err := rows.Scan(&family); err != nil {
			return nil, fmt.Errorf("scan source revision after fork point: %w", err)
		}
		code, ok := runForkSourceAdvancedCode(family)
		if !ok {
			return nil, fmt.Errorf("unsupported revisioned source family %q; recreate the store and retry", family)
		}
		facts = append(facts, code)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate source revisions after fork point: %w", err)
	}
	return uniqueNonEmptyStrings(facts), nil
}

func ensureRunForkSourceNotAdvanced(ctx context.Context, tx *sql.Tx, lineage runForkActivationLineage) error {
	facts, err := collectRunForkSourceAdvancedFacts(ctx, tx, lineage)
	if err != nil {
		return err
	}
	if len(facts) == 0 {
		return nil
	}
	return runForkReplayResumeError(facts[0], runfork.RunForkReplayResumeFactSourceAdvanced, fmt.Sprintf("fork activation blocked: %s", facts[0]))
}

func runForkSourceAdvancedCode(family string) (string, bool) {
	switch strings.TrimSpace(family) {
	case "events":
		return "source_events_advanced_after_fork_point", true
	case "entity_mutations":
		return "source_mutations_advanced_after_fork_point", true
	case "entity_metadata":
		return "source_current_state_advanced_after_fork_point", true
	case "event_deliveries":
		return "source_deliveries_advanced_after_fork_point", true
	case "committed_replay_scopes":
		return "source_committed_replay_scope_advanced_after_fork_point", true
	case "event_receipts":
		return "source_receipts_advanced_after_fork_point", true
	case "dead_letters":
		return "source_dead_letters_advanced_after_fork_point", true
	case "timers":
		return "source_timers_advanced_after_fork_point", true
	case "agent_sessions":
		return "source_sessions_advanced_after_fork_point", true
	case "agent_turns":
		return "source_turns_advanced_after_fork_point", true
	case "agent_conversation_audits":
		return "source_conversation_audits_advanced_after_fork_point", true
	case "reply_contexts":
		return "source_reply_contexts_advanced_after_fork_point", true
	case string(privaterunforkrevision.FamilyFanOutObligations):
		return "source_fan_out_obligations_advanced_after_fork_point", true
	default:
		return "", false
	}
}

func ensureRunForkActivationNoForkReplayState(ctx context.Context, tx *sql.Tx, deliveries *storedelivery.Adapter, forkRunID string) error {
	if deliveries == nil {
		return fmt.Errorf("fork delivery inspection owner is required")
	}
	hasDeliveries, err := deliveries.RunHasDeliveryObligations(ctx, tx, forkRunID)
	if err != nil {
		return fmt.Errorf("check fork_deliveries_already_exist: %w", err)
	}
	if hasDeliveries {
		return runForkReplayResumeError("fork_deliveries_already_exist", runfork.RunForkReplayResumeFactForkReplayState, "fork activation blocked: fork_deliveries_already_exist")
	}
	checks := []struct {
		code  string
		query string
	}{
		{"fork_events_already_exist", `SELECT EXISTS (SELECT 1 FROM events WHERE run_id = $1)`},
		{"fork_sessions_already_exist", `SELECT EXISTS (SELECT 1 FROM agent_sessions WHERE run_id = $1)`},
		{"fork_conversation_audits_already_exist", `SELECT EXISTS (SELECT 1 FROM agent_conversation_audits WHERE run_id = $1)`},
		{"fork_turns_already_exist", `SELECT EXISTS (SELECT 1 FROM agent_turns WHERE run_id = $1)`},
	}
	for _, check := range checks {
		var exists bool
		if err := tx.QueryRowContext(ctx, check.query, forkRunID).Scan(&exists); err != nil {
			return fmt.Errorf("check %s: %w", check.code, err)
		}
		if exists {
			return runForkReplayResumeError(check.code, runfork.RunForkReplayResumeFactForkReplayState, fmt.Sprintf("fork activation blocked: %s", check.code))
		}
	}
	return nil
}
