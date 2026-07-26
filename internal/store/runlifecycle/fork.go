package runlifecycle

import (
	"context"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

type InsertForkOptions struct {
	BundleSourceFact runtimecorrelation.BundleSourceFact
}

func InsertFork(ctx context.Context, db DBTX, forkRunID, status, sourceRunID, forkEventID string, entityCount int, startedAt time.Time, opts InsertForkOptions) error {
	if db == nil {
		return fmt.Errorf("run lifecycle database is required")
	}
	forkRunID = strings.TrimSpace(forkRunID)
	status = strings.TrimSpace(status)
	sourceRunID = strings.TrimSpace(sourceRunID)
	forkEventID = strings.TrimSpace(forkEventID)
	if forkRunID == "" {
		return fmt.Errorf("fork run_id is required")
	}
	if status == "" {
		return fmt.Errorf("fork run status is required")
	}
	if sourceRunID == "" {
		return fmt.Errorf("source run_id is required")
	}
	if forkEventID == "" {
		return fmt.Errorf("fork event_id is required")
	}
	if err := runtimeauthoractivity.Require(ctx); err != nil {
		return fmt.Errorf("insert fork run: %w", err)
	}
	if err := opts.BundleSourceFact.Validate(); err != nil {
		return fmt.Errorf("insert fork run: %w", err)
	}
	if err := admitBundleIdentityMutation(ctx, db, DialectPostgres, opts.BundleSourceFact); err != nil {
		return fmt.Errorf("insert fork run: %w", err)
	}
	bundleHash, bundleSource := opts.BundleSourceFact.StorageValues()
	occurrenceScope, err := runtimeauthoractivity.BundleScopeForSource(ctx, bundleHash)
	if err != nil {
		return fmt.Errorf("insert fork run: %w", err)
	}

	cols := []string{
		"run_id",
		"status",
		"forked_from_run_id",
		"forked_from_event_id",
		"entity_count",
		"event_count",
		"started_at",
	}
	values := []string{"$1::uuid", "$2", "$3::uuid", "$4::uuid", "$5", "0", "$6"}
	args := []any{forkRunID, status, sourceRunID, forkEventID, entityCount, startedAt}
	args = append(args, bundleHash, bundleSource)
	cols = append(cols, "bundle_hash", "bundle_source")
	values = append(values, fmt.Sprintf("$%d", len(args)-1), fmt.Sprintf("$%d", len(args)))
	_, err = db.ExecContext(ctx, `
		INSERT INTO runs (`+strings.Join(cols, ", ")+`)
		VALUES (`+strings.Join(values, ", ")+`)
	`, args...)
	if err != nil {
		return err
	}
	transition := "started"
	if status == "paused" {
		transition = "fork_prepared"
	}
	return runtimeauthoractivity.Record(ctx, runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindRunLifecycle, Transition: transition,
		SourceOwner: "runs", SourceIdentity: forkRunID, DedupKey: "run-created:" + forkRunID,
		OccurredAt: startedAt.UTC(), RunID: forkRunID, Scope: occurrenceScope,
		Projection: runtimeauthoractivity.Projection{
			SubjectType: "run", SubjectID: forkRunID, ParentRunID: sourceRunID, TriggerEventType: "run.fork",
		},
	})
}
