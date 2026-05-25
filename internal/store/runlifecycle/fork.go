package runlifecycle

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type InsertForkOptions struct {
	HasBundleHashCol   bool
	HasBundleSourceCol bool
	BundleHash         string
	BundleSource       string
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
	bundleSource, err := CanonicalBundleSource(opts.BundleSource)
	if err != nil {
		return err
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
	if opts.HasBundleHashCol {
		args = append(args, strings.TrimSpace(opts.BundleHash))
		cols = append(cols, "bundle_hash")
		values = append(values, fmt.Sprintf("NULLIF($%d, '')", len(args)))
	}
	if opts.HasBundleSourceCol {
		args = append(args, bundleSource)
		cols = append(cols, "bundle_source")
		values = append(values, fmt.Sprintf("$%d", len(args)))
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO runs (`+strings.Join(cols, ", ")+`)
		VALUES (`+strings.Join(values, ", ")+`)
	`, args...)
	return err
}
