package runbundle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimebundleidentity "github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	runtimerunbundle "github.com/division-sh/swarm/internal/runtime/runbundle"
)

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func LoadAvailability(ctx context.Context, db queryer, runID string) (runtimerunbundle.Availability, error) {
	runID = strings.TrimSpace(runID)
	if db == nil {
		return runtimerunbundle.Availability{}, fmt.Errorf("bundle availability database is required")
	}
	if runID == "" {
		return runtimerunbundle.Availability{}, fmt.Errorf("run_id is required")
	}
	var row availabilityRow
	err := db.QueryRowContext(ctx, `
		SELECT
			run_id::text,
			COALESCE(status, ''),
			COALESCE(bundle_hash, ''),
			COALESCE(bundle_source, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&row.RunID, &row.Status, &row.BundleHash, &row.BundleSource)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimerunbundle.Availability{}, fmt.Errorf("run %s not found: %w", runID, runtimerunbundle.ErrRunNotFound)
	}
	if err != nil {
		return runtimerunbundle.Availability{}, fmt.Errorf("load run bundle availability: %w", err)
	}
	return classifyRow(ctx, db, row)
}

func ListActiveAvailabilities(ctx context.Context, db queryer) ([]runtimerunbundle.Availability, error) {
	if db == nil {
		return nil, fmt.Errorf("bundle availability database is required")
	}
	rows, err := db.QueryContext(ctx, `
		SELECT
			run_id::text,
			COALESCE(status, ''),
			COALESCE(bundle_hash, ''),
			COALESCE(bundle_source, '')
		FROM runs
		WHERE lower(COALESCE(status, '')) IN ('running', 'paused')
		ORDER BY run_id
	`)
	if err != nil {
		return nil, fmt.Errorf("load active run bundle availability: %w", err)
	}
	defer rows.Close()

	out := []runtimerunbundle.Availability{}
	for rows.Next() {
		var row availabilityRow
		if err := rows.Scan(&row.RunID, &row.Status, &row.BundleHash, &row.BundleSource); err != nil {
			return nil, fmt.Errorf("scan active run bundle availability: %w", err)
		}
		availability, err := classifyRow(ctx, db, row)
		if err != nil {
			return nil, err
		}
		out = append(out, availability)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read active run bundle availability: %w", err)
	}
	return out, nil
}

func ListActiveConflicts(ctx context.Context, db queryer) ([]runtimerunbundle.Availability, error) {
	availabilities, err := ListActiveAvailabilities(ctx, db)
	if err != nil {
		return nil, err
	}
	conflicts := make([]runtimerunbundle.Availability, 0, len(availabilities))
	for _, availability := range availabilities {
		if !availability.Available() {
			conflicts = append(conflicts, availability)
		}
	}
	return conflicts, nil
}

type availabilityRow struct {
	RunID        string
	Status       string
	BundleHash   string
	BundleSource string
}

func classifyRow(ctx context.Context, db queryer, row availabilityRow) (runtimerunbundle.Availability, error) {
	source, err := runtimerunbundle.DecodeAvailabilitySource(row.BundleSource)
	if err != nil {
		return runtimerunbundle.Availability{}, err
	}
	availability := runtimerunbundle.Availability{
		RunID:        strings.TrimSpace(row.RunID),
		Status:       strings.TrimSpace(row.Status),
		BundleHash:   row.BundleHash,
		BundleSource: source,
	}
	if err := runtimebundleidentity.ValidateCanonicalHash(availability.BundleHash); err != nil {
		availability.ErrorCode = runtimerunbundle.CodeBundleDataIntegrityError
		availability.Cause = "invalid_bundle_hash"
		return availability, nil
	}
	switch source {
	case runtimerunbundle.AvailabilitySourcePersisted:
		exists, err := bundleRowExists(ctx, db, availability.BundleHash)
		if err != nil {
			return runtimerunbundle.Availability{}, err
		}
		availability.BundleRowPresent = exists
		if !exists {
			availability.ErrorCode = runtimerunbundle.CodeBundleDataIntegrityError
			availability.Cause = "persisted_missing_bundle_row"
		}
	case runtimerunbundle.AvailabilitySourceEphemeral, runtimerunbundle.AvailabilitySourceDeleted:
		availability.ErrorCode = runtimerunbundle.CodeBundleUnavailable
		availability.Cause = source.String()
	default:
		return runtimerunbundle.Availability{}, fmt.Errorf("unsupported bundle source %q", source)
	}
	return availability, nil
}

func bundleRowExists(ctx context.Context, db queryer, bundleHash string) (bool, error) {
	bundleHash = strings.TrimSpace(bundleHash)
	if bundleHash == "" {
		return false, nil
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM bundles
			WHERE bundle_hash = $1
		)
	`, bundleHash).Scan(&exists); err != nil {
		return false, fmt.Errorf("load bundle row presence for %s: %w", bundleHash, err)
	}
	return exists, nil
}
