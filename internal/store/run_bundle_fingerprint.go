package store

import (
	"context"
	"fmt"
	"strings"
)

type ActiveRunBundleMismatch struct {
	RunID             string
	Status            string
	BundleFingerprint string
}

func (s *PostgresStore) ActiveRunBundleMismatches(ctx context.Context, bootFingerprint string) ([]ActiveRunBundleMismatch, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required")
	}
	bootFingerprint = strings.TrimSpace(bootFingerprint)
	if bootFingerprint == "" {
		return nil, fmt.Errorf("boot bundle fingerprint is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if !caps.Events.HasRuns || !caps.Events.RunBundleFingerprint {
		return nil, nil
	}
	orderBy := "run_id"
	if caps.Events.RunStartedAt {
		orderBy = "started_at ASC, run_id"
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT run_id::text, COALESCE(status, ''), COALESCE(bundle_fingerprint, '')
		FROM runs
		WHERE lower(COALESCE(status, '')) IN ('running', 'paused')
		  AND NULLIF(bundle_fingerprint, '') IS NOT NULL
		  AND bundle_fingerprint <> $1
		ORDER BY `+orderBy, bootFingerprint)
	if err != nil {
		return nil, fmt.Errorf("load active run bundle mismatches: %w", err)
	}
	defer rows.Close()

	var mismatches []ActiveRunBundleMismatch
	for rows.Next() {
		var mismatch ActiveRunBundleMismatch
		if err := rows.Scan(&mismatch.RunID, &mismatch.Status, &mismatch.BundleFingerprint); err != nil {
			return nil, fmt.Errorf("scan active run bundle mismatch: %w", err)
		}
		mismatch.RunID = strings.TrimSpace(mismatch.RunID)
		mismatch.Status = strings.TrimSpace(mismatch.Status)
		mismatch.BundleFingerprint = strings.TrimSpace(mismatch.BundleFingerprint)
		mismatches = append(mismatches, mismatch)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read active run bundle mismatches: %w", err)
	}
	return mismatches, nil
}
