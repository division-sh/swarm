package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/store/runbundle"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/google/uuid"
)

func (s *SQLiteRuntimeStore) LoadRunBundleAvailability(ctx context.Context, runID string) (runbundle.Availability, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runbundle.Availability{}, err
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return runbundle.Availability{}, ErrRunNotFound
	}
	if _, err := uuid.Parse(runID); err != nil {
		return runbundle.Availability{}, ErrRunNotFound
	}
	var availability runbundle.Availability
	err := s.DB.QueryRowContext(ctx, `
		SELECT
			run_id,
			COALESCE(status, ''),
			COALESCE(bundle_hash, ''),
			COALESCE(bundle_source, ''),
			COALESCE(bundle_fingerprint, '')
		FROM runs
		WHERE run_id = ?
	`, runID).Scan(
		&availability.RunID,
		&availability.Status,
		&availability.BundleHash,
		&availability.BundleSource,
		&availability.BundleFingerprint,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return runbundle.Availability{}, ErrRunNotFound
	}
	if err != nil {
		return runbundle.Availability{}, fmt.Errorf("load sqlite run bundle availability: %w", err)
	}
	return s.classifySQLiteRunBundleAvailability(ctx, availability)
}

func (s *SQLiteRuntimeStore) classifySQLiteRunBundleAvailability(ctx context.Context, availability runbundle.Availability) (runbundle.Availability, error) {
	source, err := storerunlifecycle.CanonicalBundleSource(availability.BundleSource)
	if err != nil {
		return runbundle.Availability{}, err
	}
	availability.RunID = strings.TrimSpace(availability.RunID)
	availability.Status = strings.TrimSpace(availability.Status)
	availability.BundleHash = strings.TrimSpace(availability.BundleHash)
	availability.BundleSource = source
	availability.BundleFingerprint = strings.TrimSpace(availability.BundleFingerprint)
	switch source {
	case storerunlifecycle.BundleSourcePersisted:
		if availability.BundleHash == "" {
			availability.ErrorCode = runbundle.CodeBundleDataIntegrityError
			availability.Cause = "persisted_missing_hash"
			return availability, nil
		}
		exists, err := s.sqliteBundleRowExists(ctx, availability.BundleHash)
		if err != nil {
			return runbundle.Availability{}, err
		}
		availability.BundleRowPresent = exists
		if !exists {
			availability.ErrorCode = runbundle.CodeBundleDataIntegrityError
			availability.Cause = "persisted_missing_bundle_row"
		}
	case storerunlifecycle.BundleSourceEphemeral, storerunlifecycle.BundleSourceDeleted, storerunlifecycle.BundleSourceLegacy:
		availability.ErrorCode = runbundle.CodeBundleUnavailable
		availability.Cause = source
	default:
		return runbundle.Availability{}, fmt.Errorf("unsupported bundle source %q", source)
	}
	return availability, nil
}

func (s *SQLiteRuntimeStore) sqliteBundleRowExists(ctx context.Context, bundleHash string) (bool, error) {
	bundleHash = strings.TrimSpace(bundleHash)
	if bundleHash == "" {
		return false, nil
	}
	var exists bool
	if err := s.DB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM bundles
			WHERE bundle_hash = ?
		)
	`, bundleHash).Scan(&exists); err != nil {
		return false, fmt.Errorf("load sqlite bundle row presence for %s: %w", bundleHash, err)
	}
	return exists, nil
}
