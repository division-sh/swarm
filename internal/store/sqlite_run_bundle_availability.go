package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimebundleidentity "github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
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
	var rawSource string
	err := s.backend.db.QueryRowContext(ctx, `
		SELECT
			run_id,
			COALESCE(status, ''),
			COALESCE(bundle_hash, ''),
			COALESCE(bundle_source, '')
		FROM runs
		WHERE run_id = ?
	`, runID).Scan(
		&availability.RunID,
		&availability.Status,
		&availability.BundleHash,
		&rawSource,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return runbundle.Availability{}, ErrRunNotFound
	}
	if err != nil {
		return runbundle.Availability{}, fmt.Errorf("load sqlite run bundle availability: %w", err)
	}
	source, err := runbundle.DecodeAvailabilitySource(rawSource)
	if err != nil {
		return runbundle.Availability{}, err
	}
	availability.BundleSource = source
	return s.classifySQLiteRunBundleAvailability(ctx, availability)
}

func (s *SQLiteRuntimeStore) classifySQLiteRunBundleAvailability(ctx context.Context, availability runbundle.Availability) (runbundle.Availability, error) {
	availability.RunID = strings.TrimSpace(availability.RunID)
	availability.Status = strings.TrimSpace(availability.Status)
	if err := runtimebundleidentity.ValidateCanonicalHash(availability.BundleHash); err != nil {
		availability.ErrorCode = runbundle.CodeBundleDataIntegrityError
		availability.Cause = "invalid_bundle_hash"
		return availability, nil
	}
	switch availability.BundleSource {
	case runbundle.AvailabilitySourcePersisted:
		exists, err := s.sqliteBundleRowExists(ctx, availability.BundleHash)
		if err != nil {
			return runbundle.Availability{}, err
		}
		availability.BundleRowPresent = exists
		if !exists {
			availability.ErrorCode = runbundle.CodeBundleDataIntegrityError
			availability.Cause = "persisted_missing_bundle_row"
		}
	case runbundle.AvailabilitySourceEphemeral, runbundle.AvailabilitySourceDeleted:
		availability.ErrorCode = runbundle.CodeBundleUnavailable
		availability.Cause = availability.BundleSource.String()
	default:
		return runbundle.Availability{}, fmt.Errorf("unsupported bundle source %q", availability.BundleSource.String())
	}
	return availability, nil
}

func (s *SQLiteRuntimeStore) sqliteBundleRowExists(ctx context.Context, bundleHash string) (bool, error) {
	bundleHash = strings.TrimSpace(bundleHash)
	if bundleHash == "" {
		return false, nil
	}
	var exists bool
	if err := s.backend.db.QueryRowContext(ctx, `
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
