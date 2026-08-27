package runbundle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimebundleidentity "github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
	runtimerunbundle "github.com/division-sh/swarm/internal/runtime/runbundle"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/google/uuid"
)

type Postgres struct{ backend *postgresbackend.Backend }
type SQLite struct{ backend *sqlitebackend.Backend }

func NewPostgres(backend *postgresbackend.Backend) (*Postgres, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("postgres run-bundle owner requires backend")
	}
	return &Postgres{backend: backend}, nil
}

func NewSQLite(backend *sqlitebackend.Backend) (*SQLite, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("sqlite run-bundle owner requires backend")
	}
	return &SQLite{backend: backend}, nil
}

func (o *Postgres) Load(ctx context.Context, runID string) (runtimerunbundle.Availability, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return runtimerunbundle.Availability{}, fmt.Errorf("run_id is required")
	}
	var availability runtimerunbundle.Availability
	err := o.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		availability, err = loadPostgres(txctx, tx, runID)
		return err
	})
	return availability, err
}

func loadPostgres(ctx context.Context, queryer rowQueryer, runID string) (runtimerunbundle.Availability, error) {
	var row availabilityRow
	err := queryer.QueryRowContext(ctx, `
		SELECT run_id::text, status, bundle_hash, bundle_source
		FROM runs
		WHERE run_id = $1::uuid
		FOR SHARE
	`, runID).Scan(&row.RunID, &row.Status, &row.BundleHash, &row.BundleSource)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimerunbundle.Availability{}, fmt.Errorf("run %s not found: %w", runID, runtimerunbundle.ErrRunNotFound)
	}
	if err != nil {
		return runtimerunbundle.Availability{}, fmt.Errorf("load postgres run bundle availability: %w", err)
	}
	if err := loadPostgresPersistedBundlePresence(ctx, queryer, &row); err != nil {
		return runtimerunbundle.Availability{}, err
	}
	return classify(row)
}

func (o *SQLite) Load(ctx context.Context, runID string) (runtimerunbundle.Availability, error) {
	runID = strings.TrimSpace(runID)
	if _, err := uuid.Parse(runID); err != nil {
		return runtimerunbundle.Availability{}, fmt.Errorf("run %s not found: %w", runID, runtimerunbundle.ErrRunNotFound)
	}
	var availability runtimerunbundle.Availability
	err := o.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		availability, err = loadSQLite(txctx, tx, runID)
		return err
	})
	return availability, err
}

func loadSQLite(ctx context.Context, queryer rowQueryer, runID string) (runtimerunbundle.Availability, error) {
	var row availabilityRow
	err := queryer.QueryRowContext(ctx, `
		SELECT run_id, status, bundle_hash, bundle_source
		FROM runs
		WHERE run_id = ?
	`, runID).Scan(&row.RunID, &row.Status, &row.BundleHash, &row.BundleSource)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimerunbundle.Availability{}, fmt.Errorf("run %s not found: %w", runID, runtimerunbundle.ErrRunNotFound)
	}
	if err != nil {
		return runtimerunbundle.Availability{}, fmt.Errorf("load sqlite run bundle availability: %w", err)
	}
	if err := loadSQLitePersistedBundlePresence(ctx, queryer, &row); err != nil {
		return runtimerunbundle.Availability{}, err
	}
	return classify(row)
}

type rowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadPostgresPersistedBundlePresence(ctx context.Context, queryer rowQueryer, row *availabilityRow) error {
	source, err := runtimerunbundle.DecodeAvailabilitySource(row.BundleSource)
	if err != nil || !source.IsPersisted() {
		return err
	}
	if err := queryer.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM bundles WHERE bundle_hash = $1)`, row.BundleHash).Scan(&row.BundleRowPresent); err != nil {
		return fmt.Errorf("load postgres run bundle row presence: %w", err)
	}
	return nil
}

func loadSQLitePersistedBundlePresence(ctx context.Context, queryer rowQueryer, row *availabilityRow) error {
	source, err := runtimerunbundle.DecodeAvailabilitySource(row.BundleSource)
	if err != nil || !source.IsPersisted() {
		return err
	}
	if err := queryer.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM bundles WHERE bundle_hash = ?)`, row.BundleHash).Scan(&row.BundleRowPresent); err != nil {
		return fmt.Errorf("load sqlite run bundle row presence: %w", err)
	}
	return nil
}

func (o *Postgres) ListActive(ctx context.Context) ([]runtimerunbundle.Availability, error) {
	rows, err := o.backend.QueryContext(ctx, `
		SELECT run_id::text, status, bundle_hash, bundle_source,
		       EXISTS (SELECT 1 FROM bundles b WHERE b.bundle_hash = runs.bundle_hash)
		FROM runs
		WHERE status IN ($1, $2)
		ORDER BY run_id
	`, string(runtimerunlifecycle.StateRunning), string(runtimerunlifecycle.StatePaused))
	if err != nil {
		return nil, fmt.Errorf("list active postgres run bundle availability: %w", err)
	}
	defer rows.Close()
	return scanAvailabilities(rows)
}

func (o *SQLite) ListActive(ctx context.Context) ([]runtimerunbundle.Availability, error) {
	rows, err := o.backend.QueryContext(ctx, `
		SELECT run_id, status, bundle_hash, bundle_source,
		       EXISTS (SELECT 1 FROM bundles b WHERE b.bundle_hash = runs.bundle_hash)
		FROM runs
		WHERE status IN (?, ?)
		ORDER BY run_id
	`, string(runtimerunlifecycle.StateRunning), string(runtimerunlifecycle.StatePaused))
	if err != nil {
		return nil, fmt.Errorf("list active sqlite run bundle availability: %w", err)
	}
	defer rows.Close()
	return scanAvailabilities(rows)
}

type availabilityRow struct {
	RunID            string
	Status           string
	BundleHash       string
	BundleSource     string
	BundleRowPresent bool
}

type rowIterator interface {
	Next() bool
	Scan(...any) error
	Err() error
}

func scanAvailabilities(rows rowIterator) ([]runtimerunbundle.Availability, error) {
	var result []runtimerunbundle.Availability
	for rows.Next() {
		var row availabilityRow
		if err := rows.Scan(&row.RunID, &row.Status, &row.BundleHash, &row.BundleSource, &row.BundleRowPresent); err != nil {
			return nil, fmt.Errorf("scan active run bundle availability: %w", err)
		}
		availability, err := classify(row)
		if err != nil {
			return nil, err
		}
		result = append(result, availability)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active run bundle availability: %w", err)
	}
	return result, nil
}

func classify(row availabilityRow) (runtimerunbundle.Availability, error) {
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
		availability.BundleRowPresent = row.BundleRowPresent
		if !row.BundleRowPresent {
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
