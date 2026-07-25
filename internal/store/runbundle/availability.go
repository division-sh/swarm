package runbundle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimebundleidentity "github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
)

const (
	CodeBundleUnavailable        = "BUNDLE_UNAVAILABLE"
	CodeBundleDataIntegrityError = "BUNDLE_DATA_INTEGRITY_ERROR"
)

var ErrRunNotFound = errors.New("run bundle: run not found")

type AvailabilitySource uint8

const (
	AvailabilitySourcePersisted AvailabilitySource = iota + 1
	AvailabilitySourceEphemeral
	AvailabilitySourceDeleted
)

func DecodeAvailabilitySource(raw string) (AvailabilitySource, error) {
	if raw != strings.TrimSpace(raw) {
		return 0, fmt.Errorf("bundle_source must not contain surrounding whitespace")
	}
	switch raw {
	case "persisted":
		return AvailabilitySourcePersisted, nil
	case "ephemeral":
		return AvailabilitySourceEphemeral, nil
	case "deleted":
		return AvailabilitySourceDeleted, nil
	default:
		return 0, fmt.Errorf("bundle_source must be persisted, ephemeral, or deleted")
	}
}

func (s AvailabilitySource) String() string {
	switch s {
	case AvailabilitySourcePersisted:
		return "persisted"
	case AvailabilitySourceEphemeral:
		return "ephemeral"
	case AvailabilitySourceDeleted:
		return "deleted"
	default:
		return ""
	}
}

func (s AvailabilitySource) IsPersisted() bool {
	return s == AvailabilitySourcePersisted
}

func (s AvailabilitySource) IsEphemeral() bool {
	return s == AvailabilitySourceEphemeral
}

func (s AvailabilitySource) IsDeleted() bool {
	return s == AvailabilitySourceDeleted
}

type Availability struct {
	RunID            string
	Status           string
	BundleHash       string
	BundleSource     AvailabilitySource
	BundleRowPresent bool
	ErrorCode        string
	Cause            string
}

type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (a Availability) Available() bool {
	return a.ErrorCode == "" &&
		a.BundleSource.IsPersisted() &&
		a.BundleHash != "" &&
		a.BundleRowPresent
}

func (a Availability) Unavailable() bool {
	return a.ErrorCode == CodeBundleUnavailable
}

func (a Availability) DataIntegrityError() bool {
	return a.ErrorCode == CodeBundleDataIntegrityError
}

func (a Availability) DetailString() string {
	parts := []string{
		"run_id=" + strings.TrimSpace(a.RunID),
		"status=" + strings.TrimSpace(a.Status),
		"bundle_source=" + a.BundleSource.String(),
	}
	if a.BundleHash != "" {
		parts = append(parts, "bundle_hash="+a.BundleHash)
	}
	if a.ErrorCode != "" {
		parts = append(parts, "code="+a.ErrorCode)
	}
	if a.Cause != "" {
		parts = append(parts, "cause="+a.Cause)
	}
	return strings.Join(parts, " ")
}

func LoadAvailability(ctx context.Context, db queryer, runID string) (Availability, error) {
	runID = strings.TrimSpace(runID)
	if db == nil {
		return Availability{}, fmt.Errorf("bundle availability database is required")
	}
	if runID == "" {
		return Availability{}, fmt.Errorf("run_id is required")
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
		return Availability{}, fmt.Errorf("run %s not found: %w", runID, ErrRunNotFound)
	}
	if err != nil {
		return Availability{}, fmt.Errorf("load run bundle availability: %w", err)
	}
	return classifyRow(ctx, db, row)
}

func ListActiveAvailabilities(ctx context.Context, db queryer) ([]Availability, error) {
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

	out := []Availability{}
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

func ListActiveConflicts(ctx context.Context, db queryer) ([]Availability, error) {
	availabilities, err := ListActiveAvailabilities(ctx, db)
	if err != nil {
		return nil, err
	}
	conflicts := make([]Availability, 0, len(availabilities))
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

func classifyRow(ctx context.Context, db queryer, row availabilityRow) (Availability, error) {
	source, err := DecodeAvailabilitySource(row.BundleSource)
	if err != nil {
		return Availability{}, err
	}
	availability := Availability{
		RunID:        strings.TrimSpace(row.RunID),
		Status:       strings.TrimSpace(row.Status),
		BundleHash:   row.BundleHash,
		BundleSource: source,
	}
	if err := runtimebundleidentity.ValidateCanonicalHash(availability.BundleHash); err != nil {
		availability.ErrorCode = CodeBundleDataIntegrityError
		availability.Cause = "invalid_bundle_hash"
		return availability, nil
	}
	switch source {
	case AvailabilitySourcePersisted:
		exists, err := bundleRowExists(ctx, db, availability.BundleHash)
		if err != nil {
			return Availability{}, err
		}
		availability.BundleRowPresent = exists
		if !exists {
			availability.ErrorCode = CodeBundleDataIntegrityError
			availability.Cause = "persisted_missing_bundle_row"
		}
	case AvailabilitySourceEphemeral, AvailabilitySourceDeleted:
		availability.ErrorCode = CodeBundleUnavailable
		availability.Cause = source.String()
	default:
		return Availability{}, fmt.Errorf("unsupported bundle source %q", source)
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
