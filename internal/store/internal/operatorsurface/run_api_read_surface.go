package operatorsurface

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

type runHeaderCursor struct {
	StartedAt string `json:"started_at"`
	RunID     string `json:"run_id"`
}

func defaultRunHeaderListOptions(opts operatorread.RunHeaderListOptions) operatorread.RunHeaderListOptions {
	opts.Status = strings.ToLower(strings.TrimSpace(opts.Status))
	opts.BundleHash = strings.TrimSpace(opts.BundleHash)
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	if opts.Limit <= 0 {
		opts.Limit = 50
	}
	if opts.Limit > 500 {
		opts.Limit = 500
	}
	return opts
}

func (s *RunPostgres) requireRunHeaderAccess() error {
	return s.requireCurrentSchema()
}

func (s *RunPostgres) LoadRunHeader(ctx context.Context, runID string) (operatorread.RunHeader, error) {
	if s == nil || s.backend == nil {
		return operatorread.RunHeader{}, fmt.Errorf("postgres store is required")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return operatorread.RunHeader{}, operatorread.ErrRunNotFound
	}
	if _, err := uuid.Parse(runID); err != nil {
		return operatorread.RunHeader{}, operatorread.ErrRunNotFound
	}
	if err := s.requireRunHeaderAccess(); err != nil {
		return operatorread.RunHeader{}, err
	}
	row := s.backend.QueryRowContext(ctx, runHeaderSelectSQL()+`
WHERE r.run_id = $1::uuid
`, runID)
	header, err := scanRunHeader(row)
	if errors.Is(err, sql.ErrNoRows) {
		return operatorread.RunHeader{}, operatorread.ErrRunNotFound
	}
	if err != nil {
		return operatorread.RunHeader{}, err
	}
	return header, nil
}

func (s *RunPostgres) LoadRunOrigin(ctx context.Context, runID string) (runtimerunlifecycle.RunOrigin, error) {
	header, err := s.LoadRunHeader(ctx, runID)
	if err != nil {
		return runtimerunlifecycle.RunOrigin{}, err
	}
	return header.Origin, nil
}

func (s *RunPostgres) ListRunHeaders(ctx context.Context, opts operatorread.RunHeaderListOptions) ([]operatorread.RunHeader, string, error) {
	if s == nil || s.backend == nil {
		return nil, "", fmt.Errorf("postgres store is required")
	}
	if err := s.requireRunHeaderAccess(); err != nil {
		return nil, "", err
	}
	opts = defaultRunHeaderListOptions(opts)
	args := make([]any, 0, 6)
	where := []string{"TRUE"}
	if opts.Status != "" {
		args = append(args, opts.Status)
		where = append(where, fmt.Sprintf("lower(r.status) = $%d", len(args)))
	}
	if opts.BundleHash != "" {
		args = append(args, opts.BundleHash)
		where = append(where, fmt.Sprintf("r.bundle_hash = $%d", len(args)))
	}
	if opts.Since != nil {
		args = append(args, opts.Since.UTC())
		where = append(where, fmt.Sprintf("r.started_at >= $%d", len(args)))
	}
	if opts.Until != nil {
		args = append(args, opts.Until.UTC())
		where = append(where, fmt.Sprintf("r.started_at <= $%d", len(args)))
	}
	if opts.Cursor != "" {
		startedAt, runID, err := decodeRunHeaderCursor(opts.Cursor)
		if err != nil {
			return nil, "", err
		}
		args = append(args, startedAt.UTC(), runID)
		where = append(where, fmt.Sprintf("(r.started_at < $%d OR (r.started_at = $%d AND r.run_id::text < $%d))", len(args)-1, len(args)-1, len(args)))
	}
	args = append(args, opts.Limit+1)
	query := runHeaderSelectSQL() + "\nWHERE " + strings.Join(where, " AND ") + fmt.Sprintf(`
ORDER BY r.started_at DESC, r.run_id::text DESC
LIMIT $%d
`, len(args))
	rows, err := s.backend.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	headers := make([]operatorread.RunHeader, 0, opts.Limit)
	for rows.Next() {
		header, err := scanRunHeader(rows)
		if err != nil {
			return nil, "", err
		}
		headers = append(headers, header)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	nextCursor := ""
	if len(headers) > opts.Limit {
		headers = headers[:opts.Limit]
		nextCursor = encodeRunHeaderCursor(headers[len(headers)-1])
	}
	return headers, nextCursor, nil
}

func runHeaderSelectSQL() string {
	return `
SELECT
	r.run_id::text,
	lower(r.status),
	r.bundle_hash,
	r.origin_kind,
	COALESCE(r.trigger_event_id::text, ''),
	COALESCE(r.trigger_event_type, ''),
	COALESCE(r.origin_service_id::text, ''),
	COALESCE(r.origin_generation, 0),
	COALESCE(r.forked_from_run_id::text, ''),
	COALESCE(r.forked_from_event_id::text, ''),
	(SELECT COUNT(*)::integer FROM standing_service_generations ssg WHERE ssg.run_id = r.run_id),
	(
		SELECT COUNT(*)::integer
		FROM standing_service_generations ssg
		WHERE ssg.run_id = r.run_id
		  AND ssg.service_id = r.origin_service_id
		  AND ssg.generation = r.origin_generation
	),
	COALESCE(entity_summary.entity_count, 0),
	COALESCE(NULLIF(r.event_count, 0), summary.event_count, 0),
	r.started_at,
	r.ended_at,
	COALESCE(r.continued_as_run_id::text, ''),
	r.failure,
	COALESCE(rc.reason, '')
FROM runs r
	LEFT JOIN run_control_state rc ON rc.run_id = r.run_id
LEFT JOIN LATERAL (
	SELECT COUNT(*)::integer AS event_count
	FROM events e
	WHERE e.run_id = r.run_id
) summary ON TRUE
LEFT JOIN LATERAL (
	SELECT COUNT(DISTINCT es.entity_id)::integer AS entity_count
	FROM entity_state es
	WHERE es.run_id = r.run_id
) entity_summary ON TRUE
`
}

type runHeaderScanner interface {
	Scan(dest ...any) error
}

func scanRunHeader(row runHeaderScanner) (operatorread.RunHeader, error) {
	var header operatorread.RunHeader
	var endedAt sql.NullTime
	var failureRaw []byte
	var bundleHash string
	var originKind, eventID, eventType, serviceID, sourceRunID, sourceEventID string
	var generation int64
	var standingRelationCount, matchingStandingRelationCount int
	if err := row.Scan(
		&header.RunID,
		&header.Status,
		&bundleHash,
		&originKind,
		&eventID,
		&eventType,
		&serviceID,
		&generation,
		&sourceRunID,
		&sourceEventID,
		&standingRelationCount,
		&matchingStandingRelationCount,
		&header.EntityCount,
		&header.EventCount,
		&header.StartedAt,
		&endedAt,
		&header.ContinuedAsRunID,
		&failureRaw,
		&header.ControlReason,
	); err != nil {
		return operatorread.RunHeader{}, err
	}
	header.Status = strings.ToLower(strings.TrimSpace(header.Status))
	var err error
	header.Origin, err = runtimerunlifecycle.DecodeRunOrigin(
		originKind, eventID, eventType, serviceID, generation, sourceRunID, sourceEventID,
	)
	if err != nil {
		return operatorread.RunHeader{}, fmt.Errorf("run %s origin: %w", header.RunID, err)
	}
	if err := validateRunHeaderStandingRelation(
		header.RunID, header.Origin, standingRelationCount, matchingStandingRelationCount,
	); err != nil {
		return operatorread.RunHeader{}, err
	}
	header.ContinuedAsRunID = strings.TrimSpace(header.ContinuedAsRunID)
	header.ControlReason = strings.TrimSpace(header.ControlReason)
	failure, err := decodeStoredFailure(failureRaw)
	if err != nil {
		return operatorread.RunHeader{}, err
	}
	header.Failure = failure
	if endedAt.Valid {
		value := endedAt.Time.UTC()
		header.EndedAt = &value
	}
	header.StartedAt = header.StartedAt.UTC()
	if err := validateRunHeaderLifecycle(header, bundleHash); err != nil {
		return operatorread.RunHeader{}, err
	}
	return header, nil
}

func validateRunHeaderLifecycle(header operatorread.RunHeader, bundleHash string) error {
	state, err := runtimerunlifecycle.ParseState(header.Status)
	if err != nil {
		return fmt.Errorf("run %s lifecycle: %w", header.RunID, err)
	}
	snapshot := runtimerunlifecycle.Snapshot{
		RunID: header.RunID, State: state,
		Origin:     header.Origin,
		BundleHash: strings.TrimSpace(bundleHash),
		EventCount: header.EventCount, EntityCount: header.EntityCount,
		Failure: header.Failure, ContinuedAsRunID: header.ContinuedAsRunID,
		StartedAt: header.StartedAt, EndedAt: header.EndedAt,
	}
	if err := snapshot.Validate(); err != nil {
		return fmt.Errorf("run %s lifecycle: %w", header.RunID, err)
	}
	return nil
}

func validateRunHeaderStandingRelation(
	runID string,
	origin runtimerunlifecycle.RunOrigin,
	relationCount int,
	matchingRelationCount int,
) error {
	if origin.Kind() == runtimerunlifecycle.OriginStandingGeneration {
		if relationCount != 1 || matchingRelationCount != 1 {
			return fmt.Errorf(
				"run %s standing generation origin relation is invalid: relations=%d matching=%d",
				runID,
				relationCount,
				matchingRelationCount,
			)
		}
		return nil
	}
	if relationCount != 0 || matchingRelationCount != 0 {
		return fmt.Errorf(
			"run %s non-standing origin has standing generation relation: relations=%d matching=%d",
			runID,
			relationCount,
			matchingRelationCount,
		)
	}
	return nil
}

func encodeRunHeaderCursor(header operatorread.RunHeader) string {
	raw, _ := json.Marshal(runHeaderCursor{
		StartedAt: header.StartedAt.UTC().Format(time.RFC3339Nano),
		RunID:     strings.TrimSpace(header.RunID),
	})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeRunHeaderCursor(cursor string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(cursor))
	if err != nil {
		return time.Time{}, "", operatorread.ErrInvalidRunListCursor
	}
	var decoded runHeaderCursor
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return time.Time{}, "", operatorread.ErrInvalidRunListCursor
	}
	startedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(decoded.StartedAt))
	if err != nil {
		return time.Time{}, "", operatorread.ErrInvalidRunListCursor
	}
	runID := strings.TrimSpace(decoded.RunID)
	if runID == "" {
		return time.Time{}, "", operatorread.ErrInvalidRunListCursor
	}
	return startedAt, runID, nil
}
