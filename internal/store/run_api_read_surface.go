package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/google/uuid"
)

type RunHeader struct {
	RunID            string                    `json:"run_id"`
	Status           string                    `json:"status"`
	TriggerEventType string                    `json:"trigger_event_type"`
	TriggerEventID   string                    `json:"trigger_event_id"`
	EntityCount      int                       `json:"entity_count"`
	EventCount       int                       `json:"event_count"`
	StartedAt        time.Time                 `json:"started_at"`
	EndedAt          *time.Time                `json:"ended_at,omitempty"`
	ForkedFromRunID  string                    `json:"forked_from_run_id,omitempty"`
	ContinuedAsRunID string                    `json:"continued_as_run_id,omitempty"`
	Failure          *runtimefailures.Envelope `json:"failure,omitempty"`
	ControlReason    string                    `json:"control_reason,omitempty"`
}

type RunHeaderListOptions struct {
	Status     string
	BundleHash string
	Since      *time.Time
	Until      *time.Time
	Limit      int
	Cursor     string
}

type runHeaderCursor struct {
	StartedAt string `json:"started_at"`
	RunID     string `json:"run_id"`
}

func defaultRunHeaderListOptions(opts RunHeaderListOptions) RunHeaderListOptions {
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

func (s *PostgresStore) requireRunHeaderAccess() error {
	return s.requireCurrentSchema()
}

func (s *PostgresStore) LoadRunHeader(ctx context.Context, runID string) (RunHeader, error) {
	if s == nil || s.DB == nil {
		return RunHeader{}, fmt.Errorf("postgres store is required")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return RunHeader{}, ErrRunNotFound
	}
	if _, err := uuid.Parse(runID); err != nil {
		return RunHeader{}, ErrRunNotFound
	}
	if err := s.requireRunHeaderAccess(); err != nil {
		return RunHeader{}, err
	}
	row := s.DB.QueryRowContext(ctx, runHeaderSelectSQL()+`
WHERE r.run_id = $1::uuid
`, runID)
	header, err := scanRunHeader(row)
	if errors.Is(err, sql.ErrNoRows) {
		return RunHeader{}, ErrRunNotFound
	}
	if err != nil {
		return RunHeader{}, err
	}
	if strings.TrimSpace(header.TriggerEventID) == "" || strings.TrimSpace(header.TriggerEventType) == "" {
		return RunHeader{}, fmt.Errorf("run %s is missing trigger event identity", runID)
	}
	return header, nil
}

func (s *PostgresStore) ListRunHeaders(ctx context.Context, opts RunHeaderListOptions) ([]RunHeader, string, error) {
	if s == nil || s.DB == nil {
		return nil, "", fmt.Errorf("postgres store is required")
	}
	if err := s.requireRunHeaderAccess(); err != nil {
		return nil, "", err
	}
	opts = defaultRunHeaderListOptions(opts)
	args := make([]any, 0, 6)
	where := []string{"(r.trigger_event_id IS NOT NULL OR root.event_id IS NOT NULL)"}
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
	rows, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	headers := make([]RunHeader, 0, opts.Limit)
	for rows.Next() {
		header, err := scanRunHeader(rows)
		if err != nil {
			return nil, "", err
		}
		if strings.TrimSpace(header.TriggerEventID) == "" || strings.TrimSpace(header.TriggerEventType) == "" {
			return nil, "", fmt.Errorf("run %s is missing trigger event identity", header.RunID)
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
	COALESCE(r.trigger_event_type, root.event_name, ''),
	COALESCE(r.trigger_event_id::text, root.event_id::text, ''),
	COALESCE(entity_summary.entity_count, 0),
	COALESCE(NULLIF(r.event_count, 0), summary.event_count, 0),
	r.started_at,
	r.ended_at,
	COALESCE(r.forked_from_run_id::text, ''),
	COALESCE(r.continued_as_run_id::text, ''),
	r.failure,
	COALESCE(rc.reason, '')
FROM runs r
	LEFT JOIN run_control_state rc ON rc.run_id = r.run_id
LEFT JOIN LATERAL (
	SELECT e.event_id, e.event_name
	FROM events e
	WHERE e.run_id = r.run_id
	ORDER BY e.created_at ASC, e.event_id ASC
	LIMIT 1
) root ON TRUE
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

func scanRunHeader(row runHeaderScanner) (RunHeader, error) {
	var header RunHeader
	var endedAt sql.NullTime
	var failureRaw []byte
	if err := row.Scan(
		&header.RunID,
		&header.Status,
		&header.TriggerEventType,
		&header.TriggerEventID,
		&header.EntityCount,
		&header.EventCount,
		&header.StartedAt,
		&endedAt,
		&header.ForkedFromRunID,
		&header.ContinuedAsRunID,
		&failureRaw,
		&header.ControlReason,
	); err != nil {
		return RunHeader{}, err
	}
	header.Status = strings.ToLower(strings.TrimSpace(header.Status))
	header.TriggerEventType = strings.TrimSpace(header.TriggerEventType)
	header.TriggerEventID = strings.TrimSpace(header.TriggerEventID)
	header.ForkedFromRunID = strings.TrimSpace(header.ForkedFromRunID)
	header.ContinuedAsRunID = strings.TrimSpace(header.ContinuedAsRunID)
	header.ControlReason = strings.TrimSpace(header.ControlReason)
	failure, err := decodeStoredFailure(failureRaw)
	if err != nil {
		return RunHeader{}, err
	}
	header.Failure = failure
	if err := storerunlifecycle.ValidateStatusFailure(header.Status, header.Failure); err != nil {
		return RunHeader{}, fmt.Errorf("run %s terminal evidence: %w", header.RunID, err)
	}
	if endedAt.Valid {
		value := endedAt.Time.UTC()
		header.EndedAt = &value
	}
	header.StartedAt = header.StartedAt.UTC()
	return header, nil
}

func encodeRunHeaderCursor(header RunHeader) string {
	raw, _ := json.Marshal(runHeaderCursor{
		StartedAt: header.StartedAt.UTC().Format(time.RFC3339Nano),
		RunID:     strings.TrimSpace(header.RunID),
	})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeRunHeaderCursor(cursor string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(cursor))
	if err != nil {
		return time.Time{}, "", ErrInvalidRunListCursor
	}
	var decoded runHeaderCursor
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return time.Time{}, "", ErrInvalidRunListCursor
	}
	startedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(decoded.StartedAt))
	if err != nil {
		return time.Time{}, "", ErrInvalidRunListCursor
	}
	runID := strings.TrimSpace(decoded.RunID)
	if runID == "" {
		return time.Time{}, "", ErrInvalidRunListCursor
	}
	return startedAt, runID, nil
}
