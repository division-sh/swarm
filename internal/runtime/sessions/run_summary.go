package sessions

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type SummaryDialect string

const (
	SummaryDialectPostgres SummaryDialect = "postgres"
	SummaryDialectSQLite   SummaryDialect = "sqlite"
)

type SummaryQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// RunSummary is the session owner's selected-store-time view of execution
// leases that can still perform work for one run.
type RunSummary struct {
	RunID          string
	ActiveLeases   int
	MalformedLease int
	NextExpiry     time.Time
	ObservedAt     time.Time
}

func (s RunSummary) Validate() error {
	if strings.TrimSpace(s.RunID) == "" {
		return fmt.Errorf("session run summary requires run_id")
	}
	if s.ActiveLeases < 0 {
		return fmt.Errorf("session run summary active lease count cannot be negative")
	}
	if s.MalformedLease < 0 {
		return fmt.Errorf("session run summary malformed lease count cannot be negative")
	}
	if s.ObservedAt.IsZero() {
		return fmt.Errorf("session run summary requires selected-store observation time")
	}
	if s.ActiveLeases == 0 && !s.NextExpiry.IsZero() {
		return fmt.Errorf("settled session run summary forbids next expiry")
	}
	if s.ActiveLeases > 0 && s.NextExpiry.IsZero() {
		return fmt.Errorf("active session run summary requires next expiry")
	}
	return nil
}

func (s RunSummary) BlocksCompletion() bool {
	return s.ActiveLeases > 0 || s.MalformedLease > 0
}

func ReadRunSummary(
	ctx context.Context,
	queryer SummaryQueryer,
	dialect SummaryDialect,
	runID string,
	observedAt time.Time,
) (RunSummary, error) {
	runID = strings.TrimSpace(runID)
	observedAt = canonicalSummaryTime(observedAt)
	if queryer == nil || runID == "" || observedAt.IsZero() {
		return RunSummary{}, fmt.Errorf("session run summary requires selected store, run_id, and observation time")
	}
	query := `
		SELECT status, lease_holder, lease_expires_at
		FROM agent_sessions
		WHERE run_id = ?
		ORDER BY session_id
	`
	args := []any{runID}
	switch dialect {
	case SummaryDialectSQLite:
	case SummaryDialectPostgres:
		query = `
			SELECT status, lease_holder, lease_expires_at
			FROM agent_sessions
			WHERE run_id = $1::uuid
			ORDER BY session_id
			FOR SHARE
		`
	default:
		return RunSummary{}, fmt.Errorf("session run summary requires selected store dialect")
	}
	rows, err := queryer.QueryContext(ctx, query, args...)
	if err != nil {
		return RunSummary{}, fmt.Errorf("read session run summary: %w", err)
	}
	defer rows.Close()

	summary := RunSummary{RunID: runID, ObservedAt: observedAt}
	for rows.Next() {
		var (
			status string
			holder sql.NullString
			raw    any
		)
		if err := rows.Scan(&status, &holder, &raw); err != nil {
			return RunSummary{}, fmt.Errorf("scan session run summary: %w", err)
		}
		status = strings.TrimSpace(status)
		holderPresent := holder.Valid && strings.TrimSpace(holder.String) != ""
		expiresAt, expiryPresent, expiryErr := summaryTimeValue(raw)
		if expiryErr != nil {
			summary.MalformedLease++
			continue
		}
		switch status {
		case "active":
			if holderPresent != expiryPresent {
				summary.MalformedLease++
				continue
			}
			if !holderPresent {
				continue
			}
			expiresAt = canonicalSummaryTime(expiresAt)
			if !expiresAt.After(observedAt) {
				continue
			}
			summary.ActiveLeases++
			if summary.NextExpiry.IsZero() || expiresAt.Before(summary.NextExpiry) {
				summary.NextExpiry = expiresAt
			}
		case "suspended", "terminated":
			if holderPresent || expiryPresent {
				summary.MalformedLease++
			}
		default:
			summary.MalformedLease++
		}
	}
	if err := rows.Err(); err != nil {
		return RunSummary{}, fmt.Errorf("iterate session run summary: %w", err)
	}
	return summary, summary.Validate()
}

func canonicalSummaryTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.UTC().Round(time.Microsecond)
}

func summaryTimeValue(raw any) (time.Time, bool, error) {
	switch value := raw.(type) {
	case nil:
		return time.Time{}, false, nil
	case time.Time:
		return value.UTC(), true, nil
	case string:
		parsed, err := parseSummaryTime(value)
		return parsed, true, err
	case []byte:
		parsed, err := parseSummaryTime(string(value))
		return parsed, true, err
	default:
		return time.Time{}, true, fmt.Errorf("unsupported session lease timestamp %T", raw)
	}
}

func parseSummaryTime(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	for _, format := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999 -0700 MST",
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05",
	} {
		if parsed, err := time.Parse(format, raw); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid session lease timestamp %q", raw)
}
