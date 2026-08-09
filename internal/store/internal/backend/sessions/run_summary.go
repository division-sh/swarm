package sessionstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
)

type SummaryDialect string

const (
	SummaryDialectPostgres SummaryDialect = "postgres"
	SummaryDialectSQLite   SummaryDialect = "sqlite"
)

type SummaryQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func ReadRunSummary(
	ctx context.Context,
	queryer SummaryQueryer,
	dialect SummaryDialect,
	runID string,
	observedAt time.Time,
) (runtimesessions.RunSummary, error) {
	runID = strings.TrimSpace(runID)
	observedAt = canonicalSummaryTime(observedAt)
	if queryer == nil || runID == "" || observedAt.IsZero() {
		return runtimesessions.RunSummary{}, fmt.Errorf("session run summary requires selected store, run_id, and observation time")
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
		return runtimesessions.RunSummary{}, fmt.Errorf("session run summary requires selected store dialect")
	}
	rows, err := queryer.QueryContext(ctx, query, args...)
	if err != nil {
		return runtimesessions.RunSummary{}, fmt.Errorf("read session run summary: %w", err)
	}
	defer rows.Close()

	summary := runtimesessions.RunSummary{RunID: runID, ObservedAt: observedAt}
	for rows.Next() {
		var (
			status string
			holder sql.NullString
			raw    any
		)
		if err := rows.Scan(&status, &holder, &raw); err != nil {
			return runtimesessions.RunSummary{}, fmt.Errorf("scan session run summary: %w", err)
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
		return runtimesessions.RunSummary{}, fmt.Errorf("iterate session run summary: %w", err)
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
