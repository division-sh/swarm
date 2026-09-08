package decisionpersistence

import (
	"context"
	"fmt"
	"strings"
	"time"

	runstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	runhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

const runLifecycleActiveStateSQLValues = runstate.ActiveStateSQLValues

func sqliteNullTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

type runLifecycleCandidateHandoffReservation = runhandoff.CandidateHandoff

func reserveRunLifecycleCandidateHandoff(ctx context.Context) (*runLifecycleCandidateHandoffReservation, error) {
	return runhandoff.ReserveCandidateHandoff(ctx)
}

func withRunLifecycleCandidateHandoff(ctx context.Context, fn func(*runLifecycleCandidateHandoffReservation) error) error {
	return runhandoff.WithCandidateHandoff(ctx, fn)
}

func requirePostgresRunActiveQuery(ctx context.Context, queryer runstate.RowQueryer, runID string) error {
	return runstate.RequirePostgresActiveQuery(ctx, queryer, runID)
}

func requireSQLiteRunActiveQuery(ctx context.Context, queryer runstate.RowQueryer, runID string) error {
	return runstate.RequireSQLiteActiveQuery(ctx, queryer, runID)
}

func sqliteTimeValue(raw any) (time.Time, bool, error) {
	switch value := raw.(type) {
	case nil:
		return time.Time{}, false, nil
	case time.Time:
		return value.UTC(), !value.IsZero(), nil
	case string:
		return parseSQLiteTime(value)
	case []byte:
		return parseSQLiteTime(string(value))
	default:
		return time.Time{}, false, fmt.Errorf("unsupported SQLite time value %T", raw)
	}
}

func parseSQLiteTime(raw string) (time.Time, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false, nil
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999 -0700 MST", "2006-01-02 15:04:05 -0700 MST"} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC(), true, nil
		}
	}
	return time.Time{}, false, fmt.Errorf("invalid SQLite time %q", raw)
}
