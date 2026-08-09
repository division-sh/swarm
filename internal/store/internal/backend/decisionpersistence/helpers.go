package decisionpersistence

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	runhandoff "github.com/division-sh/swarm/internal/store/internal/runhandoff"
)

const runLifecycleActiveStateSQLValues = runstate.ActiveStateSQLValues

type decisionCursor struct {
	CreatedAt time.Time `json:"created_at"`
	CardID    string    `json:"card_id"`
}

func decodeDecisionCursor(raw string) (decisionCursor, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return decisionCursor{}, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return decisionCursor{}, fmt.Errorf("invalid decision-card cursor")
	}
	var cursor decisionCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil || cursor.CreatedAt.IsZero() || strings.TrimSpace(cursor.CardID) == "" {
		return decisionCursor{}, fmt.Errorf("invalid decision-card cursor")
	}
	return cursor, nil
}

func encodeDecisionCursor(createdAt time.Time, cardID string) string {
	raw, _ := json.Marshal(decisionCursor{CreatedAt: createdAt.UTC(), CardID: strings.TrimSpace(cardID)})
	return base64.RawURLEncoding.EncodeToString(raw)
}

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
