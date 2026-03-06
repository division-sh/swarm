package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	"empireai/internal/events"
)

type RuntimeLogEntry struct {
	Level     string
	Component string
	Action    string
	EventID    string
	EventType  string
	AgentID    string
	VerticalID string
	CampaignID string
	ScanID     string
	SessionID  string
	Detail     any
	Error      string
	DurationUS int
}

type Bus interface {
	Subscribe(agentID string, eventTypes ...events.EventType) <-chan events.Event
	Publish(ctx context.Context, evt events.Event) error
	PublishDirect(ctx context.Context, evt events.Event, recipients []string) error
	ResolveSubscribedRecipients(eventType string) []string
	LogRuntime(ctx context.Context, entry RuntimeLogEntry)
}

func runtimeWarn(component string, format string, args ...any) {
	component = strings.TrimSpace(component)
	if component == "" {
		component = "runtime"
	}
	msg := strings.TrimSpace(fmt.Sprintf(format, args...))
	if msg == "" {
		return
	}
	log.Printf("runtime.warn component=%s message=%s", component, msg)
}

func snippetForLog(raw string, max int) string {
	raw = strings.TrimSpace(raw)
	if max <= 0 {
		max = 180
	}
	if len(raw) <= max {
		return raw
	}
	return raw[:max] + "..."
}

func uniqueStrings(in []string) []string {
	if len(in) <= 1 {
		return in
	}
	set := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := set[v]; ok {
			continue
		}
		set[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func asInt(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case float64:
		return int(t)
	case string:
		t = strings.TrimSpace(t)
		if t == "" {
			return 0
		}
		var n int
		_, _ = fmt.Sscanf(t, "%d", &n)
		return n
	default:
		return 0
	}
}

func asFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

func asArray(v any) ([]any, bool) {
	switch t := v.(type) {
	case []any:
		return t, true
	case []string:
		out := make([]any, 0, len(t))
		for _, s := range t { out = append(out, s) }
		return out, true
	default:
		return nil, false
	}
}

func asObject(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	return m, ok
}

type sqlTxContextKey struct{}

func withSQLTxContext(ctx context.Context, tx *sql.Tx) context.Context {
	if tx == nil {
		return ctx
	}
	return context.WithValue(ctx, sqlTxContextKey{}, tx)
}

func sqlTxFromContext(ctx context.Context) (*sql.Tx, bool) {
	if ctx == nil { return nil, false }
	tx, ok := ctx.Value(sqlTxContextKey{}).(*sql.Tx)
	return tx, ok && tx != nil
}

func withoutSQLTxContext(ctx context.Context) context.Context {
	if ctx == nil { return context.Background() }
	return context.WithValue(ctx, sqlTxContextKey{}, (*sql.Tx)(nil))
}

func dbExecContext(ctx context.Context, db *sql.DB, query string, args ...any) (sql.Result, error) {
	exec := func() (sql.Result, error) {
		if tx, ok := sqlTxFromContext(ctx); ok {
			return tx.ExecContext(ctx, query, args...)
		}
		return db.ExecContext(ctx, query, args...)
	}
	res, err := exec()
	if err != nil && shouldSQLDebugLog() {
		log.Printf("runtime.sql.exec error=%v query=%q args=%d", err, compactSQLSnippet(query), len(args))
	}
	return res, err
}

func dbQueryContext(ctx context.Context, db *sql.DB, query string, args ...any) (*sql.Rows, error) {
	exec := func() (*sql.Rows, error) {
		if tx, ok := sqlTxFromContext(ctx); ok {
			return tx.QueryContext(ctx, query, args...)
		}
		return db.QueryContext(ctx, query, args...)
	}
	rows, err := exec()
	if err != nil && shouldSQLDebugLog() {
		log.Printf("runtime.sql.query error=%v query=%q args=%d", err, compactSQLSnippet(query), len(args))
	}
	return rows, err
}

func dbQueryRowContext(ctx context.Context, db *sql.DB, query string, args ...any) *sql.Row {
	if tx, ok := sqlTxFromContext(ctx); ok {
		return tx.QueryRowContext(ctx, query, args...)
	}
	return db.QueryRowContext(ctx, query, args...)
}

func shouldSQLDebugLog() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("EMPIREAI_SQL_DEBUG")))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func compactSQLSnippet(q string) string {
	q = strings.Join(strings.Fields(strings.TrimSpace(q)), " ")
	if len(q) > 240 {
		return q[:240] + "..."
	}
	return q
}

func normalizeScanMode(raw string) string     { return NormalizeScanMode(raw) }
func normalizeScanPriority(raw string) string { return NormalizeScanPriority(raw) }

func coalesce(values ...string) string {
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v != "" {
			return v
		}
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	return coalesce(values...)
}
