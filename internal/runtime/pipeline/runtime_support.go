package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	"empireai/internal/events"
	runtimeengine "empireai/internal/runtime/engine"
	runtimescanmode "empireai/internal/runtime/scanmode"
	"empireai/internal/runtime/semanticview"
	runtimesharedjson "empireai/internal/runtime/sharedjson"
)

type RuntimeLogEntry struct {
	Level      string
	Component  string
	Action     string
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
	EngineOutbox() runtimeengine.OutboxWriter
	EngineDispatcher() runtimeengine.PostCommitDispatcher
}

type noOpEngineOutbox struct{}

func (noOpEngineOutbox) WriteOutbox(context.Context, []runtimeengine.EmitIntent) error { return nil }

type noOpEngineDispatcher struct{}

func (noOpEngineDispatcher) DispatchPostCommit(context.Context, []runtimeengine.EmitIntent) error {
	return nil
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
	return runtimesharedjson.AsFloat64(v)
}

func asArray(v any) ([]any, bool) {
	return runtimesharedjson.AsArray(v)
}

func asObject(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	return m, ok
}

type sqlTxContextKey struct{}
type pipelinePostCommitActionsKey struct{}

func withSQLTxContext(ctx context.Context, tx *sql.Tx) context.Context {
	if tx == nil {
		return ctx
	}
	return context.WithValue(ctx, sqlTxContextKey{}, tx)
}

func WithPipelineSQLTxContext(ctx context.Context, tx *sql.Tx) context.Context {
	return withSQLTxContext(ctx, tx)
}

func sqlTxFromContext(ctx context.Context) (*sql.Tx, bool) {
	if ctx == nil {
		return nil, false
	}
	tx, ok := ctx.Value(sqlTxContextKey{}).(*sql.Tx)
	return tx, ok && tx != nil
}

func PipelineSQLTxFromContext(ctx context.Context) (*sql.Tx, bool) {
	return sqlTxFromContext(ctx)
}

func withoutSQLTxContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return context.WithValue(ctx, sqlTxContextKey{}, (*sql.Tx)(nil))
}

func withPipelinePostCommitActions(ctx context.Context, actions *[]func()) context.Context {
	if actions == nil {
		return ctx
	}
	return context.WithValue(ctx, pipelinePostCommitActionsKey{}, actions)
}

func WithPipelinePostCommitActions(ctx context.Context, actions *[]func()) context.Context {
	return withPipelinePostCommitActions(ctx, actions)
}

func queuePipelinePostCommitAction(ctx context.Context, fn func()) bool {
	if ctx == nil || fn == nil {
		return false
	}
	actions, ok := ctx.Value(pipelinePostCommitActionsKey{}).(*[]func())
	if !ok || actions == nil {
		return false
	}
	*actions = append(*actions, fn)
	return true
}

func flushPipelinePostCommitActions(actions []func()) {
	for _, fn := range actions {
		if fn != nil {
			fn()
		}
	}
}

func FlushPipelinePostCommitActions(actions []func()) {
	flushPipelinePostCommitActions(actions)
}

func CollectPipelineEmitIntents(ctx context.Context, intents []runtimeengine.EmitIntent) bool {
	if ctx == nil || len(intents) == 0 {
		return false
	}
	collected := false
	if collector, ok := ctx.Value(pipelineEmitIntentCollectorKey{}).(*[]runtimeengine.EmitIntent); ok && collector != nil {
		cloned := make([]runtimeengine.EmitIntent, 0, len(intents))
		for _, intent := range intents {
			copyIntent := intent
			copyIntent.Event = cloneEvent(intent.Event)
			copyIntent.Recipients = append([]string{}, intent.Recipients...)
			cloned = append(cloned, copyIntent)
		}
		*collector = append(*collector, cloned...)
		collected = true
	}
	if collector, ok := ctx.Value(pipelineEmitCollectorKey{}).(*[]events.Event); ok && collector != nil {
		for _, intent := range intents {
			*collector = append(*collector, cloneEvent(intent.Event))
		}
		collected = true
	}
	return collected
}

func WithPipelineEmitCollectors(ctx context.Context, eventsCollector *[]events.Event, intentCollector *[]runtimeengine.EmitIntent) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if eventsCollector != nil {
		ctx = context.WithValue(ctx, pipelineEmitCollectorKey{}, eventsCollector)
	}
	if intentCollector != nil {
		ctx = context.WithValue(ctx, pipelineEmitIntentCollectorKey{}, intentCollector)
	}
	return ctx
}

func pipelinePostCommitActionsFromContext(ctx context.Context) (*[]func(), bool) {
	if ctx == nil {
		return nil, false
	}
	actions, ok := ctx.Value(pipelinePostCommitActionsKey{}).(*[]func())
	return actions, ok && actions != nil
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

func normalizeScanMode(raw string) string     { return runtimescanmode.NormalizeMode(raw) }
func normalizeScanPriority(raw string) string { return runtimescanmode.NormalizePriority(raw) }

func bundleDefaultScanMode(source semanticview.Source) string {
	if source != nil {
		if value, ok := scanModePolicyValue(source, "default_scan_mode"); ok {
			if mode := strings.TrimSpace(asString(value)); mode != "" {
				return normalizeScanMode(mode)
			}
		}
	}
	return normalizeScanMode(runtimescanmode.DefaultMode())
}

func compatibilityExpectedScannerCount(mode string, expectedScanners []string, storedExpected int) int {
	if storedExpected > 0 {
		return storedExpected
	}
	if len(expectedScanners) == 1 && strings.TrimSpace(expectedScanners[0]) == "scanner" {
		if expected := runtimescanmode.ExpectedScannerCount(mode); expected > 0 {
			return expected
		}
	}
	return len(expectedScanners)
}

func scanDispatchExpectedAgents(mode string, expectedScanners []string) int {
	if strings.TrimSpace(mode) != "" && len(expectedScanners) > 0 {
		if expected := runtimescanmode.ExpectedScannerCount(mode); expected > 0 {
			return expected
		}
	}
	if len(expectedScanners) > 0 {
		return len(expectedScanners)
	}
	return 1
}

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
