package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimesharedjson "github.com/division-sh/swarm/internal/runtime/sharedjson"
)

type RuntimeLogEntry = diaglog.RunEntry

func pipelineRuntimeFailure(err error, component, operation string) *runtimefailures.Envelope {
	if err == nil {
		return nil
	}
	failure := runtimefailures.Normalize(err, component, operation)
	return &failure
}

func pipelineDependencyFailure(err error, detailCode, component, operation string) *runtimefailures.Envelope {
	if err == nil {
		return nil
	}
	failure := runtimefailures.Normalize(runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, detailCode, component, operation, nil, err), component, operation)
	return &failure
}

type Bus interface {
	Publish(ctx context.Context, evt events.Event) error
	PublishDirect(ctx context.Context, evt events.Event, recipients []string) error
	ResolveSubscribedRecipients(eventType string) []string
	LogRuntime(ctx context.Context, entry RuntimeLogEntry) error
	EngineDispatcher() runtimeengine.PostCommitDispatcher
}

type noOpEngineDispatcher struct{}

func (noOpEngineDispatcher) DispatchPostCommit(context.Context, []runtimeengine.EmitIntent) error {
	return nil
}

type noOpActivityIntentWriter struct{}

func (noOpActivityIntentWriter) WriteActivityIntents(context.Context, []runtimeengine.ActivityIntent) error {
	return nil
}

type noOpActivityDispatcher struct{}

func (noOpActivityDispatcher) DispatchActivities(context.Context, []runtimeengine.ActivityIntent) error {
	return nil
}

type pipelineFlowScopeKey struct{}
type workflowNodeDeliveryRouteKey struct{}

func withWorkflowNodeDeliveryRoute(ctx context.Context, route events.DeliveryRoute) context.Context {
	if ctx == nil {
		return nil
	}
	route = route.Normalized()
	ctx = events.WithDeliveryContext(ctx, route.Context)
	ctx = runtimedelivery.WithRoute(ctx, route)
	return context.WithValue(ctx, workflowNodeDeliveryRouteKey{}, route)
}

func workflowNodeDeliveryRoute(ctx context.Context) (events.DeliveryRoute, bool) {
	if ctx == nil {
		return events.DeliveryRoute{}, false
	}
	route, ok := ctx.Value(workflowNodeDeliveryRouteKey{}).(events.DeliveryRoute)
	if !ok {
		return events.DeliveryRoute{}, false
	}
	route = route.Normalized()
	return route, route.Recipient.IsNode()
}

func withPipelineFlowScope(ctx context.Context, flowID string) context.Context {
	if ctx == nil {
		return nil
	}
	flowID = strings.TrimSpace(flowID)
	return context.WithValue(ctx, pipelineFlowScopeKey{}, flowID)
}

func pipelineFlowScope(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	flowID, _ := ctx.Value(pipelineFlowScopeKey{}).(string)
	return strings.TrimSpace(flowID)
}

func pipelineCollectorExecutionContext(ctx context.Context) (context.Context, *[]events.Event, *[]runtimeengine.EmitIntent, bool) {
	if ctx == nil {
		return ctx, nil, nil, false
	}
	parentCollector, ok := ctx.Value(pipelineEmitCollectorKey{}).(*[]events.Event)
	if !ok || parentCollector == nil {
		return ctx, nil, nil, false
	}
	if _, ok := ctx.Value(pipelineEmitIntentCollectorKey{}).(*[]runtimeengine.EmitIntent); ok {
		return ctx, parentCollector, nil, false
	}
	collected := []runtimeengine.EmitIntent{}
	ctx = WithPipelineEmitCollectors(ctx, nil, &collected)
	return ctx, parentCollector, &collected, true
}

func flushCollectedPipelineEmitIntents(parentCollector *[]events.Event, collected *[]runtimeengine.EmitIntent) {
	if parentCollector == nil || collected == nil || len(*collected) == 0 {
		return
	}
	appendEmitIntentsAsEvents(parentCollector, *collected)
}

const DefaultSystemNodeRetryLimit = 5

func mustJSON(v any) []byte {
	return runtimesharedjson.MustJSON(v)
}

func parsePayloadMap(raw []byte) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil || out == nil {
		return map[string]any{}
	}
	return out
}

func payloadMap(v any) map[string]any {
	if v == nil {
		return map[string]any{}
	}
	switch typed := v.(type) {
	case map[string]any:
		return cloneMap(typed)
	default:
		var out map[string]any
		if err := json.Unmarshal(mustJSON(v), &out); err != nil || out == nil {
			return map[string]any{}
		}
		return out
	}
}

func cloneMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneStringAnyMap(in map[string]any) map[string]any {
	return cloneMap(in)
}

func policyDocumentToMap(doc runtimecontracts.PolicyDocument) map[string]any {
	if len(doc.Values) == 0 {
		return nil
	}
	m := make(map[string]any, len(doc.Values))
	for k, v := range doc.Values {
		m[k] = v.Value
	}
	return m
}

func asString(v any) string {
	return strings.TrimSpace(runtimesharedjson.AsString(v))
}

func boolFromAny(v any) bool {
	switch typed := v.(type) {
	case bool:
		return typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "true", "yes", "on":
			return true
		default:
			return false
		}
	default:
		return asInt(v) > 0
	}
}

func firstNonEmptyString(vals ...string) string {
	for _, val := range vals {
		if trimmed := strings.TrimSpace(val); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func workflowExpressionLookupPath(root map[string]any, path string) (any, bool) {
	current := any(root)
	for _, segment := range strings.Split(strings.TrimSpace(path), ".") {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			return nil, false
		}
		m, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		value, ok := m[segment]
		if !ok {
			return nil, false
		}
		current = value
	}
	return current, true
}

func processWarn(component string, format string, args ...any) {
	component = strings.TrimSpace(component)
	if component == "" {
		component = "runtime"
	}
	msg := strings.TrimSpace(fmt.Sprintf(format, args...))
	if msg == "" {
		return
	}
	diaglog.ProcessLog("warn", component, msg)
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

func CollectPipelineEmitIntents(ctx context.Context, intents []runtimeengine.EmitIntent) bool {
	if ctx == nil || len(intents) == 0 {
		return false
	}
	collected := false
	if collector, ok := ctx.Value(pipelineEmitIntentCollectorKey{}).(*[]runtimeengine.EmitIntent); ok && collector != nil {
		*collector = append(*collector, cloneEmitIntents(intents)...)
		collected = true
	}
	return collected
}

func cloneEmitIntents(intents []runtimeengine.EmitIntent) []runtimeengine.EmitIntent {
	if len(intents) == 0 {
		return nil
	}
	cloned := make([]runtimeengine.EmitIntent, 0, len(intents))
	for _, intent := range intents {
		copyIntent := intent
		copyIntent.Event = cloneEvent(intent.Event)
		copyIntent.Recipients = append([]string{}, intent.Recipients...)
		cloned = append(cloned, copyIntent)
	}
	return cloned
}

func appendEmitIntentsAsEvents(collector *[]events.Event, intents []runtimeengine.EmitIntent) {
	if collector == nil || len(intents) == 0 {
		return
	}
	for _, intent := range intents {
		emitted := cloneEvent(intent.Event)
		if !intent.Context.Empty() {
			emitted = events.NewContextDeliveryEvent(emitted, intent.Context).Event()
		}
		*collector = append(*collector, emitted)
	}
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

func withoutPipelineEmitCollectors(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	var eventsCollector *[]events.Event
	var intentCollector *[]runtimeengine.EmitIntent
	ctx = context.WithValue(ctx, pipelineEmitCollectorKey{}, eventsCollector)
	return context.WithValue(ctx, pipelineEmitIntentCollectorKey{}, intentCollector)
}

func shouldSQLDebugLog() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("SWARM_SQL_DEBUG")))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func compactSQLSnippet(q string) string {
	q = strings.Join(strings.Fields(strings.TrimSpace(q)), " ")
	if len(q) > 240 {
		return q[:240] + "..."
	}
	return q
}
