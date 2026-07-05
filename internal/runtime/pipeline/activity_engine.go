package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/google/uuid"
)

const activityRequestEventType = events.EventType("platform.activity_requested")

type pipelineActivityIntentWriter struct {
	coordinator *PipelineCoordinator
}

func (w pipelineActivityIntentWriter) WriteActivityIntents(ctx context.Context, intents []runtimeengine.ActivityIntent) error {
	if len(intents) == 0 || w.coordinator == nil || w.coordinator.bus == nil {
		return nil
	}
	outbox := w.coordinator.bus.EngineOutbox()
	if outbox == nil {
		return fmt.Errorf("activity intent writer requires pipeline outbox")
	}
	requests, err := activityRequestEmitIntents(intents)
	if err != nil {
		return err
	}
	if err := outbox.WriteOutbox(ctx, requests); err != nil {
		return err
	}
	for _, intent := range intents {
		intent = intent.Normalized()
		if err := w.coordinator.bus.LogRuntime(ctx, RuntimeLogEntry{
			Level:     "info",
			Component: "activity",
			Action:    "intent_persisted",
			EventID:   activityRequestEventID(intent),
			EventType: intent.SuccessEvent,
			EntityID:  intent.EntityID.String(),
			Detail: map[string]any{
				"activity_id":        intent.ActivityID,
				"tool":               intent.Tool,
				"effect_class":       string(intent.EffectClass),
				"success_event":      intent.SuccessEvent,
				"failure_event":      intent.FailureEvent,
				"retry_max_attempts": intent.RetryMaxAttempts,
				"retry_backoff":      intent.RetryBackoff,
				"fork_policy":        string(intent.ForkPolicy),
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

type pipelineActivityDispatcher struct {
	coordinator *PipelineCoordinator
	client      *http.Client
}

func (d pipelineActivityDispatcher) DispatchActivities(ctx context.Context, intents []runtimeengine.ActivityIntent) error {
	if len(intents) == 0 {
		return nil
	}
	if d.coordinator == nil || d.coordinator.bus == nil {
		return fmt.Errorf("activity dispatcher requires pipeline bus")
	}
	dispatcher := d.coordinator.bus.EngineDispatcher()
	if dispatcher == nil {
		return fmt.Errorf("activity dispatcher requires pipeline outbox dispatcher")
	}
	requests, err := activityRequestEmitIntents(intents)
	if err != nil {
		return err
	}
	return dispatcher.DispatchPostCommit(ctx, requests)
}

func (d pipelineActivityDispatcher) executeActivityIntent(ctx context.Context, intent runtimeengine.ActivityIntent) error {
	source := d.coordinator.SemanticSource()
	if source == nil {
		return fmt.Errorf("activity dispatcher requires semantic source")
	}
	client := d.client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	intent = intent.Normalized()
	tool, ok := source.ToolEntries()[intent.Tool]
	if !ok {
		return d.publishActivityFailure(ctx, intent, fmt.Errorf("activity tool %q is not declared", intent.Tool))
	}
	toolEffectClass := runtimecontracts.NormalizeActivityEffectClass(tool.EffectClass)
	if toolEffectClass != intent.EffectClass {
		return d.publishActivityFailure(ctx, intent, fmt.Errorf("activity tool %q effect_class changed from request %q to %q", intent.Tool, intent.EffectClass, toolEffectClass))
	}
	if !runtimecontracts.SupportedActivityEffectClass(toolEffectClass) {
		return d.publishActivityFailure(ctx, intent, fmt.Errorf("activity tool %q effect_class %q is not executable in Stage 1", intent.Tool, toolEffectClass))
	}
	maxAttempts := activityRetryMaxAttempts(intent, toolEffectClass)
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		attemptIntent := intent
		attemptIntent.Attempt = attempt
		result, err := executeActivityHTTPTool(ctx, client, attemptIntent, tool)
		if err == nil {
			return d.publishActivitySuccess(ctx, attemptIntent, result)
		}
		lastErr = err
		if attempt < maxAttempts {
			if err := waitActivityRetryBackoff(ctx, intent.RetryBackoff, attempt); err != nil {
				return err
			}
		}
	}
	failureIntent := intent
	failureIntent.Attempt = maxAttempts
	return d.publishActivityFailure(ctx, failureIntent, lastErr)
}

func (pc *PipelineCoordinator) handleActivityRequestEvent(ctx context.Context, evt events.Event) (bool, error) {
	if pc == nil || evt.Type() != activityRequestEventType {
		return false, nil
	}
	intent, err := activityIntentFromRequestEvent(evt)
	if err != nil {
		return true, err
	}
	dispatcher := pipelineActivityDispatcher{coordinator: pc}
	if err := dispatcher.executeActivityIntent(ctx, intent); err != nil {
		return true, err
	}
	return true, nil
}

type activityRequestPayload struct {
	ActivityID       string         `json:"activity_id"`
	Tool             string         `json:"tool"`
	Input            map[string]any `json:"input"`
	EffectClass      string         `json:"effect_class"`
	SuccessEvent     string         `json:"success_event"`
	FailureEvent     string         `json:"failure_event"`
	RetryMaxAttempts int            `json:"retry_max_attempts"`
	RetryBackoff     string         `json:"retry_backoff"`
	ForkPolicy       string         `json:"fork_policy"`
	EntityID         string         `json:"entity_id"`
	NodeID           string         `json:"node_id"`
	FlowID           string         `json:"flow_id"`
	HandlerEventKey  string         `json:"handler_event_key"`
	SourceEventID    string         `json:"source_event_id"`
	SourceRunID      string         `json:"source_run_id"`
	SourceTaskID     string         `json:"source_task_id"`
	ParentEventID    string         `json:"parent_event_id"`
	ChainDepth       int            `json:"chain_depth"`
	Attempt          int            `json:"attempt"`
}

func activityRequestEmitIntents(intents []runtimeengine.ActivityIntent) ([]runtimeengine.EmitIntent, error) {
	if len(intents) == 0 {
		return nil, nil
	}
	out := make([]runtimeengine.EmitIntent, 0, len(intents))
	for _, intent := range intents {
		request, err := activityRequestEmitIntent(intent)
		if err != nil {
			return nil, err
		}
		out = append(out, request)
	}
	return out, nil
}

func activityRequestEmitIntent(intent runtimeengine.ActivityIntent) (runtimeengine.EmitIntent, error) {
	intent = intent.Normalized()
	payload := activityRequestPayloadFromIntent(intent)
	raw, err := json.Marshal(payload)
	if err != nil {
		return runtimeengine.EmitIntent{}, err
	}
	evt := events.NewChildEventWithLineage(
		activityRequestEventID(intent),
		activityRequestEventType,
		runtimeWorkflowID,
		intent.SourceTaskID,
		raw,
		intent.ChainDepth+1,
		events.EventLineage{
			RunID:         intent.SourceRunID,
			ParentEventID: firstNonEmptyString(intent.SourceEventID, intent.ParentEventID),
			TaskID:        intent.SourceTaskID,
		},
		events.EventEnvelope{
			EntityID: intent.EntityID.String(),
			Source: events.RouteIdentity{
				FlowID:   intent.FlowID.String(),
				EntityID: intent.EntityID.String(),
			},
		},
		time.Now().UTC(),
	)
	return runtimeengine.EmitIntent{Event: evt}, nil
}

func activityRequestEventID(intent runtimeengine.ActivityIntent) string {
	intent = intent.Normalized()
	parts := []string{
		intent.SourceRunID,
		intent.SourceEventID,
		intent.ParentEventID,
		intent.EntityID.String(),
		intent.FlowID.String(),
		intent.NodeID.String(),
		intent.HandlerEventKey,
		intent.ActivityID,
		fmt.Sprintf("%d", intent.Attempt),
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("swarm:activity-request:"+strings.Join(parts, "\x00"))).String()
}

func activityResultEventID(intent runtimeengine.ActivityIntent, eventType string) string {
	intent = intent.Normalized()
	parts := []string{
		intent.SourceRunID,
		intent.SourceEventID,
		intent.ParentEventID,
		intent.EntityID.String(),
		intent.FlowID.String(),
		intent.NodeID.String(),
		intent.HandlerEventKey,
		intent.ActivityID,
		intent.Tool,
		eventidentity.Normalize(eventType),
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("swarm:activity-result:"+strings.Join(parts, "\x00"))).String()
}

func activityRetryMaxAttempts(intent runtimeengine.ActivityIntent, effectClass runtimecontracts.ActivityEffectClass) int {
	if intent.RetryMaxAttempts > 0 {
		return intent.RetryMaxAttempts
	}
	defaults := runtimecontracts.ActivityRetryDefaultsForEffectClass(effectClass)
	if defaults.MaxAttempts > 0 {
		return defaults.MaxAttempts
	}
	return 1
}

func waitActivityRetryBackoff(ctx context.Context, backoff string, completedAttempt int) error {
	delay := activityRetryDelay(backoff, completedAttempt)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func activityRetryDelay(backoff string, completedAttempt int) time.Duration {
	switch strings.TrimSpace(strings.ToLower(backoff)) {
	case "", "none":
		return 0
	case "exponential":
		if completedAttempt < 1 {
			completedAttempt = 1
		}
		delay := 10 * time.Millisecond
		for i := 1; i < completedAttempt && delay < time.Second; i++ {
			delay *= 2
		}
		if delay > time.Second {
			return time.Second
		}
		return delay
	default:
		return 10 * time.Millisecond
	}
}

func activityRequestPayloadFromIntent(intent runtimeengine.ActivityIntent) activityRequestPayload {
	intent = intent.Normalized()
	return activityRequestPayload{
		ActivityID:       intent.ActivityID,
		Tool:             intent.Tool,
		Input:            cloneStringAnyMap(intent.Input),
		EffectClass:      string(intent.EffectClass),
		SuccessEvent:     intent.SuccessEvent,
		FailureEvent:     intent.FailureEvent,
		RetryMaxAttempts: intent.RetryMaxAttempts,
		RetryBackoff:     intent.RetryBackoff,
		ForkPolicy:       string(intent.ForkPolicy),
		EntityID:         intent.EntityID.String(),
		NodeID:           intent.NodeID.String(),
		FlowID:           intent.FlowID.String(),
		HandlerEventKey:  intent.HandlerEventKey,
		SourceEventID:    intent.SourceEventID,
		SourceRunID:      intent.SourceRunID,
		SourceTaskID:     intent.SourceTaskID,
		ParentEventID:    intent.ParentEventID,
		ChainDepth:       intent.ChainDepth,
		Attempt:          intent.Attempt,
	}
}

func activityIntentFromRequestEvent(evt events.Event) (runtimeengine.ActivityIntent, error) {
	var payload activityRequestPayload
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		return runtimeengine.ActivityIntent{}, fmt.Errorf("decode activity request %s: %w", evt.ID(), err)
	}
	intent := runtimeengine.ActivityIntent{
		ActivityID:       payload.ActivityID,
		Tool:             payload.Tool,
		Input:            cloneStringAnyMap(payload.Input),
		EffectClass:      runtimecontracts.NormalizeActivityEffectClass(payload.EffectClass),
		SuccessEvent:     payload.SuccessEvent,
		FailureEvent:     payload.FailureEvent,
		RetryMaxAttempts: payload.RetryMaxAttempts,
		RetryBackoff:     payload.RetryBackoff,
		ForkPolicy:       runtimecontracts.ActivityForkPolicy(strings.TrimSpace(payload.ForkPolicy)),
		EntityID:         identity.NormalizeEntityID(payload.EntityID),
		NodeID:           identity.NormalizeNodeID(payload.NodeID),
		FlowID:           identity.NormalizeFlowID(payload.FlowID),
		HandlerEventKey:  payload.HandlerEventKey,
		SourceEventID:    payload.SourceEventID,
		SourceRunID:      payload.SourceRunID,
		SourceTaskID:     payload.SourceTaskID,
		ParentEventID:    payload.ParentEventID,
		ChainDepth:       payload.ChainDepth,
		Attempt:          payload.Attempt,
	}.Normalized()
	if intent.ActivityID == "" || intent.Tool == "" || intent.SuccessEvent == "" || intent.FailureEvent == "" {
		return runtimeengine.ActivityIntent{}, fmt.Errorf("activity request %s is missing required activity identity", evt.ID())
	}
	return intent, nil
}

func executeActivityHTTPTool(ctx context.Context, client *http.Client, intent runtimeengine.ActivityIntent, tool runtimecontracts.ToolSchemaEntry) (any, error) {
	if tool.HTTP == nil {
		return nil, fmt.Errorf("activity tool %s is missing http block", intent.Tool)
	}
	if strings.TrimSpace(tool.RateLimit) != "" || strings.TrimSpace(tool.RateLimitMaxWait) != "" {
		return nil, fmt.Errorf("activity tool %s uses rate_limit; activity HTTP rate-limit admission is not admitted in Stage 1", intent.Tool)
	}
	if len(tool.ResponseMapping) > 0 {
		return nil, fmt.Errorf("activity tool %s uses response_mapping; activity HTTP response mapping is not admitted in Stage 1", intent.Tool)
	}
	if len(tool.Credentials) > 0 || tool.ManagedCredential != nil {
		return nil, fmt.Errorf("activity tool %s uses credentials; credentialed activity HTTP execution is not admitted in Stage 1", intent.Tool)
	}
	env := map[string]any{"input": cloneStringAnyMap(intent.Input)}
	url, err := resolveActivityHTTPURLTemplate(tool.HTTP.URL, env)
	if err != nil {
		return nil, err
	}
	url = strings.TrimSpace(url)
	if url == "" {
		return nil, fmt.Errorf("activity tool %s resolved an empty url", intent.Tool)
	}
	var body io.Reader
	if tool.HTTP.Body != nil {
		resolvedBody, err := resolveActivityTemplateTree(tool.HTTP.Body, env)
		if err != nil {
			return nil, err
		}
		raw, err := json.Marshal(resolvedBody)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(raw)
	}
	method := strings.ToUpper(strings.TrimSpace(tool.HTTP.Method))
	if method == "" {
		method = http.MethodGet
	}
	reqCtx := ctx
	cancel := func() {}
	if tool.HTTP.TimeoutSeconds > 0 {
		reqCtx, cancel = context.WithTimeout(ctx, time.Duration(tool.HTTP.TimeoutSeconds)*time.Second)
	}
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, method, url, body)
	if err != nil {
		return nil, err
	}
	for key, value := range tool.HTTP.Headers {
		resolved, err := resolveActivityTemplateString(value, env)
		if err != nil {
			return nil, err
		}
		req.Header.Set(strings.TrimSpace(key), strings.TrimSpace(resolved))
	}
	if body != nil && strings.TrimSpace(req.Header.Get("Content-Type")) == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	parsed := parseHTTPActivityResponse(raw)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("activity http tool %s returned status %d: %s", intent.Tool, resp.StatusCode, strings.TrimSpace(asString(parsed)))
	}
	return parsed, nil
}

func parseHTTPActivityResponse(raw []byte) any {
	if len(bytes.TrimSpace(raw)) == 0 {
		return map[string]any{}
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return string(raw)
	}
	return out
}

func resolveActivityTemplateTree(value any, env map[string]any) (any, error) {
	switch typed := value.(type) {
	case string:
		return resolveActivityTemplateString(typed, env)
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, value := range typed {
			resolved, err := resolveActivityTemplateTree(value, env)
			if err != nil {
				return nil, err
			}
			out[key] = resolved
		}
		return out, nil
	case []any:
		out := make([]any, len(typed))
		for idx, value := range typed {
			resolved, err := resolveActivityTemplateTree(value, env)
			if err != nil {
				return nil, err
			}
			out[idx] = resolved
		}
		return out, nil
	default:
		return value, nil
	}
}

func resolveActivityTemplateString(template string, env map[string]any) (string, error) {
	out := strings.TrimSpace(template)
	matches, err := activityTemplateMatches(out)
	if err != nil {
		return "", err
	}
	if len(matches) == 0 {
		return out, nil
	}
	var builder strings.Builder
	last := 0
	for _, match := range matches {
		builder.WriteString(out[last:match.start])
		value, ok := workflowExpressionLookupPath(env, match.expr)
		if !ok {
			return "", fmt.Errorf("activity template expression %q did not resolve", match.expr)
		}
		builder.WriteString(asString(value))
		last = match.end
	}
	builder.WriteString(out[last:])
	return builder.String(), nil
}

func resolveActivityHTTPURLTemplate(template string, env map[string]any) (string, error) {
	out := strings.TrimSpace(template)
	matches, err := activityTemplateMatches(out)
	if err != nil {
		return "", err
	}
	if len(matches) == 0 {
		return out, nil
	}
	if len(matches) == 1 && matches[0].start == 0 && matches[0].end == len(out) {
		value, ok := workflowExpressionLookupPath(env, matches[0].expr)
		if !ok {
			return "", fmt.Errorf("activity template expression %q did not resolve", matches[0].expr)
		}
		return asString(value), nil
	}
	var builder strings.Builder
	last := 0
	for _, match := range matches {
		builder.WriteString(out[last:match.start])
		value, ok := workflowExpressionLookupPath(env, match.expr)
		if !ok {
			return "", fmt.Errorf("activity template expression %q did not resolve", match.expr)
		}
		builder.WriteString(escapeActivityHTTPURLTemplateComponent(out, match.start, match.end, asString(value)))
		last = match.end
	}
	builder.WriteString(out[last:])
	return builder.String(), nil
}

type activityTemplateMatch struct {
	start int
	end   int
	expr  string
}

func activityTemplateMatches(template string) ([]activityTemplateMatch, error) {
	matches := make([]activityTemplateMatch, 0, 2)
	cursor := 0
	for {
		relativeStart := strings.Index(template[cursor:], "{{")
		if relativeStart < 0 {
			return matches, nil
		}
		start := cursor + relativeStart
		relativeEnd := strings.Index(template[start+2:], "}}")
		if relativeEnd < 0 {
			return nil, fmt.Errorf("unterminated activity template expression in %q", template)
		}
		end := start + 2 + relativeEnd
		expr := strings.TrimSpace(template[start+2 : end])
		matches = append(matches, activityTemplateMatch{start: start, end: end + 2, expr: expr})
		cursor = end + 2
	}
}

func escapeActivityHTTPURLTemplateComponent(raw string, start, end int, value string) string {
	if activityHTTPURLTemplateOffsetInQuery(raw, start) {
		return strings.ReplaceAll(url.QueryEscape(value), "+", "%20")
	}
	if activityHTTPURLTemplatePlaceholderInURLBaseOrAuthority(raw, start, end, value) {
		return value
	}
	return url.PathEscape(value)
}

func activityHTTPURLTemplateOffsetInQuery(raw string, offset int) bool {
	queryStart := strings.Index(raw, "?")
	if queryStart < 0 || queryStart > offset {
		return false
	}
	fragmentStart := strings.Index(raw, "#")
	return fragmentStart < 0 || offset < fragmentStart
}

func activityHTTPURLTemplatePlaceholderInURLBaseOrAuthority(raw string, start, end int, value string) bool {
	prefix := raw[:start]
	suffix := raw[end:]
	if strings.HasPrefix(suffix, "://") {
		return true
	}
	if strings.HasSuffix(prefix, "://") {
		return true
	}
	schemeIndex := strings.LastIndex(prefix, "://")
	if schemeIndex >= 0 {
		authorityPrefix := prefix[schemeIndex+len("://"):]
		return !strings.ContainsAny(authorityPrefix, "/?#")
	}
	if start == 0 {
		return activityHTTPURLTemplateValueHasSchemeAuthority(value)
	}
	return false
}

func activityHTTPURLTemplateValueHasSchemeAuthority(value string) bool {
	parsed, err := url.Parse(strings.TrimSpace(value))
	return err == nil && parsed.Scheme != "" && parsed.Host != ""
}

func (d pipelineActivityDispatcher) publishActivitySuccess(ctx context.Context, intent runtimeengine.ActivityIntent, result any) error {
	payload := map[string]any{
		"activity_id":  intent.ActivityID,
		"tool":         intent.Tool,
		"effect_class": string(intent.EffectClass),
		"attempt":      intent.Attempt,
		"result":       result,
	}
	return d.publishActivityResult(ctx, intent, intent.SuccessEvent, payload)
}

func (d pipelineActivityDispatcher) publishActivityFailure(ctx context.Context, intent runtimeengine.ActivityIntent, cause error) error {
	payload := map[string]any{
		"activity_id":  intent.ActivityID,
		"tool":         intent.Tool,
		"effect_class": string(intent.EffectClass),
		"attempt":      intent.Attempt,
		"error":        strings.TrimSpace(cause.Error()),
	}
	return d.publishActivityResult(ctx, intent, intent.FailureEvent, payload)
}

func (d pipelineActivityDispatcher) publishActivityResult(ctx context.Context, intent runtimeengine.ActivityIntent, eventType string, payload map[string]any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	evt := events.NewChildEventWithLineage(
		activityResultEventID(intent, eventType),
		events.EventType(eventType),
		intent.NodeID.String(),
		intent.SourceTaskID,
		raw,
		intent.ChainDepth+1,
		events.EventLineage{
			RunID:         intent.SourceRunID,
			ParentEventID: firstNonEmptyString(intent.SourceEventID, intent.ParentEventID),
			TaskID:        intent.SourceTaskID,
		},
		events.EventEnvelope{
			EntityID: intent.EntityID.String(),
			Source: events.RouteIdentity{
				FlowID:   intent.FlowID.String(),
				EntityID: intent.EntityID.String(),
			},
		},
		time.Now().UTC(),
	)
	if collector, ok := ctx.Value(pipelineEmitCollectorKey{}).(*[]events.Event); ok && collector != nil {
		*collector = append(*collector, evt)
		return nil
	}
	return d.coordinator.bus.Publish(ctx, evt)
}
