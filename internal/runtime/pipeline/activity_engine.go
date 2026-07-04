package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/google/uuid"
)

type pipelineActivityIntentWriter struct {
	coordinator *PipelineCoordinator
}

func (w pipelineActivityIntentWriter) WriteActivityIntents(ctx context.Context, intents []runtimeengine.ActivityIntent) error {
	if len(intents) == 0 || w.coordinator == nil || w.coordinator.bus == nil {
		return nil
	}
	for _, intent := range intents {
		intent = intent.Normalized()
		if err := w.coordinator.bus.LogRuntime(ctx, RuntimeLogEntry{
			Level:     "info",
			Component: "activity",
			Action:    "intent_persisted",
			EventID:   intent.SourceEventID,
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
	source := d.coordinator.SemanticSource()
	if source == nil {
		return fmt.Errorf("activity dispatcher requires semantic source")
	}
	client := d.client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	for _, intent := range intents {
		intent = intent.Normalized()
		tool, ok := source.ToolEntries()[intent.Tool]
		if !ok {
			if err := d.publishActivityFailure(ctx, intent, fmt.Errorf("activity tool %q is not declared", intent.Tool)); err != nil {
				return err
			}
			continue
		}
		result, err := executeActivityHTTPTool(ctx, client, intent, tool)
		if err != nil {
			if publishErr := d.publishActivityFailure(ctx, intent, err); publishErr != nil {
				return publishErr
			}
			continue
		}
		if err := d.publishActivitySuccess(ctx, intent, result); err != nil {
			return err
		}
	}
	return nil
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
	url, err := resolveActivityTemplateString(tool.HTTP.URL, env)
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
	for {
		start := strings.Index(out, "{{")
		if start < 0 {
			return out, nil
		}
		end := strings.Index(out[start+2:], "}}")
		if end < 0 {
			return "", fmt.Errorf("unterminated activity template expression in %q", template)
		}
		end += start + 2
		expr := strings.TrimSpace(out[start+2 : end])
		value, ok := workflowExpressionLookupPath(env, expr)
		if !ok {
			return "", fmt.Errorf("activity template expression %q did not resolve", expr)
		}
		out = out[:start] + asString(value) + out[end+2:]
	}
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
		uuid.NewString(),
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
	return d.coordinator.bus.Publish(ctx, evt)
}
