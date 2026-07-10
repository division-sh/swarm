package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/httpresponsesuccess"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
	intent = intent.Normalized()
	source := d.coordinator.SemanticSource()
	if source == nil {
		return fmt.Errorf("activity dispatcher requires semantic source")
	}
	client := d.client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
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
	if toolEffectClass == runtimecontracts.ActivityEffectClassNonIdempotentWrite {
		return d.executeNonIdempotentActivityIntent(ctx, intent, tool)
	}
	if recorded, ok, err := d.recordedActivityResult(ctx, intent); err != nil {
		return err
	} else if ok {
		d.logActivityRuntime(ctx, intent, "result_reused", map[string]any{
			"activity_id":       intent.ActivityID,
			"tool":              intent.Tool,
			"effect_class":      string(intent.EffectClass),
			"result_event_id":   recorded.EventID,
			"result_event_type": recorded.EventType,
		})
		return nil
	}
	maxAttempts := activityRetryMaxAttempts(intent, toolEffectClass)
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		attemptIntent := intent
		attemptIntent.Attempt = attempt
		d.logActivityRuntime(ctx, attemptIntent, "attempt_started", map[string]any{
			"activity_id":        attemptIntent.ActivityID,
			"tool":               attemptIntent.Tool,
			"effect_class":       string(attemptIntent.EffectClass),
			"attempt":            attempt,
			"retry_max_attempts": maxAttempts,
		})
		result, err := d.executeActivityHTTPTool(ctx, client, attemptIntent, tool)
		if err == nil {
			return d.publishActivitySuccess(ctx, attemptIntent, result)
		}
		lastErr = err
		d.logActivityRuntime(ctx, attemptIntent, "attempt_failed", map[string]any{
			"activity_id":        attemptIntent.ActivityID,
			"tool":               attemptIntent.Tool,
			"effect_class":       string(attemptIntent.EffectClass),
			"attempt":            attempt,
			"retry_max_attempts": maxAttempts,
			"error":              strings.TrimSpace(err.Error()),
		})
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

func (d pipelineActivityDispatcher) executeNonIdempotentActivityIntent(ctx context.Context, intent runtimeengine.ActivityIntent, tool runtimecontracts.ToolSchemaEntry) error {
	if d.coordinator == nil || d.coordinator.workflowStore == nil || !d.coordinator.workflowStore.Enabled() {
		return d.publishActivityFailure(ctx, intent, fmt.Errorf("activity tool %s effect_class non_idempotent_write requires the activity_attempts journal", intent.Tool))
	}
	client := d.client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	intent.Attempt = 1
	requestEventID := activityRequestEventID(intent)
	if existing, ok, err := d.coordinator.workflowStore.LoadActivityAttempt(ctx, requestEventID); err != nil {
		return d.publishActivityFailure(ctx, intent, err)
	} else if ok {
		return d.publishExistingActivityAttempt(ctx, intent, existing)
	}
	prepared, err := d.prepareActivityHTTPTool(ctx, client, intent, tool)
	if err != nil {
		return d.publishActivityFailure(ctx, intent, err)
	}
	startRecord := activityAttemptStartRecord(intent, prepared.inputHash)
	started, inserted, err := d.coordinator.workflowStore.StartActivityAttempt(ctx, startRecord)
	if err != nil {
		return d.publishActivityFailure(ctx, intent, err)
	}
	if !inserted {
		return d.publishExistingActivityAttempt(ctx, intent, started)
	}
	result, err := executePreparedActivityHTTPTool(ctx, prepared)
	var terminal ActivityAttemptRecord
	if err != nil {
		redacted := runtimemanagedcredentials.RedactString(err.Error(), prepared.secrets...)
		cause := fmt.Errorf("%s", redacted)
		status := ActivityAttemptStatusFailed
		if activityHTTPOutcomeUncertain(err) {
			status = ActivityAttemptStatusUncertain
			cause = fmt.Errorf("activity non_idempotent_write provider outcome is uncertain after dispatch: %s", redacted)
		}
		payload := activityFailurePayload(intent, cause)
		terminal = started.withTerminal(status, activityResultEventID(intent, intent.FailureEvent), intent.FailureEvent, payload, cause.Error())
	} else {
		payload := activitySuccessPayload(intent, result)
		terminal = started.withTerminal(ActivityAttemptStatusSucceeded, activityResultEventID(intent, intent.SuccessEvent), intent.SuccessEvent, payload, "")
	}
	var stored ActivityAttemptRecord
	if terminal.Status == ActivityAttemptStatusUncertain {
		stored, err = d.coordinator.workflowStore.MarkActivityAttemptUncertain(ctx, terminal)
	} else {
		stored, err = d.coordinator.workflowStore.CompleteActivityAttempt(ctx, terminal)
	}
	if err != nil {
		return err
	}
	return d.publishJournaledActivityResult(ctx, intent, stored)
}

type activityRecordedResult struct {
	EventID   string
	EventType string
	Payload   json.RawMessage
}

func (d pipelineActivityDispatcher) recordedActivityResult(ctx context.Context, intent runtimeengine.ActivityIntent) (activityRecordedResult, bool, error) {
	if d.coordinator == nil || d.coordinator.db == nil {
		return activityRecordedResult{}, false, nil
	}
	db := d.coordinator.db
	successID := activityResultEventID(intent, intent.SuccessEvent)
	failureID := activityResultEventID(intent, intent.FailureEvent)
	var (
		rows *sql.Rows
		err  error
	)
	if d.coordinator.workflowStore != nil && d.coordinator.workflowStore.isSQLite() {
		rows, err = db.QueryContext(ctx, `
			SELECT event_id, event_name, payload
			FROM events
			WHERE event_id IN (?, ?)
		`, successID, failureID)
	} else {
		rows, err = db.QueryContext(ctx, `
			SELECT event_id::text, event_name, payload::text
			FROM events
			WHERE event_id IN ($1::uuid, $2::uuid)
		`, successID, failureID)
	}
	if err != nil {
		return activityRecordedResult{}, false, fmt.Errorf("lookup recorded activity result %s: %w", intent.ActivityID, err)
	}
	defer rows.Close()
	var found []activityRecordedResult
	for rows.Next() {
		var result activityRecordedResult
		var payload string
		if err := rows.Scan(&result.EventID, &result.EventType, &payload); err != nil {
			return activityRecordedResult{}, false, fmt.Errorf("scan recorded activity result %s: %w", intent.ActivityID, err)
		}
		result.Payload = json.RawMessage(payload)
		found = append(found, result)
	}
	if err := rows.Err(); err != nil {
		return activityRecordedResult{}, false, fmt.Errorf("iterate recorded activity result %s: %w", intent.ActivityID, err)
	}
	switch len(found) {
	case 0:
		return activityRecordedResult{}, false, nil
	case 1:
		return found[0], true, nil
	default:
		return activityRecordedResult{}, false, fmt.Errorf("activity request %s has both success and failure results recorded", activityRequestEventID(intent))
	}
}

func (d pipelineActivityDispatcher) logActivityRuntime(ctx context.Context, intent runtimeengine.ActivityIntent, action string, detail map[string]any) {
	if d.coordinator == nil || d.coordinator.bus == nil {
		return
	}
	intent = intent.Normalized()
	if detail == nil {
		detail = map[string]any{}
	}
	detail["request_event_id"] = activityRequestEventID(intent)
	_ = d.coordinator.bus.LogRuntime(ctx, RuntimeLogEntry{
		Level:     "info",
		Component: "activity",
		Action:    action,
		EventID:   activityRequestEventID(intent),
		EventType: intent.SuccessEvent,
		EntityID:  intent.EntityID.String(),
		Detail:    detail,
	})
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
	return runtimeengine.EmitIntent{Event: evt, Context: intent.Context}, nil
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
		Context:          evt.DeliveryContext(),
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

type preparedActivityHTTPTool struct {
	toolName    string
	method      string
	url         string
	headers     http.Header
	body        []byte
	timeout     time.Duration
	client      *http.Client
	secrets     []string
	managedAuth *activityManagedHTTPAuth
	success     *runtimecontracts.HTTPResponseSuccess
	inputHash   string
}

func (d pipelineActivityDispatcher) executeActivityHTTPTool(ctx context.Context, client *http.Client, intent runtimeengine.ActivityIntent, tool runtimecontracts.ToolSchemaEntry) (any, error) {
	prepared, err := d.prepareActivityHTTPTool(ctx, client, intent, tool)
	if err != nil {
		return nil, err
	}
	return executePreparedActivityHTTPTool(ctx, prepared)
}

func (d pipelineActivityDispatcher) prepareActivityHTTPTool(ctx context.Context, client *http.Client, intent runtimeengine.ActivityIntent, tool runtimecontracts.ToolSchemaEntry) (preparedActivityHTTPTool, error) {
	if tool.HTTP == nil {
		return preparedActivityHTTPTool{}, fmt.Errorf("activity tool %s is missing http block", intent.Tool)
	}
	if strings.TrimSpace(tool.RateLimit) != "" || strings.TrimSpace(tool.RateLimitMaxWait) != "" {
		return preparedActivityHTTPTool{}, fmt.Errorf("activity tool %s uses rate_limit; activity HTTP rate-limit admission is not admitted in Stage 1", intent.Tool)
	}
	if len(tool.ResponseMapping) > 0 {
		return preparedActivityHTTPTool{}, fmt.Errorf("activity tool %s uses response_mapping; activity HTTP response mapping is not admitted in Stage 1", intent.Tool)
	}
	credentials := map[string]any{}
	secrets := []string{}
	if len(tool.Credentials) > 0 && tool.ManagedCredential != nil {
		return preparedActivityHTTPTool{}, fmt.Errorf("activity tool %s must not declare both static credentials and managed_credential", intent.Tool)
	}
	if len(tool.Credentials) > 0 {
		if intent.EffectClass != runtimecontracts.ActivityEffectClassNonIdempotentWrite {
			return preparedActivityHTTPTool{}, fmt.Errorf("activity tool %s uses static credentials; static credential activity HTTP execution is supported only for non_idempotent_write activities", intent.Tool)
		}
		resolved, secretValues, err := d.resolveActivityToolCredentials(ctx, intent, tool.Credentials)
		if err != nil {
			return preparedActivityHTTPTool{}, err
		}
		credentials = resolved
		secrets = secretValues
	}
	if tool.ManagedCredential != nil {
		if intent.EffectClass != runtimecontracts.ActivityEffectClassNonIdempotentWrite || !strings.EqualFold(strings.TrimSpace(tool.Category), "provider_connector") {
			return preparedActivityHTTPTool{}, fmt.Errorf("activity tool %s uses managed_credential; managed credential activity HTTP execution is supported only for non_idempotent_write provider connector activities", intent.Tool)
		}
	}
	input := cloneStringAnyMap(intent.Input)
	env := map[string]any{"input": input, "credentials": credentials}
	url, err := resolveActivityHTTPURLTemplate(tool.HTTP.URL, env)
	if err != nil {
		return preparedActivityHTTPTool{}, redactActivityError(err, secrets)
	}
	url = strings.TrimSpace(url)
	if url == "" {
		return preparedActivityHTTPTool{}, fmt.Errorf("activity tool %s resolved an empty url", intent.Tool)
	}
	var body []byte
	if tool.HTTP.Body != nil {
		resolvedBody, err := resolveActivityTemplateTree(tool.HTTP.Body, env)
		if err != nil {
			return preparedActivityHTTPTool{}, redactActivityError(err, secrets)
		}
		raw, err := json.Marshal(resolvedBody)
		if err != nil {
			return preparedActivityHTTPTool{}, err
		}
		body = raw
	}
	method := strings.ToUpper(strings.TrimSpace(tool.HTTP.Method))
	if method == "" {
		method = http.MethodGet
	}
	timeout := 30 * time.Second
	if tool.HTTP.TimeoutSeconds > 0 {
		timeout = time.Duration(tool.HTTP.TimeoutSeconds) * time.Second
	}
	headers := make(http.Header, len(tool.HTTP.Headers))
	for key, value := range tool.HTTP.Headers {
		resolved, err := resolveActivityTemplateString(value, env)
		if err != nil {
			return preparedActivityHTTPTool{}, redactActivityError(err, secrets)
		}
		headers.Set(strings.TrimSpace(key), strings.TrimSpace(resolved))
	}
	if len(body) > 0 && strings.TrimSpace(headers.Get("Content-Type")) == "" {
		headers.Set("Content-Type", "application/json")
	}
	if client == nil {
		client = &http.Client{Timeout: timeout}
	}
	managedAuth, err := d.resolveActivityManagedCredential(ctx, client, intent, tool)
	if err != nil {
		return preparedActivityHTTPTool{}, redactActivityError(err, secrets)
	}
	if managedAuth != nil {
		if err := runtimemanagedcredentials.ApplyHTTPAuthorization(headers, managedAuth.HTTPAuthorization(), false); err != nil {
			return preparedActivityHTTPTool{}, redactActivityError(err, append(secrets, managedAuth.SecretValues()...))
		}
		secrets = append(secrets, managedAuth.SecretValues()...)
	}
	return preparedActivityHTTPTool{
		toolName:    intent.Tool,
		method:      method,
		url:         url,
		headers:     headers,
		body:        body,
		timeout:     timeout,
		client:      client,
		secrets:     secrets,
		managedAuth: managedAuth,
		success:     cloneActivityResponseSuccess(tool.ResponseSuccess),
		inputHash:   activityInputHash(input),
	}, nil
}

func executePreparedActivityHTTPTool(ctx context.Context, prepared preparedActivityHTTPTool) (any, error) {
	reqCtx, cancel := context.WithTimeout(ctx, prepared.timeout)
	defer cancel()
	refreshedAfterUnauthorized := false
	for {
		var body io.Reader
		if len(prepared.body) > 0 {
			body = bytes.NewReader(prepared.body)
		}
		req, err := http.NewRequestWithContext(reqCtx, prepared.method, prepared.url, body)
		if err != nil {
			return nil, redactActivityError(err, prepared.secrets)
		}
		for key, values := range prepared.headers {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
		resp, err := prepared.client.Do(req)
		if err != nil {
			return nil, activityHTTPUncertainError{err: redactActivityError(err, prepared.secrets)}
		}
		raw, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, activityHTTPUncertainError{err: redactActivityError(readErr, prepared.secrets)}
		}
		parsed := parseHTTPActivityResponse(raw)
		parsed = runtimemanagedcredentials.RedactValue(parsed, prepared.secrets...)
		if prepared.managedAuth != nil && resp.StatusCode == http.StatusUnauthorized && !refreshedAfterUnauthorized {
			refreshedAfterUnauthorized = true
			token, record, refreshErr := prepared.managedAuth.TokenSource.Refresh(ctx, prepared.managedAuth.StoreKey)
			if refreshErr != nil {
				return nil, fmt.Errorf("%s", runtimemanagedcredentials.RedactString(refreshErr.Error(), append(prepared.secrets, record.SecretValues()...)...))
			}
			prepared.managedAuth.Token = token
			prepared.managedAuth.Record = record
			prepared.secrets = append(prepared.secrets, prepared.managedAuth.SecretValues()...)
			if err := runtimemanagedcredentials.ApplyHTTPAuthorization(prepared.headers, prepared.managedAuth.HTTPAuthorization(), true); err != nil {
				return nil, redactActivityError(err, prepared.secrets)
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("activity http tool %s returned status %d: %s", prepared.toolName, resp.StatusCode, strings.TrimSpace(asString(parsed)))
		}
		responseEnv := map[string]any{
			"response": map[string]any{
				"status":  resp.StatusCode,
				"headers": flattenActivityHTTPHeaders(resp.Header),
				"body":    parsed,
			},
		}
		if err := httpresponsesuccess.Evaluate("activity http tool "+strings.TrimSpace(prepared.toolName), prepared.success, responseEnv, prepared.secrets); err != nil {
			return nil, err
		}
		return parsed, nil
	}
}

func cloneActivityResponseSuccess(check *runtimecontracts.HTTPResponseSuccess) *runtimecontracts.HTTPResponseSuccess {
	if check == nil {
		return nil
	}
	out := *check
	return &out
}

func flattenActivityHTTPHeaders(headers http.Header) map[string]any {
	out := make(map[string]any, len(headers))
	for key, values := range headers {
		key = strings.ToLower(strings.TrimSpace(key))
		if key == "" || len(values) == 0 {
			continue
		}
		if len(values) == 1 {
			out[key] = values[0]
			continue
		}
		items := make([]any, 0, len(values))
		for _, value := range values {
			items = append(items, value)
		}
		out[key] = items
	}
	return out
}

type activityManagedHTTPAuth struct {
	StoreKey    string
	Token       string
	Record      runtimemanagedcredentials.Record
	Header      string
	Prefix      string
	TokenSource *runtimemanagedcredentials.TokenSource
}

func (a *activityManagedHTTPAuth) SecretValues() []string {
	if a == nil {
		return nil
	}
	secrets := a.Record.SecretValues()
	token := strings.TrimSpace(a.Token)
	if token != "" {
		secrets = append(secrets, token)
	}
	return secrets
}

func (a *activityManagedHTTPAuth) HTTPAuthorization() runtimemanagedcredentials.HTTPAuthorization {
	if a == nil {
		return runtimemanagedcredentials.HTTPAuthorization{}
	}
	return runtimemanagedcredentials.HTTPAuthorization{
		CredentialKey: a.StoreKey,
		AccessToken:   a.Token,
		Header:        a.Header,
		Prefix:        a.Prefix,
	}
}

func (d pipelineActivityDispatcher) resolveActivityManagedCredential(ctx context.Context, client *http.Client, intent runtimeengine.ActivityIntent, tool runtimecontracts.ToolSchemaEntry) (*activityManagedHTTPAuth, error) {
	if tool.ManagedCredential == nil {
		return nil, nil
	}
	ref := *tool.ManagedCredential
	key := strings.TrimSpace(ref.Key)
	if key == "" {
		return nil, fmt.Errorf("activity tool %s managed_credential.key is required", intent.Tool)
	}
	source := semanticview.Source(nil)
	var store runtimemanagedcredentials.Store
	if d.coordinator != nil {
		source = d.coordinator.SemanticSource()
		store = d.coordinator.managedCredentials
	}
	flowID := intent.FlowID.String()
	storeKey, mapped := semanticview.CredentialStoreKeyForFlow(source, flowID, key)
	if mapped && strings.TrimSpace(storeKey) == "" {
		return nil, fmt.Errorf("managed credential %q is not declared and bound for imported package flow %s", key, flowID)
	}
	storeKey = strings.TrimSpace(storeKey)
	if storeKey == "" {
		return nil, fmt.Errorf("managed credential %q does not resolve to a deployment credential key", key)
	}
	tokenSource := &runtimemanagedcredentials.TokenSource{
		Store:      store,
		HTTPClient: client,
	}
	token, record, err := tokenSource.AccessToken(ctx, runtimemanagedcredentials.AccessTokenRequest{
		Key:            storeKey,
		GrantType:      ref.GrantType,
		Scopes:         ref.Scopes,
		GrantModel:     ref.GrantModel,
		TokenRequest:   ref.TokenRequest,
		InstallationID: activityManagedCredentialInputValue(intent.Input, ref.InstallationIDInput),
	})
	if err != nil {
		return nil, fmt.Errorf("%s", runtimemanagedcredentials.RedactString(err.Error(), record.SecretValues()...))
	}
	return &activityManagedHTTPAuth{
		StoreKey:    storeKey,
		Token:       token,
		Record:      record,
		Header:      ref.Header,
		Prefix:      ref.Prefix,
		TokenSource: tokenSource,
	}, nil
}

func activityManagedCredentialInputValue(input map[string]any, key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return ""
	}
	value, ok := input[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case json.Number:
		return strings.TrimSpace(typed.String())
	case float64:
		return strings.TrimSpace(strconv.FormatFloat(typed, 'f', -1, 64))
	case float32:
		return strings.TrimSpace(strconv.FormatFloat(float64(typed), 'f', -1, 32))
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case uint64:
		return strconv.FormatUint(typed, 10)
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}

type activityHTTPUncertainError struct {
	err error
}

func (e activityHTTPUncertainError) Error() string {
	if e.err == nil {
		return "activity http outcome uncertain"
	}
	return e.err.Error()
}

func (e activityHTTPUncertainError) Unwrap() error {
	return e.err
}

func activityHTTPOutcomeUncertain(err error) bool {
	var target activityHTTPUncertainError
	return errors.As(err, &target)
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

func (d pipelineActivityDispatcher) resolveActivityToolCredentials(ctx context.Context, intent runtimeengine.ActivityIntent, keys []string) (map[string]any, []string, error) {
	out := make(map[string]any, len(keys))
	secrets := make([]string, 0, len(keys))
	store := runtimecredentials.Store(nil)
	if d.coordinator != nil {
		store = d.coordinator.credentials
	}
	if store == nil {
		return nil, nil, fmt.Errorf("credential store is not configured")
	}
	source := semanticview.Source(nil)
	if d.coordinator != nil {
		source = d.coordinator.SemanticSource()
	}
	flowID := intent.FlowID.String()
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		storeKey, mapped := semanticview.CredentialStoreKeyForFlow(source, flowID, key)
		if mapped && strings.TrimSpace(storeKey) == "" {
			return nil, nil, fmt.Errorf("credential %q is not declared and bound for imported package flow %s", key, flowID)
		}
		storeKey = strings.TrimSpace(storeKey)
		if storeKey == "" {
			return nil, nil, fmt.Errorf("credential %q does not resolve to a deployment credential key", key)
		}
		value, ok, err := store.Get(ctx, storeKey)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			return nil, nil, fmt.Errorf("missing credential %q", storeKey)
		}
		out[key] = value
		secrets = append(secrets, value)
	}
	return out, secrets, nil
}

func redactActivityError(err error, secrets []string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s", runtimemanagedcredentials.RedactString(err.Error(), secrets...))
}

func resolveActivityTemplateTree(value any, env map[string]any) (any, error) {
	switch typed := value.(type) {
	case string:
		return resolveActivityTemplateValue(typed, env)
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

func resolveActivityTemplateValue(template string, env map[string]any) (any, error) {
	out := strings.TrimSpace(template)
	matches, err := activityTemplateMatches(out)
	if err != nil {
		return nil, err
	}
	if len(matches) == 1 && matches[0].start == 0 && matches[0].end == len(out) {
		value, ok := workflowExpressionLookupPath(env, matches[0].expr)
		if !ok {
			return nil, fmt.Errorf("activity template expression %q did not resolve", matches[0].expr)
		}
		return value, nil
	}
	return resolveActivityTemplateString(out, env)
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
	return d.publishActivityResult(ctx, intent, intent.SuccessEvent, activitySuccessPayload(intent, result))
}

func activitySuccessPayload(intent runtimeengine.ActivityIntent, result any) map[string]any {
	return map[string]any{
		"activity_id":  intent.ActivityID,
		"tool":         intent.Tool,
		"effect_class": string(intent.EffectClass),
		"attempt":      intent.Attempt,
		"result":       result,
	}
}

func (d pipelineActivityDispatcher) publishActivityFailure(ctx context.Context, intent runtimeengine.ActivityIntent, cause error) error {
	return d.publishActivityResult(ctx, intent, intent.FailureEvent, activityFailurePayload(intent, cause))
}

func activityFailurePayload(intent runtimeengine.ActivityIntent, cause error) map[string]any {
	errText := ""
	if cause != nil {
		errText = strings.TrimSpace(cause.Error())
	}
	return map[string]any{
		"activity_id":  intent.ActivityID,
		"tool":         intent.Tool,
		"effect_class": string(intent.EffectClass),
		"attempt":      intent.Attempt,
		"error":        errText,
	}
}

func (d pipelineActivityDispatcher) publishActivityResult(ctx context.Context, intent runtimeengine.ActivityIntent, eventType string, payload map[string]any) error {
	return d.publishActivityResultWithID(ctx, intent, activityResultEventID(intent, eventType), eventType, payload)
}

func (d pipelineActivityDispatcher) publishActivityResultWithID(ctx context.Context, intent runtimeengine.ActivityIntent, eventID, eventType string, payload map[string]any) error {
	ctx = events.WithDeliveryContext(ctx, intent.Context)
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	evt := events.NewChildEventWithLineage(
		eventID,
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
		d.logActivityRuntime(ctx, intent, "result_published", map[string]any{
			"activity_id":       intent.ActivityID,
			"tool":              intent.Tool,
			"effect_class":      string(intent.EffectClass),
			"attempt":           intent.Attempt,
			"result_event_id":   evt.ID(),
			"result_event_type": string(evt.Type()),
		})
		return nil
	}
	if err := d.coordinator.bus.Publish(ctx, evt); err != nil {
		return err
	}
	d.logActivityRuntime(ctx, intent, "result_published", map[string]any{
		"activity_id":       intent.ActivityID,
		"tool":              intent.Tool,
		"effect_class":      string(intent.EffectClass),
		"attempt":           intent.Attempt,
		"result_event_id":   evt.ID(),
		"result_event_type": string(evt.Type()),
	})
	return nil
}

func (d pipelineActivityDispatcher) publishExistingActivityAttempt(ctx context.Context, intent runtimeengine.ActivityIntent, rec ActivityAttemptRecord) error {
	rec = rec.normalized()
	if rec.Status == ActivityAttemptStatusStarted {
		return nil
	}
	return d.publishJournaledActivityResult(ctx, intent, rec)
}

func (d pipelineActivityDispatcher) publishJournaledActivityResult(ctx context.Context, intent runtimeengine.ActivityIntent, rec ActivityAttemptRecord) error {
	rec = rec.normalized()
	if rec.ResultEventID == "" || rec.ResultEventType == "" || rec.ResultPayload == nil {
		return fmt.Errorf("activity attempt %s has no terminal journal result", rec.RequestEventID)
	}
	intent.Attempt = rec.Attempt
	if id := strings.TrimSpace(rec.ReplyContextID); id != "" {
		intent.Context = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: id}}
	}
	return d.publishActivityResultWithID(ctx, intent, rec.ResultEventID, rec.ResultEventType, rec.ResultPayload)
}

func activityAttemptStartRecord(intent runtimeengine.ActivityIntent, inputHash string) ActivityAttemptRecord {
	intent = intent.Normalized()
	return ActivityAttemptRecord{
		RequestEventID:  activityRequestEventID(intent),
		RunID:           intent.SourceRunID,
		SourceEventID:   intent.SourceEventID,
		ParentEventID:   intent.ParentEventID,
		EntityID:        intent.EntityID.String(),
		FlowInstance:    intent.FlowID.String(),
		NodeID:          intent.NodeID.String(),
		HandlerEventKey: intent.HandlerEventKey,
		ActivityID:      intent.ActivityID,
		Tool:            intent.Tool,
		EffectClass:     string(intent.EffectClass),
		Attempt:         1,
		Status:          ActivityAttemptStatusStarted,
		SuccessEvent:    intent.SuccessEvent,
		FailureEvent:    intent.FailureEvent,
		InputHash:       inputHash,
		ReplyContextID:  intent.Context.ReplyContextID(),
	}
}

func (rec ActivityAttemptRecord) withTerminal(status, eventID, eventType string, payload map[string]any, errText string) ActivityAttemptRecord {
	rec = rec.normalized()
	rec.Status = status
	rec.ResultEventID = strings.TrimSpace(eventID)
	rec.ResultEventType = strings.TrimSpace(eventType)
	rec.ResultPayload = cloneStringAnyMap(payload)
	rec.Error = strings.TrimSpace(errText)
	return rec
}

func activityInputHash(input map[string]any) string {
	raw, err := json.Marshal(input)
	if err != nil {
		raw = []byte(fmt.Sprintf("%#v", input))
	}
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("sha256:%x", sum[:])
}
