package tools

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

const emitToolOutcomeAction = "emit_tool_outcome"

var emitDiagnosticIdentifier = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,96}$`)
var emitDiagnosticSQLState = regexp.MustCompile(`^[0-9A-Z]{5}$`)

// Preserve native branch identity without serializing private error text or attributes.
func emitPublicationDiagnostic(err error, remaining *int) map[string]any {
	if err == nil {
		return nil
	}
	if *remaining == 0 {
		return map[string]any{"truncated": true}
	}
	*remaining -= 1
	identifier := func(value string) string {
		if emitDiagnosticIdentifier.MatchString(value) {
			return value
		}
		return "[opaque]"
	}
	node := map[string]any{"type": TruncateTelemetry(fmt.Sprintf("%T", err), 96)}
	switch typed := err.(type) {
	case *failures.Error:
		if typed == nil {
			node["opaque"] = true
			break
		}
		node["class"], node["code"] = identifier(string(typed.Failure.Class)), identifier(typed.Failure.Detail.Code)
		node["component"], node["operation"] = identifier(typed.Failure.Component), identifier(typed.Failure.Operation)
	case interface{ SQLState() string }:
		state := typed.SQLState()
		if emitDiagnosticSQLState.MatchString(state) {
			node["sqlstate"] = state
		} else {
			node["opaque"] = true
		}
	default:
		if err == context.Canceled || err == context.DeadlineExceeded {
			node["sentinel"] = err.Error()
		} else {
			node["opaque"], node["message_sha256"] = true, fmt.Sprintf("%x", sha256.Sum256([]byte(err.Error())))
		}
	}
	var branches []error
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		branches = wrapped.Unwrap()
	case interface{ Unwrap() error }:
		branches = []error{wrapped.Unwrap()}
	}
	var children []any
	for _, branch := range branches {
		if branch == nil {
			continue
		}
		if *remaining == 0 {
			children = append(children, map[string]any{"truncated": true})
			break
		}
		children = append(children, emitPublicationDiagnostic(branch, remaining))
	}
	if len(children) != 0 {
		node["causes"] = children
	}
	return node
}

func emitReceiverRefusals(err error) []failures.Envelope {
	var refusals []failures.Envelope
	seen := make(map[string]bool)
	if !failures.OnlyBranches(err, func(branch error) bool {
		typed, ok := branch.(*failures.Error)
		if !ok || typed.Failure.Class != failures.ClassUnexpectedArrival || typed.Failure.Detail.Code != "no_route" {
			return false
		}
		refusal := failures.FromError(branch, "tool-executor", "handle_emit_tool.publish").Failure
		instance, _ := refusal.Detail.Attributes["flow_instance"].(string)
		if !seen[instance] {
			seen[instance] = true
			refusals = append(refusals, refusal)
		}
		return true
	}) {
		return nil
	}
	return refusals
}

func (e *Executor) emitRefusalOutput(ctx context.Context, actor models.AgentConfig, toolName, requestedEventType, resolvedEventType string,
	preValidationPayload, postEnrichmentPayload map[string]any, emitted events.Event, output map[string]any, refusals []failures.Envelope, failureStage string) map[string]any {
	if output == nil {
		output = map[string]any{"event_type": string(emitted.Type()), "event_id": emitted.ID()}
	}
	output["status"], output["refusals"] = "refused", refusals
	for _, refusal := range refusals {
		e.logEmitToolOutcome(ctx, actor, toolName, requestedEventType, resolvedEventType, preValidationPayload, postEnrichmentPayload, events.SomeEvent(emitted), "refused", "no_route", failureStage, &failures.Error{Failure: refusal})
	}
	return output
}

func (e *Executor) logEmitToolOutcome(
	ctx context.Context,
	actor models.AgentConfig,
	toolName string,
	requestedEventType string,
	resolvedEventType string,
	preValidationPayload map[string]any,
	postEnrichmentPayload map[string]any,
	emitted events.OptionalEvent,
	outcome string,
	failureClass string,
	failureStage string,
	execErr error,
) {
	logger := e.runtimeLogSink()
	if logger == nil {
		return
	}
	toolName = normalizeNativeToolName(toolName)
	requestedEventType = strings.TrimSpace(requestedEventType)
	resolvedEventType = strings.TrimSpace(resolvedEventType)
	level := "info"
	if execErr != nil {
		level = "warn"
	}
	detail := map[string]any{
		"tool_family":          "emit",
		"tool_name":            toolName,
		"requested_event_type": requestedEventType,
		"resolved_event_type":  resolvedEventType,
		"outcome":              strings.TrimSpace(outcome),
		"ok":                   execErr == nil,
	}
	if v := strings.TrimSpace(failureClass); v != "" {
		detail["failure_class"] = v
	}
	if v := strings.TrimSpace(failureStage); v != "" {
		detail["failure_stage"] = v
	}
	if pre := safeTelemetryPayloadSnapshot(preValidationPayload); pre != nil {
		detail["pre_validation_payload"] = pre
	}
	if post := safeTelemetryPayloadSnapshot(postEnrichmentPayload); post != nil {
		detail["post_enrichment_payload"] = post
	}
	if event, ok := emitted.Get(); ok {
		if v := strings.TrimSpace(event.ID()); v != "" {
			detail["emitted_event_id"] = v
		}
		if v := strings.TrimSpace(string(event.Type())); v != "" {
			detail["emitted_event_type"] = v
		}
		if v := strings.TrimSpace(event.EntityID()); v != "" {
			detail["emitted_entity_id"] = v
		}
		if v := strings.TrimSpace(event.TaskID()); v != "" {
			detail["emitted_task_id"] = v
		}
	}
	if v := strings.TrimSpace(actor.EffectiveEntityID()); v != "" {
		detail["actor_entity_id"] = v
	}
	logger.LogRuntime(toolExecutorRuntimeLogContext(ctx), runtimepipeline.RuntimeLogEntry{
		Level:     diaglog.NormalizeLevel(level),
		Message:   emitToolOutcomeMessage(toolName, outcome, execErr),
		Component: "tool-executor",
		Action:    emitToolOutcomeAction,
		AgentID:   strings.TrimSpace(actor.ID),
		EntityID:  strings.TrimSpace(actor.EffectiveEntityID()),
		Detail:    detail,
		Failure:   toolExecFailure(execErr),
	})
}

func emitToolOutcomeMessage(toolName, outcome string, execErr error) string {
	toolName = strings.TrimSpace(toolName)
	outcome = strings.TrimSpace(outcome)
	switch outcome {
	case "published":
		if toolName == "" {
			return "Emit tool published event successfully"
		}
		return fmt.Sprintf("Emit tool %s published event successfully", toolName)
	case "payload_shape_failed":
		if toolName == "" {
			return "Emit tool payload shape failed before validation"
		}
		return fmt.Sprintf("Emit tool %s payload shape failed before validation", toolName)
	case "schema_validation_failed":
		if toolName == "" {
			return "Emit tool schema validation failed"
		}
		return fmt.Sprintf("Emit tool %s schema validation failed", toolName)
	case "event_publish_failed":
		if toolName == "" {
			return "Emit tool publish failed"
		}
		return fmt.Sprintf("Emit tool %s publish failed", toolName)
	case "invalid_emit_tool_name":
		if toolName == "" {
			return "Emit tool name was invalid"
		}
		return fmt.Sprintf("Emit tool %s was invalid", toolName)
	default:
		if execErr == nil {
			if toolName == "" {
				return "Emit tool outcome recorded"
			}
			return fmt.Sprintf("Emit tool %s outcome recorded", toolName)
		}
		if toolName == "" {
			return "Emit tool failed"
		}
		return fmt.Sprintf("Emit tool %s failed", toolName)
	}
}

func safeTelemetryPayloadSnapshot(payload map[string]any) any {
	if len(payload) == 0 {
		return nil
	}
	redacted := RedactTelemetryValue(payload)
	raw, err := json.Marshal(redacted)
	if err == nil && len(raw) <= maxToolTelemetryChars {
		return redacted
	}
	return map[string]any{
		"truncated": true,
		"summary":   SafeTelemetryText(payload),
	}
}

func isEmitToolName(toolName string) bool {
	return strings.HasPrefix(normalizeNativeToolName(toolName), "emit_")
}
