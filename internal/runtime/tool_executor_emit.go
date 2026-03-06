package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	"github.com/google/uuid"
)

func (e *RuntimeToolExecutor) handleEmitTool(ctx context.Context, actor models.AgentConfig, toolName string, input any) (any, error) {
	eventType, ok := eventTypeFromEmitToolName(toolName)
	if !ok {
		return nil, NewRuntimeError(
			"invalid_emit_tool_name",
			"tool-executor",
			"handle_emit_tool.resolve_event_type",
			false,
			"invalid emit tool name: %s",
			toolName,
		)
	}
	if !IsEmitToolAllowedForRole(actor.Role, toolName) {
		return nil, NewRuntimeError(
			"emit_tool_not_allowed",
			"tool-executor",
			"handle_emit_tool.authorize",
			false,
			"event type %q is not allowed for role %q",
			eventType,
			canonicalRuntimeRole(actor.Role),
		)
	}
	if e.bus == nil {
		return nil, NewRuntimeError(
			"dependency_unavailable",
			"tool-executor",
			"handle_emit_tool.publish",
			true,
			"event bus is not configured",
		)
	}

	payloadMap := map[string]any{}
	if err := decodeToolInput(input, &payloadMap); err != nil {
		return nil, WrapRuntimeError(
			"invalid_tool_input",
			"tool-executor",
			"handle_emit_tool.decode_input",
			false,
			err,
			"invalid emit tool input",
		)
	}
	if payloadMap == nil {
		payloadMap = map[string]any{}
	}

	inbound, _ := InboundEventFromContext(ctx)
	if err := e.trackTransitionPrerequisites(actor, inbound); err != nil {
		return nil, WrapRuntimeError(
			"emit_transition_prerequisite_failed",
			"tool-executor",
			"handle_emit_tool.track_prerequisites",
			false,
			err,
			"emit transition prerequisites failed",
		)
	}

	payloadMap = e.enrichEmitPayloadContext(actor, inbound, eventType, payloadMap)
	payloadMap = e.preNormalizeEmitPayload(actor, inbound, eventType, payloadMap)
	payloadMap = trimEmitPayloadToSchema(eventType, payloadMap)
	if err := ValidateEventPayloadAgainstSchema(eventType, payloadMap); err != nil {
		return nil, WrapRuntimeError(
			"schema_validation_failed",
			"tool-executor",
			"handle_emit_tool.validate_schema_pre_normalize",
			false,
			err,
			"emit payload schema validation failed",
		)
	}
	payloadMap = e.normalizeEmitPayload(actor, inbound, eventType, payloadMap)
	payloadMap = trimEmitPayloadToSchema(eventType, payloadMap)
	if err := ValidateEventPayloadAgainstSchema(eventType, payloadMap); err != nil {
		return nil, WrapRuntimeError(
			"schema_validation_failed",
			"tool-executor",
			"handle_emit_tool.validate_schema_post_normalize",
			false,
			err,
			"emit payload schema validation failed",
		)
	}
	if err := e.enforceMigrationGuardrail(ctx, actor, eventType, payloadMap); err != nil {
		return nil, err
	}

	emitted := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType(eventType),
		SourceAgent: actor.ID,
		TaskID:      strings.TrimSpace(asString(payloadMap["task_id"])),
		VerticalID:  strings.TrimSpace(asString(payloadMap["vertical_id"])),
		Payload:     mustJSON(payloadMap),
		CreatedAt:   time.Now(),
	}
	if emitted.VerticalID == "" {
		emitted.VerticalID = strings.TrimSpace(actor.VerticalID)
	}
	if emitted.TaskID == "" {
		emitted.TaskID = strings.TrimSpace(inbound.TaskID)
	}
	if emitted.VerticalID == "" {
		emitted.VerticalID = strings.TrimSpace(inbound.VerticalID)
	}

	if err := e.validateEmitTransition(actor, inbound, emitted); err != nil {
		return nil, WrapRuntimeError(
			"emit_transition_guardrail_violation",
			"tool-executor",
			"handle_emit_tool.validate_transition",
			false,
			err,
			"emit transition rejected by guardrail",
		)
	}
	if err := e.bus.Publish(ctx, emitted); err != nil {
		return nil, WrapRuntimeError(
			"event_publish_failed",
			"tool-executor",
			"handle_emit_tool.publish",
			true,
			err,
			"failed to publish emitted event type=%s event_id=%s",
			eventType,
			emitted.ID,
		)
	}

	if rec, ok := EmittedEventsRecorderFromContext(ctx); ok && rec != nil {
		rec.Append(emitted)
	}
	return map[string]any{
		"status":     "published",
		"event_id":   emitted.ID,
		"event_type": eventType,
	}, nil
}

func (e *RuntimeToolExecutor) preNormalizeEmitPayload(actor models.AgentConfig, inbound events.Event, eventType string, payload map[string]any) map[string]any {
	if payload == nil {
		payload = map[string]any{}
	}
	if eventType == "source.scraped" {
		return preNormalizeSourceScrapedPayload(inbound, payload)
	}
	if eventType == "vertical.derived" {
		return preNormalizeVerticalDerivedPayload(payload)
	}
	role := canonicalRuntimeRole(actor.Role)
	if role == "empire-coordinator" && eventType == "scan.requested" {
		return preNormalizeCoordinatorScanRequestedPayload(inbound, payload)
	}
	if shouldFlattenLegacyNestedEmitPayload(eventType) {
		return preNormalizeLegacyNestedEmitPayload(payload)
	}
	return payload
}

func preNormalizeVerticalDerivedPayload(payload map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range payload {
		out[k] = v
	}
	if rationale, ok := out["derivation_rationale"]; ok {
		if _, isObj := rationale.(map[string]any); !isObj {
			if summary := strings.TrimSpace(asString(rationale)); summary != "" {
				out["derivation_rationale"] = map[string]any{
					"summary": summary,
				}
			}
		}
	}
	return out
}

func (e *RuntimeToolExecutor) enrichEmitPayloadContext(actor models.AgentConfig, inbound events.Event, eventType string, payload map[string]any) map[string]any {
	if payload == nil {
		payload = map[string]any{}
	}
	out := make(map[string]any, len(payload)+2)
	for k, v := range payload {
		out[k] = v
	}
	if emitSchemaAllowsProperty(eventType, "task_id") && strings.TrimSpace(asString(out["task_id"])) == "" {
		out["task_id"] = strings.TrimSpace(inbound.TaskID)
	}
	if emitSchemaAllowsProperty(eventType, "vertical_id") && strings.TrimSpace(asString(out["vertical_id"])) == "" {
		verticalID := strings.TrimSpace(actor.VerticalID)
		if verticalID == "" {
			verticalID = strings.TrimSpace(inbound.VerticalID)
		}
		out["vertical_id"] = verticalID
	}
	return out
}

func (e *RuntimeToolExecutor) normalizeEmitPayload(actor models.AgentConfig, inbound events.Event, eventType string, payload map[string]any) map[string]any {
	if payload == nil {
		payload = map[string]any{}
	}
	role := canonicalRuntimeRole(actor.Role)
	if role == "empire-coordinator" && eventType == "scan.requested" {
		return normalizeCoordinatorScanRequestedPayload(inbound, payload)
	}
	if role == "empire-coordinator" && strings.HasPrefix(eventType, "budget.") && strings.TrimSpace(string(inbound.Type)) == "budget.threshold_crossed" {
		payload["event_type"] = eventType
		if _, ok := payload["threshold_event_id"]; !ok {
			payload["threshold_event_id"] = strings.TrimSpace(inbound.ID)
		}
	}
	if eventType == "portfolio.digest_compiled" {
		msg := strings.TrimSpace(asString(payload["message"]))
		legacy := strings.TrimSpace(asString(payload["digest_text"]))
		switch {
		case msg == "" && legacy != "":
			payload["message"] = legacy
		case msg != "" && legacy == "":
			payload["digest_text"] = msg
		}
	}
	return payload
}

func normalizeCoordinatorScanRequestedPayload(inbound events.Event, current map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range current {
		out[k] = v
	}

	directiveText := ""
	if len(inbound.Payload) > 0 {
		var payload map[string]any
		if err := json.Unmarshal(inbound.Payload, &payload); err == nil {
			directiveText = strings.TrimSpace(asString(payload["directive_text"]))
		}
	}

	mode := normalizeScanModeCompat(asString(out["mode"]))
	if mode == "" {
		mode = inferDiscoveryMode(directiveText)
	}
	if mode == "" {
		mode = "saas_gap"
	}
	out["mode"] = mode

	priority := normalizeScanPriorityCompat(asString(out["priority"]))
	if priority == "" {
		priority = "normal"
	}
	out["priority"] = priority

	if strings.TrimSpace(asString(out["geography"])) == "" && strings.TrimSpace(asString(out["geography_id"])) == "" {
		if geo := inferGeographyHint(directiveText); geo != "" {
			out["geography"] = geo
		} else {
			out["geography"] = "unspecified"
		}
	}
	if _, ok := out["taxonomy_categories"]; !ok {
		if categories := extractCategoryList(out); len(categories) > 0 {
			out["taxonomy_categories"] = categories
		} else {
			out["taxonomy_categories"] = []string{}
		}
	}
	if _, ok := out["campaign_context"]; !ok {
		modes := []string{strings.TrimSpace(asString(out["mode"]))}
		if modes[0] == "" {
			modes[0] = "saas_gap"
		}
		strategicContext := strings.TrimSpace(asString(out["strategic_context"]))
		if strategicContext == "" {
			strategicContext = directiveText
		}
		directiveID := strings.TrimSpace(asString(out["directive_id"]))
		if directiveID == "" {
			directiveID = strings.TrimSpace(inbound.ID)
		}
		out["campaign_context"] = map[string]any{
			"modes":             modes,
			"strategic_context": strategicContext,
			"directive_id":      directiveID,
		}
	}
	delete(out, "vertical")
	delete(out, "focus")
	delete(out, "criteria")
	delete(out, "payload")
	return out
}

func emitSchemaAllowsProperty(eventType, property string) bool {
	eventType = strings.TrimSpace(eventType)
	property = strings.TrimSpace(property)
	if eventType == "" || property == "" {
		return false
	}
	schema := schemaForEventType(eventType).Schema
	props, ok := schema["properties"].(map[string]any)
	if !ok || len(props) == 0 {
		return false
	}
	_, ok = props[property]
	return ok
}

func trimEmitPayloadToSchema(eventType string, payload map[string]any) map[string]any {
	if payload == nil {
		return map[string]any{}
	}
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return payload
	}
	schema := schemaForEventType(eventType).Schema
	if schemaAdditionalProps(schema["additionalProperties"]) {
		return payload
	}
	props := schemaProperties(schema["properties"])
	if len(props) == 0 {
		return payload
	}
	out := make(map[string]any, len(payload))
	for k, v := range payload {
		if _, ok := props[strings.TrimSpace(k)]; !ok {
			continue
		}
		out[k] = v
	}
	return out
}

func (e *RuntimeToolExecutor) enforceMigrationGuardrail(ctx context.Context, actor models.AgentConfig, eventType string, payload map[string]any) error {
	eventType = strings.TrimSpace(eventType)
	if eventType != "devops.deploy_requested" && eventType != "devops.rollback_requested" {
		return nil
	}
	migrationSQL := strings.TrimSpace(extractMigrationSQL(eventType, payload))
	if migrationSQL == "" {
		return nil
	}
	classification := ClassifyMigration(migrationSQL)
	if !classification.RequiresApproval {
		return nil
	}
	if e.mailboxStore != nil {
		contextPayload := map[string]any{
			"event_type":          eventType,
			"vertical_id":         strings.TrimSpace(asString(payload["vertical_id"])),
			"requesting_agent":    strings.TrimSpace(asString(payload["requesting_agent"])),
			"destructive_ops":     classification.DestructiveOps,
			"requires_approval":   classification.RequiresApproval,
			"migration_statement": migrationSQL,
		}
		if _, err := e.mailboxStore.InsertMailboxItem(ctx, MailboxItem{
			VerticalID: strings.TrimSpace(asString(payload["vertical_id"])),
			FromAgent:  actor.ID,
			Type:       "migration_approval",
			Priority:   "critical",
			Status:     "pending",
			Context:    mustJSON(contextPayload),
			Summary:    "Destructive migration requires human approval before deploy",
		}); err != nil {
			runtimeWarn("tool-executor", "failed to insert migration_approval mailbox item: %v", err)
		}
	}
	return NewRuntimeError(
		"migration_requires_approval",
		"tool-executor",
		"handle_emit_tool.migration_guardrail",
		false,
		"migration contains destructive operations and requires approval: %s",
		strings.Join(classification.DestructiveOps, "; "),
	)
}

func extractMigrationSQL(eventType string, payload map[string]any) string {
	if payload == nil {
		return ""
	}
	switch strings.TrimSpace(eventType) {
	case "devops.deploy_requested":
		if raw := strings.TrimSpace(asString(payload["migration_sql"])); raw != "" {
			return raw
		}
		if manifest, ok := payload["manifest"].(map[string]any); ok {
			return strings.TrimSpace(asString(manifest["migration_sql"]))
		}
	case "devops.rollback_requested":
		if raw := strings.TrimSpace(asString(payload["rollback_migration"])); raw != "" {
			return raw
		}
		if manifest, ok := payload["manifest"].(map[string]any); ok {
			return strings.TrimSpace(asString(manifest["rollback_migration"]))
		}
	}
	return ""
}

func preNormalizeCoordinatorScanRequestedPayload(inbound events.Event, current map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range current {
		out[k] = v
	}
	directiveText := ""
	if len(inbound.Payload) > 0 {
		var payload map[string]any
		if err := json.Unmarshal(inbound.Payload, &payload); err == nil {
			directiveText = strings.TrimSpace(asString(payload["directive_text"]))
		}
	}
	originalMode := strings.TrimSpace(asString(out["mode"]))
	originalPriority := strings.TrimSpace(asString(out["priority"]))

	if nested, ok := asObject(out["payload"]); ok {
		runtimeWarn(
			"emit-normalization",
			"flattening coordinator scan.requested nested payload event_id=%s source=%s keys=%d",
			strings.TrimSpace(inbound.ID),
			strings.TrimSpace(inbound.SourceAgent),
			len(nested),
		)
		for k, v := range nested {
			if existing, exists := out[k]; !exists || strings.TrimSpace(asString(existing)) == "" {
				out[k] = v
			}
		}
	}

	modeRaw := asString(out["mode"])
	if mode := normalizeScanModeCompat(modeRaw); mode != "" {
		out["mode"] = mode
	} else if strings.TrimSpace(modeRaw) != "" {
		directiveText := ""
		if len(inbound.Payload) > 0 {
			var payload map[string]any
			if err := json.Unmarshal(inbound.Payload, &payload); err == nil {
				directiveText = strings.TrimSpace(asString(payload["directive_text"]))
			}
		}
		inferred := inferDiscoveryMode(directiveText)
		if inferred != "" {
			runtimeWarn(
				"emit-normalization",
				"coercing invalid coordinator mode raw=%q inferred=%q event_id=%s",
				strings.TrimSpace(modeRaw),
				inferred,
				strings.TrimSpace(inbound.ID),
			)
		}
		out["mode"] = inferred
	}
	if priority := normalizeScanPriorityCompat(asString(out["priority"])); priority != "" {
		out["priority"] = priority
	}
	if strings.TrimSpace(asString(out["geography"])) == "" && strings.TrimSpace(asString(out["geography_id"])) == "" {
		if geo := inferGeographyHint(directiveText); geo != "" {
			out["geography"] = geo
		} else {
			out["geography"] = "unspecified"
		}
	}
	if _, ok := out["campaign_context"]; !ok {
		modes := []string{strings.TrimSpace(asString(out["mode"]))}
		if modes[0] == "" {
			modes[0] = "saas_gap"
		}
		strategicContext := strings.TrimSpace(asString(out["strategic_context"]))
		if strategicContext == "" {
			strategicContext = directiveText
		}
		directiveID := strings.TrimSpace(asString(out["directive_id"]))
		if directiveID == "" {
			directiveID = strings.TrimSpace(inbound.ID)
		}
		out["campaign_context"] = map[string]any{
			"modes":             modes,
			"strategic_context": strategicContext,
			"directive_id":      directiveID,
		}
	}
	if coercedMode := strings.TrimSpace(asString(out["mode"])); originalMode != "" && coercedMode != "" && !strings.EqualFold(originalMode, coercedMode) {
		runtimeWarn(
			"emit-normalization",
			"coordinator scan.requested mode normalized raw=%q normalized=%q event_id=%s",
			originalMode,
			coercedMode,
			strings.TrimSpace(inbound.ID),
		)
	}
	if coercedPriority := strings.TrimSpace(asString(out["priority"])); originalPriority != "" && coercedPriority != "" && !strings.EqualFold(originalPriority, coercedPriority) {
		runtimeWarn(
			"emit-normalization",
			"coordinator scan.requested priority normalized raw=%q normalized=%q event_id=%s",
			originalPriority,
			coercedPriority,
			strings.TrimSpace(inbound.ID),
		)
	}

	delete(out, "vertical")
	delete(out, "focus")
	delete(out, "criteria")
	delete(out, "payload")
	return out
}

func shouldFlattenLegacyNestedEmitPayload(eventType string) bool {
	switch strings.TrimSpace(eventType) {
	case "category.assessed",
		"trend.identified",
		"source.scraped",
		"market_research.scan_complete",
		"trend_research.scan_complete",
		"scanner.google_maps.scan_complete",
		"scanner.instagram.scan_complete",
		"scanner.reviews.scan_complete",
		"scanner.directories.scan_complete",
		"scanner.yelp.scan_complete":
		return true
	default:
		return false
	}
}

func preNormalizeLegacyNestedEmitPayload(current map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range current {
		out[k] = v
	}
	if nested, ok := asObject(out["payload"]); ok {
		runtimeWarn(
			"emit-normalization",
			"flattening legacy nested emit payload keys=%d",
			len(nested),
		)
		for k, v := range nested {
			if existing, exists := out[k]; !exists || strings.TrimSpace(asString(existing)) == "" {
				out[k] = v
			}
		}
	}
	delete(out, "payload")
	return out
}

func preNormalizeSourceScrapedPayload(inbound events.Event, current map[string]any) map[string]any {
	out := preNormalizeLegacyNestedEmitPayload(current)
	currentGeo := strings.TrimSpace(asString(out["geography"]))
	if !isPlaceholderGeography(currentGeo) {
		return out
	}
	if inferred := extractAssignedGeography(inbound); inferred != "" {
		out["geography"] = inferred
	}
	return out
}

func extractAssignedGeography(inbound events.Event) string {
	payload := parsePayloadMap(inbound.Payload)
	if len(payload) == 0 {
		return ""
	}
	for _, key := range []string{"geography", "geography_label"} {
		if value := strings.TrimSpace(asString(payload[key])); !isPlaceholderGeography(value) {
			return value
		}
	}
	if shard, ok := asObject(payload["shard"]); ok {
		if scope, ok := asObject(shard["scope"]); ok {
			for _, key := range []string{"geography", "geography_label"} {
				if value := strings.TrimSpace(asString(scope[key])); !isPlaceholderGeography(value) {
					return value
				}
			}
			if geoID := strings.TrimSpace(asString(scope["geography_id"])); geoID != "" {
				return geoID
			}
		}
	}
	if geoID := strings.TrimSpace(asString(payload["geography_id"])); geoID != "" {
		return geoID
	}
	return ""
}

func isPlaceholderGeography(value string) bool {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return true
	}
	tokens := strings.FieldsFunc(value, func(r rune) bool {
		switch r {
		case ',', '/', '|', ';':
			return true
		default:
			return false
		}
	})
	if len(tokens) == 0 {
		tokens = []string{value}
	}
	placeholder := map[string]struct{}{
		"unspecified": {},
		"unknown":     {},
		"n/a":         {},
		"na":          {},
		"none":        {},
		"null":        {},
		"-":           {},
	}
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if _, ok := placeholder[token]; !ok {
			return false
		}
	}
	return true
}

func normalizeScanModeCompat(raw string) string {
	if mode := normalizeScanMode(raw); mode != "" {
		return mode
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "discovery", "scan", "default", "automation", "micro", "automation-micro", "automation_micro":
		return "saas_gap"
	case "saas":
		return "saas_gap"
	case "trend", "trend_scan", "saas-trend":
		return "saas_trend"
	case "local", "local_service", "local-services", "services":
		return "local_services"
	case "corpus_mode", "signal_corpus", "corpus":
		return "corpus"
	case "derived":
		return "derived"
	default:
		return ""
	}
}

func normalizeScanPriorityCompat(raw string) string {
	if priority := normalizeScanPriority(raw); priority != "" {
		return priority
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "med", "medium", "default":
		return "normal"
	case "urgent":
		return "critical"
	default:
		return ""
	}
}

func asObject(v any) (map[string]any, bool) {
	switch t := v.(type) {
	case map[string]any:
		return t, true
	default:
		return nil, false
	}
}

func (e *RuntimeToolExecutor) trackTransitionPrerequisites(actor models.AgentConfig, inbound events.Event) error {
	role := canonicalRuntimeRole(actor.Role)
	inboundType := strings.TrimSpace(string(inbound.Type))
	if inboundType == "" {
		return nil
	}
	switch role {
	case "validation-coordinator":
		if inboundType == "vertical.needs_more_data" {
			e.clearOneShot(actor.ID, "vertical.ready_for_review", transitionContextKey(inbound, inbound))
		}
	case "business-research-agent":
		if inboundType == "validation.started" || inboundType == "spec.revision_requested" {
			e.clearOneShot(actor.ID, "spec.approved", transitionContextKey(inbound, inbound))
		}
	}
	return nil
}

func (e *RuntimeToolExecutor) validateEmitTransition(actor models.AgentConfig, inbound events.Event, emitted events.Event) error {
	role := canonicalRuntimeRole(actor.Role)
	inboundType := strings.TrimSpace(string(inbound.Type))
	emittedType := strings.TrimSpace(string(emitted.Type))
	switch {
	case role == "empire-coordinator" && emittedType == "opco.spinup_requested":
		if inboundType != "vertical.approved" {
			return fmt.Errorf("guardrail_violation transition_violation: opco.spinup_requested requires inbound vertical.approved, got %s", inboundType)
		}
	case role == "empire-coordinator" && emittedType == "template.migration_completed":
		if inboundType != "template.migration_approved" {
			return fmt.Errorf("guardrail_violation transition_violation: template.migration_completed requires inbound template.migration_approved, got %s", inboundType)
		}
	case role == "empire-coordinator" && strings.HasPrefix(emittedType, "budget.") && inboundType == "budget.threshold_crossed":
		expected := strings.TrimSpace(string(budgetEventTypeFromThresholdPayload(inbound.Payload)))
		if expected != "" && expected != emittedType {
			return fmt.Errorf("guardrail_violation transition_violation: expected %s for inbound budget.threshold_crossed, got %s", expected, emittedType)
		}
	case role == "factory-cto" && emittedType == "template.version_published":
		if inboundType != "spec.validation_passed" {
			return fmt.Errorf("guardrail_violation transition_violation: template.version_published requires inbound spec.validation_passed, got %s", inboundType)
		}
	case role == "validation-coordinator" && emittedType == "vertical.ready_for_review":
		if inboundType != "validation.package_ready" {
			return fmt.Errorf("guardrail_violation transition_violation: vertical.ready_for_review requires inbound validation.package_ready, got %s", inboundType)
		}
		key := transitionContextKey(emitted, inbound)
		if inboundID := strings.TrimSpace(inbound.ID); inboundID != "" {
			key = key + "|" + inboundID
		}
		if e.isOneShotEmitted(actor.ID, emittedType, key) {
			return fmt.Errorf("guardrail_violation duplicate_emission: vertical.ready_for_review already emitted for context=%s", key)
		}
		e.markOneShotEmitted(actor.ID, emittedType, key)
	case role == "business-research-agent" && emittedType == "spec.requested":
		if inboundType != "validation.started" && inboundType != "spec.revision_requested" {
			return fmt.Errorf("guardrail_violation transition_violation: spec.requested requires inbound validation.started or spec.revision_requested, got %s", inboundType)
		}
	case role == "business-research-agent" && emittedType == "spec_review.requested":
		if inboundType != "spec.draft_ready" {
			return fmt.Errorf("guardrail_violation transition_violation: spec_review.requested requires inbound spec.draft_ready, got %s", inboundType)
		}
	case role == "business-research-agent" && emittedType == "spec.approved":
		if inboundType != "spec_review.passed" {
			return fmt.Errorf("guardrail_violation transition_violation: spec.approved requires inbound spec_review.passed, got %s", inboundType)
		}
		key := transitionContextKey(emitted, inbound)
		if e.isOneShotEmitted(actor.ID, emittedType, key) {
			return fmt.Errorf("guardrail_violation duplicate_emission: spec.approved already emitted for context=%s", key)
		}
		e.markOneShotEmitted(actor.ID, emittedType, key)
	}
	return nil
}

func (e *RuntimeToolExecutor) oneShotKey(agentID, eventType, contextKey string) string {
	return strings.TrimSpace(agentID) + "|" + strings.TrimSpace(eventType) + "|" + strings.TrimSpace(contextKey)
}

func (e *RuntimeToolExecutor) isOneShotEmitted(agentID, eventType, contextKey string) bool {
	if strings.TrimSpace(agentID) == "" || strings.TrimSpace(eventType) == "" || strings.TrimSpace(contextKey) == "" {
		return false
	}
	key := e.oneShotKey(agentID, eventType, contextKey)
	e.oneShotMu.Lock()
	defer e.oneShotMu.Unlock()
	_, ok := e.oneShotEmits[key]
	return ok
}

func (e *RuntimeToolExecutor) markOneShotEmitted(agentID, eventType, contextKey string) {
	if strings.TrimSpace(agentID) == "" || strings.TrimSpace(eventType) == "" || strings.TrimSpace(contextKey) == "" {
		return
	}
	key := e.oneShotKey(agentID, eventType, contextKey)
	e.oneShotMu.Lock()
	e.oneShotEmits[key] = struct{}{}
	e.oneShotMu.Unlock()
}

func (e *RuntimeToolExecutor) clearOneShot(agentID, eventType, contextKey string) {
	if strings.TrimSpace(agentID) == "" || strings.TrimSpace(eventType) == "" || strings.TrimSpace(contextKey) == "" {
		return
	}
	key := e.oneShotKey(agentID, eventType, contextKey)
	e.oneShotMu.Lock()
	delete(e.oneShotEmits, key)
	e.oneShotMu.Unlock()
}
