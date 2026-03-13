package tools

import (
	"context"
	"fmt"
	"strings"

	"empireai/internal/events"
	models "empireai/internal/runtime/actors"
)

func (e *Executor) enforceMigrationGuardrail(ctx context.Context, actor models.AgentConfig, eventType string, payload map[string]any) error {
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

func (e *Executor) trackTransitionPrerequisites(actor models.AgentConfig, inbound events.Event) error {
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

func (e *Executor) validateEmitTransition(actor models.AgentConfig, inbound events.Event, emitted events.Event) error {
	role := canonicalRuntimeRole(actor.Role)
	if err := validateEmitTransitionSemantics(role, inbound, emitted); err != nil {
		return err
	}
	inboundType := strings.TrimSpace(string(inbound.Type))
	emittedType := strings.TrimSpace(string(emitted.Type))
	switch {
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

func EnforceRequiredEmitContract(role string, inbound events.Event, emitted []events.Event) error {
	req := emitTurnRequirementFor(role, inbound)
	if req == nil {
		return nil
	}
	for _, evt := range emitted {
		if req.matches(evt) {
			return nil
		}
	}
	return fmt.Errorf(req.violation)
}

func RequiredEmitToolContractText(role string, inbound events.Event) string {
	req := emitTurnRequirementFor(role, inbound)
	if req == nil {
		return ""
	}
	return req.requirement
}

func EmitContractRemediationPrompt(role string, inbound events.Event, _ error) (string, bool) {
	req := emitTurnRequirementFor(role, inbound)
	if req == nil || strings.TrimSpace(req.remediation) == "" {
		return "", false
	}
	return req.remediation, true
}

type emitTurnRequirement struct {
	requirement string
	remediation string
	violation   string
	matches     func(events.Event) bool
}

func emitTurnRequirementFor(role string, inbound events.Event) *emitTurnRequirement {
	role = canonicalRuntimeRole(role)
	inboundType := strings.TrimSpace(string(inbound.Type))
	switch {
	case role == "empire-coordinator" && inboundType == "system.directive" && emitDirectiveRequiresScanRequest(inbound):
		return &emitTurnRequirement{
			requirement: "\n- REQUIRED for this turn: call emit_scan_requested exactly once (with mode, geography_id when known, and priority).",
			remediation: "Runtime contract remediation: your prior response did not satisfy the required event emission.\n" +
				"Call emit_scan_requested exactly once now with valid arguments (include mode; include priority; include geography_id when known).\n" +
				"Do not return prose. Use the tool call now.",
			violation: "system.directive handling must emit scan.requested via emit_scan_requested",
			matches: func(evt events.Event) bool {
				return strings.TrimSpace(string(evt.Type)) == "scan.requested"
			},
		}
	case role == "empire-coordinator" && inboundType == "budget.threshold_crossed":
		return &emitTurnRequirement{
			requirement: "\n- REQUIRED for this turn: call exactly one emit_budget_* tool to reflect the threshold decision.",
			remediation: "Runtime contract remediation: your prior response did not satisfy the required event emission.\n" +
				"Call exactly one emit_budget_* tool now to reflect the budget decision.\n" +
				"Do not return prose. Use the tool call now.",
			violation: "budget.threshold_crossed handling must emit one budget.* event via emit_budget_* tool",
			matches: func(evt events.Event) bool {
				return strings.HasPrefix(strings.TrimSpace(string(evt.Type)), "budget.")
			},
		}
	default:
		return nil
	}
}

func validateEmitTransitionSemantics(role string, inbound events.Event, emitted events.Event) error {
	role = canonicalRuntimeRole(role)
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
	}
	return nil
}

func (e *Executor) oneShotKey(agentID, eventType, contextKey string) string {
	return strings.TrimSpace(agentID) + "|" + strings.TrimSpace(eventType) + "|" + strings.TrimSpace(contextKey)
}

func (e *Executor) isOneShotEmitted(agentID, eventType, contextKey string) bool {
	if strings.TrimSpace(agentID) == "" || strings.TrimSpace(eventType) == "" || strings.TrimSpace(contextKey) == "" {
		return false
	}
	key := e.oneShotKey(agentID, eventType, contextKey)
	e.oneShotMu.Lock()
	defer e.oneShotMu.Unlock()
	_, ok := e.oneShotEmits[key]
	return ok
}

func (e *Executor) markOneShotEmitted(agentID, eventType, contextKey string) {
	if strings.TrimSpace(agentID) == "" || strings.TrimSpace(eventType) == "" || strings.TrimSpace(contextKey) == "" {
		return
	}
	key := e.oneShotKey(agentID, eventType, contextKey)
	e.oneShotMu.Lock()
	e.oneShotEmits[key] = struct{}{}
	e.oneShotMu.Unlock()
}

func (e *Executor) clearOneShot(agentID, eventType, contextKey string) {
	if strings.TrimSpace(agentID) == "" || strings.TrimSpace(eventType) == "" || strings.TrimSpace(contextKey) == "" {
		return
	}
	key := e.oneShotKey(agentID, eventType, contextKey)
	e.oneShotMu.Lock()
	delete(e.oneShotEmits, key)
	e.oneShotMu.Unlock()
}
