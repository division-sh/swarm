package tools

import (
	"sort"
	"strings"
)

// StrictDefaultEventSchemas enumerates produced events that currently use the
// default agent schema contract until catalog-authored payloads are added.
var StrictDefaultEventSchemas = map[string]EmitSchema{
	"analyst.anti_pattern_advisory":      DefaultAgentEventSchema("analyst.anti_pattern_advisory"),
	"analyst.bootstrap_upgrade_proposal": DefaultAgentEventSchema("analyst.bootstrap_upgrade_proposal"),
	"analyst.prompt_refinement_proposal": DefaultAgentEventSchema("analyst.prompt_refinement_proposal"),
	"brand.candidates_ready":             DefaultAgentEventSchema("brand.candidates_ready"),
	"budget.emergency":                   DefaultAgentEventSchema("budget.emergency"),
	"budget.resumed":                     DefaultAgentEventSchema("budget.resumed"),
	"budget.throttle":                    DefaultAgentEventSchema("budget.throttle"),
	"budget.warning":                     DefaultAgentEventSchema("budget.warning"),
	"bug_fix_deployed":                   DefaultAgentEventSchema("bug_fix_deployed"),
	"bug_reported":                       DefaultAgentEventSchema("bug_reported"),
	"build_blocked":                      DefaultAgentEventSchema("build_blocked"),
	"build_complete":                     DefaultAgentEventSchema("build_complete"),
	"build_progress":                     DefaultAgentEventSchema("build_progress"),
	"channel_blocked":                    DefaultAgentEventSchema("channel_blocked"),
	"churn_risk":                         DefaultAgentEventSchema("churn_risk"),
	"cross_domain_report":                DefaultAgentEventSchema("cross_domain_report"),
	"cto.architecture_directive":         DefaultAgentEventSchema("cto.architecture_directive"),
	"cto.extraction_recommended":         DefaultAgentEventSchema("cto.extraction_recommended"),
	"cto.pattern_detected":               DefaultAgentEventSchema("cto.pattern_detected"),
	"cto.spec_approved":                  DefaultAgentEventSchema("cto.spec_approved"),
	"cto.spec_revision_needed":           DefaultAgentEventSchema("cto.spec_revision_needed"),
	"cto.spec_vetoed":                    DefaultAgentEventSchema("cto.spec_vetoed"),
	"cto.tech_spec_feedback":             DefaultAgentEventSchema("cto.tech_spec_feedback"),
	"cto.tech_spec_review_requested":     DefaultAgentEventSchema("cto.tech_spec_review_requested"),
	"dedup.resolved":                     DefaultAgentEventSchema("dedup.resolved"),
	"deploy_requested":                   DefaultAgentEventSchema("deploy_requested"),
	"devops.capacity_warning":            DefaultAgentEventSchema("devops.capacity_warning"),
	"devops.deploy_complete":             DefaultAgentEventSchema("devops.deploy_complete"),
	"devops.deploy_failed":               DefaultAgentEventSchema("devops.deploy_failed"),
	"devops.deploy_requested":            DefaultAgentEventSchema("devops.deploy_requested"),
	"devops.health_check_failed":         DefaultAgentEventSchema("devops.health_check_failed"),
	"devops.infra_change_needed":         DefaultAgentEventSchema("devops.infra_change_needed"),
	"devops.rollback_complete":           DefaultAgentEventSchema("devops.rollback_complete"),
	"devops.rollback_failed":             DefaultAgentEventSchema("devops.rollback_failed"),
	"devops.rollback_requested":          DefaultAgentEventSchema("devops.rollback_requested"),
	"devops.ssl_provisioned":             DefaultAgentEventSchema("devops.ssl_provisioned"),
	"feature_deployed":                   DefaultAgentEventSchema("feature_deployed"),
	"feature_request":                    DefaultAgentEventSchema("feature_request"),
	"growth_escalation":                  DefaultAgentEventSchema("growth_escalation"),
	"growth_report":                      DefaultAgentEventSchema("growth_report"),
	"human_task.approved":                DefaultAgentEventSchema("human_task.approved"),
	"human_task.deferred":                DefaultAgentEventSchema("human_task.deferred"),
	"human_task.rejected":                DefaultAgentEventSchema("human_task.rejected"),
	"market_signals":                     DefaultAgentEventSchema("market_signals"),
	"opco.ceo_report":                    DefaultAgentEventSchema("opco.ceo_report"),
	"opco.deploy_review":                 DefaultAgentEventSchema("opco.deploy_review"),
	"opco.escalation":                    DefaultAgentEventSchema("opco.escalation"),
	"opco.founder_input":                 DefaultAgentEventSchema("opco.founder_input"),
	"opco.launched":                      DefaultAgentEventSchema("opco.launched"),
	"opco.product_spec_review":           DefaultAgentEventSchema("opco.product_spec_review"),
	"opco.spend_request":                 DefaultAgentEventSchema("opco.spend_request"),
	"opco.steady_state_reached":          DefaultAgentEventSchema("opco.steady_state_reached"),
	"outreach_digest":                    DefaultAgentEventSchema("outreach_digest"),
	"prelaunch_ready":                    DefaultAgentEventSchema("prelaunch_ready"),
	"product_escalation":                 DefaultAgentEventSchema("product_escalation"),
	"product_report":                     DefaultAgentEventSchema("product_report"),
	"product_spec_ready":                 DefaultAgentEventSchema("product_spec_ready"),
	"qa.validation_failed":               DefaultAgentEventSchema("qa.validation_failed"),
	"qa.validation_passed":               DefaultAgentEventSchema("qa.validation_passed"),
	"research.completed":                 DefaultAgentEventSchema("research.completed"),
	"research.vertical_rejected":         DefaultAgentEventSchema("research.vertical_rejected"),
	"runtime.reset":                      DefaultAgentEventSchema("runtime.reset"),
	"spec.approved":                      DefaultAgentEventSchema("spec.approved"),
	"spec.draft_ready":                   DefaultAgentEventSchema("spec.draft_ready"),
	"spec.requested":                     DefaultAgentEventSchema("spec.requested"),
	"spec.revision_needed":               DefaultAgentEventSchema("spec.revision_needed"),
	"spec.validation_requested":          DefaultAgentEventSchema("spec.validation_requested"),
	"spec_review.issues_found":           DefaultAgentEventSchema("spec_review.issues_found"),
	"spec_review.passed":                 DefaultAgentEventSchema("spec_review.passed"),
	"spec_review.requested":              DefaultAgentEventSchema("spec_review.requested"),
	"spend_needed":                       DefaultAgentEventSchema("spend_needed"),
	"spend_request":                      DefaultAgentEventSchema("spend_request"),
	"support_critical":                   DefaultAgentEventSchema("support_critical"),
	"support_digest":                     DefaultAgentEventSchema("support_digest"),
	"synthesis.resolved":                 DefaultAgentEventSchema("synthesis.resolved"),
	"technical_spec_ready":               DefaultAgentEventSchema("technical_spec_ready"),
	"template.version_published":         DefaultAgentEventSchema("template.version_published"),
	"user_onboarded":                     DefaultAgentEventSchema("user_onboarded"),
}

func MissingProducerEventSchemas(producerRoles func() []string, producerEvents func(string) []string, registry map[string]EmitSchema) []string {
	missing := make([]string, 0, 16)
	for _, role := range producerRoles() {
		for _, eventType := range producerEvents(role) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			if _, ok := registry[eventType]; ok {
				continue
			}
			missing = append(missing, eventType)
		}
	}
	return UniqueNonEmpty(missing)
}

func EnsureSchemaContextFields(registry map[string]EmitSchema) {
	for eventType, entry := range registry {
		root := entry.Schema
		if root == nil {
			continue
		}
		rootType := strings.TrimSpace(AsString(root["type"]))
		if rootType != "" && rootType != "object" {
			continue
		}
		props, ok := root["properties"].(map[string]any)
		if !ok || props == nil {
			props = map[string]any{}
		}
		root["properties"] = props
		entry.Schema = root
		registry[eventType] = entry
	}
}

// EnsureSchemaPayloadParity aligns registry properties with the contract event
// payload field list (exhaustive-exact keys). Existing field schema definitions
// are preserved; missing fields are backfilled as strings.
func EnsureSchemaPayloadParity(registry map[string]EmitSchema, contractEventPayloadFields map[string][]string) {
	for eventType, payloadFields := range contractEventPayloadFields {
		entry, ok := registry[eventType]
		if !ok {
			continue
		}
		root := entry.Schema
		if root == nil {
			root = map[string]any{}
		}
		if strings.TrimSpace(AsString(root["type"])) == "" {
			root["type"] = "object"
		}
		props := schemaProperties(root["properties"])
		aligned := make(map[string]any, len(payloadFields))
		allowed := make(map[string]struct{}, len(payloadFields))
		for _, field := range payloadFields {
			field = strings.TrimSpace(field)
			if field == "" {
				continue
			}
			allowed[field] = struct{}{}
			if existing, ok := props[field]; ok && existing != nil {
				aligned[field] = existing
				continue
			}
			aligned[field] = map[string]any{"type": "string"}
		}
		root["properties"] = aligned

		existingRequired := requiredList(root["required"])
		filteredRequired := make([]string, 0, len(existingRequired))
		for _, field := range existingRequired {
			field = strings.TrimSpace(AsString(field))
			if field == "" {
				continue
			}
			if _, ok := allowed[field]; !ok {
				continue
			}
			filteredRequired = append(filteredRequired, field)
		}
		filteredRequired = UniqueNonEmpty(filteredRequired)
		if len(filteredRequired) > 0 {
			root["required"] = filteredRequired
		} else {
			delete(root, "required")
		}

		entry.Schema = root
		registry[eventType] = entry
	}
}

func SeedAgentEventSchemaDefaults(producerRoles func() []string, producerEvents func(string) []string, registry map[string]EmitSchema) {
	for _, role := range producerRoles() {
		for _, eventType := range producerEvents(role) {
			eventType = strings.TrimSpace(eventType)
			if eventType == "" {
				continue
			}
			if _, ok := registry[eventType]; ok {
				continue
			}
			registry[eventType] = DefaultAgentEventSchema(eventType)
		}
	}
}

func DefaultAgentEventSchema(eventType string) EmitSchema {
	props := map[string]any{
		"vertical_id":     map[string]any{"type": "string"},
		"task_id":         map[string]any{"type": "string"},
		"scan_id":         map[string]any{"type": "string"},
		"campaign_id":     map[string]any{"type": "string"},
		"mode":            map[string]any{"type": "string"},
		"geography":       map[string]any{"type": "string"},
		"geography_id":    map[string]any{"type": "string"},
		"name":            map[string]any{"type": "string"},
		"vertical_name":   map[string]any{"type": "string"},
		"priority":        map[string]any{"type": "string"},
		"status":          map[string]any{"type": "string"},
		"severity":        map[string]any{"type": "string"},
		"action":          map[string]any{"type": "string"},
		"reason":          map[string]any{"type": "string"},
		"notes":           map[string]any{"type": "string"},
		"summary":         map[string]any{"type": "string"},
		"message":         map[string]any{"type": "string"},
		"evidence":        map[string]any{"type": "string"},
		"score":           map[string]any{"type": "number"},
		"composite_score": map[string]any{"type": "number"},
		"viability_score": map[string]any{"type": "number"},
		"signal_strength": map[string]any{"type": "number"},
		"confidence":      map[string]any{"type": "string"},
		"passed":          map[string]any{"type": "boolean"},
		"version":         map[string]any{"type": "string"},
		"from_version":    map[string]any{"type": "string"},
		"to_version":      map[string]any{"type": "string"},
		"migration_id":    map[string]any{"type": "string"},
		"error":           map[string]any{"type": "string"},
		"requested_by":    map[string]any{"type": "string"},
		"requested_at":    map[string]any{"type": "string"},
		"completed_at":    map[string]any{"type": "string"},
		"failed_at":       map[string]any{"type": "string"},
		"digest_text":     map[string]any{"type": "string"},
		"recommendation":  map[string]any{"type": "string"},
		"snapshot":        map[string]any{"type": "object", "additionalProperties": true},
		"payload":         map[string]any{"type": "object", "additionalProperties": true},
		"metadata":        map[string]any{"type": "object", "additionalProperties": true},
		"context":         map[string]any{"type": "object", "additionalProperties": true},
		"details":         map[string]any{"type": "object", "additionalProperties": true},
		"trend_data":      map[string]any{"type": "object", "additionalProperties": true},
		"mandate":         map[string]any{"type": "object", "additionalProperties": true},
		"spec":            map[string]any{"type": "object", "additionalProperties": true},
		"business_brief":  map[string]any{"type": "object", "additionalProperties": true},
		"scoring_payload": map[string]any{"type": "object", "additionalProperties": true},
		"dimensions":      map[string]any{"type": "object", "additionalProperties": true},
		"template_diff":   map[string]any{"type": "object", "additionalProperties": true},
		"issues":          map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
		"items":           map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
		"candidates":      map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
		"events":          map[string]any{"type": "array", "items": map[string]any{"type": "object"}},
	}
	required := []string{}

	switch eventType {
	case "dedup.resolved":
		props["dedup_event_id"] = map[string]any{"type": "string"}
		props["action"] = map[string]any{"type": "string", "enum": []string{"merge", "keep_both"}}
		required = append(required, "dedup_event_id", "action")
	case "synthesis.resolved":
		props["resolution"] = map[string]any{"type": "string"}
		props["rationale"] = map[string]any{"type": "string"}
	case "portfolio.digest_compiled":
		props["message"] = map[string]any{"type": "string"}
		props["digest_text"] = map[string]any{"type": "string"}
		props["trigger_reason"] = map[string]any{"type": "string"}
		props["snapshot"] = map[string]any{"type": "object", "additionalProperties": true}
		required = append(required, "message")
	case "template.version_published":
		props["version"] = map[string]any{"type": "string"}
		required = append(required, "version")
	case "template.migration_planned", "template.migration_completed", "template.migration_failed":
		props["migration_id"] = map[string]any{"type": "string"}
		props["from_version"] = map[string]any{"type": "string"}
		props["to_version"] = map[string]any{"type": "string"}
		props["error"] = map[string]any{"type": "string"}
	case "human_task.requested", "human_task.approved", "human_task.rejected", "human_task.deferred", "human_task.completed", "human_task.expired":
		props["task_id"] = map[string]any{"type": "string"}
		required = append(required, "task_id")
	case "brand.candidates_ready":
		props["candidates"] = map[string]any{"type": "array", "items": map[string]any{"type": "object"}}
		required = append(required, "vertical_id", "candidates")
	}
	if strings.Contains(eventType, ".scan_complete") {
		required = append(required, "scan_id")
	}
	if strings.Contains(eventType, ".scan_assigned") {
		required = append(required, "scan_id")
	}
	if strings.HasPrefix(eventType, "opco.") && !strings.Contains(eventType, "teardown_complete") {
		required = append(required, "vertical_id")
	}
	if strings.HasPrefix(eventType, "spec.") || strings.HasPrefix(eventType, "cto.spec_") {
		required = append(required, "vertical_id")
	}
	if strings.HasPrefix(eventType, "vertical.") {
		required = append(required, "vertical_id")
	}
	if strings.HasPrefix(eventType, "budget.") {
		props["state"] = map[string]any{"type": "string"}
		props["next_event_type"] = map[string]any{"type": "string"}
	}
	required = UniqueNonEmpty(required)

	schema := map[string]any{
		"type":                 "object",
		"properties":           props,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		schema["required"] = required
	}
	return EmitSchema{
		Description: "Emit " + eventType + " event",
		Schema:      schema,
	}
}

func SnapshotEmitSchemas(registry map[string]EmitSchema) map[string]EmitSchema {
	out := make(map[string]EmitSchema, len(registry))
	for eventType, entry := range registry {
		schemaCopy, _ := deepCloneJSONValue(entry.Schema).(map[string]any)
		if schemaCopy == nil {
			schemaCopy = map[string]any{}
		}
		out[eventType] = EmitSchema{Description: entry.Description, Schema: schemaCopy}
	}
	return out
}

func UniqueNonEmpty(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}
