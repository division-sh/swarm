package tools

import (
	"net/http"
	"strings"
	"time"

	llm "empireai/internal/runtime/llm"
)

type ToolInputValidator struct {
	definitions func() ([]llm.ToolDefinition, error)
}

func NewToolInputValidator(definitions func() ([]llm.ToolDefinition, error)) *ToolInputValidator {
	return &ToolInputValidator{definitions: definitions}
}

func (v *ToolInputValidator) Validate(name string, input any) error {
	name = strings.TrimSpace(name)
	if name == "" || strings.HasPrefix(name, "emit_") {
		return nil
	}
	input = validatorNormalizeRuntimeToolInput(name, input)
	payload := map[string]any{}
	if err := decodeToolInput(input, &payload); err != nil {
		return err
	}
	if payload == nil {
		payload = map[string]any{}
	}

	defs, defsErr := v.definitions()
	if defsErr != nil {
		return defsErr
	}

	contractSchema, foundContract := validatorToolSchemaForName(defs, name)
	if foundContract && contractSchema != nil {
		return ValidatePayloadAgainstSchema(contractSchema, validatorPruneSchemaUnknownKeys(payload, contractSchema))
	}
	return nil
}

func validatorToolSchemaForName(defs []llm.ToolDefinition, name string) (map[string]any, bool) {
	name = strings.TrimSpace(name)
	for _, def := range defs {
		if strings.TrimSpace(def.Name) != name {
			continue
		}
		schema, ok := def.Schema.(map[string]any)
		return schema, ok
	}
	return nil, false
}

func validatorPruneSchemaUnknownKeys(payload map[string]any, schema map[string]any) map[string]any {
	if payload == nil {
		return map[string]any{}
	}
	props := schemaProperties(schema["properties"])
	if len(props) == 0 {
		return payload
	}
	out := make(map[string]any, len(payload))
	for key, value := range payload {
		if _, ok := props[key]; ok {
			out[key] = value
		}
	}
	return out
}

func validatorNormalizeRuntimeToolInput(name string, input any) any {
	if strings.TrimSpace(name) == "" || strings.HasPrefix(strings.TrimSpace(name), "emit_") {
		return input
	}
	var payload map[string]any
	if err := decodeToolInput(input, &payload); err != nil || payload == nil {
		return input
	}

	switch name {
	case "agent_message":
		if strings.TrimSpace(asString(payload["to"])) == "" {
			if target := strings.TrimSpace(asString(payload["target_agent_id"])); target != "" {
				payload["to"] = target
			}
		}
		if strings.TrimSpace(asString(payload["target_agent_id"])) == "" {
			if to := strings.TrimSpace(asString(payload["to"])); to != "" {
				payload["target_agent_id"] = to
			}
		}
		if strings.TrimSpace(asString(payload["message"])) == "" {
			if data, ok := payload["payload"].(map[string]any); ok {
				if msg := strings.TrimSpace(asString(data["message"])); msg != "" {
					payload["message"] = msg
				}
			}
			if strings.TrimSpace(asString(payload["message"])) == "" {
				payload["message"] = "runtime_tool"
			}
		}
	case "schedule":
		if strings.TrimSpace(asString(payload["action"])) == "" {
			if eventType := strings.TrimSpace(asString(payload["event_type"])); eventType != "" {
				payload["action"] = eventType
			}
		}
		if strings.TrimSpace(asString(payload["event_type"])) == "" {
			if action := strings.TrimSpace(asString(payload["action"])); action != "" {
				payload["event_type"] = action
			}
		}
		if asInt(payload["delay_seconds"]) <= 0 {
			if at := strings.TrimSpace(asString(payload["at"])); at != "" {
				if parsed, err := time.Parse(time.RFC3339, at); err == nil {
					delay := int(time.Until(parsed).Seconds())
					if delay < 0 {
						delay = 0
					}
					payload["delay_seconds"] = delay
				}
			}
		}
		if payload["payload"] == nil && payload["context"] != nil {
			payload["payload"] = payload["context"]
		}
		if strings.TrimSpace(asString(payload["at"])) == "" {
			if rawDelay, ok := payload["delay_seconds"]; ok {
				delaySeconds := asInt(rawDelay)
				if delaySeconds < 0 {
					delaySeconds = 0
				}
				payload["mode"] = "once"
				payload["at"] = time.Now().Add(time.Duration(delaySeconds) * time.Second).UTC().Format(time.RFC3339)
			}
		}
	case "configure_routing":
		if strings.TrimSpace(asString(payload["operation"])) == "" {
			switch strings.ToLower(strings.TrimSpace(asString(payload["status"]))) {
			case "deactivated":
				payload["operation"] = "remove"
			default:
				payload["operation"] = "add"
			}
		}
		if strings.TrimSpace(asString(payload["event_type"])) == "" {
			if pattern := strings.TrimSpace(asString(payload["event_pattern"])); pattern != "" {
				payload["event_type"] = pattern
			}
		}
		if strings.TrimSpace(asString(payload["event_pattern"])) == "" {
			if eventType := strings.TrimSpace(asString(payload["event_type"])); eventType != "" {
				payload["event_pattern"] = eventType
			}
		}
		if strings.TrimSpace(asString(payload["status"])) == "" {
			switch strings.ToLower(strings.TrimSpace(asString(payload["operation"]))) {
			case "remove":
				payload["status"] = "deactivated"
			case "add", "modify":
				payload["status"] = "active"
			}
		}
	case "agent_hire":
		if strings.TrimSpace(asString(payload["agent_id"])) == "" {
			if config, ok := payload["config"].(map[string]any); ok {
				payload["agent_id"] = strings.TrimSpace(asString(config["id"]))
			}
		}
		if strings.TrimSpace(asString(payload["role"])) == "" {
			if config, ok := payload["config"].(map[string]any); ok {
				payload["role"] = strings.TrimSpace(asString(config["role"]))
			}
		}
		if payload["config"] == nil {
			config := map[string]any{
				"id":   strings.TrimSpace(asString(payload["agent_id"])),
				"role": strings.TrimSpace(asString(payload["role"])),
			}
			if mode := strings.TrimSpace(asString(payload["mode"])); mode != "" {
				config["mode"] = mode
			}
			if entityID := strings.TrimSpace(asString(payload["entity_id"])); entityID != "" {
				config["entity_id"] = entityID
			}
			rawConfig := map[string]any{}
			if modelTier := strings.TrimSpace(asString(payload["model_tier"])); modelTier != "" {
				rawConfig["model_tier"] = modelTier
			}
			if systemPrompt := strings.TrimSpace(asString(payload["system_prompt"])); systemPrompt != "" {
				rawConfig["system_prompt"] = systemPrompt
			}
			if len(rawConfig) > 0 {
				config["config"] = rawConfig
			}
			payload["config"] = config
		}
	case "agent_fire":
		if strings.TrimSpace(asString(payload["reason"])) == "" {
			payload["reason"] = "runtime_tool"
		}
	case "agent_reconfigure":
		if payload["config"] == nil {
			config := map[string]any{}
			if modelTier := strings.TrimSpace(asString(payload["model_tier"])); modelTier != "" {
				config["model_tier"] = modelTier
			}
			if systemPrompt := strings.TrimSpace(asString(payload["system_prompt"])); systemPrompt != "" {
				config["system_prompt"] = systemPrompt
			}
			if maxTurns := asInt(payload["max_turns_per_task"]); maxTurns > 0 {
				config["max_turns_per_task"] = maxTurns
			}
			payload["config"] = config
		}
	case "mailbox_send":
		if mailboxType, err := NormalizeMailboxType(asString(payload["type"])); err == nil && mailboxType != "" {
			payload["type"] = mailboxType
		}
		if priority, err := NormalizeMailboxPriority(asString(payload["priority"])); err == nil && priority != "" {
			payload["priority"] = priority
		}
		if strings.TrimSpace(asString(payload["subject"])) == "" {
			if summary := strings.TrimSpace(asString(payload["summary"])); summary != "" {
				payload["subject"] = summary
			}
		}
		if payload["payload"] == nil && payload["context"] != nil {
			payload["payload"] = payload["context"]
		}
		if strings.TrimSpace(asString(payload["summary"])) == "" {
			if subject := strings.TrimSpace(asString(payload["subject"])); subject != "" {
				payload["summary"] = subject
			}
		}
		if payload["context"] == nil && payload["payload"] != nil {
			payload["context"] = payload["payload"]
		}
	case "human_task_request":
		if entityID := strings.TrimSpace(asString(payload["entity_id"])); entityID != "" {
			payload["entity_id"] = entityID
		}
		if strings.TrimSpace(asString(payload["deadline"])) == "" &&
			strings.TrimSpace(asString(payload["deadline_at"])) == "" &&
			strings.TrimSpace(asString(payload["deadline_rfc3339"])) == "" {
			if hours := asInt(payload["deadline_hours"]); hours > 0 {
				payload["deadline_at"] = time.Now().Add(time.Duration(hours) * time.Hour).UTC().Format(time.RFC3339)
			}
		}
	case "human_task_decide":
		switch strings.ToLower(strings.TrimSpace(asString(payload["decision"]))) {
		case "approve":
			payload["decision"] = "approved"
		case "reject":
			payload["decision"] = "rejected"
		case "defer":
			payload["decision"] = "deferred"
		}
	case "systemd_control":
		if strings.TrimSpace(asString(payload["service"])) == "" {
			if unit := strings.TrimSpace(asString(payload["unit"])); unit != "" {
				payload["service"] = unit
			}
		}
		if strings.TrimSpace(asString(payload["unit"])) == "" {
			if service := strings.TrimSpace(asString(payload["service"])); service != "" {
				payload["unit"] = service
			}
		}
	case "email_api":
		if arr, ok := payload["to"].([]string); ok && len(arr) == 1 {
			payload["to"] = strings.TrimSpace(arr[0])
		}
	case "whatsapp_business_api":
		if body, ok := payload["body"].(map[string]any); ok {
			if strings.TrimSpace(asString(payload["to"])) == "" {
				payload["to"] = strings.TrimSpace(asString(body["to"]))
			}
			if strings.TrimSpace(asString(payload["message"])) == "" {
				payload["message"] = strings.TrimSpace(asString(body["message"]))
			}
		}
		NormalizeExternalContractPayload(payload, http.MethodPost)
	case "instagram_api":
		NormalizeExternalContractPayload(payload, http.MethodPost)
	case "domain_purchase":
		NormalizeExternalContractPayload(payload, http.MethodPost)
	case "domain_availability_check":
		if strings.TrimSpace(asString(payload["domain"])) == "" {
			if query, ok := payload["query"].(map[string]any); ok {
				if domain := strings.TrimSpace(asString(query["domain"])); domain != "" {
					payload["domain"] = domain
				}
			}
		}
		if strings.TrimSpace(asString(payload["method"])) == "" {
			payload["method"] = http.MethodGet
		}
		if payload["query"] == nil && strings.TrimSpace(asString(payload["domain"])) != "" {
			payload["query"] = map[string]any{"domain": strings.TrimSpace(asString(payload["domain"]))}
		}
	case "dns_configure":
		NormalizeExternalContractPayload(payload, http.MethodPost)
	case "whatsapp_name_check":
		if strings.TrimSpace(asString(payload["name"])) == "" {
			if query, ok := payload["query"].(map[string]any); ok {
				if name := strings.TrimSpace(asString(query["name"])); name != "" {
					payload["name"] = name
				}
			}
		}
		NormalizeExternalContractPayload(payload, http.MethodPost)
	}
	return payload
}
