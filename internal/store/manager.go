package store

import (
	"encoding/json"
	"regexp"
	"strings"

	"swarm/internal/events"
	runtimeactors "swarm/internal/runtime/core/actors"
)

type persistedAgentRuntimeDescriptor struct {
	Type            string                         `json:"type,omitempty"`
	Mode            string                         `json:"mode,omitempty"`
	MaxTurnsPerTask int                            `json:"max_turns_per_task,omitempty"`
	NativeTools     runtimeactors.NativeToolConfig `json:"native_tools,omitempty"`
	WorkspaceClass  string                         `json:"workspace_class,omitempty"`
	ManagerFallback string                         `json:"manager_fallback,omitempty"`
}

var runtimeConfigKeys = map[string]struct{}{
	"type":               {},
	"mode":               {},
	"model_tier":         {},
	"llm_backend":        {},
	"conversation_mode":  {},
	"max_turns_per_task": {},
	"subscriptions":      {},
	"emit_events":        {},
	"tools":              {},
	"permissions":        {},
	"native_tools":       {},
	"workspace_class":    {},
	"manager_fallback":   {},
	"flow_path":          {},
	"flow_instance":      {},
}

func mergeAgentConfigJSON(cfg runtimeactors.AgentConfig) ([]byte, error) {
	return sanitizeOpaqueAgentConfig(cfg.Config)
}

func sanitizeOpaqueAgentConfig(raw json.RawMessage) ([]byte, error) {
	obj := map[string]any{}
	if len(raw) > 0 && json.Valid(raw) {
		_ = json.Unmarshal(raw, &obj)
	}
	for key := range runtimeConfigKeys {
		delete(obj, key)
	}
	if constraints, ok := obj["constraints"].(map[string]any); ok {
		delete(constraints, "conversation_mode")
		delete(constraints, "max_turns_per_task")
		if len(constraints) == 0 {
			delete(obj, "constraints")
		} else {
			obj["constraints"] = constraints
		}
	}
	if len(obj) == 0 {
		obj = map[string]any{}
	}
	return json.Marshal(obj)
}

func marshalPersistedAgentRuntimeDescriptor(cfg runtimeactors.AgentConfig) ([]byte, error) {
	desc := persistedAgentRuntimeDescriptor{
		Type:            strings.TrimSpace(cfg.Type),
		Mode:            strings.TrimSpace(cfg.Mode),
		MaxTurnsPerTask: cfg.MaxTurnsPerTask,
		NativeTools:     cfg.NativeTools,
		WorkspaceClass:  strings.TrimSpace(cfg.WorkspaceClass),
		ManagerFallback: strings.TrimSpace(cfg.ManagerFallback),
	}
	if !desc.NativeTools.Any() {
		desc.NativeTools = runtimeactors.NativeToolConfig{}
	}
	return json.Marshal(desc)
}

func decodePersistedAgentRuntimeDescriptor(raw []byte) persistedAgentRuntimeDescriptor {
	if len(raw) == 0 || !json.Valid(raw) {
		return persistedAgentRuntimeDescriptor{}
	}
	var desc persistedAgentRuntimeDescriptor
	if err := json.Unmarshal(raw, &desc); err != nil {
		return persistedAgentRuntimeDescriptor{}
	}
	desc.Type = strings.TrimSpace(desc.Type)
	desc.Mode = strings.TrimSpace(desc.Mode)
	desc.WorkspaceClass = strings.TrimSpace(desc.WorkspaceClass)
	desc.ManagerFallback = strings.TrimSpace(desc.ManagerFallback)
	return desc
}

func extractSubscriptions(raw []byte) []string {
	if len(raw) == 0 || !json.Valid(raw) {
		return nil
	}
	var obj struct {
		Subscriptions []string `json:"subscriptions"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil
	}
	return obj.Subscriptions
}

func extractPermissions(raw []byte) []string {
	if len(raw) == 0 || !json.Valid(raw) {
		return nil
	}
	var obj struct {
		Permissions []string `json:"permissions"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil
	}
	return obj.Permissions
}

func extractStringField(raw []byte, key string) string {
	if len(raw) == 0 || !json.Valid(raw) {
		return ""
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return ""
	}
	val, _ := obj[strings.TrimSpace(key)].(string)
	return strings.TrimSpace(val)
}

func extractStringListField(raw []byte, key string) []string {
	if len(raw) == 0 || !json.Valid(raw) {
		return nil
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil
	}
	list, _ := obj[strings.TrimSpace(key)].([]any)
	if len(list) == 0 {
		return nil
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		if v, ok := item.(string); ok {
			v = strings.TrimSpace(v)
			if v != "" {
				out = append(out, v)
			}
		}
	}
	return out
}

func normalizeJSONPayload(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	if json.Valid(raw) {
		var v any
		if err := json.Unmarshal(raw, &v); err == nil {
			v = redactPayloadValue("", v)
			b, err := json.Marshal(v)
			if err == nil {
				return string(b)
			}
		}
		return string(raw)
	}
	b, _ := json.Marshal(map[string]string{"raw": redactText(string(raw))})
	return string(b)
}

func matchesAnySubscription(eventType string, patterns []events.EventType) bool {
	for _, p := range patterns {
		if subscriptionMatch(string(p), eventType) {
			return true
		}
	}
	return false
}

func subscriptionMatch(pattern, eventType string) bool {
	switch {
	case pattern == "", pattern == "*":
		return true
	case strings.HasSuffix(pattern, "*"):
		return strings.HasPrefix(eventType, strings.TrimSuffix(pattern, "*"))
	default:
		return pattern == eventType
	}
}

func nullable(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func sanitizeSchemaIdent(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func quoteIdent(v string) string {
	return `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
}

var (
	emailRegex = regexp.MustCompile(`(?i)\b[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}\b`)
	// Match likely phone formats while avoiding ISO timestamps (e.g. 2026-02-21T02:47:05Z).
	phoneRegex      = regexp.MustCompile(`(?:\+\d[\d\s().-]{7,}\d|\b\d{3}[-.\s]\d{3}[-.\s]\d{4}\b|\(\d{3}\)\s*\d{3}[-.\s]\d{4}\b)`)
	paymentRefRegex = regexp.MustCompile(`\b(?:pi|pm|ch|cs|txn|tx|tr|pay)_[a-zA-Z0-9]{6,}\b`)
)

func redactPayloadValue(key string, v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, vv := range t {
			out[k] = redactPayloadValue(strings.ToLower(strings.TrimSpace(k)), vv)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i := range t {
			out[i] = redactPayloadValue(key, t[i])
		}
		return out
	case string:
		if isNameKey(key) {
			return redactName(t)
		}
		if isPaymentKey(key) && strings.TrimSpace(t) != "" {
			return "[PAYMENT_REF]"
		}
		return redactText(t)
	default:
		return v
	}
}

func redactText(s string) string {
	s = strings.ToValidUTF8(s, "\uFFFD")
	s = emailRegex.ReplaceAllString(s, "[EMAIL]")
	s = phoneRegex.ReplaceAllString(s, "[PHONE]")
	s = paymentRefRegex.ReplaceAllString(s, "[PAYMENT_REF]")
	return strings.ToValidUTF8(s, "\uFFFD")
}

func redactName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return name
	}
	runes := []rune(name)
	if len(runes) == 0 {
		return name
	}
	return strings.ToUpper(string(runes[0])) + "."
}

func isNameKey(k string) bool {
	switch k {
	case "name", "full_name", "customer_name", "first_name", "last_name":
		return true
	default:
		return false
	}
}

func isPaymentKey(k string) bool {
	k = strings.ToLower(strings.TrimSpace(k))
	if k == "" {
		return false
	}
	for _, needle := range []string{
		"payment", "transaction", "charge", "invoice", "billing", "checkout",
		"payment_ref", "payment_reference", "payment_id", "transaction_id",
	} {
		if strings.Contains(k, needle) {
			return true
		}
	}
	return false
}
