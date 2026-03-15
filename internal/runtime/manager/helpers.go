package manager

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"

	"empireai/internal/events"
	runtimeactors "empireai/internal/runtime/core/actors"
	"github.com/google/uuid"
)

func OrderAgentsByParent(in []PersistedAgent) ([]PersistedAgent, error) {
	if len(in) == 0 {
		return in, nil
	}
	inSet := make(map[string]struct{}, len(in))
	for _, a := range in {
		if id := strings.TrimSpace(a.Config.ID); id != "" {
			inSet[id] = struct{}{}
		}
	}

	done := make(map[string]struct{}, len(in))
	pending := append([]PersistedAgent(nil), in...)
	out := make([]PersistedAgent, 0, len(in))

	for len(pending) > 0 {
		progress := false
		next := pending[:0]
		for _, a := range pending {
			id := strings.TrimSpace(a.Config.ID)
			parent := strings.TrimSpace(a.ParentAgentID)
			if parent == "" {
				parent = strings.TrimSpace(a.Config.ParentAgent)
			}
			if parent == "" {
				out = append(out, a)
				if id != "" {
					done[id] = struct{}{}
				}
				progress = true
				continue
			}
			if _, ok := inSet[parent]; !ok {
				return nil, fmt.Errorf("agent %s references missing parent %s", id, parent)
			}
			if _, ok := done[parent]; ok {
				out = append(out, a)
				if id != "" {
					done[id] = struct{}{}
				}
				progress = true
				continue
			}
			next = append(next, a)
		}
		if !progress {
			ids := make([]string, 0, len(next))
			for _, a := range next {
				ids = append(ids, strings.TrimSpace(a.Config.ID))
			}
			sort.Strings(ids)
			return nil, fmt.Errorf("cyclic parent links: %v", ids)
		}
		pending = next
	}
	return out, nil
}

func RenderMandateText(m runtimeactors.MandateDocument) string {
	entityID := m.EffectiveEntityID()
	obj := map[string]any{"entity_id": entityID}
	if len(m.Metadata) > 0 {
		obj["metadata"] = json.RawMessage(m.Metadata)
	}
	b, _ := json.MarshalIndent(obj, "", "  ")
	return string(b)
}

func DefaultJSON(raw, fallback []byte) []byte {
	if len(raw) == 0 {
		return fallback
	}
	return raw
}

func NormalizeStringList(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		v := strings.TrimSpace(raw)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func RuntimeWarn(component string, format string, args ...any) {
	component = strings.TrimSpace(component)
	if component == "" {
		component = "runtime"
	}
	msg := strings.TrimSpace(fmt.Sprintf(format, args...))
	if msg == "" {
		return
	}
	log.Printf("runtime.warn component=%s message=%s", component, msg)
}

func MustJSON(v any) []byte {
	raw, err := json.Marshal(v)
	if err != nil || len(raw) == 0 {
		return []byte("{}")
	}
	return raw
}

func mustJSON(v any) []byte {
	return MustJSON(v)
}

func FirstNonEmptyString(vals ...string) string {
	for _, val := range vals {
		if trimmed := strings.TrimSpace(val); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func MergeAgentConfig(base, patch runtimeactors.AgentConfig) runtimeactors.AgentConfig {
	out := base
	if patch.ID != "" {
		out.ID = patch.ID
	}
	if patch.Type != "" {
		out.Type = patch.Type
	}
	if patch.Role != "" {
		out.Role = patch.Role
	}
	if patch.Mode != "" {
		out.Mode = patch.Mode
	}
	if patch.LLMBackend != "" {
		out.LLMBackend = patch.LLMBackend
	}
	if patch.EntityID != "" {
		out.EntityID = patch.EntityID
	}
	if patch.ParentAgent != "" {
		out.ParentAgent = patch.ParentAgent
	}
	if len(patch.Subscriptions) > 0 {
		out.Subscriptions = patch.Subscriptions
	}
	if len(patch.Permissions) > 0 {
		out.Permissions = patch.Permissions
	}
	if len(patch.Config) > 0 {
		out.Config = patch.Config
	}
	if patch.BudgetEnvelope != 0 {
		out.BudgetEnvelope = patch.BudgetEnvelope
	}
	out.NormalizeEntityID()
	return out
}

func ExtractSystemPromptFromConfig(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return ""
	}
	v, _ := obj["system_prompt"].(string)
	return strings.TrimSpace(v)
}

func WithSystemPrompt(raw json.RawMessage, prompt string) json.RawMessage {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return raw
	}
	obj := map[string]any{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &obj)
	}
	obj["system_prompt"] = prompt
	b, err := json.Marshal(obj)
	if err != nil || len(b) == 0 {
		return json.RawMessage([]byte("{}"))
	}
	return json.RawMessage(b)
}

func ExpandConfigPromptTemplate(prompt string, raw json.RawMessage) string {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" || len(raw) == 0 {
		return prompt
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil || len(obj) == 0 {
		return prompt
	}
	replacer := make([]string, 0, len(obj)*2)
	for key, value := range obj {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		rendered := stringifyPromptTemplateValue(value)
		replacer = append(replacer,
			"{{"+key+"}}", rendered,
			"{"+key+"}", rendered,
		)
	}
	if len(replacer) == 0 {
		return prompt
	}
	return strings.NewReplacer(replacer...).Replace(prompt)
}

func stringifyPromptTemplateValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case json.RawMessage:
		return strings.TrimSpace(string(typed))
	default:
		if raw, err := json.MarshalIndent(value, "", "  "); err == nil {
			return strings.TrimSpace(string(raw))
		}
		return strings.TrimSpace(fmt.Sprint(value))
	}
}

func DeterministicOutputEventID(inbound events.Event, agentID string, index int, out events.Event) string {
	seed := strings.Join([]string{
		strings.TrimSpace(inbound.ID),
		strings.TrimSpace(agentID),
		fmt.Sprintf("%d", index),
		strings.TrimSpace(string(out.Type)),
		strings.TrimSpace(out.EntityID()),
	}, "|")
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(seed)).String()
}

func ExtractDirectiveText(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	var obj map[string]any
	if err := json.Unmarshal(payload, &obj); err != nil {
		return ""
	}
	text, _ := obj["directive_text"].(string)
	if strings.TrimSpace(text) == "" {
		text, _ = obj["message"].(string)
	}
	if strings.TrimSpace(text) == "" {
		if directive, ok := obj["directive"].(map[string]any); ok && len(directive) > 0 {
			if structured, _ := directive["text"].(string); strings.TrimSpace(structured) != "" {
				text = structured
			} else if encoded, err := json.Marshal(directive); err == nil {
				text = string(encoded)
			}
		}
	}
	return strings.TrimSpace(text)
}
