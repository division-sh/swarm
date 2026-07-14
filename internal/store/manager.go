package store

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

type persistedAgentRuntimeDescriptor struct {
	Type                 string                         `json:"type,omitempty"`
	FlowID               string                         `json:"flow_id,omitempty"`
	Model                string                         `json:"model,omitempty"`
	ResolvedModel        string                         `json:"resolved_model,omitempty"`
	ResolvedLLMProvider  string                         `json:"resolved_llm_provider,omitempty"`
	ResolvedLLMTransport string                         `json:"resolved_llm_transport,omitempty"`
	MaxTurnsPerTask      int                            `json:"max_turns_per_task,omitempty"`
	NativeTools          runtimeactors.NativeToolConfig `json:"native_tools,omitempty"`
	WorkspaceClass       string                         `json:"workspace_class,omitempty"`
	ManagerFallback      string                         `json:"manager_fallback,omitempty"`
}

type persistedAgentProjection struct {
	AgentID           string
	FlowInstance      string
	Role              string
	Model             string
	LLMBackend        string
	MemoryEnabled     bool
	MemorySource      string
	ParentAgentID     string
	EntityID          string
	ConfigJSON        []byte
	RuntimeDescriptor []byte
	SubscriptionsJSON []byte
	EmitEventsJSON    []byte
	ToolsJSON         []byte
	PermissionsJSON   []byte
}

var runtimeConfigKeys = map[string]struct{}{
	"type":                    {},
	"mode":                    {},
	"model":                   {},
	"model_tier":              {},
	"llm_backend":             {},
	"resolved_model":          {},
	"resolved_llm_provider":   {},
	"resolved_llm_transport":  {},
	"conversation_mode":       {},
	"session_scope":           {},
	"session_scope_authority": {},
	"memory":                  {},
	"max_turns_per_task":      {},
	"subscriptions":           {},
	"emit_events":             {},
	"tools":                   {},
	"permissions":             {},
	"native_tools":            {},
	"workspace_class":         {},
	"manager_fallback":        {},
	"flow_path":               {},
	"flow_id":                 {},
	"flow_instance":           {},
}

var retiredAgentMemoryConfigKeys = map[string]struct{}{
	"mode":                    {},
	"conversation_mode":       {},
	"session_scope":           {},
	"session_scope_authority": {},
}

var persistedAgentRuntimeDescriptorKeys = map[string]struct{}{
	"type":                   {},
	"flow_id":                {},
	"model":                  {},
	"resolved_model":         {},
	"resolved_llm_provider":  {},
	"resolved_llm_transport": {},
	"max_turns_per_task":     {},
	"native_tools":           {},
	"workspace_class":        {},
	"manager_fallback":       {},
}

func mergeAgentConfigJSON(cfg runtimeactors.AgentConfig) ([]byte, error) {
	return sanitizeOpaqueAgentConfig(cfg.Config)
}

func sanitizeOpaqueAgentConfig(raw json.RawMessage) ([]byte, error) {
	obj := map[string]any{}
	if len(raw) > 0 && json.Valid(raw) {
		_ = json.Unmarshal(raw, &obj)
	}
	retired := make([]string, 0)
	for key := range retiredAgentMemoryConfigKeys {
		if _, exists := obj[key]; exists {
			retired = append(retired, key)
		}
	}
	if constraints, ok := obj["constraints"].(map[string]any); ok {
		for key := range retiredAgentMemoryConfigKeys {
			if _, exists := constraints[key]; exists {
				retired = append(retired, "constraints."+key)
			}
		}
	}
	if len(retired) > 0 {
		sort.Strings(retired)
		return nil, fmt.Errorf("retired agent memory fields are not accepted: %s; use memory", strings.Join(retired, ", "))
	}
	for key := range runtimeConfigKeys {
		delete(obj, key)
	}
	if constraints, ok := obj["constraints"].(map[string]any); ok {
		delete(constraints, "conversation_mode")
		delete(constraints, "session_scope")
		delete(constraints, "session_scope_authority")
		delete(constraints, "memory")
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

func projectPersistedAgentConfig(cfg runtimeactors.AgentConfig, parentAgentID string) (persistedAgentProjection, error) {
	cfg.NormalizeEntityID()
	cfg.NormalizeRuntimeDescriptor()
	modelAlias, err := agentModel(cfg)
	if err != nil {
		return persistedAgentProjection{}, err
	}
	memory, err := cfg.Memory.Normalize()
	if err != nil {
		return persistedAgentProjection{}, fmt.Errorf("invalid memory plan: %w", err)
	}
	if err := agentmemory.ValidateFlowOwnership(memory, cfg.FlowPath); err != nil {
		return persistedAgentProjection{}, err
	}
	llmBackend, err := agentLLMBackend(cfg)
	if err != nil {
		return persistedAgentProjection{}, fmt.Errorf("invalid llm_backend: %w", err)
	}
	configJSON, err := mergeAgentConfigJSON(cfg)
	if err != nil {
		return persistedAgentProjection{}, fmt.Errorf("marshal agent config: %w", err)
	}
	runtimeDescriptorJSON, err := marshalPersistedAgentRuntimeDescriptor(cfg, modelAlias)
	if err != nil {
		return persistedAgentProjection{}, fmt.Errorf("marshal agent runtime descriptor: %w", err)
	}
	return persistedAgentProjection{
		AgentID:           strings.TrimSpace(cfg.ID),
		FlowInstance:      agentFlowInstance(cfg),
		Role:              strings.TrimSpace(cfg.Role),
		Model:             modelAlias,
		LLMBackend:        llmBackend,
		MemoryEnabled:     memory.Enabled,
		MemorySource:      string(memory.Source),
		ParentAgentID:     nullable(strings.TrimSpace(parentAgentID), strings.TrimSpace(cfg.ParentAgent)),
		EntityID:          cfg.EffectiveEntityID(),
		ConfigJSON:        configJSON,
		RuntimeDescriptor: runtimeDescriptorJSON,
		SubscriptionsJSON: mustJSONBytes(cfg.Subscriptions, "[]"),
		EmitEventsJSON:    mustJSONBytes(cfg.EmitEvents, "[]"),
		ToolsJSON:         mustJSONBytes(cfg.Tools, "[]"),
		PermissionsJSON:   mustJSONBytes(cfg.Permissions, "[]"),
	}, nil
}

func hydratePersistedAgentConfig(row persistedAgentProjection) (runtimeactors.AgentConfig, error) {
	if strings.TrimSpace(row.AgentID) == "" {
		return runtimeactors.AgentConfig{}, fmt.Errorf("agent row missing agent_id")
	}
	if strings.TrimSpace(row.Role) == "" {
		return runtimeactors.AgentConfig{}, fmt.Errorf("agent %s missing role", strings.TrimSpace(row.AgentID))
	}
	modelAlias := strings.TrimSpace(row.Model)
	if modelAlias == "" {
		return runtimeactors.AgentConfig{}, fmt.Errorf("agent %s missing model", strings.TrimSpace(row.AgentID))
	}
	llmBackend := strings.TrimSpace(row.LLMBackend)
	if llmBackend == "" {
		return runtimeactors.AgentConfig{}, fmt.Errorf("agent %s missing llm_backend", strings.TrimSpace(row.AgentID))
	}
	profile, err := llmselection.ResolvePersistedBackend(llmBackend)
	if err != nil {
		return runtimeactors.AgentConfig{}, fmt.Errorf("agent %s invalid llm_backend %q: %w", strings.TrimSpace(row.AgentID), llmBackend, err)
	}
	llmBackend = profile.ID
	memory, err := agentmemory.NewPlan(row.MemoryEnabled, agentmemory.Source(row.MemorySource))
	if err != nil {
		return runtimeactors.AgentConfig{}, fmt.Errorf("agent %s invalid memory plan: %w", strings.TrimSpace(row.AgentID), err)
	}
	if err := validateOpaqueAgentConfig(row.ConfigJSON); err != nil {
		return runtimeactors.AgentConfig{}, fmt.Errorf("agent %s invalid opaque config: %w", strings.TrimSpace(row.AgentID), err)
	}
	desc, err := decodePersistedAgentRuntimeDescriptor(row.RuntimeDescriptor)
	if err != nil {
		return runtimeactors.AgentConfig{}, fmt.Errorf("agent %s invalid runtime_descriptor: %w", strings.TrimSpace(row.AgentID), err)
	}
	cfg := runtimeactors.AgentConfig{
		ID:                   strings.TrimSpace(row.AgentID),
		Type:                 desc.Type,
		Role:                 strings.TrimSpace(row.Role),
		FlowID:               desc.FlowID,
		Model:                modelAlias,
		LLMBackend:           llmBackend,
		ResolvedModel:        strings.TrimSpace(desc.ResolvedModel),
		ResolvedLLMProvider:  strings.TrimSpace(desc.ResolvedLLMProvider),
		ResolvedLLMTransport: strings.TrimSpace(desc.ResolvedLLMTransport),
		Memory:               memory,
		MaxTurnsPerTask:      desc.MaxTurnsPerTask,
		Subscriptions:        decodeJSONStringList(row.SubscriptionsJSON),
		EmitEvents:           decodeJSONStringList(row.EmitEventsJSON),
		Tools:                decodeJSONStringList(row.ToolsJSON),
		Permissions:          decodeJSONStringList(row.PermissionsJSON),
		NativeTools:          desc.NativeTools,
		WorkspaceClass:       desc.WorkspaceClass,
		ManagerFallback:      desc.ManagerFallback,
		FlowPath:             strings.Trim(strings.TrimSpace(row.FlowInstance), "/"),
		EntityID:             strings.TrimSpace(row.EntityID),
		ParentAgent:          strings.TrimSpace(row.ParentAgentID),
		Config:               append(json.RawMessage(nil), row.ConfigJSON...),
	}
	cfg.NormalizeEntityID()
	cfg.NormalizeRuntimeDescriptor()
	if err := agentmemory.ValidateFlowOwnership(cfg.Memory, cfg.FlowPath); err != nil {
		return runtimeactors.AgentConfig{}, fmt.Errorf("agent %s invalid memory plan: %w", strings.TrimSpace(row.AgentID), err)
	}
	return cfg, nil
}

func marshalPersistedAgentRuntimeDescriptor(cfg runtimeactors.AgentConfig, modelAlias string) ([]byte, error) {
	desc := persistedAgentRuntimeDescriptor{
		Type:                 agentPersistedType(cfg, modelAlias),
		FlowID:               strings.TrimSpace(cfg.FlowID),
		Model:                strings.TrimSpace(modelAlias),
		ResolvedModel:        strings.TrimSpace(cfg.ResolvedModel),
		ResolvedLLMProvider:  strings.TrimSpace(cfg.ResolvedLLMProvider),
		ResolvedLLMTransport: strings.TrimSpace(cfg.ResolvedLLMTransport),
		MaxTurnsPerTask:      cfg.MaxTurnsPerTask,
		NativeTools:          cfg.NativeTools,
		WorkspaceClass:       strings.TrimSpace(cfg.WorkspaceClass),
		ManagerFallback:      strings.TrimSpace(cfg.ManagerFallback),
	}
	if !desc.NativeTools.Any() {
		desc.NativeTools = runtimeactors.NativeToolConfig{}
	}
	return json.Marshal(desc)
}

func decodePersistedAgentRuntimeDescriptor(raw []byte) (persistedAgentRuntimeDescriptor, error) {
	obj := map[string]json.RawMessage{}
	if len(raw) == 0 {
		raw = []byte(`{}`)
	}
	if !json.Valid(raw) {
		return persistedAgentRuntimeDescriptor{}, fmt.Errorf("runtime_descriptor must be valid json")
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return persistedAgentRuntimeDescriptor{}, fmt.Errorf("decode runtime_descriptor: %w", err)
	}
	if unknown := invalidPersistedAgentRuntimeDescriptorKeys(obj); len(unknown) > 0 {
		return persistedAgentRuntimeDescriptor{}, fmt.Errorf("runtime_descriptor contains unsupported keys: %s", strings.Join(unknown, ", "))
	}
	var desc persistedAgentRuntimeDescriptor
	if err := json.Unmarshal(raw, &desc); err != nil {
		return persistedAgentRuntimeDescriptor{}, fmt.Errorf("decode runtime_descriptor: %w", err)
	}
	desc.Type = strings.TrimSpace(desc.Type)
	desc.FlowID = strings.TrimSpace(desc.FlowID)
	desc.Model = strings.TrimSpace(desc.Model)
	desc.ResolvedModel = strings.TrimSpace(desc.ResolvedModel)
	desc.ResolvedLLMProvider = strings.TrimSpace(desc.ResolvedLLMProvider)
	desc.ResolvedLLMTransport = strings.TrimSpace(desc.ResolvedLLMTransport)
	desc.WorkspaceClass = strings.TrimSpace(desc.WorkspaceClass)
	desc.ManagerFallback = strings.TrimSpace(desc.ManagerFallback)
	if desc.Type == "" {
		return persistedAgentRuntimeDescriptor{}, fmt.Errorf("missing type")
	}
	return desc, nil
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

func validateOpaqueAgentConfig(raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	if !json.Valid(raw) {
		return fmt.Errorf("config must be valid json")
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return fmt.Errorf("decode config: %w", err)
	}
	conflicts := make([]string, 0)
	for key := range runtimeConfigKeys {
		if _, ok := obj[key]; ok {
			conflicts = append(conflicts, key)
		}
	}
	if constraints, ok := obj["constraints"].(map[string]any); ok {
		for _, key := range []string{"conversation_mode", "session_scope", "session_scope_authority", "memory", "max_turns_per_task"} {
			if _, exists := constraints[key]; exists {
				conflicts = append(conflicts, "constraints."+key)
			}
		}
	}
	if len(conflicts) == 0 {
		return nil
	}
	sort.Strings(conflicts)
	return fmt.Errorf("config contains runtime-owned keys: %s", strings.Join(conflicts, ", "))
}

func invalidPersistedAgentRuntimeDescriptorKeys(obj map[string]json.RawMessage) []string {
	if len(obj) == 0 {
		return nil
	}
	unknown := make([]string, 0)
	for key := range obj {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, ok := persistedAgentRuntimeDescriptorKeys[key]; !ok {
			unknown = append(unknown, key)
		}
	}
	sort.Strings(unknown)
	return unknown
}

func mustJSONBytes(v any, fallback string) []byte {
	b, err := json.Marshal(v)
	if err != nil || len(b) == 0 {
		return []byte(fallback)
	}
	return b
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
