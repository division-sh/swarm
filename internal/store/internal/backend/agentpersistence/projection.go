package agentpersistence

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
)

type PersistedAgentRuntimeDescriptor struct {
	Type                 string                         `json:"type,omitempty"`
	FlowID               string                         `json:"flow_id,omitempty"`
	Model                string                         `json:"model,omitempty"`
	AuthoredLLMBackend   string                         `json:"authored_llm_backend,omitempty"`
	ResolvedModel        string                         `json:"resolved_model,omitempty"`
	ResolvedLLMProvider  string                         `json:"resolved_llm_provider,omitempty"`
	ResolvedLLMTransport string                         `json:"resolved_llm_transport,omitempty"`
	MaxTurnsPerTask      int                            `json:"max_turns_per_task,omitempty"`
	NativeTools          runtimeactors.NativeToolConfig `json:"native_tools,omitempty"`
	WorkspaceClass       string                         `json:"workspace_class,omitempty"`
	ManagerFallback      string                         `json:"manager_fallback,omitempty"`
	ExecutionMode        runtimeeffects.ExecutionMode   `json:"execution_mode"`
	Mock                 mockperformance.Performance    `json:"mock,omitempty"`
	Intent               runtimeagentintent.Resolved    `json:"intent"`
	Criteria             []string                       `json:"criteria,omitempty"`
	FlowDataAccess       []string                       `json:"flow_data_access,omitempty"`
	BudgetEnvelope       float64                        `json:"budget_envelope,omitempty"`
}

type PersistedAgentProjection struct {
	Identity          runtimeagentidentity.StorageFields
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
	"resolved_llm_backend":    {},
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
	"flow_data_access":        {},
	"budget_envelope":         {},
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
	"authored_llm_backend":   {},
	"resolved_model":         {},
	"resolved_llm_provider":  {},
	"resolved_llm_transport": {},
	"max_turns_per_task":     {},
	"native_tools":           {},
	"workspace_class":        {},
	"manager_fallback":       {},
	"execution_mode":         {},
	"mock":                   {},
	"intent":                 {},
	"criteria":               {},
	"flow_data_access":       {},
	"budget_envelope":        {},
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

func ProjectPersistedAgentConfig(cfg runtimeactors.AgentConfig, parentAgentID string) (PersistedAgentProjection, error) {
	cfg.NormalizeEntityID()
	cfg.NormalizeRuntimeDescriptor()
	identity, err := cfg.ConcreteIdentity()
	if err != nil {
		return PersistedAgentProjection{}, err
	}
	identityFields, err := identity.StorageFields()
	if err != nil {
		return PersistedAgentProjection{}, err
	}
	modelAlias, err := agentModel(cfg)
	if err != nil {
		return PersistedAgentProjection{}, err
	}
	memory, err := cfg.Memory.Normalize()
	if err != nil {
		return PersistedAgentProjection{}, fmt.Errorf("invalid memory plan: %w", err)
	}
	if err := agentmemory.ValidateFlowOwnership(memory, cfg.FlowPath); err != nil {
		return PersistedAgentProjection{}, err
	}
	llmBackend, err := agentLLMBackend(cfg)
	if err != nil {
		return PersistedAgentProjection{}, fmt.Errorf("invalid llm_backend: %w", err)
	}
	if err := runtimeactors.ValidateNoAuthoredSystemPrompt(cfg.Config); err != nil {
		return PersistedAgentProjection{}, err
	}
	if err := cfg.ValidateIntentInputs(); err != nil {
		return PersistedAgentProjection{}, fmt.Errorf("agent %s intent inputs: %w", strings.TrimSpace(cfg.ID), err)
	}
	configJSON, err := mergeAgentConfigJSON(cfg)
	if err != nil {
		return PersistedAgentProjection{}, fmt.Errorf("marshal agent config: %w", err)
	}
	runtimeDescriptorJSON, err := marshalPersistedAgentRuntimeDescriptor(cfg, modelAlias, llmBackend)
	if err != nil {
		return PersistedAgentProjection{}, fmt.Errorf("marshal agent runtime descriptor: %w", err)
	}
	return PersistedAgentProjection{
		Identity:          identityFields,
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

func HydratePersistedAgentConfig(row PersistedAgentProjection) (runtimeactors.AgentConfig, error) {
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
	if !descExecutionModeMatchesBackend(profile.ID, desc.ExecutionMode) {
		return runtimeactors.AgentConfig{}, fmt.Errorf("agent %s execution_mode %q conflicts with llm_backend %q", strings.TrimSpace(row.AgentID), desc.ExecutionMode, llmBackend)
	}
	if profile.ID == llmselection.BackendMock && !desc.Mock.Configured() {
		return runtimeactors.AgentConfig{}, fmt.Errorf("agent %s mock runtime descriptor is missing its performance artifact", strings.TrimSpace(row.AgentID))
	}
	if profile.ID != llmselection.BackendMock && desc.Mock.Configured() {
		return runtimeactors.AgentConfig{}, fmt.Errorf("agent %s live runtime descriptor carries a mock performance artifact", strings.TrimSpace(row.AgentID))
	}
	subscriptions, err := decodePersistedAgentStringList(row.SubscriptionsJSON, "subscriptions")
	if err != nil {
		return runtimeactors.AgentConfig{}, fmt.Errorf("agent %s: %w", strings.TrimSpace(row.AgentID), err)
	}
	emitEvents, err := decodePersistedAgentStringList(row.EmitEventsJSON, "emit_events")
	if err != nil {
		return runtimeactors.AgentConfig{}, fmt.Errorf("agent %s: %w", strings.TrimSpace(row.AgentID), err)
	}
	tools, err := decodePersistedAgentStringList(row.ToolsJSON, "tools")
	if err != nil {
		return runtimeactors.AgentConfig{}, fmt.Errorf("agent %s: %w", strings.TrimSpace(row.AgentID), err)
	}
	permissions, err := decodePersistedAgentStringList(row.PermissionsJSON, "permissions")
	if err != nil {
		return runtimeactors.AgentConfig{}, fmt.Errorf("agent %s: %w", strings.TrimSpace(row.AgentID), err)
	}
	identity, err := runtimeagentidentity.FromStorageFields(row.Identity)
	if err != nil {
		return runtimeactors.AgentConfig{}, fmt.Errorf("agent %s invalid concrete identity: %w", strings.TrimSpace(row.AgentID), err)
	}
	if identity.AgentID() != strings.TrimSpace(row.AgentID) {
		return runtimeactors.AgentConfig{}, fmt.Errorf("agent row identity disagrees with agent_id")
	}
	cfg := runtimeactors.AgentConfig{
		ID:                   strings.TrimSpace(row.AgentID),
		Type:                 desc.Type,
		Role:                 strings.TrimSpace(row.Role),
		FlowID:               desc.FlowID,
		Model:                modelAlias,
		LLMBackend:           desc.AuthoredLLMBackend,
		ResolvedLLMBackend:   llmBackend,
		ResolvedModel:        strings.TrimSpace(desc.ResolvedModel),
		ResolvedLLMProvider:  strings.TrimSpace(desc.ResolvedLLMProvider),
		ResolvedLLMTransport: strings.TrimSpace(desc.ResolvedLLMTransport),
		ExecutionMode:        desc.ExecutionMode,
		Mock:                 desc.Mock,
		Intent:               desc.Intent,
		Memory:               memory,
		MaxTurnsPerTask:      desc.MaxTurnsPerTask,
		Subscriptions:        subscriptions,
		EmitEvents:           emitEvents,
		Tools:                tools,
		Permissions:          permissions,
		Criteria:             append([]string(nil), desc.Criteria...),
		FlowDataAccess:       append([]string(nil), desc.FlowDataAccess...),
		BudgetEnvelope:       desc.BudgetEnvelope,
		NativeTools:          desc.NativeTools,
		WorkspaceClass:       desc.WorkspaceClass,
		ManagerFallback:      desc.ManagerFallback,
		FlowPath:             strings.Trim(strings.TrimSpace(row.FlowInstance), "/"),
		EntityID:             strings.TrimSpace(row.EntityID),
		ParentAgent:          strings.TrimSpace(row.ParentAgentID),
		Config:               append(json.RawMessage(nil), row.ConfigJSON...),
		Identity:             identity,
	}
	cfg.NormalizeEntityID()
	cfg.NormalizeRuntimeDescriptor()
	if err := cfg.ValidateIntentInputs(); err != nil {
		return runtimeactors.AgentConfig{}, fmt.Errorf("agent %s intent carrier: %w", strings.TrimSpace(row.AgentID), err)
	}
	if err := agentmemory.ValidateFlowOwnership(cfg.Memory, cfg.FlowPath); err != nil {
		return runtimeactors.AgentConfig{}, fmt.Errorf("agent %s invalid memory plan: %w", strings.TrimSpace(row.AgentID), err)
	}
	return cfg, nil
}

func marshalPersistedAgentRuntimeDescriptor(cfg runtimeactors.AgentConfig, modelAlias, llmBackend string) ([]byte, error) {
	desc := PersistedAgentRuntimeDescriptor{
		Type:                 strings.TrimSpace(cfg.Type),
		FlowID:               strings.TrimSpace(cfg.FlowID),
		Model:                strings.TrimSpace(modelAlias),
		AuthoredLLMBackend:   strings.TrimSpace(cfg.LLMBackend),
		ResolvedModel:        strings.TrimSpace(cfg.ResolvedModel),
		ResolvedLLMProvider:  strings.TrimSpace(cfg.ResolvedLLMProvider),
		ResolvedLLMTransport: strings.TrimSpace(cfg.ResolvedLLMTransport),
		MaxTurnsPerTask:      cfg.MaxTurnsPerTask,
		NativeTools:          cfg.NativeTools,
		WorkspaceClass:       strings.TrimSpace(cfg.WorkspaceClass),
		ManagerFallback:      strings.TrimSpace(cfg.ManagerFallback),
		ExecutionMode:        cfg.ExecutionMode,
		Mock:                 cfg.Mock,
		Intent:               cfg.Intent,
		Criteria:             append([]string(nil), cfg.Criteria...),
		FlowDataAccess:       append([]string(nil), cfg.FlowDataAccess...),
		BudgetEnvelope:       cfg.BudgetEnvelope,
	}
	if !descExecutionModeMatchesBackend(llmBackend, desc.ExecutionMode) {
		return nil, fmt.Errorf("execution_mode %q conflicts with llm_backend %q", desc.ExecutionMode, strings.TrimSpace(llmBackend))
	}
	if desc.ExecutionMode == runtimeeffects.ExecutionModeMock && !desc.Mock.Configured() {
		return nil, fmt.Errorf("mock runtime descriptor requires a captured performance artifact")
	}
	if desc.ExecutionMode == runtimeeffects.ExecutionModeLive && desc.Mock.Configured() {
		return nil, fmt.Errorf("live runtime descriptor cannot carry a mock performance artifact")
	}
	if !desc.NativeTools.Any() {
		desc.NativeTools = runtimeactors.NativeToolConfig{}
	}
	return json.Marshal(desc)
}

func decodePersistedAgentRuntimeDescriptor(raw []byte) (PersistedAgentRuntimeDescriptor, error) {
	obj := map[string]json.RawMessage{}
	if len(raw) == 0 {
		return PersistedAgentRuntimeDescriptor{}, fmt.Errorf("runtime_descriptor is required")
	}
	if !json.Valid(raw) {
		return PersistedAgentRuntimeDescriptor{}, fmt.Errorf("runtime_descriptor must be valid json")
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return PersistedAgentRuntimeDescriptor{}, fmt.Errorf("decode runtime_descriptor: %w", err)
	}
	if unknown := invalidPersistedAgentRuntimeDescriptorKeys(obj); len(unknown) > 0 {
		return PersistedAgentRuntimeDescriptor{}, fmt.Errorf("runtime_descriptor contains unsupported keys: %s", strings.Join(unknown, ", "))
	}
	var desc PersistedAgentRuntimeDescriptor
	if err := json.Unmarshal(raw, &desc); err != nil {
		return PersistedAgentRuntimeDescriptor{}, fmt.Errorf("decode runtime_descriptor: %w", err)
	}
	desc.Type = strings.TrimSpace(desc.Type)
	desc.FlowID = strings.TrimSpace(desc.FlowID)
	desc.Model = strings.TrimSpace(desc.Model)
	desc.AuthoredLLMBackend = strings.TrimSpace(desc.AuthoredLLMBackend)
	desc.ResolvedModel = strings.TrimSpace(desc.ResolvedModel)
	desc.ResolvedLLMProvider = strings.TrimSpace(desc.ResolvedLLMProvider)
	desc.ResolvedLLMTransport = strings.TrimSpace(desc.ResolvedLLMTransport)
	desc.WorkspaceClass = strings.TrimSpace(desc.WorkspaceClass)
	desc.ManagerFallback = strings.TrimSpace(desc.ManagerFallback)
	desc.ExecutionMode = runtimeeffects.ExecutionMode(strings.TrimSpace(string(desc.ExecutionMode)))
	desc.Mock.Kind = strings.TrimSpace(desc.Mock.Kind)
	desc.Mock.Module = strings.TrimSpace(desc.Mock.Module)
	desc.Mock.Digest = strings.TrimSpace(desc.Mock.Digest)
	desc.Mock.SourcePath = strings.TrimSpace(desc.Mock.SourcePath)
	desc.Mock.Source = append([]byte(nil), desc.Mock.Source...)
	return desc, nil
}

func decodePersistedAgentStringList(raw []byte, field string) ([]string, error) {
	if len(raw) == 0 || !json.Valid(raw) {
		return nil, fmt.Errorf("%s must be a valid json array", field)
	}
	var out []string
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("%s must be a json string array: %w", field, err)
	}
	if out == nil {
		return nil, fmt.Errorf("%s must be a json string array", field)
	}
	return out, nil
}

func descExecutionModeMatchesBackend(backend string, mode runtimeeffects.ExecutionMode) bool {
	backend = strings.TrimSpace(backend)
	if backend == llmselection.BackendMock {
		return mode == runtimeeffects.ExecutionModeMock
	}
	return mode == runtimeeffects.ExecutionModeLive
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
		return fmt.Errorf("config is required")
	}
	if !json.Valid(raw) {
		return fmt.Errorf("config must be valid json")
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		return fmt.Errorf("decode config: %w", err)
	}
	if obj == nil {
		return fmt.Errorf("config must be a json object")
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
	if err != nil || len(b) == 0 || string(b) == "null" {
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
