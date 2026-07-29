package contracts

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	managedcredentialmodel "github.com/division-sh/swarm/internal/runtime/managedcredentials/model"
)

type ToolHandlerKind uint8

const (
	ToolHandlerUnspecified ToolHandlerKind = iota
	ToolHandlerPlatformBuiltin
	ToolHandlerHTTP
	ToolHandlerMCP
	ToolHandlerChannel
)

func (k ToolHandlerKind) String() string {
	switch k {
	case ToolHandlerUnspecified:
		return ""
	case ToolHandlerPlatformBuiltin:
		return "platform_builtin"
	case ToolHandlerHTTP:
		return "http"
	case ToolHandlerMCP:
		return "mcp"
	case ToolHandlerChannel:
		return "channel"
	default:
		return ""
	}
}

func ParseToolHandlerKind(raw string) (ToolHandlerKind, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return ToolHandlerUnspecified, nil
	case "platform_builtin":
		return ToolHandlerPlatformBuiltin, nil
	case "http":
		return ToolHandlerHTTP, nil
	case "mcp":
		return ToolHandlerMCP, nil
	case "channel":
		return ToolHandlerChannel, nil
	default:
		return ToolHandlerUnspecified, fmt.Errorf("unsupported handler_type %q", raw)
	}
}

func MustToolHandlerKind(raw string) ToolHandlerKind {
	handler, err := ParseToolHandlerKind(raw)
	if err != nil {
		panic(err)
	}
	return handler
}

type toolSchemaEntryValue struct {
	category             ToolCategory
	description          string
	handler              ToolHandlerKind
	effect               ActivityEffectClass
	permission           ToolPermission
	ratePolicy           ToolRatePolicy
	inputSchema          ToolInputSchema
	outputSchema         ToolInputSchema
	generatedSchema      bool
	http                 ToolHTTPExecution
	hasHTTP              bool
	mcp                  ToolMCPBinding
	hasMCP               bool
	responseMapping      ToolResponseMapping
	hasResponseMapping   bool
	responseSuccess      ToolResponseSuccessPolicy
	hasResponseSuccess   bool
	credentials          []toolCredentialKey
	managedCredential    ToolManagedCredential
	hasManagedCredential bool
	compiledResult       ToolCompiledResultProjection
	hasCompiledResult    bool
}

// ToolSchemaEntry is the immutable admitted tool execution contract. Authored
// syntax and caller-owned maps terminate at its constructors.
type ToolSchemaEntry struct {
	value *toolSchemaEntryValue
}

type toolSchemaEntryDraft struct {
	value toolSchemaEntryValue
}

type ToolSchemaEntryOption interface {
	applyToolSchemaEntry(*toolSchemaEntryDraft) error
}

type toolSchemaEntryOption func(*toolSchemaEntryDraft) error

func (o toolSchemaEntryOption) applyToolSchemaEntry(draft *toolSchemaEntryDraft) error {
	return o(draft)
}

func WithToolCategory(category string) ToolSchemaEntryOption {
	return toolSchemaEntryOption(func(draft *toolSchemaEntryDraft) error {
		admitted, err := ParseToolCategory(category)
		if err != nil {
			return err
		}
		draft.value.category = admitted
		return nil
	})
}

func WithToolDescription(description string) ToolSchemaEntryOption {
	return toolSchemaEntryOption(func(draft *toolSchemaEntryDraft) error {
		if !utf8.ValidString(description) {
			return fmt.Errorf("description is not valid UTF-8")
		}
		draft.value.description = strings.TrimSpace(description)
		return nil
	})
}

func WithToolHandler(handler ToolHandlerKind) ToolSchemaEntryOption {
	return toolSchemaEntryOption(func(draft *toolSchemaEntryDraft) error {
		if handler.String() == "" && handler != ToolHandlerUnspecified {
			return fmt.Errorf("handler kind is invalid")
		}
		draft.value.handler = handler
		return nil
	})
}

func WithToolEffect(effect ActivityEffectClass) ToolSchemaEntryOption {
	return toolSchemaEntryOption(func(draft *toolSchemaEntryDraft) error {
		normalized := NormalizeActivityEffectClass(string(effect))
		if effect != "" && normalized == "" {
			return fmt.Errorf("unsupported effect_class %q", effect)
		}
		draft.value.effect = normalized
		return nil
	})
}

func WithToolPermission(permission string) ToolSchemaEntryOption {
	return toolSchemaEntryOption(func(draft *toolSchemaEntryDraft) error {
		admitted, err := NewToolPermission(permission)
		if err != nil {
			return err
		}
		draft.value.permission = admitted
		return nil
	})
}

func WithToolRateLimit(rateLimit, maxWait string) ToolSchemaEntryOption {
	return toolSchemaEntryOption(func(draft *toolSchemaEntryDraft) error {
		policy, err := NewToolRatePolicy(rateLimit, maxWait)
		if err != nil {
			return err
		}
		draft.value.ratePolicy = policy
		return nil
	})
}

func WithToolGeneratedSchema(generated bool) ToolSchemaEntryOption {
	return toolSchemaEntryOption(func(draft *toolSchemaEntryDraft) error {
		draft.value.generatedSchema = generated
		return nil
	})
}

func WithToolSchemas(input, output ToolInputSchema) ToolSchemaEntryOption {
	return toolSchemaEntryOption(func(draft *toolSchemaEntryDraft) error {
		draft.value.inputSchema = input
		draft.value.outputSchema = output
		return nil
	})
}

func WithToolHTTP(spec HTTPToolSpec) ToolSchemaEntryOption {
	return toolSchemaEntryOption(func(draft *toolSchemaEntryDraft) error {
		admitted, err := compileHTTPToolSpec(spec)
		if err != nil {
			return err
		}
		draft.value.http = ToolHTTPExecution{value: &admitted}
		draft.value.hasHTTP = true
		return nil
	})
}

func WithToolMCP(binding ToolMCPBinding) ToolSchemaEntryOption {
	return toolSchemaEntryOption(func(draft *toolSchemaEntryDraft) error {
		if binding.IsZero() {
			return fmt.Errorf("MCP binding is missing")
		}
		draft.value.mcp = binding
		draft.value.hasMCP = true
		return nil
	})
}

func WithToolResponseMapping(mapping map[string]any) ToolSchemaEntryOption {
	return toolSchemaEntryOption(func(draft *toolSchemaEntryDraft) error {
		value, err := compileToolResponseMapping(mapping)
		if err != nil {
			return err
		}
		draft.value.responseMapping = value
		draft.value.hasResponseMapping = true
		return nil
	})
}

func WithToolResponseSuccess(success HTTPResponseSuccess) ToolSchemaEntryOption {
	return toolSchemaEntryOption(func(draft *toolSchemaEntryDraft) error {
		admitted, err := compileToolResponseSuccess(success)
		if err != nil {
			return err
		}
		draft.value.responseSuccess = admitted
		draft.value.hasResponseSuccess = true
		return nil
	})
}

func WithToolCredentials(credentials ...string) ToolSchemaEntryOption {
	return toolSchemaEntryOption(func(draft *toolSchemaEntryDraft) error {
		admitted, err := admitToolCredentialKeys(credentials)
		if err != nil {
			return err
		}
		draft.value.credentials = admitted
		return nil
	})
}

func WithToolManagedCredential(ref ManagedCredentialRef) ToolSchemaEntryOption {
	return toolSchemaEntryOption(func(draft *toolSchemaEntryDraft) error {
		admitted, err := admitManagedCredential(ref)
		if err != nil {
			return err
		}
		draft.value.managedCredential = admitted
		draft.value.hasManagedCredential = true
		return nil
	})
}

func WithToolCompiledResult(result CompiledResultProjection) ToolSchemaEntryOption {
	return toolSchemaEntryOption(func(draft *toolSchemaEntryDraft) error {
		admitted, err := compileToolResultProjection(result)
		if err != nil {
			return err
		}
		draft.value.compiledResult = ToolCompiledResultProjection{value: &admitted}
		draft.value.hasCompiledResult = true
		return nil
	})
}

func NewToolSchemaEntry(options ...ToolSchemaEntryOption) (ToolSchemaEntry, error) {
	draft := toolSchemaEntryDraft{}
	for _, option := range options {
		if option == nil {
			return ToolSchemaEntry{}, fmt.Errorf("tool option is nil")
		}
		if err := option.applyToolSchemaEntry(&draft); err != nil {
			return ToolSchemaEntry{}, err
		}
	}
	entry := ToolSchemaEntry{value: &draft.value}
	if err := entry.validate(); err != nil {
		return ToolSchemaEntry{}, err
	}
	return entry, nil
}

func MustToolSchemaEntry(options ...ToolSchemaEntryOption) ToolSchemaEntry {
	entry, err := NewToolSchemaEntry(options...)
	if err != nil {
		panic(err)
	}
	return entry
}

func (e ToolSchemaEntry) validate() error {
	if e.value == nil {
		return fmt.Errorf("tool contract is missing")
	}
	if err := e.value.inputSchema.ValidateDefinition(); err != nil {
		return fmt.Errorf("input_schema: %w", err)
	}
	if !e.value.outputSchema.IsZero() {
		if err := e.value.outputSchema.ValidateDefinition(); err != nil {
			return fmt.Errorf("output_schema: %w", err)
		}
	}
	if e.value.category.String() == "" && e.value.category != ToolCategoryUnspecified {
		return fmt.Errorf("tool category is invalid")
	}
	if e.value.handler.String() == "" && e.value.handler != ToolHandlerUnspecified {
		return fmt.Errorf("tool handler is invalid")
	}
	if e.value.ratePolicy.Enabled() && e.value.handler != ToolHandlerHTTP {
		return fmt.Errorf("rate_limit is only supported for handler_type http")
	}
	if e.value.handler == ToolHandlerHTTP && !e.value.hasHTTP {
		return fmt.Errorf("handler_type http requires an http block")
	}
	if e.value.handler != ToolHandlerHTTP && e.value.hasHTTP {
		return fmt.Errorf("http block requires handler_type http")
	}
	if e.value.handler != ToolHandlerMCP && e.value.hasMCP {
		return fmt.Errorf("MCP binding requires handler_type mcp")
	}
	if e.value.hasHTTP && e.value.hasMCP {
		return fmt.Errorf("HTTP and MCP execution bindings are mutually exclusive")
	}
	if e.value.hasCompiledResult {
		if err := e.value.compiledResult.OutputSchema().ValidateDefinition(); err != nil {
			return fmt.Errorf("compiled_result.output_schema: %w", err)
		}
	}
	if len(e.value.credentials) > 0 && e.value.hasManagedCredential {
		return fmt.Errorf("static credentials and managed_credential are mutually exclusive")
	}
	if e.value.hasManagedCredential && e.value.handler != ToolHandlerHTTP {
		return fmt.Errorf("managed_credential is only supported for handler_type http")
	}
	if (e.value.hasResponseMapping || e.value.hasResponseSuccess) && e.value.handler != ToolHandlerHTTP {
		return fmt.Errorf("HTTP response execution semantics require handler_type http")
	}
	return nil
}

func (e ToolSchemaEntry) IsZero() bool { return e.value == nil }
func (e ToolSchemaEntry) Validate() error {
	return e.validate()
}
func (e ToolSchemaEntry) Category() ToolCategory {
	if e.value == nil {
		return ToolCategoryUnspecified
	}
	return e.value.category
}
func (e ToolSchemaEntry) Description() string {
	if e.value == nil {
		return ""
	}
	return e.value.description
}
func (e ToolSchemaEntry) Handler() ToolHandlerKind {
	if e.value == nil {
		return ToolHandlerUnspecified
	}
	return e.value.handler
}
func (e ToolSchemaEntry) Effect() ActivityEffectClass {
	if e.value == nil {
		return ""
	}
	return e.value.effect
}
func (e ToolSchemaEntry) Permission() ToolPermission {
	if e.value == nil {
		return ToolPermission{}
	}
	return e.value.permission
}
func (e ToolSchemaEntry) RatePolicy() ToolRatePolicy {
	if e.value == nil {
		return ToolRatePolicy{}
	}
	return e.value.ratePolicy
}
func (e ToolSchemaEntry) GeneratedSchema() bool {
	return e.value != nil && e.value.generatedSchema
}
func (e ToolSchemaEntry) InputSchema() ToolInputSchema {
	if e.value == nil {
		return ToolInputSchema{}
	}
	return e.value.inputSchema
}
func (e ToolSchemaEntry) OutputSchema() ToolInputSchema {
	if e.value == nil {
		return ToolInputSchema{}
	}
	return e.value.outputSchema
}
func (e ToolSchemaEntry) HTTP() (HTTPToolSpec, bool) {
	if e.value == nil || !e.value.hasHTTP {
		return HTTPToolSpec{}, false
	}
	return e.value.http.syntax(), true
}
func (e ToolSchemaEntry) HTTPExecution() (ToolHTTPExecution, bool) {
	if e.value == nil || !e.value.hasHTTP {
		return ToolHTTPExecution{}, false
	}
	return e.value.http, true
}
func (e ToolSchemaEntry) MCP() (ToolMCPBinding, bool) {
	if e.value == nil || !e.value.hasMCP {
		return ToolMCPBinding{}, false
	}
	return e.value.mcp, true
}
func (e ToolSchemaEntry) ResponseMapping() (map[string]any, bool) {
	if e.value == nil || !e.value.hasResponseMapping {
		return nil, false
	}
	return e.value.responseMapping.syntax(), true
}
func (e ToolSchemaEntry) CompiledResponseMapping() (ToolResponseMapping, bool) {
	if e.value == nil || !e.value.hasResponseMapping {
		return ToolResponseMapping{}, false
	}
	return e.value.responseMapping, true
}
func (e ToolSchemaEntry) ResponseSuccess() (HTTPResponseSuccess, bool) {
	if e.value == nil || !e.value.hasResponseSuccess {
		return HTTPResponseSuccess{}, false
	}
	return e.value.responseSuccess.syntax(), true
}
func (e ToolSchemaEntry) ResponseSuccessPolicy() (ToolResponseSuccessPolicy, bool) {
	if e.value == nil || !e.value.hasResponseSuccess {
		return ToolResponseSuccessPolicy{}, false
	}
	return e.value.responseSuccess, true
}
func (e ToolSchemaEntry) Credentials() []string {
	if e.value == nil {
		return nil
	}
	return toolCredentialKeyStrings(e.value.credentials)
}
func (e ToolSchemaEntry) ManagedCredential() (ManagedCredentialRef, bool) {
	if e.value == nil || !e.value.hasManagedCredential {
		return ManagedCredentialRef{}, false
	}
	return e.value.managedCredential.syntax(), true
}
func (e ToolSchemaEntry) ManagedCredentialExecution() (ToolManagedCredential, bool) {
	if e.value == nil || !e.value.hasManagedCredential {
		return ToolManagedCredential{}, false
	}
	return e.value.managedCredential, true
}
func (e ToolSchemaEntry) CompiledResult() (CompiledResultProjection, bool) {
	if e.value == nil || !e.value.hasCompiledResult {
		return CompiledResultProjection{}, false
	}
	return e.value.compiledResult.syntax(), true
}
func (e ToolSchemaEntry) CompiledResultExecution() (ToolCompiledResultProjection, bool) {
	if e.value == nil || !e.value.hasCompiledResult {
		return ToolCompiledResultProjection{}, false
	}
	return e.value.compiledResult, true
}

func (e ToolSchemaEntry) WithSchemas(input, output ToolInputSchema) (ToolSchemaEntry, error) {
	if e.value == nil {
		return ToolSchemaEntry{}, fmt.Errorf("tool contract is missing")
	}
	copyValue := *e.value
	copyValue.inputSchema = input
	copyValue.outputSchema = output
	out := ToolSchemaEntry{value: &copyValue}
	if err := out.validate(); err != nil {
		return ToolSchemaEntry{}, err
	}
	return out, nil
}

func (e ToolSchemaEntry) WithEffect(effect ActivityEffectClass) (ToolSchemaEntry, error) {
	if e.value == nil {
		return ToolSchemaEntry{}, fmt.Errorf("tool contract is missing")
	}
	normalized := NormalizeActivityEffectClass(string(effect))
	if effect != "" && normalized == "" {
		return ToolSchemaEntry{}, fmt.Errorf("unsupported effect_class %q", effect)
	}
	copyValue := *e.value
	copyValue.effect = normalized
	out := ToolSchemaEntry{value: &copyValue}
	if err := out.validate(); err != nil {
		return ToolSchemaEntry{}, err
	}
	return out, nil
}

func (e ToolSchemaEntry) WithRateLimit(rateLimit, maxWait string) (ToolSchemaEntry, error) {
	if e.value == nil {
		return ToolSchemaEntry{}, fmt.Errorf("tool contract is missing")
	}
	policy, err := NewToolRatePolicy(rateLimit, maxWait)
	if err != nil {
		return ToolSchemaEntry{}, err
	}
	copyValue := *e.value
	copyValue.ratePolicy = policy
	out := ToolSchemaEntry{value: &copyValue}
	if err := out.validate(); err != nil {
		return ToolSchemaEntry{}, err
	}
	return out, nil
}

func (e ToolSchemaEntry) WithHTTP(spec HTTPToolSpec) (ToolSchemaEntry, error) {
	if e.value == nil {
		return ToolSchemaEntry{}, fmt.Errorf("tool contract is missing")
	}
	admitted, err := compileHTTPToolSpec(spec)
	if err != nil {
		return ToolSchemaEntry{}, err
	}
	copyValue := *e.value
	copyValue.http = ToolHTTPExecution{value: &admitted}
	copyValue.hasHTTP = true
	out := ToolSchemaEntry{value: &copyValue}
	if err := out.validate(); err != nil {
		return ToolSchemaEntry{}, err
	}
	return out, nil
}

func (e ToolSchemaEntry) WithResponseMapping(mapping map[string]any) (ToolSchemaEntry, error) {
	if e.value == nil {
		return ToolSchemaEntry{}, fmt.Errorf("tool contract is missing")
	}
	value, err := compileToolResponseMapping(mapping)
	if err != nil {
		return ToolSchemaEntry{}, err
	}
	copyValue := *e.value
	copyValue.responseMapping = value
	copyValue.hasResponseMapping = true
	out := ToolSchemaEntry{value: &copyValue}
	if err := out.validate(); err != nil {
		return ToolSchemaEntry{}, err
	}
	return out, nil
}

func (e ToolSchemaEntry) WithStaticCredentials(credentials ...string) (ToolSchemaEntry, error) {
	if e.value == nil {
		return ToolSchemaEntry{}, fmt.Errorf("tool contract is missing")
	}
	copyValue := *e.value
	admitted, err := admitToolCredentialKeys(credentials)
	if err != nil {
		return ToolSchemaEntry{}, err
	}
	copyValue.credentials = admitted
	copyValue.managedCredential = ToolManagedCredential{}
	copyValue.hasManagedCredential = false
	out := ToolSchemaEntry{value: &copyValue}
	if err := out.validate(); err != nil {
		return ToolSchemaEntry{}, err
	}
	return out, nil
}

func (e ToolSchemaEntry) WithManagedCredential(ref ManagedCredentialRef) (ToolSchemaEntry, error) {
	if e.value == nil {
		return ToolSchemaEntry{}, fmt.Errorf("tool contract is missing")
	}
	admitted, err := admitManagedCredential(ref)
	if err != nil {
		return ToolSchemaEntry{}, err
	}
	copyValue := *e.value
	copyValue.credentials = nil
	copyValue.managedCredential = admitted
	copyValue.hasManagedCredential = true
	out := ToolSchemaEntry{value: &copyValue}
	if err := out.validate(); err != nil {
		return ToolSchemaEntry{}, err
	}
	return out, nil
}

func (e ToolSchemaEntry) WithCompiledResult(result CompiledResultProjection) (ToolSchemaEntry, error) {
	if e.value == nil {
		return ToolSchemaEntry{}, fmt.Errorf("tool contract is missing")
	}
	admitted, err := compileToolResultProjection(result)
	if err != nil {
		return ToolSchemaEntry{}, err
	}
	copyValue := *e.value
	copyValue.compiledResult = ToolCompiledResultProjection{value: &admitted}
	copyValue.hasCompiledResult = true
	out := ToolSchemaEntry{value: &copyValue}
	if err := out.validate(); err != nil {
		return ToolSchemaEntry{}, err
	}
	return out, nil
}

func (e ToolSchemaEntry) CanonicalValue() (map[string]any, error) {
	if err := e.validate(); err != nil {
		return nil, err
	}
	input, err := e.value.inputSchema.Project()
	if err != nil {
		return nil, err
	}
	var output map[string]any
	if !e.value.outputSchema.IsZero() {
		output, err = e.value.outputSchema.Project()
		if err != nil {
			return nil, err
		}
	}
	rateLimit, rateLimitMaxWait := e.value.ratePolicy.Syntax()
	out := map[string]any{
		"category": e.value.category.String(), "description": e.value.description,
		"handler_type": e.value.handler.String(), "effect_class": string(e.value.effect),
		"permission": e.value.permission.String(),
		"rate_limit": rateLimit, "rate_limit_max_wait": rateLimitMaxWait,
		"input_schema": input, "output_schema": output,
		"credentials":      toolCredentialKeyStrings(e.value.credentials),
		"generated_schema": e.value.generatedSchema,
	}
	if e.value.hasHTTP {
		out["http"] = e.value.http.syntax()
	}
	if e.value.hasMCP {
		out["mcp"] = map[string]any{"server": e.value.mcp.Server(), "remote": e.value.mcp.Remote()}
	}
	if e.value.hasResponseMapping {
		out["response_mapping"] = e.value.responseMapping.syntax()
	}
	if e.value.hasResponseSuccess {
		out["response_success"] = e.value.responseSuccess.syntax()
	}
	if e.value.hasManagedCredential {
		out["managed_credential"] = e.value.managedCredential.syntax()
	}
	if e.value.hasCompiledResult {
		out["compiled_result"] = e.value.compiledResult.syntax()
	}
	return out, nil
}

func (e ToolSchemaEntry) CanonicalHash() (string, error) {
	value, err := e.CanonicalValue()
	if err != nil {
		return "", err
	}
	return canonicaljson.Hash(value)
}

func (e ToolSchemaEntry) MarshalYAML() (any, error) {
	if err := e.validate(); err != nil {
		return nil, err
	}
	type authoredToolYAML struct {
		Category          string                `yaml:"category,omitempty"`
		Description       string                `yaml:"description,omitempty"`
		HandlerType       string                `yaml:"handler_type,omitempty"`
		EffectClass       string                `yaml:"effect_class,omitempty"`
		Permission        string                `yaml:"permission,omitempty"`
		RateLimit         string                `yaml:"rate_limit,omitempty"`
		RateLimitMaxWait  string                `yaml:"rate_limit_max_wait,omitempty"`
		InputSchema       ToolInputSchema       `yaml:"input_schema"`
		OutputSchema      ToolInputSchema       `yaml:"output_schema"`
		HTTP              *HTTPToolSpec         `yaml:"http,omitempty"`
		ResponseMapping   map[string]any        `yaml:"response_mapping,omitempty"`
		ResponseSuccess   *HTTPResponseSuccess  `yaml:"response_success,omitempty"`
		Credentials       []string              `yaml:"credentials,omitempty"`
		ManagedCredential *ManagedCredentialRef `yaml:"managed_credential,omitempty"`
	}
	out := authoredToolYAML{
		Category:     e.Category().String(),
		Description:  e.Description(),
		HandlerType:  e.Handler().String(),
		EffectClass:  string(e.Effect()),
		Permission:   e.Permission().String(),
		InputSchema:  e.InputSchema(),
		OutputSchema: e.OutputSchema(),
		Credentials:  e.Credentials(),
	}
	out.RateLimit, out.RateLimitMaxWait = e.RatePolicy().Syntax()
	if value, ok := e.HTTP(); ok {
		out.HTTP = &value
	}
	if value, ok := e.ResponseMapping(); ok {
		out.ResponseMapping = value
	}
	if value, ok := e.ResponseSuccess(); ok {
		out.ResponseSuccess = &value
	}
	if value, ok := e.ManagedCredential(); ok {
		if managedcredentialmodel.TokenRequestProfileEqual(value.TokenRequest, managedcredentialmodel.DefaultTokenRequestProfile()) {
			value.TokenRequest = managedcredentialmodel.TokenRequestProfile{}
		}
		out.ManagedCredential = &value
	}
	return out, nil
}
