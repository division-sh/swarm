package contracts

import (
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	managedcredentialmodel "github.com/division-sh/swarm/internal/runtime/managedcredentials/model"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
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
	category             string
	description          string
	handler              ToolHandlerKind
	effect               ActivityEffectClass
	permission           string
	requiredPermission   string
	rateLimit            string
	rateLimitMaxWait     string
	inputSchema          ToolInputSchema
	outputSchema         ToolInputSchema
	http                 HTTPToolSpec
	hasHTTP              bool
	responseMapping      semanticvalue.Value
	hasResponseMapping   bool
	responseSuccess      HTTPResponseSuccess
	hasResponseSuccess   bool
	credentials          []string
	managedCredential    ManagedCredentialRef
	hasManagedCredential bool
	compiledResult       compiledResultProjectionValue
	hasCompiledResult    bool
}

type compiledResultProjectionValue struct {
	fields       map[string]CompiledResultField
	outputSchema ToolInputSchema
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
		category = strings.TrimSpace(category)
		if !utf8.ValidString(category) {
			return fmt.Errorf("category is not valid UTF-8")
		}
		draft.value.category = category
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

func WithToolPermissions(permission, required string) ToolSchemaEntryOption {
	return toolSchemaEntryOption(func(draft *toolSchemaEntryDraft) error {
		draft.value.permission = strings.TrimSpace(permission)
		draft.value.requiredPermission = strings.TrimSpace(required)
		return nil
	})
}

func WithToolRateLimit(rateLimit, maxWait string) ToolSchemaEntryOption {
	return toolSchemaEntryOption(func(draft *toolSchemaEntryDraft) error {
		draft.value.rateLimit = strings.TrimSpace(rateLimit)
		draft.value.rateLimitMaxWait = strings.TrimSpace(maxWait)
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
		admitted, err := admitHTTPToolSpec(spec)
		if err != nil {
			return err
		}
		draft.value.http = admitted
		draft.value.hasHTTP = true
		return nil
	})
}

func WithToolResponseMapping(mapping map[string]any) ToolSchemaEntryOption {
	return toolSchemaEntryOption(func(draft *toolSchemaEntryDraft) error {
		value, err := canonicaljson.FromGo(mapping)
		if err != nil {
			return fmt.Errorf("response_mapping: %w", err)
		}
		if value.Kind() != semanticvalue.KindObject {
			return fmt.Errorf("response_mapping must be an object")
		}
		draft.value.responseMapping = value
		draft.value.hasResponseMapping = true
		return nil
	})
}

func WithToolResponseSuccess(success HTTPResponseSuccess) ToolSchemaEntryOption {
	return toolSchemaEntryOption(func(draft *toolSchemaEntryDraft) error {
		admitted, err := admitHTTPResponseSuccess(success)
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
		draft.value.credentials = normalizeStrings(credentials)
		return nil
	})
}

func WithToolManagedCredential(ref ManagedCredentialRef) ToolSchemaEntryOption {
	return toolSchemaEntryOption(func(draft *toolSchemaEntryDraft) error {
		admitted, err := admitManagedCredentialRef(ref)
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
		admitted, err := admitCompiledResultProjection(result)
		if err != nil {
			return err
		}
		draft.value.compiledResult = admitted
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
	if err := e.value.outputSchema.ValidateDefinition(); err != nil {
		return fmt.Errorf("output_schema: %w", err)
	}
	if e.value.handler == ToolHandlerHTTP && !e.value.hasHTTP {
		return fmt.Errorf("handler_type http requires an http block")
	}
	if e.value.handler != ToolHandlerHTTP && e.value.hasHTTP {
		return fmt.Errorf("http block requires handler_type http")
	}
	if e.value.hasCompiledResult {
		if err := e.value.compiledResult.outputSchema.ValidateDefinition(); err != nil {
			return fmt.Errorf("compiled_result.output_schema: %w", err)
		}
	}
	if len(e.value.credentials) > 0 && e.value.hasManagedCredential {
		return fmt.Errorf("static credentials and managed_credential are mutually exclusive")
	}
	return nil
}

func (e ToolSchemaEntry) IsZero() bool { return e.value == nil }
func (e ToolSchemaEntry) Validate() error {
	return e.validate()
}
func (e ToolSchemaEntry) Category() string {
	if e.value == nil {
		return ""
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
func (e ToolSchemaEntry) Permission() string {
	if e.value == nil {
		return ""
	}
	return e.value.permission
}
func (e ToolSchemaEntry) RequiredPermission() string {
	if e.value == nil {
		return ""
	}
	return e.value.requiredPermission
}
func (e ToolSchemaEntry) RateLimitSyntax() (string, string) {
	if e.value == nil {
		return "", ""
	}
	return e.value.rateLimit, e.value.rateLimitMaxWait
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
	return snapshotHTTPToolSpec(e.value.http), true
}
func (e ToolSchemaEntry) ResponseMapping() (map[string]any, bool) {
	if e.value == nil || !e.value.hasResponseMapping {
		return nil, false
	}
	mapping, _ := e.value.responseMapping.Interface().(map[string]any)
	return mapping, true
}
func (e ToolSchemaEntry) ResponseSuccess() (HTTPResponseSuccess, bool) {
	if e.value == nil || !e.value.hasResponseSuccess {
		return HTTPResponseSuccess{}, false
	}
	return snapshotHTTPResponseSuccess(e.value.responseSuccess), true
}
func (e ToolSchemaEntry) Credentials() []string {
	if e.value == nil {
		return nil
	}
	return append([]string(nil), e.value.credentials...)
}
func (e ToolSchemaEntry) ManagedCredential() (ManagedCredentialRef, bool) {
	if e.value == nil || !e.value.hasManagedCredential {
		return ManagedCredentialRef{}, false
	}
	return snapshotManagedCredentialRef(e.value.managedCredential), true
}
func (e ToolSchemaEntry) CompiledResult() (CompiledResultProjection, bool) {
	if e.value == nil || !e.value.hasCompiledResult {
		return CompiledResultProjection{}, false
	}
	return snapshotCompiledResultProjection(e.value.compiledResult), true
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
	copyValue := *e.value
	copyValue.rateLimit = strings.TrimSpace(rateLimit)
	copyValue.rateLimitMaxWait = strings.TrimSpace(maxWait)
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
	admitted, err := admitHTTPToolSpec(spec)
	if err != nil {
		return ToolSchemaEntry{}, err
	}
	copyValue := *e.value
	copyValue.http = admitted
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
	value, err := canonicaljson.FromGo(mapping)
	if err != nil {
		return ToolSchemaEntry{}, fmt.Errorf("response_mapping: %w", err)
	}
	if value.Kind() != semanticvalue.KindObject {
		return ToolSchemaEntry{}, fmt.Errorf("response_mapping must be an object")
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
	copyValue.credentials = normalizeStrings(credentials)
	copyValue.managedCredential = ManagedCredentialRef{}
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
	admitted, err := admitManagedCredentialRef(ref)
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
	admitted, err := admitCompiledResultProjection(result)
	if err != nil {
		return ToolSchemaEntry{}, err
	}
	copyValue := *e.value
	copyValue.compiledResult = admitted
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
	output, err := e.value.outputSchema.Project()
	if err != nil {
		return nil, err
	}
	out := map[string]any{
		"category": e.value.category, "description": e.value.description,
		"handler_type": e.value.handler.String(), "effect_class": string(e.value.effect),
		"permission": e.value.permission, "required_permission": e.value.requiredPermission,
		"rate_limit": e.value.rateLimit, "rate_limit_max_wait": e.value.rateLimitMaxWait,
		"input_schema": input, "output_schema": output,
		"credentials": append([]string(nil), e.value.credentials...),
	}
	if e.value.hasHTTP {
		out["http"] = snapshotHTTPToolSpec(e.value.http)
	}
	if e.value.hasResponseMapping {
		out["response_mapping"] = e.value.responseMapping.Interface()
	}
	if e.value.hasResponseSuccess {
		out["response_success"] = snapshotHTTPResponseSuccess(e.value.responseSuccess)
	}
	if e.value.hasManagedCredential {
		out["managed_credential"] = snapshotManagedCredentialRef(e.value.managedCredential)
	}
	if e.value.hasCompiledResult {
		out["compiled_result"] = snapshotCompiledResultProjection(e.value.compiledResult)
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
		Category           string                `yaml:"category,omitempty"`
		Description        string                `yaml:"description,omitempty"`
		HandlerType        string                `yaml:"handler_type,omitempty"`
		EffectClass        string                `yaml:"effect_class,omitempty"`
		Permission         string                `yaml:"permission,omitempty"`
		RequiredPermission string                `yaml:"required_permission,omitempty"`
		RateLimit          string                `yaml:"rate_limit,omitempty"`
		RateLimitMaxWait   string                `yaml:"rate_limit_max_wait,omitempty"`
		InputSchema        ToolInputSchema       `yaml:"input_schema"`
		OutputSchema       ToolInputSchema       `yaml:"output_schema"`
		HTTP               *HTTPToolSpec         `yaml:"http,omitempty"`
		ResponseMapping    map[string]any        `yaml:"response_mapping,omitempty"`
		ResponseSuccess    *HTTPResponseSuccess  `yaml:"response_success,omitempty"`
		Credentials        []string              `yaml:"credentials,omitempty"`
		ManagedCredential  *ManagedCredentialRef `yaml:"managed_credential,omitempty"`
	}
	out := authoredToolYAML{
		Category:           e.Category(),
		Description:        e.Description(),
		HandlerType:        e.Handler().String(),
		EffectClass:        string(e.Effect()),
		Permission:         e.Permission(),
		RequiredPermission: e.RequiredPermission(),
		InputSchema:        e.InputSchema(),
		OutputSchema:       e.OutputSchema(),
		Credentials:        e.Credentials(),
	}
	out.RateLimit, out.RateLimitMaxWait = e.RateLimitSyntax()
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

func admitHTTPToolSpec(spec HTTPToolSpec) (HTTPToolSpec, error) {
	spec.Method = strings.ToUpper(strings.TrimSpace(spec.Method))
	spec.URL = strings.TrimSpace(spec.URL)
	headers := make(map[string]string, len(spec.Headers))
	for key, value := range spec.Headers {
		key = strings.TrimSpace(key)
		if key == "" {
			return HTTPToolSpec{}, fmt.Errorf("http header name is required")
		}
		headers[key] = value
	}
	spec.Headers = headers
	if spec.Body != nil {
		value, err := canonicaljson.FromGo(spec.Body)
		if err != nil {
			return HTTPToolSpec{}, fmt.Errorf("http.body: %w", err)
		}
		spec.Body = value.Interface()
	}
	if spec.TimeoutSeconds < 0 {
		return HTTPToolSpec{}, fmt.Errorf("http.timeout_seconds must be non-negative")
	}
	return spec, nil
}

func snapshotHTTPToolSpec(spec HTTPToolSpec) HTTPToolSpec {
	out := spec
	out.Headers = make(map[string]string, len(spec.Headers))
	for key, value := range spec.Headers {
		out.Headers[key] = value
	}
	if spec.Body != nil {
		value, _ := canonicaljson.FromGo(spec.Body)
		out.Body = value.Interface()
	}
	return out
}

func admitHTTPResponseSuccess(success HTTPResponseSuccess) (HTTPResponseSuccess, error) {
	success.Kind = strings.TrimSpace(success.Kind)
	success.Path = strings.TrimSpace(success.Path)
	if success.Equals != nil {
		value, err := canonicaljson.FromGo(success.Equals)
		if err != nil {
			return HTTPResponseSuccess{}, fmt.Errorf("response_success.equals: %w", err)
		}
		success.Equals = value.Interface()
	}
	return success, nil
}

func snapshotHTTPResponseSuccess(success HTTPResponseSuccess) HTTPResponseSuccess {
	out := success
	if success.Equals != nil {
		value, _ := canonicaljson.FromGo(success.Equals)
		out.Equals = value.Interface()
	}
	return out
}

func admitManagedCredentialRef(ref ManagedCredentialRef) (ManagedCredentialRef, error) {
	ref.Key = strings.TrimSpace(ref.Key)
	ref.Header = strings.TrimSpace(ref.Header)
	ref.Prefix = strings.TrimSpace(ref.Prefix)
	ref.GrantType = strings.TrimSpace(ref.GrantType)
	ref.Scopes = normalizeStrings(ref.Scopes)
	ref.InstallationIDInput = strings.TrimSpace(ref.InstallationIDInput)
	if err := validateManagedCredentialModel(ref); err != nil {
		return ManagedCredentialRef{}, err
	}
	ref.GrantModel = managedcredentialmodel.NormalizeGrantModel(ref.GrantModel)
	ref.TokenRequest = managedcredentialmodel.NormalizeTokenRequestProfile(ref.TokenRequest)
	return snapshotManagedCredentialRef(ref), nil
}

func validateManagedCredentialModel(ref ManagedCredentialRef) error {
	if err := managedcredentialmodel.ValidateGrantModel(ref.GrantModel); err != nil {
		return err
	}
	if err := managedcredentialmodel.ValidateTokenRequestProfile(ref.TokenRequest); err != nil {
		return err
	}
	return nil
}

func snapshotManagedCredentialRef(ref ManagedCredentialRef) ManagedCredentialRef {
	out := ref
	out.Scopes = append([]string(nil), ref.Scopes...)
	out.TokenRequest.StaticHeaders = make(map[string]string, len(ref.TokenRequest.StaticHeaders))
	for key, value := range ref.TokenRequest.StaticHeaders {
		out.TokenRequest.StaticHeaders[key] = value
	}
	return out
}

func admitCompiledResultProjection(result CompiledResultProjection) (compiledResultProjectionValue, error) {
	if err := result.OutputSchema.ValidateDefinition(); err != nil {
		return compiledResultProjectionValue{}, err
	}
	fields := make(map[string]CompiledResultField, len(result.Fields))
	for target, field := range result.Fields {
		target = strings.TrimSpace(target)
		field.From = strings.TrimSpace(field.From)
		if target == "" || field.From == "" {
			return compiledResultProjectionValue{}, fmt.Errorf("compiled result field target and source are required")
		}
		fields[target] = field
	}
	return compiledResultProjectionValue{fields: fields, outputSchema: result.OutputSchema}, nil
}

func snapshotCompiledResultProjection(result compiledResultProjectionValue) CompiledResultProjection {
	fields := make(map[string]CompiledResultField, len(result.fields))
	keys := make([]string, 0, len(result.fields))
	for key := range result.fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fields[key] = result.fields[key]
	}
	return CompiledResultProjection{Fields: fields, OutputSchema: result.outputSchema}
}
