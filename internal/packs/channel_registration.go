package packs

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

const (
	RegistrationOperationIdentify = "identify"
	RegistrationOperationApply    = "apply"
	RegistrationOperationReadback = "readback"
)

type ChannelRegistrationTarget struct {
	PackageKey string
	FlowID     string
	Provider   string
}

func ParseChannelRegistrationTarget(raw string) (ChannelRegistrationTarget, error) {
	parts := strings.Split(strings.TrimSpace(raw), ":")
	if len(parts) != 4 || parts[0] != "ingress" {
		return ChannelRegistrationTarget{}, fmt.Errorf("registration target must use ingress:<package>:<flow>:<provider>")
	}
	target := ChannelRegistrationTarget{PackageKey: parts[1], FlowID: parts[2], Provider: parts[3]}
	for _, value := range []string{target.PackageKey, target.FlowID, target.Provider} {
		if value == "" || value == "." || value == ".." || strings.ContainsAny(value, "/%?# \t\r\n") {
			return ChannelRegistrationTarget{}, fmt.Errorf("registration target must use ingress:<package>:<flow>:<provider> with valid identity segments")
		}
	}
	return target, nil
}

// CompiledChannelRegistration is the immutable provider-neutral registration
// recipe owned by one compiled channel plan.
type CompiledChannelRegistration struct {
	provider            channelPlanIdentity
	slotNamespace       channelPlanIdentity
	providerCredentials []string
	signingCredential   channelPlanIdentity
	identify            compiledRegistrationOperation
	apply               compiledRegistrationOperation
	readback            compiledRegistrationOperation
}

type compiledRegistrationOperation struct {
	name   string
	toolID channelPlanIdentity
	tool   runtimecontracts.ToolSchemaEntry
	input  []compiledChannelMapping
	output []compiledChannelMapping
}

type RegistrationOperationPlan struct {
	operation compiledRegistrationOperation
}

func (p SatisfactionPlan) Registration() (CompiledChannelRegistration, bool) {
	if p.registration == nil {
		return CompiledChannelRegistration{}, false
	}
	return p.registration.clone(), true
}

func (r CompiledChannelRegistration) Provider() string      { return r.provider.String() }
func (r CompiledChannelRegistration) SlotNamespace() string { return r.slotNamespace.String() }
func (r CompiledChannelRegistration) SigningCredential() string {
	return r.signingCredential.String()
}
func (r CompiledChannelRegistration) ProviderCredentials() []string {
	return append([]string(nil), r.providerCredentials...)
}

func (r CompiledChannelRegistration) Operation(name string) (RegistrationOperationPlan, error) {
	var operation compiledRegistrationOperation
	switch strings.TrimSpace(name) {
	case RegistrationOperationIdentify:
		operation = r.identify
	case RegistrationOperationApply:
		operation = r.apply
	case RegistrationOperationReadback:
		operation = r.readback
	default:
		return RegistrationOperationPlan{}, fmt.Errorf("channel registration operation %q is unsupported", name)
	}
	if operation.toolID.String() == "" {
		return RegistrationOperationPlan{}, fmt.Errorf("channel registration operation %q is missing", name)
	}
	return RegistrationOperationPlan{operation: operation.clone()}, nil
}

func (p RegistrationOperationPlan) Name() string                           { return p.operation.name }
func (p RegistrationOperationPlan) ToolID() string                         { return p.operation.toolID.String() }
func (p RegistrationOperationPlan) Tool() runtimecontracts.ToolSchemaEntry { return p.operation.tool }

func (p RegistrationOperationPlan) Prepare(contextValue map[string]any) (map[string]any, error) {
	out := map[string]any{}
	environment := map[string]any{"context": contextValue}
	for _, mapping := range p.operation.input {
		value, ok := mapping.source.lookup(environment)
		if !ok {
			return nil, fmt.Errorf("channel registration %s source %q is missing", p.operation.name, mapping.source.syntax)
		}
		if err := mapping.target.set(out, value); err != nil {
			return nil, err
		}
	}
	if err := p.operation.tool.InputSchema().Validate(out); err != nil {
		return nil, fmt.Errorf("channel registration %s projected input: %w", p.operation.name, err)
	}
	return out, nil
}

func (p RegistrationOperationPlan) Project(result any) (map[string]any, error) {
	if err := p.operation.tool.OutputSchema().Validate(result); err != nil {
		return nil, fmt.Errorf("channel registration %s tool output: %w", p.operation.name, err)
	}
	out := map[string]any{}
	environment := map[string]any{"result": result}
	for _, mapping := range p.operation.output {
		value, ok := mapping.source.lookup(environment)
		if !ok {
			return nil, fmt.Errorf("channel registration %s output source %q is missing", p.operation.name, mapping.source.syntax)
		}
		if err := mapping.target.set(out, value); err != nil {
			return nil, err
		}
	}
	for key, value := range out {
		text := strings.TrimSpace(fmt.Sprint(value))
		if text == "" || text == "<nil>" {
			return nil, fmt.Errorf("channel registration %s projection %q must be a non-empty opaque string", p.operation.name, key)
		}
		out[key] = text
	}
	return out, nil
}

func validateChannelRegistrationManifest(packID string, profile ChannelRegistrationProfile) error {
	subject := "channel pack " + packID + " registration"
	if !channelPathSegmentPattern.MatchString(strings.TrimSpace(profile.Slot.Namespace)) {
		return fmt.Errorf("%s slot.namespace must be a stable identifier", subject)
	}
	if len(profile.Credentials.Provider) == 0 {
		return fmt.Errorf("%s credentials.provider requires at least one logical credential", subject)
	}
	seen := map[string]struct{}{}
	for _, raw := range profile.Credentials.Provider {
		credential := strings.TrimSpace(raw)
		if !channelPathSegmentPattern.MatchString(credential) {
			return fmt.Errorf("%s provider credential %q is invalid", subject, raw)
		}
		if _, duplicate := seen[credential]; duplicate {
			return fmt.Errorf("%s provider credential %q is duplicated", subject, credential)
		}
		seen[credential] = struct{}{}
	}
	signing := strings.TrimSpace(profile.Credentials.Signing)
	if !channelPathSegmentPattern.MatchString(signing) {
		return fmt.Errorf("%s credentials.signing must be a stable logical credential", subject)
	}
	if _, duplicate := seen[signing]; duplicate {
		return fmt.Errorf("%s signing credential %q must be distinct from provider credentials", subject, signing)
	}
	operations := []struct {
		name      string
		operation ChannelRegistrationOperation
		output    string
		input     bool
	}{
		{RegistrationOperationIdentify, profile.Slot.Identify, "resource_id", false},
		{RegistrationOperationApply, profile.Apply, "", true},
		{RegistrationOperationReadback, profile.Readback, "callback_url", false},
	}
	for _, candidate := range operations {
		if strings.TrimSpace(candidate.operation.Tool) == "" {
			return fmt.Errorf("%s %s.tool is required", subject, candidate.name)
		}
		for target, mapping := range candidate.operation.Input {
			if err := validateChannelTargetAndMapping(subject+" "+candidate.name+" input", target, mapping); err != nil {
				return err
			}
			if mapping.Each != "" || !strings.HasPrefix(strings.TrimSpace(mapping.From), "context.") {
				return fmt.Errorf("%s %s input %q must map one context value", subject, candidate.name, target)
			}
			if !candidate.input {
				return fmt.Errorf("%s %s must not declare input mappings", subject, candidate.name)
			}
			if strings.TrimSpace(mapping.From) != "context.callback_url" {
				return fmt.Errorf("%s %s input %q must map context.callback_url", subject, candidate.name, target)
			}
			if strings.Contains(mapping.From, "signing_secret") || strings.Contains(mapping.From, "credentials.") {
				return fmt.Errorf("%s %s input must not carry secret values; use declared credential substitution", subject, candidate.name)
			}
		}
		if candidate.output == "" && len(candidate.operation.Output) != 0 {
			return fmt.Errorf("%s %s must not declare output mappings", subject, candidate.name)
		}
		if candidate.output != "" {
			if len(candidate.operation.Output) != 1 {
				return fmt.Errorf("%s %s must project exactly %q", subject, candidate.name, candidate.output)
			}
			mapping, ok := candidate.operation.Output[candidate.output]
			if !ok || mapping.Each != "" || !strings.HasPrefix(strings.TrimSpace(mapping.From), "result.") {
				return fmt.Errorf("%s %s must project %q from one result path", subject, candidate.name, candidate.output)
			}
		}
	}
	if len(profile.Apply.Input) == 0 {
		return fmt.Errorf("%s apply must map context.callback_url", subject)
	}
	hasCallbackURL := false
	for _, mapping := range profile.Apply.Input {
		hasCallbackURL = hasCallbackURL || strings.TrimSpace(mapping.From) == "context.callback_url"
	}
	if !hasCallbackURL {
		return fmt.Errorf("%s apply must map context.callback_url", subject)
	}
	return nil
}

func compileChannelRegistration(profile ChannelRegistrationProfile, connector ConnectorPackDescriptor, provider channelPlanIdentity) (*CompiledChannelRegistration, error) {
	providerCredentials := append([]string(nil), profile.Credentials.Provider...)
	for index := range providerCredentials {
		providerCredentials[index] = strings.TrimSpace(providerCredentials[index])
	}
	sort.Strings(providerCredentials)
	namespace, _ := admitChannelPlanIdentity("channel registration slot namespace", profile.Slot.Namespace)
	signing, _ := admitChannelPlanIdentity("channel registration signing credential", profile.Credentials.Signing)
	compiled := &CompiledChannelRegistration{
		provider: provider, slotNamespace: namespace, providerCredentials: providerCredentials, signingCredential: signing,
	}
	var err error
	compiled.identify, err = compileRegistrationOperation(RegistrationOperationIdentify, profile.Slot.Identify, connector, runtimecontracts.ActivityEffectClassReadOnly, providerCredentials, "")
	if err != nil {
		return nil, err
	}
	applyCredentials := append(append([]string(nil), providerCredentials...), signing.String())
	sort.Strings(applyCredentials)
	compiled.apply, err = compileRegistrationOperation(RegistrationOperationApply, profile.Apply, connector, runtimecontracts.ActivityEffectClassNonIdempotentWrite, applyCredentials, "")
	if err != nil {
		return nil, err
	}
	compiled.readback, err = compileRegistrationOperation(RegistrationOperationReadback, profile.Readback, connector, runtimecontracts.ActivityEffectClassReadOnly, providerCredentials, "")
	if err != nil {
		return nil, err
	}
	return compiled, nil
}

func compileRegistrationOperation(name string, declared ChannelRegistrationOperation, connector ConnectorPackDescriptor, effect runtimecontracts.ActivityEffectClass, credentials []string, _ string) (compiledRegistrationOperation, error) {
	toolID := strings.TrimSpace(declared.Tool)
	tool, ok := connector.Tools[toolID]
	if !ok {
		return compiledRegistrationOperation{}, fmt.Errorf("channel registration %s references unknown connector tool %q", name, toolID)
	}
	if tool.Category() != runtimecontracts.ToolCategoryProviderRegistration {
		return compiledRegistrationOperation{}, fmt.Errorf("channel registration %s tool %q must use provider_registration category", name, toolID)
	}
	if tool.Effect() != effect {
		return compiledRegistrationOperation{}, fmt.Errorf("channel registration %s tool %q must use %s", name, toolID, effect)
	}
	actualCredentials := append([]string(nil), tool.Credentials()...)
	sort.Strings(actualCredentials)
	if strings.Join(actualCredentials, "\x00") != strings.Join(credentials, "\x00") {
		return compiledRegistrationOperation{}, fmt.Errorf("channel registration %s tool %q credentials %v do not match declared roles %v", name, toolID, actualCredentials, credentials)
	}
	input, err := compileSimpleRegistrationMappings(declared.Input, "context", tool.InputSchema(), true)
	if err != nil {
		return compiledRegistrationOperation{}, fmt.Errorf("channel registration %s input: %w", name, err)
	}
	output, err := compileSimpleRegistrationMappings(declared.Output, "result", tool.OutputSchema(), false)
	if err != nil {
		return compiledRegistrationOperation{}, fmt.Errorf("channel registration %s output: %w", name, err)
	}
	id, _ := admitChannelPlanIdentity("channel registration tool identity", toolID)
	return compiledRegistrationOperation{name: name, toolID: id, tool: tool, input: input, output: output}, nil
}

func compileSimpleRegistrationMappings(mappings map[string]ChannelMapping, sourceRoot string, schema runtimecontracts.ToolInputSchema, input bool) ([]compiledChannelMapping, error) {
	keys := sortedKeys(mappings)
	out := make([]compiledChannelMapping, 0, len(keys))
	for _, target := range keys {
		mapping := mappings[target]
		targetPath, err := compileChannelPath(target)
		if err != nil {
			return nil, err
		}
		sourcePath, err := compileChannelPath(mapping.From)
		if err != nil {
			return nil, err
		}
		if sourcePath.segments[0] != sourceRoot {
			return nil, fmt.Errorf("source %q must start with %s", mapping.From, sourceRoot)
		}
		schemaPath := targetPath.segments
		if !input {
			schemaPath = sourcePath.segments[1:]
		}
		if _, ok := schemaAt(schema, schemaPath); !ok {
			return nil, fmt.Errorf("schema path %q is missing", strings.Join(schemaPath, "."))
		}
		out = append(out, compiledChannelMapping{target: targetPath, source: sourcePath})
	}
	return out, nil
}

func (r CompiledChannelRegistration) clone() CompiledChannelRegistration {
	r.providerCredentials = append([]string(nil), r.providerCredentials...)
	r.identify = r.identify.clone()
	r.apply = r.apply.clone()
	r.readback = r.readback.clone()
	return r
}

func (o compiledRegistrationOperation) clone() compiledRegistrationOperation {
	o.input = append([]compiledChannelMapping(nil), o.input...)
	o.output = append([]compiledChannelMapping(nil), o.output...)
	return o
}

func (r CompiledChannelRegistration) generationValue() map[string]any {
	operation := func(value compiledRegistrationOperation) map[string]any {
		tool, err := value.tool.CanonicalValue()
		if err != nil {
			panic(err)
		}
		return map[string]any{
			"tool": value.toolID.String(), "tool_schema": tool,
			"input":  compiledChannelMappingGenerationValue(value.input),
			"output": compiledChannelMappingGenerationValue(value.output),
		}
	}
	return map[string]any{
		"provider": r.provider.String(), "slot_namespace": r.slotNamespace.String(),
		"provider_credentials": append([]string(nil), r.providerCredentials...),
		"signing_credential":   r.signingCredential.String(),
		"identify":             operation(r.identify), "apply": operation(r.apply), "readback": operation(r.readback),
	}
}
