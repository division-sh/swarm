package packs

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"gopkg.in/yaml.v3"
)

const ChannelInterfaceKind = "pack_channel"

var channelPathSegmentPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type PackIdentity struct {
	ID           string `json:"id"`
	Version      string `json:"version"`
	ManifestHash string `json:"manifest_hash"`
	Type         string `json:"type"`
	Source       string `json:"source"`
}

type TriggerEventField struct {
	Type     string `json:"type"`
	Required bool   `json:"required"`
}

type TriggerEvent struct {
	Name   string                       `json:"name"`
	Fields map[string]TriggerEventField `json:"fields"`
}

// TriggerPackDescriptor is the provider-neutral immutable surface exported by
// the accepted trigger registry into the channel compiler.
type TriggerPackDescriptor struct {
	Identity     PackIdentity            `json:"identity"`
	Provider     string                  `json:"provider"`
	GenerationID string                  `json:"generation_id"`
	Events       map[string]TriggerEvent `json:"events"`
}

// ConnectorPackDescriptor is the provider-neutral immutable surface exported
// by the accepted connector registry into the channel compiler.
type ConnectorPackDescriptor struct {
	Identity PackIdentity                                `json:"identity"`
	Provider string                                      `json:"provider"`
	Tools    map[string]runtimecontracts.ToolSchemaEntry `json:"-"`
}

type InterfaceRegistry struct {
	definitions map[string]runtimecontracts.PackInterfaceDefinition
}

func NewInterfaceRegistry(platform runtimecontracts.PlatformSpecDocument) (*InterfaceRegistry, error) {
	definitions := map[string]runtimecontracts.PackInterfaceDefinition{}
	for family, versions := range platform.Interfaces {
		family = strings.TrimSpace(family)
		if family == "" {
			return nil, fmt.Errorf("platform interface family is required")
		}
		for version, definition := range versions {
			version = strings.TrimSpace(version)
			if version == "" {
				return nil, fmt.Errorf("platform interface %q version is required", family)
			}
			identity := family + "/" + version
			if _, exists := definitions[identity]; exists {
				return nil, fmt.Errorf("duplicate platform interface %q", identity)
			}
			if err := validateInterfaceDefinition(identity, definition); err != nil {
				return nil, err
			}
			definitions[identity] = cloneInterfaceDefinition(definition)
		}
	}
	return &InterfaceRegistry{definitions: definitions}, nil
}

func (r *InterfaceRegistry) Lookup(identity string) (runtimecontracts.PackInterfaceDefinition, bool) {
	if r == nil {
		return runtimecontracts.PackInterfaceDefinition{}, false
	}
	definition, ok := r.definitions[strings.TrimSpace(identity)]
	if !ok {
		return runtimecontracts.PackInterfaceDefinition{}, false
	}
	return cloneInterfaceDefinition(definition), true
}

func validateInterfaceDefinition(identity string, definition runtimecontracts.PackInterfaceDefinition) error {
	if strings.TrimSpace(definition.Kind) != ChannelInterfaceKind {
		return fmt.Errorf("platform interface %q kind must be %q", identity, ChannelInterfaceKind)
	}
	if len(definition.Schemas) == 0 || len(definition.Operations) == 0 || len(definition.Events) == 0 {
		return fmt.Errorf("platform interface %q requires schemas, operations, and events", identity)
	}
	for name, operation := range definition.Operations {
		if runtimecontracts.NormalizeActivityEffectClass(operation.EffectClass) != runtimecontracts.ActivityEffectClassNonIdempotentWrite {
			return fmt.Errorf("platform interface %q operation %q must use non_idempotent_write", identity, name)
		}
		for group, fields := range map[string]map[string]runtimecontracts.PackInterfaceField{
			"input": operation.Input, "context": operation.Context, "output": operation.Output,
		} {
			for fieldName, field := range fields {
				if err := validateInterfaceField(identity+" operation "+name+" "+group+"."+fieldName, field, definition.Schemas); err != nil {
					return err
				}
			}
		}
	}
	for name, event := range definition.Events {
		if len(event.RequiredFields) == 0 {
			return fmt.Errorf("platform interface %q event %q requires required_fields", identity, name)
		}
		for fieldName, field := range event.RequiredFields {
			if err := validateInterfaceField(identity+" event "+name+" required_fields."+fieldName, field, definition.Schemas); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateInterfaceField(subject string, field runtimecontracts.PackInterfaceField, schemas map[string]runtimecontracts.ToolInputSchema) error {
	schemaName := strings.TrimSpace(field.Schema)
	opaqueName := strings.TrimSpace(field.Opaque)
	if (schemaName == "") == (opaqueName == "") {
		return fmt.Errorf("%s must declare exactly one of schema or opaque", subject)
	}
	if schemaName != "" {
		if _, ok := schemas[schemaName]; !ok {
			return fmt.Errorf("%s references unknown schema %q", subject, schemaName)
		}
	}
	if opaqueName != "" && !channelPathSegmentPattern.MatchString(opaqueName) {
		return fmt.Errorf("%s has invalid opaque slot %q", subject, opaqueName)
	}
	return nil
}

type ChannelManifest struct {
	Provider    string                                      `yaml:"provider"`
	OpaqueTypes map[string]runtimecontracts.ToolInputSchema `yaml:"opaque_types"`
	Operations  map[string]ChannelOperationBinding          `yaml:"operations"`
	Events      map[string]ChannelEventBinding              `yaml:"events"`
}

type ChannelOperationBinding struct {
	Tool   string                    `yaml:"tool"`
	Input  map[string]ChannelMapping `yaml:"input,omitempty"`
	Output map[string]ChannelMapping `yaml:"output,omitempty"`
}

type ChannelEventBinding struct {
	Event  string            `yaml:"event"`
	Fields map[string]string `yaml:"fields"`
}

type ChannelMapping struct {
	From    string
	Convert string
	Each    string
	Item    []map[string]ChannelMapping
}

func (m *ChannelMapping) UnmarshalYAML(node *yaml.Node) error {
	if m == nil || node == nil {
		return nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		m.From = strings.TrimSpace(node.Value)
		if m.From == "" {
			return fmt.Errorf("channel mapping source path is required")
		}
		return nil
	case yaml.MappingNode:
		if err := rejectChannelMappingFields(node, "from", "convert", "each", "item"); err != nil {
			return err
		}
		type wire struct {
			From    string                      `yaml:"from"`
			Convert string                      `yaml:"convert"`
			Each    string                      `yaml:"each"`
			Item    []map[string]ChannelMapping `yaml:"item"`
		}
		var decoded wire
		if err := node.Decode(&decoded); err != nil {
			return err
		}
		m.From = strings.TrimSpace(decoded.From)
		m.Convert = strings.TrimSpace(decoded.Convert)
		m.Each = strings.TrimSpace(decoded.Each)
		m.Item = decoded.Item
		if m.Each != "" {
			if m.From != "" || m.Convert != "" || len(m.Item) == 0 {
				return fmt.Errorf("channel each mapping requires each and item only")
			}
			return nil
		}
		if m.From == "" || len(m.Item) != 0 {
			return fmt.Errorf("channel scalar mapping requires from and optional convert")
		}
		return nil
	default:
		return fmt.Errorf("channel mapping must be a source path or mapping")
	}
}

func rejectChannelMappingFields(node *yaml.Node, allowed ...string) error {
	known := make(map[string]struct{}, len(allowed))
	for _, field := range allowed {
		known[field] = struct{}{}
	}
	for index := 0; index < len(node.Content); index += 2 {
		field := strings.TrimSpace(node.Content[index].Value)
		if _, ok := known[field]; !ok {
			return fmt.Errorf("channel mapping field %q is unsupported", field)
		}
	}
	return nil
}

type LoadedChannelPack struct {
	Envelope     Envelope
	Manifest     ChannelManifest
	ManifestBody []byte
	Directory    string
	Source       string
}

func LoadChannelPackFS(fsys fs.FS, dir, runningPlatformVersion string) (LoadedChannelPack, error) {
	loaded, err := Load(fsys, dir, runningPlatformVersion)
	if err != nil {
		return LoadedChannelPack{}, err
	}
	if strings.TrimSpace(loaded.Envelope.Type) != TypeChannel {
		return LoadedChannelPack{}, fmt.Errorf("channel pack %q has unsupported type %q", loaded.Envelope.ID, loaded.Envelope.Type)
	}
	if len(loaded.Envelope.Requires.Packs) != 2 || strings.TrimSpace(loaded.Envelope.Requires.Packs[TypeTrigger]) == "" || strings.TrimSpace(loaded.Envelope.Requires.Packs[TypeConnector]) == "" {
		return LoadedChannelPack{}, fmt.Errorf("channel pack %q requires exactly trigger and connector pack roles", loaded.Envelope.ID)
	}
	var manifest ChannelManifest
	decoder := yaml.NewDecoder(bytes.NewReader(loaded.ManifestBody))
	decoder.KnownFields(true)
	if err := decoder.Decode(&manifest); err != nil {
		return LoadedChannelPack{}, fmt.Errorf("parse channel manifest for pack %q: %w", loaded.Envelope.ID, err)
	}
	if err := validateChannelManifest(loaded.Envelope.ID, manifest); err != nil {
		return LoadedChannelPack{}, err
	}
	return LoadedChannelPack{
		Envelope: loaded.Envelope, Manifest: manifest, ManifestBody: append([]byte(nil), loaded.ManifestBody...),
		Directory: loaded.Directory, Source: strings.TrimSpace(loaded.Envelope.Provenance.Source) + ":" + strings.TrimSpace(loaded.Envelope.ID),
	}, nil
}

func LoadChannelPackDirs(runningPlatformVersion, provenance string, dirs ...string) ([]LoadedChannelPack, error) {
	loaded := make([]LoadedChannelPack, 0, len(dirs))
	seen := map[string]struct{}{}
	for index, raw := range dirs {
		dir := strings.TrimSpace(raw)
		if dir == "" {
			return nil, fmt.Errorf("channel pack directory %d is empty", index)
		}
		absolute, err := filepath.Abs(dir)
		if err != nil {
			return nil, fmt.Errorf("resolve channel pack directory %q: %w", dir, err)
		}
		absolute = filepath.Clean(absolute)
		if _, exists := seen[absolute]; exists {
			return nil, fmt.Errorf("duplicate channel pack directory %q", absolute)
		}
		seen[absolute] = struct{}{}
		pack, err := LoadChannelPackFS(os.DirFS(absolute), ".", runningPlatformVersion)
		if err != nil {
			return nil, fmt.Errorf("load channel pack %q: %w", absolute, err)
		}
		if got := strings.TrimSpace(pack.Envelope.Provenance.Source); got != strings.TrimSpace(provenance) {
			return nil, fmt.Errorf("channel pack %q provenance %q does not match selected tier %q", pack.Envelope.ID, got, provenance)
		}
		pack.Directory = absolute
		pack.Source = strings.TrimSpace(provenance) + ":" + absolute
		loaded = append(loaded, pack)
	}
	return loaded, nil
}

func CompileChannelInventory(registry *InterfaceRegistry, channels []LoadedChannelPack, triggers []TriggerPackDescriptor, connectors []ConnectorPackDescriptor) ([]SatisfactionPlan, error) {
	seenIDs := map[string]PackIdentity{}
	register := func(identity PackIdentity) error {
		id := strings.TrimSpace(identity.ID)
		if id == "" {
			return fmt.Errorf("pack identity is required")
		}
		if prior, exists := seenIDs[id]; exists {
			return fmt.Errorf("duplicate accepted pack id %q across roles %q and %q", id, prior.Type, identity.Type)
		}
		seenIDs[id] = identity
		return nil
	}
	for _, trigger := range triggers {
		if err := register(trigger.Identity); err != nil {
			return nil, err
		}
	}
	for _, connector := range connectors {
		if err := register(connector.Identity); err != nil {
			return nil, err
		}
	}
	for _, channel := range channels {
		if err := register(identityFromEnvelope(channel.Envelope, channel.Source)); err != nil {
			return nil, err
		}
	}
	plans := make([]SatisfactionPlan, 0, len(channels))
	for _, channel := range channels {
		plan, err := CompileChannel(registry, channel, triggers, connectors)
		if err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	sort.Slice(plans, func(i, j int) bool { return plans[i].Channel.ID < plans[j].Channel.ID })
	return plans, nil
}

func validateChannelManifest(packID string, manifest ChannelManifest) error {
	if strings.TrimSpace(manifest.Provider) == "" {
		return fmt.Errorf("channel pack %q provider is required", packID)
	}
	if len(manifest.OpaqueTypes) == 0 || len(manifest.Operations) == 0 || len(manifest.Events) == 0 {
		return fmt.Errorf("channel pack %q requires opaque_types, operations, and events", packID)
	}
	for name, binding := range manifest.Operations {
		if strings.TrimSpace(name) == "" || strings.TrimSpace(binding.Tool) == "" {
			return fmt.Errorf("channel pack %q operation name and tool are required", packID)
		}
		for target, mapping := range binding.Input {
			if err := validateChannelTargetAndMapping(packID+" operation "+name+" input", target, mapping); err != nil {
				return err
			}
		}
		for target, mapping := range binding.Output {
			if mapping.Each != "" {
				return fmt.Errorf("channel pack %q operation %q output %q must not use each", packID, name, target)
			}
			if err := validateChannelTargetAndMapping(packID+" operation "+name+" output", target, mapping); err != nil {
				return err
			}
		}
	}
	for name, binding := range manifest.Events {
		if strings.TrimSpace(name) == "" || strings.TrimSpace(binding.Event) == "" || len(binding.Fields) == 0 {
			return fmt.Errorf("channel pack %q event %q requires event and fields", packID, name)
		}
		for target, source := range binding.Fields {
			if err := validateChannelPath(target); err != nil {
				return fmt.Errorf("channel pack %q event %q target: %w", packID, name, err)
			}
			if err := validateChannelPath(source); err != nil {
				return fmt.Errorf("channel pack %q event %q source: %w", packID, name, err)
			}
		}
	}
	return nil
}

func validateChannelTargetAndMapping(subject, target string, mapping ChannelMapping) error {
	if err := validateChannelPath(target); err != nil {
		return fmt.Errorf("%s target: %w", subject, err)
	}
	if mapping.Each != "" {
		if err := validateChannelPath(mapping.Each); err != nil {
			return fmt.Errorf("%s each: %w", subject, err)
		}
		for _, item := range mapping.Item {
			for itemTarget, itemMapping := range item {
				if itemMapping.Each != "" || itemMapping.Convert != "" {
					return fmt.Errorf("%s item mapping supports scalar identity only", subject)
				}
				if err := validateChannelPath(itemTarget); err != nil {
					return fmt.Errorf("%s item target: %w", subject, err)
				}
				if err := validateChannelPath(itemMapping.From); err != nil {
					return fmt.Errorf("%s item source: %w", subject, err)
				}
			}
		}
		return nil
	}
	if err := validateChannelPath(mapping.From); err != nil {
		return fmt.Errorf("%s source: %w", subject, err)
	}
	switch mapping.Convert {
	case "", runtimecontracts.FieldProjectionConvertNumberToText, "decimal_text_to_int32":
		return nil
	default:
		return fmt.Errorf("%s conversion %q is unsupported", subject, mapping.Convert)
	}
}

func validateChannelPath(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fmt.Errorf("path is required")
	}
	for _, segment := range strings.Split(raw, ".") {
		if !channelPathSegmentPattern.MatchString(segment) {
			return fmt.Errorf("path %q has invalid segment %q", raw, segment)
		}
	}
	return nil
}

type SatisfactionPlan struct {
	InterfaceRef string                                      `json:"interface"`
	Channel      PackIdentity                                `json:"channel"`
	Trigger      PackIdentity                                `json:"trigger"`
	Connector    PackIdentity                                `json:"connector"`
	Provider     string                                      `json:"provider"`
	Schemas      map[string]runtimecontracts.ToolInputSchema `json:"-"`
	OpaqueTypes  map[string]runtimecontracts.ToolInputSchema `json:"-"`
	Operations   map[string]CompiledChannelOperation         `json:"operations"`
	Events       map[string]CompiledChannelEvent             `json:"events"`
	Constraints  map[string]runtimecontracts.ToolInputSchema `json:"-"`
}

type CompiledChannelOperation struct {
	Name       string                                  `json:"name"`
	Tool       string                                  `json:"tool"`
	ToolSchema runtimecontracts.ToolSchemaEntry        `json:"-"`
	Input      map[string]ChannelMapping               `json:"-"`
	Output     map[string]ChannelMapping               `json:"-"`
	Interface  runtimecontracts.PackInterfaceOperation `json:"-"`
}

type CompiledChannelEvent struct {
	Name       string            `json:"name"`
	Event      string            `json:"event"`
	Fields     map[string]string `json:"fields"`
	Descriptor TriggerEvent      `json:"descriptor"`
}

type OutboundBindingPlan struct {
	ID           string              `json:"id"`
	Structural   SatisfactionPlan    `json:"structural"`
	Destination  semanticvalue.Value `json:"-"`
	Requirements []Requirement       `json:"requirements,omitempty"`
}

func NewOutboundBindingPlan(id string, structural SatisfactionPlan, destination any, requirements []Requirement) (OutboundBindingPlan, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return OutboundBindingPlan{}, fmt.Errorf("channel outbound binding id is required")
	}
	destinationSchema, ok := structural.OpaqueTypes["destination"]
	if !ok {
		return OutboundBindingPlan{}, fmt.Errorf("channel %q has no destination opaque type", structural.Channel.ID)
	}
	admitted, err := canonicaljson.FromGo(destination)
	if err != nil {
		return OutboundBindingPlan{}, fmt.Errorf("channel outbound binding %q destination admission: %w", id, err)
	}
	if err := eventschema.ValidateValueAgainstSchema(runtimecontracts.ToolInputSchemaJSONSchema(destinationSchema), admitted.Interface()); err != nil {
		return OutboundBindingPlan{}, fmt.Errorf("channel outbound binding %q destination: %w", id, err)
	}
	return OutboundBindingPlan{
		ID: id, Structural: structural.Clone(), Destination: admitted,
		Requirements: cloneRequirements(requirements),
	}, nil
}

func (p OutboundBindingPlan) Clone() OutboundBindingPlan {
	out := p
	out.Structural = p.Structural.Clone()
	out.Requirements = cloneRequirements(p.Requirements)
	return out
}

func (p OutboundBindingPlan) RuntimeToolID(operation string) string {
	return "channel." + strings.TrimSpace(p.ID) + "." + strings.TrimSpace(operation)
}

func (p OutboundBindingPlan) RuntimeTools() (map[string]runtimecontracts.ToolSchemaEntry, error) {
	out := make(map[string]runtimecontracts.ToolSchemaEntry, len(p.Structural.Operations))
	for _, name := range sortedKeys(p.Structural.Operations) {
		tool, err := p.Structural.OperationTool(name)
		if err != nil {
			return nil, err
		}
		out[p.RuntimeToolID(name)] = tool
	}
	return out, nil
}

func (p OutboundBindingPlan) PrepareOperation(operation string, input any) (string, map[string]any, error) {
	compiled, ok := p.Structural.Operations[strings.TrimSpace(operation)]
	if !ok {
		return "", nil, fmt.Errorf("channel operation %q is not compiled", operation)
	}
	contextValue := any(map[string]any{})
	if len(compiled.Interface.Context) > 0 {
		contextValue = p.Destination.Interface()
	}
	prepared, err := p.Structural.PrepareOperationInput(operation, input, contextValue)
	if err != nil {
		return "", nil, err
	}
	return p.RuntimeToolID(operation), prepared, nil
}

func (p SatisfactionPlan) CapabilitySubject() (Subject, error) {
	subject := Subject{
		ID: p.Channel.ID, Kind: SubjectChannelPack, Provider: p.Provider,
		Source: "channel_pack", Provenance: sourceProvenance(p.Channel.Source), SourcePath: p.Channel.Source,
		Applicability: "installed", Status: StatusAvailable,
		Capabilities: []Capability{{Code: CapabilitySatisfyPackInterface, Target: p.InterfaceRef}},
		Evidence: []Evidence{{Kind: "channel_plan", Fields: map[string]string{
			"interface": p.InterfaceRef, "channel_hash": p.Channel.ManifestHash,
			"trigger_id": p.Trigger.ID, "trigger_hash": p.Trigger.ManifestHash,
			"connector_id": p.Connector.ID, "connector_hash": p.Connector.ManifestHash,
		}}},
	}
	normalized, err := NormalizeSubjects([]Subject{subject})
	if err != nil {
		return Subject{}, err
	}
	return normalized[0], nil
}

func (p OutboundBindingPlan) CapabilitySubject() (Subject, error) {
	subject := Subject{
		ID: p.ID, Kind: SubjectChannelOutbound, Provider: p.Structural.Provider,
		Source: "channel_binding", Provenance: sourceProvenance(p.Structural.Channel.Source),
		SourcePath: p.Structural.Channel.Source, Applicability: "effective",
		Capabilities: []Capability{
			{Code: CapabilityDeliverChannel, Target: p.Structural.InterfaceRef},
			{Code: CapabilityLowerThroughActivity}, {Code: CapabilityJournalAttempts},
		},
		Requirements: cloneRequirements(p.Requirements),
		Evidence: []Evidence{{Kind: "channel_outbound", Fields: map[string]string{
			"interface": p.Structural.InterfaceRef, "channel_id": p.Structural.Channel.ID,
			"channel_hash":   p.Structural.Channel.ManifestHash,
			"trigger_hash":   p.Structural.Trigger.ManifestHash,
			"connector_hash": p.Structural.Connector.ManifestHash,
		}}},
	}
	for _, code := range []string{GuaranteeActivityJournal, GuaranteeNoAutomaticWriteRetry, GuaranteeCredentialRedaction} {
		guarantee, err := NewGuarantee(code)
		if err != nil {
			return Subject{}, err
		}
		subject.Guarantees = append(subject.Guarantees, guarantee)
	}
	normalized, err := NormalizeSubjects([]Subject{subject})
	if err != nil {
		return Subject{}, err
	}
	return normalized[0], nil
}

func sourceProvenance(source string) string {
	value, _, _ := strings.Cut(strings.TrimSpace(source), ":")
	return value
}

func (p SatisfactionPlan) OperationTool(name string) (runtimecontracts.ToolSchemaEntry, error) {
	operation, ok := p.Operations[strings.TrimSpace(name)]
	if !ok {
		return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("channel operation %q is not compiled", name)
	}
	tool := operation.ToolSchema
	outputSchema, err := interfaceOperationSchema(operation.Interface.Output, p.Schemas, p.OpaqueTypes)
	if err != nil {
		return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("channel operation %q output schema: %w", name, err)
	}
	fields := make(map[string]runtimecontracts.CompiledResultField, len(operation.Output))
	for target, mapping := range operation.Output {
		fields[target] = runtimecontracts.CompiledResultField{From: mapping.From, Convert: mapping.Convert}
	}
	tool.CompiledResult = &runtimecontracts.CompiledResultProjection{Fields: fields, OutputSchema: outputSchema}
	return tool, nil
}

func (p SatisfactionPlan) PrepareOperationInput(name string, input, context any) (map[string]any, error) {
	operation, ok := p.Operations[strings.TrimSpace(name)]
	if !ok {
		return nil, fmt.Errorf("channel operation %q is not compiled", name)
	}
	inputSchema, err := interfaceOperationSchema(operation.Interface.Input, p.Schemas, p.OpaqueTypes)
	if err != nil {
		return nil, err
	}
	contextSchema, err := interfaceOperationSchema(operation.Interface.Context, p.Schemas, p.OpaqueTypes)
	if err != nil {
		return nil, err
	}
	if err := eventschema.ValidateValueAgainstSchema(runtimecontracts.ToolInputSchemaJSONSchema(inputSchema), input); err != nil {
		return nil, fmt.Errorf("channel operation %q input: %w", name, err)
	}
	if err := eventschema.ValidateValueAgainstSchema(runtimecontracts.ToolInputSchemaJSONSchema(contextSchema), context); err != nil {
		return nil, fmt.Errorf("channel operation %q context: %w", name, err)
	}
	environment := map[string]any{"input": input, "context": context}
	out := map[string]any{}
	for _, target := range sortedKeys(operation.Input) {
		mapping := operation.Input[target]
		if mapping.Each != "" {
			itemsValue, ok := channelValueAtPath(environment, mapping.Each)
			if !ok {
				return nil, fmt.Errorf("channel operation %q source %q is missing", name, mapping.Each)
			}
			items, ok := itemsValue.([]any)
			if !ok {
				return nil, fmt.Errorf("channel operation %q source %q is not an array", name, mapping.Each)
			}
			projected := make([]any, 0, len(items))
			for _, item := range items {
				object := map[string]any{}
				for _, itemTargets := range mapping.Item {
					for _, itemTarget := range sortedKeys(itemTargets) {
						itemMapping := itemTargets[itemTarget]
						value, ok := channelValueAtPath(map[string]any{"item": item}, itemMapping.From)
						if !ok {
							return nil, fmt.Errorf("channel operation %q item source %q is missing", name, itemMapping.From)
						}
						if err := setChannelValueAtPath(object, itemTarget, value); err != nil {
							return nil, err
						}
					}
				}
				targetSchema, _ := schemaAt(operation.ToolSchema.InputSchema, strings.Split(target, "."))
				if targetSchema != nil && targetSchema.Items != nil && normalizeSchemaType(targetSchema.Items.Type) == "array" {
					projected = append(projected, []any{object})
				} else {
					projected = append(projected, object)
				}
			}
			if err := setChannelValueAtPath(out, target, projected); err != nil {
				return nil, err
			}
			continue
		}
		value, ok := channelValueAtPath(environment, mapping.From)
		if !ok {
			return nil, fmt.Errorf("channel operation %q source %q is missing", name, mapping.From)
		}
		converted, err := convertChannelValue(value, mapping.Convert)
		if err != nil {
			return nil, fmt.Errorf("channel operation %q source %q: %w", name, mapping.From, err)
		}
		if err := setChannelValueAtPath(out, target, converted); err != nil {
			return nil, err
		}
	}
	if err := eventschema.ValidateValueAgainstSchema(runtimecontracts.ToolInputSchemaJSONSchema(operation.ToolSchema.InputSchema), out); err != nil {
		return nil, fmt.Errorf("channel operation %q projected connector input: %w", name, err)
	}
	return out, nil
}

func interfaceOperationSchema(fields map[string]runtimecontracts.PackInterfaceField, schemas, opaque map[string]runtimecontracts.ToolInputSchema) (runtimecontracts.ToolInputSchema, error) {
	properties := make(map[string]runtimecontracts.ToolInputSchema, len(fields))
	required := make([]string, 0, len(fields))
	for _, name := range sortedKeys(fields) {
		resolved, err := resolvedInterfaceFieldSchema(fields[name], schemas, opaque)
		if err != nil {
			return runtimecontracts.ToolInputSchema{}, err
		}
		properties[name] = *resolved
		required = append(required, name)
	}
	allow := false
	return runtimecontracts.ToolInputSchema{
		Type: "object", Properties: properties, Required: required,
		AdditionalProperties: runtimecontracts.ToolAdditionalProperties{Allowed: &allow},
	}, nil
}

func channelValueAtPath(value any, path string) (any, bool) {
	current := value
	for _, segment := range strings.Split(strings.TrimSpace(path), ".") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = object[segment]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func setChannelValueAtPath(out map[string]any, path string, value any) error {
	parts := strings.Split(strings.TrimSpace(path), ".")
	if len(parts) == 0 || parts[0] == "" {
		return fmt.Errorf("channel mapping target is required")
	}
	current := out
	for _, segment := range parts[:len(parts)-1] {
		next, exists := current[segment]
		if !exists {
			object := map[string]any{}
			current[segment] = object
			current = object
			continue
		}
		object, ok := next.(map[string]any)
		if !ok {
			return fmt.Errorf("channel mapping target %q overlaps another target", path)
		}
		current = object
	}
	leaf := parts[len(parts)-1]
	if _, exists := current[leaf]; exists {
		return fmt.Errorf("channel mapping target %q is assigned more than once", path)
	}
	current[leaf] = value
	return nil
}

func convertChannelValue(value any, conversion string) (any, error) {
	switch strings.TrimSpace(conversion) {
	case "":
		return value, nil
	case "decimal_text_to_int32":
		text, ok := value.(string)
		if !ok || text == "" || !regexp.MustCompile(`^(0|[1-9][0-9]*)$`).MatchString(text) {
			return nil, fmt.Errorf("decimal_text_to_int32 requires canonical unsigned decimal text")
		}
		parsed, err := strconv.ParseInt(text, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("decimal_text_to_int32 value is outside signed int32 range")
		}
		return float64(parsed), nil
	case runtimecontracts.FieldProjectionConvertNumberToText:
		number, ok := exactInteger(value)
		if !ok || number < 0 {
			return nil, fmt.Errorf("number_to_text requires an exact non-negative integer")
		}
		return strconv.FormatInt(number, 10), nil
	default:
		return nil, fmt.Errorf("channel conversion %q is unsupported", conversion)
	}
}

func cloneRequirements(in []Requirement) []Requirement {
	out := make([]Requirement, len(in))
	for index, requirement := range in {
		out[index] = requirement
		out[index].Scopes = append([]string(nil), requirement.Scopes...)
		if requirement.Satisfied != nil {
			value := *requirement.Satisfied
			out[index].Satisfied = &value
		}
		if requirement.TokenRequest != nil {
			profile := *requirement.TokenRequest
			profile.StaticHeaders = cloneChannelStringMap(requirement.TokenRequest.StaticHeaders)
			out[index].TokenRequest = &profile
		}
	}
	return out
}

func CompileChannel(registry *InterfaceRegistry, channel LoadedChannelPack, triggers []TriggerPackDescriptor, connectors []ConnectorPackDescriptor) (SatisfactionPlan, error) {
	if registry == nil {
		return SatisfactionPlan{}, fmt.Errorf("channel interface registry is required")
	}
	interfaceRef := strings.TrimSpace(channel.Envelope.Implements[0])
	definition, ok := registry.Lookup(interfaceRef)
	if !ok {
		return SatisfactionPlan{}, fmt.Errorf("channel pack %q implements unknown interface %q", channel.Envelope.ID, interfaceRef)
	}
	trigger, err := resolveTriggerDependency(channel, triggers)
	if err != nil {
		return SatisfactionPlan{}, err
	}
	connector, err := resolveConnectorDependency(channel, connectors)
	if err != nil {
		return SatisfactionPlan{}, err
	}
	provider := strings.TrimSpace(channel.Manifest.Provider)
	if provider != strings.TrimSpace(trigger.Provider) || provider != strings.TrimSpace(connector.Provider) {
		return SatisfactionPlan{}, fmt.Errorf("channel pack %q provider %q does not match trigger %q and connector %q providers", channel.Envelope.ID, provider, trigger.Provider, connector.Provider)
	}
	if err := exactKeySet("channel opaque_types", channel.Manifest.OpaqueTypes, interfaceOpaqueSlots(definition)); err != nil {
		return SatisfactionPlan{}, err
	}
	for name, schema := range channel.Manifest.OpaqueTypes {
		if err := validateOpaqueSchema("channel opaque type "+name, schema); err != nil {
			return SatisfactionPlan{}, err
		}
	}
	if err := exactKeySet("channel operations", channel.Manifest.Operations, mapKeys(definition.Operations)); err != nil {
		return SatisfactionPlan{}, err
	}
	if err := exactKeySet("channel events", channel.Manifest.Events, mapKeys(definition.Events)); err != nil {
		return SatisfactionPlan{}, err
	}
	plan := SatisfactionPlan{
		InterfaceRef: interfaceRef,
		Channel:      identityFromEnvelope(channel.Envelope, channel.Source), Trigger: trigger.Identity, Connector: connector.Identity,
		Provider: provider, OpaqueTypes: cloneSchemaMap(channel.Manifest.OpaqueTypes),
		Schemas:    cloneSchemaMap(definition.Schemas),
		Operations: map[string]CompiledChannelOperation{}, Events: map[string]CompiledChannelEvent{}, Constraints: map[string]runtimecontracts.ToolInputSchema{},
	}
	for _, name := range sortedKeys(definition.Operations) {
		binding := channel.Manifest.Operations[name]
		operation := definition.Operations[name]
		tool, ok := connector.Tools[strings.TrimSpace(binding.Tool)]
		if !ok {
			return SatisfactionPlan{}, fmt.Errorf("channel operation %q references unknown connector tool %q", name, binding.Tool)
		}
		if runtimecontracts.NormalizeActivityEffectClass(tool.EffectClass) != runtimecontracts.NormalizeActivityEffectClass(operation.EffectClass) {
			return SatisfactionPlan{}, fmt.Errorf("channel operation %q effect class does not match connector tool %q", name, binding.Tool)
		}
		if err := validateOperationBinding(name, operation, binding, definition.Schemas, channel.Manifest.OpaqueTypes, tool); err != nil {
			return SatisfactionPlan{}, err
		}
		plan.Operations[name] = CompiledChannelOperation{Name: name, Tool: strings.TrimSpace(binding.Tool), ToolSchema: tool, Input: cloneMappingMap(binding.Input), Output: cloneMappingMap(binding.Output), Interface: operation}
	}
	plan.Constraints, err = compileSelectedChannelConstraints(plan)
	if err != nil {
		return SatisfactionPlan{}, err
	}
	for _, name := range sortedKeys(definition.Events) {
		binding := channel.Manifest.Events[name]
		descriptor, ok := trigger.Events[strings.TrimSpace(binding.Event)]
		if !ok {
			return SatisfactionPlan{}, fmt.Errorf("channel event %q references unknown accepted trigger event %q", name, binding.Event)
		}
		if err := validateEventBinding(name, definition.Events[name], binding, definition.Schemas, channel.Manifest.OpaqueTypes, descriptor); err != nil {
			return SatisfactionPlan{}, err
		}
		plan.Events[name] = CompiledChannelEvent{Name: name, Event: strings.TrimSpace(binding.Event), Fields: cloneChannelStringMap(binding.Fields), Descriptor: cloneTriggerEvent(descriptor)}
	}
	return plan.Clone(), nil
}

type selectedChannelConstraint struct {
	key        string
	sourcePath string
	itemField  string
	requireMax bool
}

func compileSelectedChannelConstraints(plan SatisfactionPlan) (map[string]runtimecontracts.ToolInputSchema, error) {
	definitions := []selectedChannelConstraint{
		{key: "presentation.text", sourcePath: "input.presentation.text", requireMax: true},
		{key: "actions", sourcePath: "input.actions", requireMax: true},
		{key: "actions[].label", sourcePath: "input.actions", itemField: "label", requireMax: true},
		{key: "actions[].token", sourcePath: "input.actions", itemField: "token", requireMax: true},
	}
	constraints := make(map[string]runtimecontracts.ToolInputSchema, len(definitions))
	for _, definition := range definitions {
		var selected *runtimecontracts.ToolInputSchema
		for _, operationName := range []string{"deliver", "edit"} {
			operation, ok := plan.Operations[operationName]
			if !ok {
				return nil, fmt.Errorf("selected channel constraint %q requires operation %q", definition.key, operationName)
			}
			interfaceSchema, err := selectedConstraintInterfaceSchema(operation, definition, plan.Schemas, plan.OpaqueTypes)
			if err != nil {
				return nil, err
			}
			connectorSchema, err := selectedConstraintConnectorSchema(operation, definition)
			if err != nil {
				return nil, err
			}
			if selected == nil {
				initial := cloneSchema(*interfaceSchema)
				selected = &initial
			}
			merged, err := intersectSelectedConstraint(definition.key, selected, connectorSchema)
			if err != nil {
				return nil, err
			}
			selected = merged
		}
		if selected == nil {
			return nil, fmt.Errorf("selected channel constraint %q has no candidates", definition.key)
		}
		if definition.requireMax {
			switch normalizeSchemaType(selected.Type) {
			case "string":
				if selected.MaxLength == nil {
					return nil, fmt.Errorf("selected channel constraint %q requires a finite maxLength", definition.key)
				}
			case "array":
				if selected.MaxItems == nil {
					return nil, fmt.Errorf("selected channel constraint %q requires a finite maxItems", definition.key)
				}
			default:
				return nil, fmt.Errorf("selected channel constraint %q has unsupported type %q", definition.key, selected.Type)
			}
		}
		constraints[definition.key] = cloneSchema(*selected)
	}
	return constraints, nil
}

func selectedConstraintInterfaceSchema(operation CompiledChannelOperation, definition selectedChannelConstraint, schemas, opaque map[string]runtimecontracts.ToolInputSchema) (*runtimecontracts.ToolInputSchema, error) {
	root, err := operationSourceSchema(operation.Interface, definition.sourcePath, schemas, opaque)
	if err != nil {
		return nil, fmt.Errorf("selected channel constraint %q: %w", definition.key, err)
	}
	if definition.itemField == "" {
		return root, nil
	}
	if normalizeSchemaType(root.Type) != "array" || root.Items == nil {
		return nil, fmt.Errorf("selected channel constraint %q source must be an item array", definition.key)
	}
	field, ok := schemaAt(*root.Items, []string{definition.itemField})
	if !ok {
		return nil, fmt.Errorf("selected channel constraint %q source item field is missing", definition.key)
	}
	return field, nil
}

func selectedConstraintConnectorSchema(operation CompiledChannelOperation, definition selectedChannelConstraint) (*runtimecontracts.ToolInputSchema, error) {
	for target, mapping := range operation.Input {
		if definition.itemField == "" && mapping.From == definition.sourcePath {
			schema, ok := schemaAt(operation.ToolSchema.InputSchema, strings.Split(target, "."))
			if !ok {
				break
			}
			return schema, nil
		}
		if mapping.Each != definition.sourcePath {
			continue
		}
		targetSchema, ok := schemaAt(operation.ToolSchema.InputSchema, strings.Split(target, "."))
		if !ok {
			break
		}
		if definition.itemField == "" {
			return targetSchema, nil
		}
		itemSchema := targetSchema
		if normalizeSchemaType(itemSchema.Type) != "array" || itemSchema.Items == nil {
			break
		}
		itemSchema = itemSchema.Items
		if normalizeSchemaType(itemSchema.Type) == "array" {
			if itemSchema.Items == nil {
				break
			}
			itemSchema = itemSchema.Items
		}
		for _, itemMappings := range mapping.Item {
			for itemTarget, itemMapping := range itemMappings {
				if itemMapping.From != "item."+definition.itemField {
					continue
				}
				field, ok := schemaAt(*itemSchema, strings.Split(itemTarget, "."))
				if ok {
					return field, nil
				}
			}
		}
	}
	return nil, fmt.Errorf("selected channel constraint %q is not mapped by operation %q", definition.key, operation.Name)
}

func intersectSelectedConstraint(name string, left, right *runtimecontracts.ToolInputSchema) (*runtimecontracts.ToolInputSchema, error) {
	if left == nil || right == nil || normalizeSchemaType(left.Type) != normalizeSchemaType(right.Type) {
		return nil, fmt.Errorf("selected channel constraint %q has incompatible types", name)
	}
	out := cloneSchema(*left)
	if left.Pattern != "" && right.Pattern != "" && left.Pattern != right.Pattern {
		return nil, fmt.Errorf("selected channel constraint %q has incompatible patterns", name)
	}
	if out.Pattern == "" {
		out.Pattern = right.Pattern
	}
	out.MinLength = maxIntPointer(left.MinLength, right.MinLength)
	out.MaxLength = minIntPointer(left.MaxLength, right.MaxLength)
	out.MinItems = maxIntPointer(left.MinItems, right.MinItems)
	out.MaxItems = minIntPointer(left.MaxItems, right.MaxItems)
	if !boundsIntersect(out.MinLength, out.MaxLength, nil, nil) || !boundsIntersect(out.MinItems, out.MaxItems, nil, nil) {
		return nil, fmt.Errorf("selected channel constraint %q has disjoint bounds", name)
	}
	return &out, nil
}

func maxIntPointer(left, right *int) *int {
	if left == nil {
		return cloneIntPointer(right)
	}
	if right == nil {
		return cloneIntPointer(left)
	}
	value := *left
	if *right > value {
		value = *right
	}
	return &value
}

func minIntPointer(left, right *int) *int {
	if left == nil {
		return cloneIntPointer(right)
	}
	if right == nil {
		return cloneIntPointer(left)
	}
	value := *left
	if *right < value {
		value = *right
	}
	return &value
}

func cloneIntPointer(value *int) *int {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func resolveTriggerDependency(channel LoadedChannelPack, descriptors []TriggerPackDescriptor) (TriggerPackDescriptor, error) {
	id := strings.TrimSpace(channel.Envelope.Requires.Packs[TypeTrigger])
	var matches []TriggerPackDescriptor
	for _, descriptor := range descriptors {
		if strings.TrimSpace(descriptor.Identity.ID) == id {
			matches = append(matches, descriptor)
		}
	}
	if len(matches) != 1 {
		return TriggerPackDescriptor{}, fmt.Errorf("channel pack %q trigger dependency %q resolved %d accepted packs; require exactly one", channel.Envelope.ID, id, len(matches))
	}
	if matches[0].Identity.Type != TypeTrigger {
		return TriggerPackDescriptor{}, fmt.Errorf("channel pack %q dependency %q has wrong role %q", channel.Envelope.ID, id, matches[0].Identity.Type)
	}
	return matches[0], nil
}

func resolveConnectorDependency(channel LoadedChannelPack, descriptors []ConnectorPackDescriptor) (ConnectorPackDescriptor, error) {
	id := strings.TrimSpace(channel.Envelope.Requires.Packs[TypeConnector])
	var matches []ConnectorPackDescriptor
	for _, descriptor := range descriptors {
		if strings.TrimSpace(descriptor.Identity.ID) == id {
			matches = append(matches, descriptor)
		}
	}
	if len(matches) != 1 {
		return ConnectorPackDescriptor{}, fmt.Errorf("channel pack %q connector dependency %q resolved %d accepted packs; require exactly one", channel.Envelope.ID, id, len(matches))
	}
	if matches[0].Identity.Type != TypeConnector {
		return ConnectorPackDescriptor{}, fmt.Errorf("channel pack %q dependency %q has wrong role %q", channel.Envelope.ID, id, matches[0].Identity.Type)
	}
	return matches[0], nil
}

func validateOperationBinding(name string, operation runtimecontracts.PackInterfaceOperation, binding ChannelOperationBinding, schemas, opaque map[string]runtimecontracts.ToolInputSchema, tool runtimecontracts.ToolSchemaEntry) error {
	usedSources := map[string]struct{}{}
	for target, mapping := range binding.Input {
		targetSchema, ok := schemaAt(tool.InputSchema, strings.Split(target, "."))
		if !ok {
			return fmt.Errorf("channel operation %q input target %q is absent from connector schema", name, target)
		}
		if mapping.Each != "" {
			sourceSchema, err := operationSourceSchema(operation, mapping.Each, schemas, opaque)
			if err != nil {
				return fmt.Errorf("channel operation %q: %w", name, err)
			}
			if sourceSchema.Type != "array" || targetSchema.Type != "array" || sourceSchema.Items == nil || targetSchema.Items == nil {
				return fmt.Errorf("channel operation %q each mapping %q -> %q requires array schemas", name, mapping.Each, target)
			}
			if err := validateEachItem(name, mapping, *sourceSchema.Items, *targetSchema.Items); err != nil {
				return err
			}
			if _, exists := usedSources[mapping.Each]; exists {
				return fmt.Errorf("channel operation %q reuses source %q", name, mapping.Each)
			}
			usedSources[mapping.Each] = struct{}{}
			continue
		}
		sourceSchema, err := operationSourceSchema(operation, mapping.From, schemas, opaque)
		if err != nil {
			return fmt.Errorf("channel operation %q: %w", name, err)
		}
		if err := validateDirectionalRelation(name+" input "+mapping.From+" -> "+target, sourceSchema, targetSchema, mapping.Convert); err != nil {
			return err
		}
		if _, exists := usedSources[mapping.From]; exists {
			return fmt.Errorf("channel operation %q reuses source %q", name, mapping.From)
		}
		usedSources[mapping.From] = struct{}{}
	}
	if err := requiredConnectorInputsMapped(name, tool.InputSchema, binding.Input); err != nil {
		return err
	}
	if err := interfaceInputsConsumed(name, operation, usedSources); err != nil {
		return err
	}
	if len(operation.Output) == 0 {
		if len(binding.Output) != 0 {
			return fmt.Errorf("channel operation %q has no interface output and must not map connector output", name)
		}
		return nil
	}
	for target, mapping := range binding.Output {
		targetSchema, err := operationOutputSchema(operation, target, schemas, opaque)
		if err != nil {
			return fmt.Errorf("channel operation %q: %w", name, err)
		}
		sourcePath := strings.TrimPrefix(mapping.From, "result.")
		if sourcePath == mapping.From {
			return fmt.Errorf("channel operation %q output source %q must start with result.", name, mapping.From)
		}
		sourceSchema, ok := schemaAt(tool.OutputSchema, strings.Split(sourcePath, "."))
		if !ok {
			return fmt.Errorf("channel operation %q output source %q is absent from connector schema", name, mapping.From)
		}
		if err := validateDirectionalRelation(name+" output "+mapping.From+" -> "+target, sourceSchema, targetSchema, mapping.Convert); err != nil {
			return err
		}
	}
	return requiredInterfaceOutputsMapped(name, operation, binding.Output, opaque)
}

func validateEachItem(name string, mapping ChannelMapping, sourceItem, targetItem runtimecontracts.ToolInputSchema) error {
	if len(mapping.Item) != 1 || len(mapping.Item[0]) == 0 {
		return fmt.Errorf("channel operation %q each mapping must construct exactly one object per source item", name)
	}
	if targetItem.Type == "array" {
		if targetItem.Items == nil {
			return fmt.Errorf("channel operation %q target row schema has no item", name)
		}
		targetItem = *targetItem.Items
	}
	if sourceItem.Type != "object" || targetItem.Type != "object" {
		return fmt.Errorf("channel operation %q each item source and target must be objects", name)
	}
	for target, itemMapping := range mapping.Item[0] {
		targetSchema, ok := schemaAt(targetItem, strings.Split(target, "."))
		if !ok {
			return fmt.Errorf("channel operation %q each item target %q is absent", name, target)
		}
		source := strings.TrimPrefix(itemMapping.From, "item.")
		if source == itemMapping.From {
			return fmt.Errorf("channel operation %q each item source %q must start with item.", name, itemMapping.From)
		}
		sourceSchema, ok := schemaAt(sourceItem, strings.Split(source, "."))
		if !ok {
			return fmt.Errorf("channel operation %q each item source %q is absent", name, itemMapping.From)
		}
		if err := validateDirectionalRelation(name+" each item "+itemMapping.From+" -> "+target, sourceSchema, targetSchema, ""); err != nil {
			return err
		}
	}
	return requiredSchemaPathsMapped(name+" each item", targetItem, mapping.Item[0])
}

func validateEventBinding(name string, event runtimecontracts.PackInterfaceEvent, binding ChannelEventBinding, schemas, opaque map[string]runtimecontracts.ToolInputSchema, descriptor TriggerEvent) error {
	if err := exactKeySet("channel event "+name+" fields", binding.Fields, requiredInterfaceFieldPaths(event.RequiredFields, opaque)); err != nil {
		return err
	}
	for target, source := range binding.Fields {
		targetSchema, err := interfaceFieldPathSchema(event.RequiredFields, target, schemas, opaque)
		if err != nil {
			return fmt.Errorf("channel event %q: %w", name, err)
		}
		fieldName := strings.TrimPrefix(source, "event.")
		if fieldName == source || strings.Contains(fieldName, ".") {
			return fmt.Errorf("channel event %q source %q must name one normalized event field", name, source)
		}
		field, ok := descriptor.Fields[fieldName]
		if !ok || !field.Required {
			return fmt.Errorf("channel event %q source %q is not a required accepted trigger field", name, source)
		}
		if err := validateDirectionalRelation(name+" event "+source+" -> "+target, &runtimecontracts.ToolInputSchema{Type: normalizeSchemaType(field.Type)}, targetSchema, ""); err != nil {
			return err
		}
	}
	return nil
}

func validateDirectionalRelation(subject string, source, target *runtimecontracts.ToolInputSchema, conversion string) error {
	if source == nil || target == nil {
		return fmt.Errorf("%s has no source or target schema", subject)
	}
	sourceType := normalizeSchemaType(source.Type)
	targetType := normalizeSchemaType(target.Type)
	switch conversion {
	case runtimecontracts.FieldProjectionConvertNumberToText:
		if sourceType != "integer" && sourceType != "number" || targetType != "string" {
			return fmt.Errorf("%s number_to_text requires numeric source and string target", subject)
		}
		return nil
	case "decimal_text_to_int32":
		if sourceType != "string" || targetType != "integer" {
			return fmt.Errorf("%s decimal_text_to_int32 requires string source and integer target", subject)
		}
		return nil
	case "":
	default:
		return fmt.Errorf("%s uses unsupported conversion %q", subject, conversion)
	}
	if sourceType != targetType {
		return fmt.Errorf("%s has incompatible types %s and %s", subject, sourceType, targetType)
	}
	if source.Pattern != "" && target.Pattern != "" && source.Pattern != target.Pattern {
		return fmt.Errorf("%s has disjoint pattern constraints", subject)
	}
	if !boundsIntersect(source.MinLength, source.MaxLength, target.MinLength, target.MaxLength) || !boundsIntersect(source.MinItems, source.MaxItems, target.MinItems, target.MaxItems) {
		return fmt.Errorf("%s has disjoint bounds", subject)
	}
	return nil
}

func boundsIntersect(leftMin, leftMax, rightMin, rightMax *int) bool {
	min := 0
	if leftMin != nil && *leftMin > min {
		min = *leftMin
	}
	if rightMin != nil && *rightMin > min {
		min = *rightMin
	}
	max := int(^uint(0) >> 1)
	if leftMax != nil && *leftMax < max {
		max = *leftMax
	}
	if rightMax != nil && *rightMax < max {
		max = *rightMax
	}
	return min <= max
}

func validateOpaqueSchema(subject string, schema runtimecontracts.ToolInputSchema) error {
	switch normalizeSchemaType(schema.Type) {
	case "string":
		if schema.MinLength == nil || *schema.MinLength < 1 {
			return fmt.Errorf("%s string must declare minLength >= 1", subject)
		}
		return nil
	case "object":
		if len(schema.Properties) == 0 || schema.AdditionalProperties.Allowed == nil || *schema.AdditionalProperties.Allowed {
			return fmt.Errorf("%s object must be non-empty and additionalProperties false", subject)
		}
		required := stringSet(schema.Required)
		if len(required) != len(schema.Properties) {
			return fmt.Errorf("%s object must require every property", subject)
		}
		for name, property := range schema.Properties {
			if _, ok := required[name]; !ok {
				return fmt.Errorf("%s object property %q must be required", subject, name)
			}
			switch normalizeSchemaType(property.Type) {
			case "string":
				if property.MinLength == nil || *property.MinLength < 1 {
					return fmt.Errorf("%s object string leaf %q must be non-empty", subject, name)
				}
			case "integer", "boolean":
			case "object":
				if err := validateOpaqueSchema(subject+"."+name, property); err != nil {
					return err
				}
			default:
				return fmt.Errorf("%s object leaf %q has unsupported type %q", subject, name, property.Type)
			}
		}
		return nil
	default:
		return fmt.Errorf("%s must be a non-empty string or closed object", subject)
	}
}

func operationSourceSchema(operation runtimecontracts.PackInterfaceOperation, path string, schemas, opaque map[string]runtimecontracts.ToolInputSchema) (*runtimecontracts.ToolInputSchema, error) {
	parts := strings.Split(path, ".")
	if len(parts) < 2 || (parts[0] != "input" && parts[0] != "context") {
		return nil, fmt.Errorf("source %q must start with input. or context.", path)
	}
	fields := operation.Input
	if parts[0] == "context" {
		fields = operation.Context
	}
	field, ok := fields[parts[1]]
	if !ok {
		return nil, fmt.Errorf("source %q is not declared by the interface", path)
	}
	root, err := resolvedInterfaceFieldSchema(field, schemas, opaque)
	if err != nil {
		return nil, err
	}
	if len(parts) == 2 {
		return root, nil
	}
	resolved, ok := schemaAt(*root, parts[2:])
	if !ok {
		return nil, fmt.Errorf("source %q is absent from its interface schema", path)
	}
	return resolved, nil
}

func operationOutputSchema(operation runtimecontracts.PackInterfaceOperation, path string, schemas, opaque map[string]runtimecontracts.ToolInputSchema) (*runtimecontracts.ToolInputSchema, error) {
	parts := strings.Split(path, ".")
	if len(parts) == 0 {
		return nil, fmt.Errorf("output target is required")
	}
	field, ok := operation.Output[parts[0]]
	if !ok {
		return nil, fmt.Errorf("output target %q is not declared by the interface", path)
	}
	root, err := resolvedInterfaceFieldSchema(field, schemas, opaque)
	if err != nil {
		return nil, err
	}
	if len(parts) == 1 {
		return root, nil
	}
	resolved, ok := schemaAt(*root, parts[1:])
	if !ok {
		return nil, fmt.Errorf("output target %q is absent from its interface schema", path)
	}
	return resolved, nil
}

func interfaceFieldPathSchema(fields map[string]runtimecontracts.PackInterfaceField, path string, schemas, opaque map[string]runtimecontracts.ToolInputSchema) (*runtimecontracts.ToolInputSchema, error) {
	parts := strings.Split(path, ".")
	field, ok := fields[parts[0]]
	if !ok {
		return nil, fmt.Errorf("target %q is not declared by the interface", path)
	}
	root, err := resolvedInterfaceFieldSchema(field, schemas, opaque)
	if err != nil {
		return nil, err
	}
	if len(parts) == 1 {
		return root, nil
	}
	resolved, ok := schemaAt(*root, parts[1:])
	if !ok {
		return nil, fmt.Errorf("target %q is absent from its interface schema", path)
	}
	return resolved, nil
}

func resolvedInterfaceFieldSchema(field runtimecontracts.PackInterfaceField, schemas, opaque map[string]runtimecontracts.ToolInputSchema) (*runtimecontracts.ToolInputSchema, error) {
	if name := strings.TrimSpace(field.Schema); name != "" {
		schema, ok := schemas[name]
		if !ok {
			return nil, fmt.Errorf("interface schema %q is missing", name)
		}
		cloned := cloneSchema(schema)
		return &cloned, nil
	}
	name := strings.TrimSpace(field.Opaque)
	schema, ok := opaque[name]
	if !ok {
		return nil, fmt.Errorf("opaque type %q is missing", name)
	}
	cloned := cloneSchema(schema)
	return &cloned, nil
}

func schemaAt(schema runtimecontracts.ToolInputSchema, path []string) (*runtimecontracts.ToolInputSchema, bool) {
	current := cloneSchema(schema)
	for _, segment := range path {
		if normalizeSchemaType(current.Type) != "object" {
			return nil, false
		}
		next, ok := current.Properties[segment]
		if !ok {
			return nil, false
		}
		current = cloneSchema(next)
	}
	return &current, true
}

func requiredConnectorInputsMapped(name string, schema runtimecontracts.ToolInputSchema, mappings map[string]ChannelMapping) error {
	for _, required := range schema.Required {
		found := false
		for target := range mappings {
			if target == required || strings.HasPrefix(target, required+".") {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("channel operation %q does not map required connector input %q", name, required)
		}
	}
	return nil
}

func interfaceInputsConsumed(name string, operation runtimecontracts.PackInterfaceOperation, used map[string]struct{}) error {
	for group, fields := range map[string]map[string]runtimecontracts.PackInterfaceField{"input": operation.Input, "context": operation.Context} {
		for fieldName := range fields {
			prefix := group + "." + fieldName
			found := false
			for source := range used {
				if source == prefix || strings.HasPrefix(source, prefix+".") {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("channel operation %q does not consume interface %s", name, prefix)
			}
		}
	}
	return nil
}

func requiredInterfaceOutputsMapped(name string, operation runtimecontracts.PackInterfaceOperation, mappings map[string]ChannelMapping, opaque map[string]runtimecontracts.ToolInputSchema) error {
	for fieldName, field := range operation.Output {
		paths := []string{fieldName}
		if field.Opaque != "" {
			paths = schemaRequiredLeafPaths(fieldName, opaque[field.Opaque])
		}
		for _, path := range paths {
			if _, ok := mappings[path]; !ok {
				return fmt.Errorf("channel operation %q does not map required interface output %q", name, path)
			}
		}
	}
	return nil
}

func requiredSchemaPathsMapped(subject string, schema runtimecontracts.ToolInputSchema, mappings map[string]ChannelMapping) error {
	for _, path := range schemaRequiredLeafPaths("", schema) {
		path = strings.TrimPrefix(path, ".")
		if _, ok := mappings[path]; !ok {
			return fmt.Errorf("%s does not map required target %q", subject, path)
		}
	}
	return nil
}

func requiredInterfaceFieldPaths(fields map[string]runtimecontracts.PackInterfaceField, opaque map[string]runtimecontracts.ToolInputSchema) []string {
	var out []string
	for name, field := range fields {
		if field.Opaque == "" {
			out = append(out, name)
			continue
		}
		out = append(out, schemaRequiredLeafPaths(name, opaque[field.Opaque])...)
	}
	sort.Strings(out)
	return out
}

func schemaRequiredLeafPaths(prefix string, schema runtimecontracts.ToolInputSchema) []string {
	if normalizeSchemaType(schema.Type) != "object" {
		return []string{prefix}
	}
	var out []string
	for _, name := range schema.Required {
		property, ok := schema.Properties[name]
		if !ok {
			continue
		}
		child := name
		if prefix != "" {
			child = prefix + "." + name
		}
		out = append(out, schemaRequiredLeafPaths(child, property)...)
	}
	return out
}

func interfaceOpaqueSlots(definition runtimecontracts.PackInterfaceDefinition) []string {
	set := map[string]struct{}{}
	add := func(fields map[string]runtimecontracts.PackInterfaceField) {
		for _, field := range fields {
			if name := strings.TrimSpace(field.Opaque); name != "" {
				set[name] = struct{}{}
			}
		}
	}
	for _, operation := range definition.Operations {
		add(operation.Input)
		add(operation.Context)
		add(operation.Output)
	}
	for _, event := range definition.Events {
		add(event.RequiredFields)
	}
	out := make([]string, 0, len(set))
	for name := range set {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func exactKeySet[T any](subject string, got map[string]T, want []string) error {
	wantSet := stringSet(want)
	var missing, extra []string
	for key := range wantSet {
		if _, ok := got[key]; !ok {
			missing = append(missing, key)
		}
	}
	for key := range got {
		if _, ok := wantSet[key]; !ok {
			extra = append(extra, key)
		}
	}
	if len(missing) == 0 && len(extra) == 0 {
		return nil
	}
	sort.Strings(missing)
	sort.Strings(extra)
	return fmt.Errorf("%s key set mismatch: missing=%v extra=%v", subject, missing, extra)
}

func mapKeys[T any](values map[string]T) []string {
	out := make([]string, 0, len(values))
	for key := range values {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func sortedKeys[T any](values map[string]T) []string { return mapKeys(values) }

func stringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		out[strings.TrimSpace(value)] = struct{}{}
	}
	return out
}

func normalizeSchemaType(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "text":
		return "string"
	case "int":
		return "integer"
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func identityFromEnvelope(envelope Envelope, source string) PackIdentity {
	return PackIdentity{ID: strings.TrimSpace(envelope.ID), Version: strings.TrimSpace(envelope.Version), ManifestHash: strings.TrimSpace(envelope.ManifestHash), Type: strings.TrimSpace(envelope.Type), Source: strings.TrimSpace(source)}
}

func cloneInterfaceDefinition(in runtimecontracts.PackInterfaceDefinition) runtimecontracts.PackInterfaceDefinition {
	out := in
	out.Schemas = cloneSchemaMap(in.Schemas)
	out.Operations = make(map[string]runtimecontracts.PackInterfaceOperation, len(in.Operations))
	for name, operation := range in.Operations {
		operation.Input = cloneInterfaceFields(operation.Input)
		operation.Context = cloneInterfaceFields(operation.Context)
		operation.Output = cloneInterfaceFields(operation.Output)
		out.Operations[name] = operation
	}
	out.Events = make(map[string]runtimecontracts.PackInterfaceEvent, len(in.Events))
	for name, event := range in.Events {
		event.RequiredFields = cloneInterfaceFields(event.RequiredFields)
		out.Events[name] = event
	}
	return out
}

func cloneInterfaceFields(in map[string]runtimecontracts.PackInterfaceField) map[string]runtimecontracts.PackInterfaceField {
	out := make(map[string]runtimecontracts.PackInterfaceField, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneSchemaMap(in map[string]runtimecontracts.ToolInputSchema) map[string]runtimecontracts.ToolInputSchema {
	out := make(map[string]runtimecontracts.ToolInputSchema, len(in))
	for key, value := range in {
		out[key] = cloneSchema(value)
	}
	return out
}

func cloneSchema(in runtimecontracts.ToolInputSchema) runtimecontracts.ToolInputSchema {
	out := in
	out.Properties = cloneSchemaMap(in.Properties)
	out.Required = append([]string(nil), in.Required...)
	out.Enum = append([]runtimecontracts.SchemaLiteral(nil), in.Enum...)
	if in.Items != nil {
		items := cloneSchema(*in.Items)
		out.Items = &items
	}
	if in.AdditionalProperties.Allowed != nil {
		allowed := *in.AdditionalProperties.Allowed
		out.AdditionalProperties.Allowed = &allowed
	}
	if in.AdditionalProperties.Schema != nil {
		schema := cloneSchema(*in.AdditionalProperties.Schema)
		out.AdditionalProperties.Schema = &schema
	}
	return out
}

func cloneMappingMap(in map[string]ChannelMapping) map[string]ChannelMapping {
	out := make(map[string]ChannelMapping, len(in))
	for key, value := range in {
		value.Item = append([]map[string]ChannelMapping(nil), value.Item...)
		out[key] = value
	}
	return out
}

func cloneChannelStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneTriggerEvent(in TriggerEvent) TriggerEvent {
	out := in
	out.Fields = make(map[string]TriggerEventField, len(in.Fields))
	for name, field := range in.Fields {
		out.Fields[name] = field
	}
	return out
}

func (p SatisfactionPlan) Clone() SatisfactionPlan {
	out := p
	out.Schemas = cloneSchemaMap(p.Schemas)
	out.OpaqueTypes = cloneSchemaMap(p.OpaqueTypes)
	out.Constraints = cloneSchemaMap(p.Constraints)
	out.Operations = make(map[string]CompiledChannelOperation, len(p.Operations))
	for name, operation := range p.Operations {
		operation.Input = cloneMappingMap(operation.Input)
		operation.Output = cloneMappingMap(operation.Output)
		operation.Interface = cloneInterfaceDefinition(runtimecontracts.PackInterfaceDefinition{Operations: map[string]runtimecontracts.PackInterfaceOperation{name: operation.Interface}}).Operations[name]
		out.Operations[name] = operation
	}
	out.Events = make(map[string]CompiledChannelEvent, len(p.Events))
	for name, event := range p.Events {
		event.Fields = cloneChannelStringMap(event.Fields)
		event.Descriptor = cloneTriggerEvent(event.Descriptor)
		out.Events[name] = event
	}
	return out
}

func ValidateOpaqueValue(schema runtimecontracts.ToolInputSchema, value any) error {
	switch normalizeSchemaType(schema.Type) {
	case "string":
		text, ok := value.(string)
		if !ok {
			return fmt.Errorf("opaque value must be a string")
		}
		length := len([]rune(text))
		if schema.MinLength != nil && length < *schema.MinLength || schema.MaxLength != nil && length > *schema.MaxLength {
			return fmt.Errorf("opaque string length is outside admitted bounds")
		}
		if schema.Pattern != "" && !regexp.MustCompile(schema.Pattern).MatchString(text) {
			return fmt.Errorf("opaque string does not match admitted pattern")
		}
		return nil
	case "object":
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("opaque value must be an object")
		}
		if len(object) != len(schema.Properties) {
			return fmt.Errorf("opaque object has undeclared or missing properties")
		}
		for name, property := range schema.Properties {
			child, ok := object[name]
			if !ok {
				return fmt.Errorf("opaque object property %q is required", name)
			}
			switch normalizeSchemaType(property.Type) {
			case "string", "object":
				if err := ValidateOpaqueValue(property, child); err != nil {
					return fmt.Errorf("opaque object property %q: %w", name, err)
				}
			case "integer":
				if _, ok := exactInteger(child); !ok {
					return fmt.Errorf("opaque object property %q must be an integer", name)
				}
			case "boolean":
				if _, ok := child.(bool); !ok {
					return fmt.Errorf("opaque object property %q must be a boolean", name)
				}
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported opaque schema type %q", schema.Type)
	}
}

func exactInteger(value any) (int64, bool) {
	switch typed := value.(type) {
	case int:
		return int64(typed), true
	case int64:
		return typed, true
	case int32:
		return int64(typed), true
	case yaml.Node:
		parsed, err := strconv.ParseInt(strings.TrimSpace(typed.Value), 10, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}
