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
	"github.com/division-sh/swarm/internal/runtime/core/manifesthash"
	"github.com/division-sh/swarm/internal/runtime/core/packidentity"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"gopkg.in/yaml.v3"
)

const ChannelInterfaceKind = "pack_channel"

var channelPathSegmentPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type PackSource struct {
	provenance string
	path       string
}

func NewPackSource(provenance, path string) (PackSource, error) {
	provenance = strings.TrimSpace(provenance)
	path = strings.TrimSpace(path)
	if provenance == "" {
		return PackSource{}, fmt.Errorf("pack provenance is required")
	}
	return PackSource{provenance: provenance, path: path}, nil
}

func MustPackSource(provenance, path string) PackSource {
	source, err := NewPackSource(provenance, path)
	if err != nil {
		panic(err)
	}
	return source
}

func (s PackSource) Provenance() string { return s.provenance }
func (s PackSource) Path() string       { return s.path }
func (s PackSource) Diagnostic() string {
	if s.path == "" {
		return s.provenance
	}
	return s.provenance + ":" + s.path
}

type PackIdentity struct {
	id           packidentity.ID
	version      packidentity.Version
	manifestHash manifesthash.Hash
	packType     packIdentityType
	source       PackSource
}

type packIdentityType uint8

const (
	packIdentityTrigger packIdentityType = iota + 1
	packIdentityConnector
	packIdentityChannel
)

func NewPackIdentity(id, version, manifestHash, packType string, source PackSource) (PackIdentity, error) {
	if id == "" || version == "" || strings.TrimSpace(manifestHash) == "" || strings.TrimSpace(packType) == "" {
		return PackIdentity{}, fmt.Errorf("pack id, version, manifest hash, and type are required")
	}
	admittedID, err := packidentity.ParseID(id)
	if err != nil {
		return PackIdentity{}, fmt.Errorf("pack identity %w", err)
	}
	admittedVersion, err := packidentity.ParseVersion(version)
	if err != nil {
		return PackIdentity{}, fmt.Errorf("pack identity %w", err)
	}
	admittedManifestHash, err := manifesthash.Parse(manifestHash)
	if err != nil {
		return PackIdentity{}, fmt.Errorf("pack identity %w", err)
	}
	admittedPackType, err := admitPackIdentityType(packType)
	if err != nil {
		return PackIdentity{}, err
	}
	if source.Provenance() == "" {
		return PackIdentity{}, fmt.Errorf("pack source is required")
	}
	return PackIdentity{id: admittedID, version: admittedVersion, manifestHash: admittedManifestHash, packType: admittedPackType, source: source}, nil
}

func MustPackIdentity(id, version, manifestHash, packType string, source PackSource) PackIdentity {
	identity, err := NewPackIdentity(id, version, manifestHash, packType, source)
	if err != nil {
		panic(err)
	}
	return identity
}

func (i PackIdentity) ID() string           { return i.id.String() }
func (i PackIdentity) Version() string      { return i.version.String() }
func (i PackIdentity) ManifestHash() string { return i.manifestHash.String() }
func (i PackIdentity) Type() string         { return i.packType.String() }
func (i PackIdentity) Source() PackSource   { return i.source }

func (i PackIdentity) MarshalJSON() ([]byte, error) {
	return canonicaljson.Bytes(map[string]any{
		"id": i.id.String(), "version": i.version.String(), "manifest_hash": i.manifestHash.String(),
		"type": i.packType.String(), "source": i.source.Diagnostic(),
	})
}

func admitPackIdentityType(raw string) (packIdentityType, error) {
	switch raw {
	case TypeTrigger:
		return packIdentityTrigger, nil
	case TypeConnector:
		return packIdentityConnector, nil
	case TypeChannel:
		return packIdentityChannel, nil
	default:
		return 0, fmt.Errorf("pack identity type %q is unsupported", raw)
	}
}

func (t packIdentityType) String() string {
	switch t {
	case packIdentityTrigger:
		return TypeTrigger
	case packIdentityConnector:
		return TypeConnector
	case packIdentityChannel:
		return TypeChannel
	default:
		return ""
	}
}

type TriggerEventField struct {
	Schema   runtimecontracts.ToolInputSchema `json:"schema"`
	Required bool                             `json:"required"`
}

type TriggerEvent struct {
	Name   string                       `json:"name"`
	Fields map[string]TriggerEventField `json:"fields"`
}

// TriggerPackDescriptor is the provider-neutral immutable surface exported by
// the accepted trigger registry into the channel compiler.
type TriggerPackDescriptor struct {
	Identity   PackIdentity                 `json:"identity"`
	Provider   string                       `json:"provider"`
	Generation triggergeneration.Generation `json:"generation"`
	Events     map[string]TriggerEvent      `json:"events"`
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
	for name, schema := range definition.Schemas {
		if err := schema.ValidateDefinition(); err != nil {
			return fmt.Errorf("platform interface %q schema %q: %w", identity, name, err)
		}
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
	Provider     string                                      `yaml:"provider"`
	OpaqueTypes  map[string]runtimecontracts.ToolInputSchema `yaml:"opaque_types"`
	Operations   map[string]ChannelOperationBinding          `yaml:"operations"`
	Events       map[string]ChannelEventBinding              `yaml:"events"`
	Registration *ChannelRegistrationProfile                 `yaml:"registration,omitempty"`
}

type ChannelRegistrationProfile struct {
	Slot        ChannelRegistrationSlot        `yaml:"slot"`
	Credentials ChannelRegistrationCredentials `yaml:"credentials"`
	Apply       ChannelRegistrationOperation   `yaml:"apply"`
	Readback    ChannelRegistrationOperation   `yaml:"readback"`
}

type ChannelRegistrationSlot struct {
	Namespace string                       `yaml:"namespace"`
	Identify  ChannelRegistrationOperation `yaml:"identify"`
}

type ChannelRegistrationCredentials struct {
	Provider []string `yaml:"provider"`
	Signing  string   `yaml:"signing"`
}

type ChannelRegistrationOperation struct {
	Tool   string                    `yaml:"tool"`
	Input  map[string]ChannelMapping `yaml:"input,omitempty"`
	Output map[string]ChannelMapping `yaml:"output,omitempty"`
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
	From string
	Each string
	Item []map[string]ChannelMapping
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
		if err := rejectChannelMappingFields(node, "from", "each", "item"); err != nil {
			return err
		}
		type wire struct {
			From string                      `yaml:"from"`
			Each string                      `yaml:"each"`
			Item []map[string]ChannelMapping `yaml:"item"`
		}
		var decoded wire
		if err := node.Decode(&decoded); err != nil {
			return err
		}
		m.From = strings.TrimSpace(decoded.From)
		m.Each = strings.TrimSpace(decoded.Each)
		m.Item = decoded.Item
		if m.Each != "" {
			if m.From != "" || len(m.Item) == 0 {
				return fmt.Errorf("channel each mapping requires each and item only")
			}
			return nil
		}
		if m.From == "" || len(m.Item) != 0 {
			return fmt.Errorf("channel scalar mapping requires from only")
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
	Source       PackSource
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
		Directory: loaded.Directory, Source: MustPackSource(loaded.Envelope.Provenance.Source, loaded.Envelope.ID),
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
		pack.Source = MustPackSource(provenance, absolute)
		loaded = append(loaded, pack)
	}
	return loaded, nil
}

func CompileChannelInventory(registry *InterfaceRegistry, channels []LoadedChannelPack, triggers []TriggerPackDescriptor, connectors []ConnectorPackDescriptor) ([]SatisfactionPlan, error) {
	seenIDs := map[string]PackIdentity{}
	register := func(identity PackIdentity) error {
		id := identity.ID()
		if id == "" {
			return fmt.Errorf("pack identity is required")
		}
		if prior, exists := seenIDs[id]; exists {
			return fmt.Errorf("duplicate accepted pack id %q across roles %q and %q", id, prior.Type(), identity.Type())
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
	sort.Slice(plans, func(i, j int) bool { return plans[i].channel.ID() < plans[j].channel.ID() })
	return plans, nil
}

func validateChannelManifest(packID string, manifest ChannelManifest) error {
	if strings.TrimSpace(manifest.Provider) == "" {
		return fmt.Errorf("channel pack %q provider is required", packID)
	}
	if len(manifest.OpaqueTypes) == 0 || len(manifest.Operations) == 0 || len(manifest.Events) == 0 {
		return fmt.Errorf("channel pack %q requires opaque_types, operations, and events", packID)
	}
	for name, schema := range manifest.OpaqueTypes {
		if err := schema.ValidateDefinition(); err != nil {
			return fmt.Errorf("channel pack %q opaque type %q: %w", packID, name, err)
		}
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
	if manifest.Registration != nil {
		if err := validateChannelRegistrationManifest(packID, *manifest.Registration); err != nil {
			return err
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
				if itemMapping.Each != "" {
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
	return nil
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
	interfaceRef      channelPlanIdentity
	channel           PackIdentity
	trigger           PackIdentity
	connector         PackIdentity
	provider          channelPlanIdentity
	triggerGeneration triggergeneration.Generation
	schemas           map[string]runtimecontracts.ToolInputSchema
	opaqueTypes       map[string]runtimecontracts.ToolInputSchema
	operations        map[string]compiledChannelOperation
	events            map[string]compiledChannelEvent
	constraints       map[string]runtimecontracts.ToolInputSchema
	registration      *CompiledChannelRegistration
	generation        plangeneration.Generation
}

type channelPlanIdentity struct {
	value string
}

func admitChannelPlanIdentity(subject, raw string) (channelPlanIdentity, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return channelPlanIdentity{}, fmt.Errorf("%s is required", subject)
	}
	return channelPlanIdentity{value: value}, nil
}

func (i channelPlanIdentity) String() string {
	return i.value
}

type compiledChannelPath struct {
	syntax   string
	segments []string
}

func compileChannelPath(raw string) (compiledChannelPath, error) {
	if err := validateChannelPath(raw); err != nil {
		return compiledChannelPath{}, err
	}
	syntax := strings.TrimSpace(raw)
	return compiledChannelPath{syntax: syntax, segments: strings.Split(syntax, ".")}, nil
}

func (p compiledChannelPath) lookup(value any) (any, bool) {
	current := value
	for _, segment := range p.segments {
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

func (p compiledChannelPath) set(out map[string]any, value any) error {
	current := out
	for _, segment := range p.segments[:len(p.segments)-1] {
		next, exists := current[segment]
		if !exists {
			object := map[string]any{}
			current[segment] = object
			current = object
			continue
		}
		object, ok := next.(map[string]any)
		if !ok {
			return fmt.Errorf("channel mapping target %q overlaps another target", p.syntax)
		}
		current = object
	}
	leaf := p.segments[len(p.segments)-1]
	if _, exists := current[leaf]; exists {
		return fmt.Errorf("channel mapping target %q is assigned more than once", p.syntax)
	}
	current[leaf] = value
	return nil
}

type compiledChannelMapping struct {
	target         compiledChannelPath
	source         compiledChannelPath
	each           compiledChannelPath
	item           []compiledChannelMapping
	usesEach       bool
	wrapItemAsList bool
}

type compiledChannelOperation struct {
	name          channelPlanIdentity
	tool          channelPlanIdentity
	toolSchema    runtimecontracts.ToolSchemaEntry
	effect        runtimecontracts.ActivityEffectClass
	inputSchema   runtimecontracts.ToolInputSchema
	contextSchema runtimecontracts.ToolInputSchema
	outputSchema  runtimecontracts.ToolInputSchema
	hasContext    bool
	input         []compiledChannelMapping
	output        []compiledChannelMapping
}

type channelOperationDraft struct {
	name           channelPlanIdentity
	tool           channelPlanIdentity
	toolSchema     runtimecontracts.ToolSchemaEntry
	effect         runtimecontracts.ActivityEffectClass
	input          map[string]ChannelMapping
	output         map[string]ChannelMapping
	interfaceValue runtimecontracts.PackInterfaceOperation
}

type compiledChannelEvent struct {
	name        channelPlanIdentity
	event       channelPlanIdentity
	fields      map[string]channelPlanIdentity
	fieldSchema map[string]runtimecontracts.ToolInputSchema
	required    map[string]bool
}

type OutboundBindingPlan struct {
	id                 channelPlanIdentity
	structural         SatisfactionPlan
	destination        semanticvalue.Value
	requirements       []Requirement
	runtimeTools       map[string]runtimecontracts.ToolSchemaEntry
	runtimeToolIDs     map[string]channelPlanIdentity
	activityTargets    map[string]PrivateActivityTargetIdentity
	credentialKeys     map[string]string
	registrationTarget string
}

func (p SatisfactionPlan) ChannelIdentity() PackIdentity {
	return p.channel
}

func (p SatisfactionPlan) OperationNames() []string {
	return sortedKeys(p.operations)
}

func (p SatisfactionPlan) Constraint(name string) (runtimecontracts.ToolInputSchema, bool) {
	schema, ok := p.constraints[strings.TrimSpace(name)]
	return schema, ok
}

func (p SatisfactionPlan) EventFieldSchema(eventName, fieldName string) (runtimecontracts.ToolInputSchema, bool) {
	event, ok := p.events[strings.TrimSpace(eventName)]
	if !ok {
		return runtimecontracts.ToolInputSchema{}, false
	}
	field, ok := event.fieldSchema[strings.TrimSpace(fieldName)]
	return field, ok
}

func (p SatisfactionPlan) ConnectorOperation(name string) (string, runtimecontracts.ToolSchemaEntry, error) {
	operation, ok := p.operations[strings.TrimSpace(name)]
	if !ok {
		return "", runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("channel operation %q is not compiled", name)
	}
	return operation.tool.String(), operation.toolSchema, nil
}

func (p SatisfactionPlan) OperationEffectClass(name string) (runtimecontracts.ActivityEffectClass, error) {
	operation, ok := p.operations[strings.TrimSpace(name)]
	if !ok {
		return "", fmt.Errorf("channel operation %q is not compiled", name)
	}
	return operation.effect, nil
}

func (p OutboundBindingPlan) BindingID() string {
	return p.id.String()
}

func (p OutboundBindingPlan) Destination() semanticvalue.Value {
	return p.destination
}

func (p OutboundBindingPlan) OperationNames() []string {
	return p.structural.OperationNames()
}

func (p OutboundBindingPlan) OperationTool(name string) (runtimecontracts.ToolSchemaEntry, error) {
	return p.structural.OperationTool(name)
}

func (p OutboundBindingPlan) OperationEffectClass(name string) (runtimecontracts.ActivityEffectClass, error) {
	return p.structural.OperationEffectClass(name)
}

func (p OutboundBindingPlan) Registration() (CompiledChannelRegistration, bool) {
	return p.structural.Registration()
}

func (p OutboundBindingPlan) PlanGeneration() (plangeneration.Generation, error) {
	return p.structural.Generation()
}

func (p OutboundBindingPlan) CredentialStoreKeys() map[string]string {
	return cloneChannelStringMap(p.credentialKeys)
}

func (p OutboundBindingPlan) RegistrationTarget() string {
	return strings.TrimSpace(p.registrationTarget)
}

func NewOutboundBindingPlan(id string, structural SatisfactionPlan, destination any, requirements []Requirement) (OutboundBindingPlan, error) {
	return NewOutboundBindingPlanWithCredentials(id, structural, destination, requirements, nil)
}

func NewOutboundBindingPlanWithCredentials(id string, structural SatisfactionPlan, destination any, requirements []Requirement, credentialKeys map[string]string) (OutboundBindingPlan, error) {
	return NewOutboundBindingPlanWithRegistration(id, structural, destination, requirements, credentialKeys, "")
}

func NewOutboundBindingPlanWithRegistration(id string, structural SatisfactionPlan, destination any, requirements []Requirement, credentialKeys map[string]string, registrationTarget string) (OutboundBindingPlan, error) {
	bindingID, err := admitChannelPlanIdentity("channel outbound binding id", id)
	if err != nil {
		return OutboundBindingPlan{}, err
	}
	destinationSchema, ok := structural.opaqueTypes["destination"]
	if !ok {
		return OutboundBindingPlan{}, fmt.Errorf("channel %q has no destination opaque type", structural.channel.ID())
	}
	admitted, err := canonicaljson.FromGo(destination)
	if err != nil {
		return OutboundBindingPlan{}, fmt.Errorf("channel outbound binding %q destination admission: %w", bindingID.String(), err)
	}
	if err := destinationSchema.Validate(admitted.Interface()); err != nil {
		return OutboundBindingPlan{}, fmt.Errorf("channel outbound binding %q destination: %w", bindingID.String(), err)
	}
	plan := OutboundBindingPlan{
		id: bindingID, structural: structural, destination: admitted,
		requirements:       cloneRequirements(requirements),
		runtimeTools:       make(map[string]runtimecontracts.ToolSchemaEntry, len(structural.operations)),
		runtimeToolIDs:     make(map[string]channelPlanIdentity, len(structural.operations)),
		activityTargets:    make(map[string]PrivateActivityTargetIdentity, len(structural.operations)),
		credentialKeys:     cloneChannelStringMap(credentialKeys),
		registrationTarget: strings.TrimSpace(registrationTarget),
	}
	generation, err := structural.Generation()
	if err != nil {
		return OutboundBindingPlan{}, fmt.Errorf("channel outbound binding %q generation: %w", bindingID.String(), err)
	}
	generationIdentity := strings.TrimPrefix(generation.Diagnostic(), "sha256:")
	for _, name := range sortedKeys(structural.operations) {
		operation := structural.operations[name]
		runtimeID, err := admitChannelPlanIdentity("channel runtime tool id", "channel."+bindingID.String()+"."+operation.name.String())
		if err != nil {
			return OutboundBindingPlan{}, err
		}
		tool, err := runtimecontracts.NewToolSchemaEntry(
			runtimecontracts.WithToolCategory("channel_operation"),
			runtimecontracts.WithToolDescription("Execute the configured "+operation.name.String()+" operation through channel binding "+bindingID.String()+"."),
			runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerChannel),
			runtimecontracts.WithToolEffect(operation.effect),
			runtimecontracts.WithToolSchemas(operation.inputSchema, operation.outputSchema),
		)
		if err != nil {
			return OutboundBindingPlan{}, fmt.Errorf("channel operation %q runtime tool: %w", operation.name.String(), err)
		}
		targetID, err := admitChannelPlanIdentity("private channel activity target id", runtimecontracts.PrivateChannelActivityPrefix+bindingID.String()+"."+operation.name.String()+".g"+generationIdentity)
		if err != nil {
			return OutboundBindingPlan{}, err
		}
		plan.runtimeToolIDs[name] = runtimeID
		plan.runtimeTools[runtimeID.String()] = tool
		plan.activityTargets[name] = PrivateActivityTargetIdentity{toolID: targetID, generation: generation, credentialKeys: cloneChannelStringMap(credentialKeys)}
	}
	return plan, nil
}

func (p OutboundBindingPlan) RuntimeToolID(operation string) string {
	identity, ok := p.runtimeToolIDs[strings.TrimSpace(operation)]
	if !ok {
		return ""
	}
	return identity.String()
}

func (p OutboundBindingPlan) RuntimeTools() (map[string]runtimecontracts.ToolSchemaEntry, error) {
	out := make(map[string]runtimecontracts.ToolSchemaEntry, len(p.runtimeTools))
	for id, tool := range p.runtimeTools {
		out[id] = tool
	}
	return out, nil
}

func (p OutboundBindingPlan) PrepareOperation(operation string, input any) (string, map[string]any, error) {
	compiled, ok := p.structural.operations[strings.TrimSpace(operation)]
	if !ok {
		return "", nil, fmt.Errorf("channel operation %q is not compiled", operation)
	}
	contextValue := any(map[string]any{})
	if compiled.hasContext {
		contextValue = map[string]any{"destination": p.destination.Interface()}
	}
	prepared, err := p.structural.PrepareOperationInput(operation, input, contextValue)
	if err != nil {
		return "", nil, err
	}
	return p.RuntimeToolID(operation), prepared, nil
}

func (p SatisfactionPlan) CapabilitySubject() (Subject, error) {
	subject := Subject{
		ID: p.channel.ID(), Kind: SubjectChannelPack, Provider: p.provider.String(),
		Source: "channel_pack", Provenance: p.channel.Source().Provenance(), SourcePath: p.channel.Source().Path(),
		Applicability: "installed", Status: StatusAvailable,
		Capabilities: []Capability{{Code: CapabilitySatisfyPackInterface, Target: p.interfaceRef.String()}},
		Evidence: []Evidence{{Kind: "channel_plan", Fields: map[string]string{
			"interface": p.interfaceRef.String(), "channel_hash": p.channel.ManifestHash(),
			"trigger_id": p.trigger.ID(), "trigger_hash": p.trigger.ManifestHash(),
			"connector_id": p.connector.ID(), "connector_hash": p.connector.ManifestHash(),
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
		ID: p.id.String(), Kind: SubjectChannelOutbound, Provider: p.structural.provider.String(),
		Source: "channel_binding", Provenance: p.structural.channel.Source().Provenance(),
		SourcePath: p.structural.channel.Source().Path(), Applicability: "effective",
		Capabilities: []Capability{
			{Code: CapabilityDeliverChannel, Target: p.structural.interfaceRef.String()},
			{Code: CapabilityLowerThroughActivity}, {Code: CapabilityJournalAttempts},
		},
		Requirements: cloneRequirements(p.requirements),
		Evidence: []Evidence{{Kind: "channel_outbound", Fields: map[string]string{
			"interface": p.structural.interfaceRef.String(), "channel_id": p.structural.channel.ID(),
			"channel_hash":   p.structural.channel.ManifestHash(),
			"trigger_hash":   p.structural.trigger.ManifestHash(),
			"connector_hash": p.structural.connector.ManifestHash(),
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

func (p SatisfactionPlan) OperationTool(name string) (runtimecontracts.ToolSchemaEntry, error) {
	operation, ok := p.operations[strings.TrimSpace(name)]
	if !ok {
		return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("channel operation %q is not compiled", name)
	}
	return operation.toolSchema, nil
}

// OperationInputSchema is the provider-neutral operation input after applying
// the finite constraints selected from the concrete connector generation.
func (p SatisfactionPlan) OperationInputSchema(name string) (runtimecontracts.ToolInputSchema, error) {
	operation, ok := p.operations[strings.TrimSpace(name)]
	if !ok {
		return runtimecontracts.ToolInputSchema{}, fmt.Errorf("channel operation %q is not compiled", name)
	}
	return operation.inputSchema, nil
}

func constrainedOperationInputSchema(operation channelOperationDraft, schemas, opaque, constraints map[string]runtimecontracts.ToolInputSchema) (runtimecontracts.ToolInputSchema, error) {
	inputSchema, err := interfaceOperationSchema(operation.interfaceValue.Input, schemas, opaque)
	if err != nil {
		return runtimecontracts.ToolInputSchema{}, err
	}
	for _, key := range sortedKeys(constraints) {
		if !strings.HasPrefix(key, "presentation.") && key != "actions" && !strings.HasPrefix(key, "actions[].") {
			continue
		}
		rootField := strings.Split(strings.TrimSpace(key), ".")[0]
		rootField = strings.TrimSuffix(rootField, "[]")
		if _, ok := operation.interfaceValue.Input[rootField]; !ok {
			continue
		}
		if err := replaceChannelSchemaPath(&inputSchema, key, constraints[key]); err != nil {
			return runtimecontracts.ToolInputSchema{}, fmt.Errorf("channel operation %q selected constraint %q: %w", operation.name.String(), key, err)
		}
	}
	return inputSchema, nil
}

func replaceChannelSchemaPath(root *runtimecontracts.ToolInputSchema, path string, selected runtimecontracts.ToolInputSchema) error {
	if root == nil {
		return fmt.Errorf("input schema is missing")
	}
	parts := strings.Split(strings.ReplaceAll(strings.TrimSpace(path), "[]", ".[]"), ".")
	updated, err := replaceChannelSchemaPathValue(*root, parts, selected, path)
	if err != nil {
		return err
	}
	*root = updated
	return nil
}

func replaceChannelSchemaPathValue(current runtimecontracts.ToolInputSchema, parts []string, selected runtimecontracts.ToolInputSchema, fullPath string) (runtimecontracts.ToolInputSchema, error) {
	for len(parts) > 0 && parts[0] == "" {
		parts = parts[1:]
	}
	if len(parts) == 0 {
		return selected, nil
	}
	part := parts[0]
	if part == "[]" {
		items, ok := current.ItemsSchema()
		if !ok {
			return runtimecontracts.ToolInputSchema{}, fmt.Errorf("array item schema is missing for %q", fullPath)
		}
		updated, err := replaceChannelSchemaPathValue(items, parts[1:], selected, fullPath)
		if err != nil {
			return runtimecontracts.ToolInputSchema{}, err
		}
		return current.WithItems(updated)
	}
	property, ok := current.Property(part)
	if !ok {
		return runtimecontracts.ToolInputSchema{}, fmt.Errorf("schema path %q is missing", fullPath)
	}
	updated, err := replaceChannelSchemaPathValue(property, parts[1:], selected, fullPath)
	if err != nil {
		return runtimecontracts.ToolInputSchema{}, err
	}
	return current.WithProperty(part, updated)
}

func (p SatisfactionPlan) PrepareOperationInput(name string, input, context any) (map[string]any, error) {
	operation, ok := p.operations[strings.TrimSpace(name)]
	if !ok {
		return nil, fmt.Errorf("channel operation %q is not compiled", name)
	}
	if err := operation.inputSchema.Validate(input); err != nil {
		return nil, fmt.Errorf("channel operation %q input: %w", name, err)
	}
	if err := operation.contextSchema.Validate(context); err != nil {
		return nil, fmt.Errorf("channel operation %q context: %w", name, err)
	}
	environment := map[string]any{"input": input, "context": context}
	out := map[string]any{}
	for _, mapping := range operation.input {
		if mapping.usesEach {
			itemsValue, ok := mapping.each.lookup(environment)
			if !ok {
				return nil, fmt.Errorf("channel operation %q source %q is missing", name, mapping.each.syntax)
			}
			items, ok := itemsValue.([]any)
			if !ok {
				return nil, fmt.Errorf("channel operation %q source %q is not an array", name, mapping.each.syntax)
			}
			projected := make([]any, 0, len(items))
			for _, item := range items {
				object := map[string]any{}
				for _, itemMapping := range mapping.item {
					value, ok := itemMapping.source.lookup(map[string]any{"item": item})
					if !ok {
						return nil, fmt.Errorf("channel operation %q item source %q is missing", name, itemMapping.source.syntax)
					}
					if err := itemMapping.target.set(object, value); err != nil {
						return nil, err
					}
				}
				if mapping.wrapItemAsList {
					projected = append(projected, []any{object})
				} else {
					projected = append(projected, object)
				}
			}
			if err := mapping.target.set(out, projected); err != nil {
				return nil, err
			}
			continue
		}
		value, ok := mapping.source.lookup(environment)
		if !ok {
			return nil, fmt.Errorf("channel operation %q source %q is missing", name, mapping.source.syntax)
		}
		if err := mapping.target.set(out, value); err != nil {
			return nil, err
		}
	}
	if err := operation.toolSchema.InputSchema().Validate(out); err != nil {
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
	return runtimecontracts.NewToolInputSchema(
		runtimecontracts.ToolSchemaObject,
		runtimecontracts.ToolSchemaProperties(properties),
		runtimecontracts.ToolSchemaRequired(required...),
		runtimecontracts.ToolSchemaAdditionalPropertiesAllowed(false),
	)
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
	interfaceRef, err := admitChannelPlanIdentity("channel interface reference", channel.Envelope.Implements[0])
	if err != nil {
		return SatisfactionPlan{}, err
	}
	definition, ok := registry.Lookup(interfaceRef.String())
	if !ok {
		return SatisfactionPlan{}, fmt.Errorf("channel pack %q implements unknown interface %q", channel.Envelope.ID, interfaceRef.String())
	}
	trigger, err := resolveTriggerDependency(channel, triggers)
	if err != nil {
		return SatisfactionPlan{}, err
	}
	connector, err := resolveConnectorDependency(channel, connectors)
	if err != nil {
		return SatisfactionPlan{}, err
	}
	if err := validateAcceptedTriggerDescriptor(trigger); err != nil {
		return SatisfactionPlan{}, err
	}
	if err := validateAcceptedConnectorDescriptor(connector); err != nil {
		return SatisfactionPlan{}, err
	}
	provider, err := admitChannelPlanIdentity("channel provider", channel.Manifest.Provider)
	if err != nil {
		return SatisfactionPlan{}, err
	}
	if provider.String() != strings.TrimSpace(trigger.Provider) || provider.String() != strings.TrimSpace(connector.Provider) {
		return SatisfactionPlan{}, fmt.Errorf("channel pack %q provider %q does not match trigger %q and connector %q providers", channel.Envelope.ID, provider.String(), trigger.Provider, connector.Provider)
	}
	if err := exactKeySet("channel opaque_types", channel.Manifest.OpaqueTypes, interfaceOpaqueSlots(definition)); err != nil {
		return SatisfactionPlan{}, err
	}
	for name, schema := range channel.Manifest.OpaqueTypes {
		if err := schema.ValidateDefinition(); err != nil {
			return SatisfactionPlan{}, fmt.Errorf("channel opaque type %s: %w", name, err)
		}
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
		interfaceRef: interfaceRef,
		channel:      identityFromEnvelope(channel.Envelope, channel.Source), trigger: trigger.Identity, connector: connector.Identity,
		provider: provider, triggerGeneration: trigger.Generation, opaqueTypes: cloneSchemaMap(channel.Manifest.OpaqueTypes),
		schemas:    cloneSchemaMap(definition.Schemas),
		operations: map[string]compiledChannelOperation{}, events: map[string]compiledChannelEvent{}, constraints: map[string]runtimecontracts.ToolInputSchema{},
	}
	drafts := make(map[string]channelOperationDraft, len(definition.Operations))
	for _, name := range sortedKeys(definition.Operations) {
		binding := channel.Manifest.Operations[name]
		operation := definition.Operations[name]
		effect := runtimecontracts.NormalizeActivityEffectClass(operation.EffectClass)
		if effect == "" {
			return SatisfactionPlan{}, fmt.Errorf("channel operation %q has unsupported effect class %q", name, operation.EffectClass)
		}
		tool, ok := connector.Tools[strings.TrimSpace(binding.Tool)]
		if !ok {
			return SatisfactionPlan{}, fmt.Errorf("channel operation %q references unknown connector tool %q", name, binding.Tool)
		}
		if tool.Effect() != effect {
			return SatisfactionPlan{}, fmt.Errorf("channel operation %q effect class does not match connector tool %q", name, binding.Tool)
		}
		operationID, err := admitChannelPlanIdentity("channel operation identity", name)
		if err != nil {
			return SatisfactionPlan{}, err
		}
		toolID, err := admitChannelPlanIdentity("channel connector tool identity", binding.Tool)
		if err != nil {
			return SatisfactionPlan{}, err
		}
		drafts[name] = channelOperationDraft{name: operationID, tool: toolID, toolSchema: tool, effect: effect, input: binding.Input, output: binding.Output, interfaceValue: operation}
	}
	plan.constraints, err = compileSelectedChannelConstraints(drafts, plan.schemas, plan.opaqueTypes)
	if err != nil {
		return SatisfactionPlan{}, err
	}
	for _, name := range sortedKeys(drafts) {
		draft := drafts[name]
		inputTopology, outputTopology, err := validateOperationBinding(name, draft.interfaceValue, channel.Manifest.Operations[name], definition.Schemas, channel.Manifest.OpaqueTypes, plan.constraints, draft.toolSchema)
		if err != nil {
			return SatisfactionPlan{}, err
		}
		compiled, err := compileAdmittedChannelOperation(draft, inputTopology, outputTopology, plan.schemas, plan.opaqueTypes, plan.constraints)
		if err != nil {
			return SatisfactionPlan{}, err
		}
		plan.operations[name] = compiled
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
		eventName, err := admitChannelPlanIdentity("channel event identity", name)
		if err != nil {
			return SatisfactionPlan{}, err
		}
		triggerEvent, err := admitChannelPlanIdentity("channel trigger event identity", binding.Event)
		if err != nil {
			return SatisfactionPlan{}, err
		}
		fields := make(map[string]channelPlanIdentity, len(binding.Fields))
		fieldSchemas := make(map[string]runtimecontracts.ToolInputSchema, len(descriptor.Fields))
		required := make(map[string]bool, len(descriptor.Fields))
		for fieldName, target := range binding.Fields {
			fieldID, err := admitChannelPlanIdentity("channel event field identity", target)
			if err != nil {
				return SatisfactionPlan{}, err
			}
			fields[fieldName] = fieldID
		}
		for fieldName, field := range descriptor.Fields {
			fieldSchemas[fieldName] = field.Schema
			required[fieldName] = field.Required
		}
		plan.events[name] = compiledChannelEvent{name: eventName, event: triggerEvent, fields: fields, fieldSchema: fieldSchemas, required: required}
	}
	if channel.Manifest.Registration != nil {
		plan.registration, err = compileChannelRegistration(*channel.Manifest.Registration, connector, provider)
		if err != nil {
			return SatisfactionPlan{}, fmt.Errorf("channel pack %q registration: %w", channel.Envelope.ID, err)
		}
	}
	plan.generation, err = compileSatisfactionPlanGeneration(plan)
	if err != nil {
		return SatisfactionPlan{}, err
	}
	return plan, nil
}

func validateAcceptedTriggerDescriptor(trigger TriggerPackDescriptor) error {
	if !trigger.Generation.Valid() {
		return fmt.Errorf("accepted trigger %q generation is missing", trigger.Identity.ID())
	}
	for eventName, event := range trigger.Events {
		for fieldName, field := range event.Fields {
			if err := field.Schema.ValidateDefinition(); err != nil {
				return fmt.Errorf("accepted trigger %q event %q field %q schema: %w", trigger.Identity.ID(), eventName, fieldName, err)
			}
		}
	}
	return nil
}

func validateAcceptedConnectorDescriptor(connector ConnectorPackDescriptor) error {
	for toolName, tool := range connector.Tools {
		if err := tool.InputSchema().ValidateDefinition(); err != nil {
			return fmt.Errorf("accepted connector %q tool %q input schema: %w", connector.Identity.ID(), toolName, err)
		}
		if err := tool.OutputSchema().ValidateDefinition(); err != nil {
			return fmt.Errorf("accepted connector %q tool %q output schema: %w", connector.Identity.ID(), toolName, err)
		}
	}
	return nil
}

type selectedChannelConstraint struct {
	key        string
	sourcePath string
	itemField  string
	requireMax bool
}

func compileSelectedChannelConstraints(operations map[string]channelOperationDraft, schemas, opaqueTypes map[string]runtimecontracts.ToolInputSchema) (map[string]runtimecontracts.ToolInputSchema, error) {
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
			operation, ok := operations[operationName]
			if !ok {
				return nil, fmt.Errorf("selected channel constraint %q requires operation %q", definition.key, operationName)
			}
			interfaceSchema, err := selectedConstraintInterfaceSchema(operation, definition, schemas, opaqueTypes)
			if err != nil {
				return nil, err
			}
			connectorSchema, err := selectedConstraintConnectorSchema(operation, definition)
			if err != nil {
				return nil, err
			}
			if selected == nil {
				initial := *interfaceSchema
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
			switch selected.Kind() {
			case runtimecontracts.ToolSchemaString:
				if _, ok := selected.MaxLength(); !ok {
					return nil, fmt.Errorf("selected channel constraint %q requires a finite maxLength", definition.key)
				}
			case runtimecontracts.ToolSchemaArray:
				if _, ok := selected.MaxItems(); !ok {
					return nil, fmt.Errorf("selected channel constraint %q requires a finite maxItems", definition.key)
				}
			default:
				return nil, fmt.Errorf("selected channel constraint %q has unsupported type %q", definition.key, selected.Kind())
			}
		}
		constraints[definition.key] = *selected
	}
	return constraints, nil
}

func selectedConstraintInterfaceSchema(operation channelOperationDraft, definition selectedChannelConstraint, schemas, opaque map[string]runtimecontracts.ToolInputSchema) (*runtimecontracts.ToolInputSchema, error) {
	root, err := operationSourceSchema(operation.interfaceValue, definition.sourcePath, schemas, opaque)
	if err != nil {
		return nil, fmt.Errorf("selected channel constraint %q: %w", definition.key, err)
	}
	if definition.itemField == "" {
		return root, nil
	}
	items, ok := root.ItemsSchema()
	if root.Kind() != runtimecontracts.ToolSchemaArray || !ok {
		return nil, fmt.Errorf("selected channel constraint %q source must be an item array", definition.key)
	}
	field, ok := schemaAt(items, []string{definition.itemField})
	if !ok {
		return nil, fmt.Errorf("selected channel constraint %q source item field is missing", definition.key)
	}
	return field, nil
}

func selectedConstraintConnectorSchema(operation channelOperationDraft, definition selectedChannelConstraint) (*runtimecontracts.ToolInputSchema, error) {
	for target, mapping := range operation.input {
		if definition.itemField == "" && mapping.From == definition.sourcePath {
			schema, ok := schemaAt(operation.toolSchema.InputSchema(), strings.Split(target, "."))
			if !ok {
				break
			}
			return schema, nil
		}
		if mapping.Each != definition.sourcePath {
			continue
		}
		targetSchema, ok := schemaAt(operation.toolSchema.InputSchema(), strings.Split(target, "."))
		if !ok {
			break
		}
		if definition.itemField == "" {
			return targetSchema, nil
		}
		itemSchema := *targetSchema
		items, ok := itemSchema.ItemsSchema()
		if itemSchema.Kind() != runtimecontracts.ToolSchemaArray || !ok {
			break
		}
		itemSchema = items
		if itemSchema.Kind() == runtimecontracts.ToolSchemaArray {
			items, ok = itemSchema.ItemsSchema()
			if !ok {
				break
			}
			itemSchema = items
		}
		for _, itemMappings := range mapping.Item {
			for itemTarget, itemMapping := range itemMappings {
				if itemMapping.From != "item."+definition.itemField {
					continue
				}
				field, ok := schemaAt(itemSchema, strings.Split(itemTarget, "."))
				if ok {
					return field, nil
				}
			}
		}
	}
	return nil, fmt.Errorf("selected channel constraint %q is not mapped by operation %q", definition.key, operation.name.String())
}

func intersectSelectedConstraint(name string, left, right *runtimecontracts.ToolInputSchema) (*runtimecontracts.ToolInputSchema, error) {
	if left == nil || right == nil || left.Kind() != right.Kind() {
		return nil, fmt.Errorf("selected channel constraint %q has incompatible types", name)
	}
	out, err := left.IntersectBounds(*right)
	if err != nil {
		return nil, fmt.Errorf("selected channel constraint %q: %w", name, err)
	}
	return &out, nil
}

func resolveTriggerDependency(channel LoadedChannelPack, descriptors []TriggerPackDescriptor) (TriggerPackDescriptor, error) {
	id := strings.TrimSpace(channel.Envelope.Requires.Packs[TypeTrigger])
	var matches []TriggerPackDescriptor
	for _, descriptor := range descriptors {
		if descriptor.Identity.ID() == id {
			matches = append(matches, descriptor)
		}
	}
	if len(matches) != 1 {
		return TriggerPackDescriptor{}, fmt.Errorf("channel pack %q trigger dependency %q resolved %d accepted packs; require exactly one", channel.Envelope.ID, id, len(matches))
	}
	if matches[0].Identity.Type() != TypeTrigger {
		return TriggerPackDescriptor{}, fmt.Errorf("channel pack %q dependency %q has wrong role %q", channel.Envelope.ID, id, matches[0].Identity.Type())
	}
	return matches[0], nil
}

func resolveConnectorDependency(channel LoadedChannelPack, descriptors []ConnectorPackDescriptor) (ConnectorPackDescriptor, error) {
	id := strings.TrimSpace(channel.Envelope.Requires.Packs[TypeConnector])
	var matches []ConnectorPackDescriptor
	for _, descriptor := range descriptors {
		if descriptor.Identity.ID() == id {
			matches = append(matches, descriptor)
		}
	}
	if len(matches) != 1 {
		return ConnectorPackDescriptor{}, fmt.Errorf("channel pack %q connector dependency %q resolved %d accepted packs; require exactly one", channel.Envelope.ID, id, len(matches))
	}
	if matches[0].Identity.Type() != TypeConnector {
		return ConnectorPackDescriptor{}, fmt.Errorf("channel pack %q dependency %q has wrong role %q", channel.Envelope.ID, id, matches[0].Identity.Type())
	}
	return matches[0], nil
}

func validateOperationBinding(name string, operation runtimecontracts.PackInterfaceOperation, binding ChannelOperationBinding, schemas, opaque, constraints map[string]runtimecontracts.ToolInputSchema, tool runtimecontracts.ToolSchemaEntry) (compiledChannelMappingTopology, compiledChannelMappingTopology, error) {
	inputTopology, err := compileChannelMappingTopology("channel operation "+name+" input", binding.Input)
	if err != nil {
		return compiledChannelMappingTopology{}, compiledChannelMappingTopology{}, err
	}
	outputTopology, err := compileChannelMappingTopology("channel operation "+name+" output", binding.Output)
	if err != nil {
		return compiledChannelMappingTopology{}, compiledChannelMappingTopology{}, err
	}
	operationID, err := admitChannelPlanIdentity("channel operation identity", name)
	if err != nil {
		return compiledChannelMappingTopology{}, compiledChannelMappingTopology{}, err
	}
	compiledOperation := channelOperationDraft{name: operationID, interfaceValue: operation}
	usedSources := newChannelPathCardinality("channel operation " + name + " source")
	usedCollections := newChannelPathCardinality("channel operation " + name + " each source")
	for _, target := range inputTopology.Targets {
		mapping := binding.Input[target]
		targetSchema, ok := schemaAt(tool.InputSchema(), strings.Split(target, "."))
		if !ok {
			return compiledChannelMappingTopology{}, compiledChannelMappingTopology{}, fmt.Errorf("channel operation %q input target %q is absent from connector schema", name, target)
		}
		if mapping.Each != "" {
			sourceSchema, err := operationEffectiveSourceSchema(compiledOperation, mapping.Each, schemas, opaque, constraints)
			if err != nil {
				return compiledChannelMappingTopology{}, compiledChannelMappingTopology{}, fmt.Errorf("channel operation %q: %w", name, err)
			}
			sourceItems, sourceHasItems := sourceSchema.ItemsSchema()
			targetItems, targetHasItems := targetSchema.ItemsSchema()
			if sourceSchema.Kind() != runtimecontracts.ToolSchemaArray || targetSchema.Kind() != runtimecontracts.ToolSchemaArray || !sourceHasItems || !targetHasItems {
				return compiledChannelMappingTopology{}, compiledChannelMappingTopology{}, fmt.Errorf("channel operation %q each mapping %q -> %q requires array schemas", name, mapping.Each, target)
			}
			itemSources, err := validateEachItem(name, mapping, inputTopology.ItemTargets[target], sourceItems, targetItems)
			if err != nil {
				return compiledChannelMappingTopology{}, compiledChannelMappingTopology{}, err
			}
			if err := usedCollections.add(mapping.Each); err != nil {
				return compiledChannelMappingTopology{}, compiledChannelMappingTopology{}, err
			}
			for _, source := range itemSources {
				if err := usedSources.add(mapping.Each + "[]." + source); err != nil {
					return compiledChannelMappingTopology{}, compiledChannelMappingTopology{}, err
				}
			}
			continue
		}
		sourceSchema, err := operationEffectiveSourceSchema(compiledOperation, mapping.From, schemas, opaque, constraints)
		if err != nil {
			return compiledChannelMappingTopology{}, compiledChannelMappingTopology{}, fmt.Errorf("channel operation %q: %w", name, err)
		}
		if err := validateDirectionalRelation(name+" input "+mapping.From+" -> "+target, sourceSchema, targetSchema); err != nil {
			return compiledChannelMappingTopology{}, compiledChannelMappingTopology{}, err
		}
		if err := usedSources.add(mapping.From); err != nil {
			return compiledChannelMappingTopology{}, compiledChannelMappingTopology{}, err
		}
	}
	if err := requiredConnectorInputsMapped(name, tool.InputSchema(), inputTopology.Targets); err != nil {
		return compiledChannelMappingTopology{}, compiledChannelMappingTopology{}, err
	}
	if err := interfaceInputsConsumed(name, operation, schemas, opaque, usedSources.values()); err != nil {
		return compiledChannelMappingTopology{}, compiledChannelMappingTopology{}, err
	}
	if len(operation.Output) == 0 {
		if len(binding.Output) != 0 {
			return compiledChannelMappingTopology{}, compiledChannelMappingTopology{}, fmt.Errorf("channel operation %q has no interface output and must not map connector output", name)
		}
		return inputTopology, outputTopology, nil
	}
	outputSources := newChannelPathCardinality("channel operation " + name + " output source")
	for _, target := range outputTopology.Targets {
		mapping := binding.Output[target]
		targetSchema, err := operationOutputSchema(operation, target, schemas, opaque)
		if err != nil {
			return compiledChannelMappingTopology{}, compiledChannelMappingTopology{}, fmt.Errorf("channel operation %q: %w", name, err)
		}
		sourcePath := strings.TrimPrefix(mapping.From, "result.")
		if sourcePath == mapping.From {
			return compiledChannelMappingTopology{}, compiledChannelMappingTopology{}, fmt.Errorf("channel operation %q output source %q must start with result.", name, mapping.From)
		}
		sourceSchema, ok := schemaAt(tool.OutputSchema(), strings.Split(sourcePath, "."))
		if !ok {
			return compiledChannelMappingTopology{}, compiledChannelMappingTopology{}, fmt.Errorf("channel operation %q output source %q is absent from connector schema", name, mapping.From)
		}
		if err := validateDirectionalRelation(name+" output "+mapping.From+" -> "+target, sourceSchema, targetSchema); err != nil {
			return compiledChannelMappingTopology{}, compiledChannelMappingTopology{}, err
		}
		if err := outputSources.add(mapping.From); err != nil {
			return compiledChannelMappingTopology{}, compiledChannelMappingTopology{}, err
		}
	}
	if err := requiredInterfaceOutputsMapped(name, operation, outputTopology.Targets, opaque); err != nil {
		return compiledChannelMappingTopology{}, compiledChannelMappingTopology{}, err
	}
	return inputTopology, outputTopology, nil
}

func compileAdmittedChannelOperation(draft channelOperationDraft, inputTopology, outputTopology compiledChannelMappingTopology, schemas, opaque, constraints map[string]runtimecontracts.ToolInputSchema) (compiledChannelOperation, error) {
	inputSchema, err := constrainedOperationInputSchema(draft, schemas, opaque, constraints)
	if err != nil {
		return compiledChannelOperation{}, err
	}
	contextSchema, err := interfaceOperationSchema(draft.interfaceValue.Context, schemas, opaque)
	if err != nil {
		return compiledChannelOperation{}, fmt.Errorf("channel operation %q context schema: %w", draft.name.String(), err)
	}
	outputSchema, err := interfaceOperationSchema(draft.interfaceValue.Output, schemas, opaque)
	if err != nil {
		return compiledChannelOperation{}, fmt.Errorf("channel operation %q output schema: %w", draft.name.String(), err)
	}
	inputMappings, err := compileAdmittedChannelMappings(draft.input, inputTopology, draft.toolSchema.InputSchema())
	if err != nil {
		return compiledChannelOperation{}, fmt.Errorf("channel operation %q input mapping: %w", draft.name.String(), err)
	}
	outputMappings, err := compileAdmittedChannelMappings(draft.output, outputTopology, runtimecontracts.ToolInputSchema{})
	if err != nil {
		return compiledChannelOperation{}, fmt.Errorf("channel operation %q output mapping: %w", draft.name.String(), err)
	}
	fields := make(map[string]runtimecontracts.CompiledResultField, len(outputMappings))
	for _, mapping := range outputMappings {
		fields[mapping.target.syntax] = runtimecontracts.CompiledResultField{From: mapping.source.syntax}
	}
	tool := draft.toolSchema
	if len(fields) > 0 {
		tool, err = tool.WithCompiledResult(runtimecontracts.CompiledResultProjection{Fields: fields, OutputSchema: outputSchema})
		if err != nil {
			return compiledChannelOperation{}, fmt.Errorf("channel operation %q result projection: %w", draft.name.String(), err)
		}
	}
	return compiledChannelOperation{
		name: draft.name, tool: draft.tool, toolSchema: tool, effect: draft.effect,
		inputSchema: inputSchema, contextSchema: contextSchema, outputSchema: outputSchema,
		hasContext: len(draft.interfaceValue.Context) > 0,
		input:      inputMappings, output: outputMappings,
	}, nil
}

func compileAdmittedChannelMappings(mappings map[string]ChannelMapping, topology compiledChannelMappingTopology, targetSchema runtimecontracts.ToolInputSchema) ([]compiledChannelMapping, error) {
	out := make([]compiledChannelMapping, 0, len(topology.Targets))
	for _, target := range topology.Targets {
		mapping := mappings[target]
		targetPath, err := compileChannelPath(target)
		if err != nil {
			return nil, err
		}
		compiled := compiledChannelMapping{target: targetPath}
		if mapping.Each != "" {
			compiled.each, err = compileChannelPath(mapping.Each)
			if err != nil {
				return nil, err
			}
			compiled.usesEach = true
			for _, itemTarget := range topology.ItemTargets[target] {
				itemMapping := mapping.Item[0][itemTarget]
				compiledTarget, err := compileChannelPath(itemTarget)
				if err != nil {
					return nil, err
				}
				compiledSource, err := compileChannelPath(itemMapping.From)
				if err != nil {
					return nil, err
				}
				compiled.item = append(compiled.item, compiledChannelMapping{target: compiledTarget, source: compiledSource})
			}
			if !targetSchema.IsZero() {
				resolved, ok := schemaAt(targetSchema, targetPath.segments)
				if ok {
					items, hasItems := resolved.ItemsSchema()
					compiled.wrapItemAsList = hasItems && items.Kind() == runtimecontracts.ToolSchemaArray
				}
			}
		} else {
			compiled.source, err = compileChannelPath(mapping.From)
			if err != nil {
				return nil, err
			}
		}
		out = append(out, compiled)
	}
	return out, nil
}

func validateEachItem(name string, mapping ChannelMapping, itemTargets []string, sourceItem, targetItem runtimecontracts.ToolInputSchema) ([]string, error) {
	if len(mapping.Item) != 1 || len(mapping.Item[0]) == 0 {
		return nil, fmt.Errorf("channel operation %q each mapping must construct exactly one object per source item", name)
	}
	if targetItem.Kind() == runtimecontracts.ToolSchemaArray {
		items, ok := targetItem.ItemsSchema()
		if !ok {
			return nil, fmt.Errorf("channel operation %q target row schema has no item", name)
		}
		targetItem = items
	}
	if sourceItem.Kind() != runtimecontracts.ToolSchemaObject || targetItem.Kind() != runtimecontracts.ToolSchemaObject {
		return nil, fmt.Errorf("channel operation %q each item source and target must be objects", name)
	}
	used := newChannelPathCardinality("channel operation " + name + " each item source")
	for _, target := range itemTargets {
		itemMapping := mapping.Item[0][target]
		targetSchema, ok := schemaAt(targetItem, strings.Split(target, "."))
		if !ok {
			return nil, fmt.Errorf("channel operation %q each item target %q is absent", name, target)
		}
		source := strings.TrimPrefix(itemMapping.From, "item.")
		if source == itemMapping.From {
			return nil, fmt.Errorf("channel operation %q each item source %q must start with item.", name, itemMapping.From)
		}
		sourceSchema, ok := schemaAt(sourceItem, strings.Split(source, "."))
		if !ok {
			return nil, fmt.Errorf("channel operation %q each item source %q is absent", name, itemMapping.From)
		}
		if err := validateDirectionalRelation(name+" each item "+itemMapping.From+" -> "+target, sourceSchema, targetSchema); err != nil {
			return nil, err
		}
		if err := used.add(source); err != nil {
			return nil, err
		}
	}
	if err := requiredSchemaPathsMapped(name+" each item", targetItem, itemTargets); err != nil {
		return nil, err
	}
	return used.values(), nil
}

func validateEventBinding(name string, event runtimecontracts.PackInterfaceEvent, binding ChannelEventBinding, schemas, opaque map[string]runtimecontracts.ToolInputSchema, descriptor TriggerEvent) error {
	if err := exactKeySet("channel event "+name+" fields", binding.Fields, requiredInterfaceFieldPaths(event.RequiredFields, opaque)); err != nil {
		return err
	}
	targets := newChannelPathCardinality("channel event " + name + " target")
	sources := newChannelPathCardinality("channel event " + name + " source")
	for _, target := range sortedKeys(binding.Fields) {
		source := binding.Fields[target]
		if err := targets.add(target); err != nil {
			return err
		}
		if err := sources.add(source); err != nil {
			return err
		}
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
		if err := validateDirectionalRelation(name+" event "+source+" -> "+target, &field.Schema, targetSchema); err != nil {
			return err
		}
	}
	return nil
}

func validateOpaqueSchema(subject string, schema runtimecontracts.ToolInputSchema) error {
	switch schema.Kind() {
	case runtimecontracts.ToolSchemaString:
		minimum, ok := schema.MinLength()
		if !ok || minimum < 1 {
			return fmt.Errorf("%s string must declare minLength >= 1", subject)
		}
		return nil
	case runtimecontracts.ToolSchemaObject:
		properties := schema.Properties()
		allowed, declared := schema.AdditionalPropertiesAllowed()
		if len(properties) == 0 || !declared || allowed {
			return fmt.Errorf("%s object must be non-empty and additionalProperties false", subject)
		}
		required := stringSet(schema.RequiredProperties())
		if len(required) != len(properties) {
			return fmt.Errorf("%s object must require every property", subject)
		}
		for name, property := range properties {
			if _, ok := required[name]; !ok {
				return fmt.Errorf("%s object property %q must be required", subject, name)
			}
			switch property.Kind() {
			case runtimecontracts.ToolSchemaString:
				minimum, ok := property.MinLength()
				if !ok || minimum < 1 {
					return fmt.Errorf("%s object string leaf %q must be non-empty", subject, name)
				}
			case runtimecontracts.ToolSchemaInteger, runtimecontracts.ToolSchemaBoolean:
			case runtimecontracts.ToolSchemaObject:
				if err := validateOpaqueSchema(subject+"."+name, property); err != nil {
					return err
				}
			default:
				return fmt.Errorf("%s object leaf %q has unsupported type %q", subject, name, property.Kind())
			}
		}
		return nil
	default:
		return fmt.Errorf("%s must be a non-empty string or closed object, got %q", subject, schema.Kind())
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

func operationEffectiveSourceSchema(operation channelOperationDraft, path string, schemas, opaque, constraints map[string]runtimecontracts.ToolInputSchema) (*runtimecontracts.ToolInputSchema, error) {
	parts := strings.Split(path, ".")
	if len(parts) < 2 || (parts[0] != "input" && parts[0] != "context") {
		return nil, fmt.Errorf("source %q must start with input. or context.", path)
	}
	if parts[0] == "context" {
		return operationSourceSchema(operation.interfaceValue, path, schemas, opaque)
	}
	root, err := constrainedOperationInputSchema(operation, schemas, opaque, constraints)
	if err != nil {
		return nil, err
	}
	resolved, ok := schemaAt(root, parts[1:])
	if !ok {
		return nil, fmt.Errorf("source %q is absent from its effective interface schema", path)
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
		return &schema, nil
	}
	name := strings.TrimSpace(field.Opaque)
	schema, ok := opaque[name]
	if !ok {
		return nil, fmt.Errorf("opaque type %q is missing", name)
	}
	return &schema, nil
}

func schemaAt(schema runtimecontracts.ToolInputSchema, path []string) (*runtimecontracts.ToolInputSchema, bool) {
	current := schema
	for _, segment := range path {
		if current.Kind() != runtimecontracts.ToolSchemaObject {
			return nil, false
		}
		next, ok := current.Property(segment)
		if !ok {
			return nil, false
		}
		current = next
	}
	return &current, true
}

func requiredConnectorInputsMapped(name string, schema runtimecontracts.ToolInputSchema, mapped []string) error {
	required := schemaRequiredLeafPaths("", schema)
	for index := range required {
		required[index] = strings.TrimPrefix(required[index], ".")
	}
	return validateRequiredPathCardinality("channel operation "+name+" connector input", required, mapped)
}

func interfaceInputsConsumed(name string, operation runtimecontracts.PackInterfaceOperation, schemas, opaque map[string]runtimecontracts.ToolInputSchema, used []string) error {
	var required []string
	for group, fields := range map[string]map[string]runtimecontracts.PackInterfaceField{"input": operation.Input, "context": operation.Context} {
		for fieldName, field := range fields {
			prefix := group + "." + fieldName
			resolved, err := resolvedInterfaceFieldSchema(field, schemas, opaque)
			if err != nil {
				return err
			}
			required = append(required, schemaRequiredLeafPaths(prefix, *resolved)...)
		}
	}
	return validateRequiredPathCardinality("channel operation "+name+" interface input/context", required, used)
}

func requiredInterfaceOutputsMapped(name string, operation runtimecontracts.PackInterfaceOperation, mapped []string, opaque map[string]runtimecontracts.ToolInputSchema) error {
	var required []string
	for fieldName, field := range operation.Output {
		paths := []string{fieldName}
		if field.Opaque != "" {
			paths = schemaRequiredLeafPaths(fieldName, opaque[field.Opaque])
		}
		required = append(required, paths...)
	}
	return validateRequiredPathCardinality("channel operation "+name+" interface output", required, mapped)
}

func requiredSchemaPathsMapped(subject string, schema runtimecontracts.ToolInputSchema, mapped []string) error {
	required := schemaRequiredLeafPaths("", schema)
	for index := range required {
		required[index] = strings.TrimPrefix(required[index], ".")
	}
	return validateRequiredPathCardinality(subject, required, mapped)
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
	switch schema.Kind() {
	case runtimecontracts.ToolSchemaArray:
		items, ok := schema.ItemsSchema()
		if !ok {
			return []string{prefix}
		}
		return schemaRequiredLeafPaths(prefix+"[]", items)
	case runtimecontracts.ToolSchemaObject:
	default:
		return []string{prefix}
	}
	var out []string
	for _, name := range schema.RequiredProperties() {
		property, ok := schema.Property(name)
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

func identityFromEnvelope(envelope Envelope, source PackSource) PackIdentity {
	return MustPackIdentity(envelope.ID, envelope.Version, envelope.ManifestHash, envelope.Type, source)
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

func ValidateOpaqueValue(schema runtimecontracts.ToolInputSchema, value any) error {
	switch schema.Kind() {
	case runtimecontracts.ToolSchemaString:
		text, ok := value.(string)
		if !ok {
			return fmt.Errorf("opaque value must be a string")
		}
		length := len([]rune(text))
		minimum, hasMinimum := schema.MinLength()
		maximum, hasMaximum := schema.MaxLength()
		if hasMinimum && length < minimum || hasMaximum && length > maximum {
			return fmt.Errorf("opaque string length is outside admitted bounds")
		}
		if pattern := schema.Pattern(); pattern != "" && !regexp.MustCompile(pattern).MatchString(text) {
			return fmt.Errorf("opaque string does not match admitted pattern")
		}
		return nil
	case runtimecontracts.ToolSchemaObject:
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("opaque value must be an object")
		}
		properties := schema.Properties()
		if len(object) != len(properties) {
			return fmt.Errorf("opaque object has undeclared or missing properties")
		}
		for name, property := range properties {
			child, ok := object[name]
			if !ok {
				return fmt.Errorf("opaque object property %q is required", name)
			}
			switch property.Kind() {
			case runtimecontracts.ToolSchemaString, runtimecontracts.ToolSchemaObject:
				if err := ValidateOpaqueValue(property, child); err != nil {
					return fmt.Errorf("opaque object property %q: %w", name, err)
				}
			case runtimecontracts.ToolSchemaInteger:
				if _, ok := exactInteger(child); !ok {
					return fmt.Errorf("opaque object property %q must be an integer", name)
				}
			case runtimecontracts.ToolSchemaBoolean:
				if _, ok := child.(bool); !ok {
					return fmt.Errorf("opaque object property %q must be a boolean", name)
				}
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported opaque schema type %q", schema.Kind())
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
