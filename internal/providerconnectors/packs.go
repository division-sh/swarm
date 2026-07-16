package providerconnectors

import (
	"embed"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/platform"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

//go:embed packs/*/pack.yaml packs/*/connector.yaml catalog/generated-packs.yaml catalog/generator-profiles/*.yaml
var builtinConnectorPackFS embed.FS

const connectorPackRoot = "packs"

type ConnectorManifest struct {
	Provider   string                                      `yaml:"provider"`
	Generation *GenerationEvidence                         `yaml:"generation,omitempty"`
	Tools      map[string]runtimecontracts.ToolSchemaEntry `yaml:"tools"`
}

type LoadedPack struct {
	Envelope     packs.Envelope
	Manifest     ConnectorManifest
	ManifestBody []byte
	Directory    string
	Source       string
}

type PackRegistry struct {
	byProvider map[string]map[string]LoadedPack
}

func (r *PackRegistry) PackDescriptors() []packs.ConnectorPackDescriptor {
	if r == nil {
		return nil
	}
	byID := map[string]LoadedPack{}
	for _, byTool := range r.byProvider {
		for _, pack := range byTool {
			byID[strings.TrimSpace(pack.Envelope.ID)] = pack
		}
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]packs.ConnectorPackDescriptor, 0, len(ids))
	for _, id := range ids {
		pack := cloneLoadedPack(byID[id])
		tools := runtimecontracts.CloneToolSchemaEntries(pack.Manifest.Tools)
		out = append(out, packs.ConnectorPackDescriptor{
			Identity: packs.PackIdentity{
				ID: pack.Envelope.ID, Version: pack.Envelope.Version, ManifestHash: pack.Envelope.ManifestHash,
				Type: packs.TypeConnector, Source: pack.Source,
			},
			Provider: pack.Manifest.Provider, Tools: tools,
		})
	}
	return out
}

type InstalledTool struct {
	Provider string
	ToolID   string
	Tool     runtimecontracts.ToolSchemaEntry
	Pack     LoadedPack
}

var (
	defaultPackRegistryOnce sync.Once
	defaultPackRegistry     *PackRegistry
)

func DefaultPackRegistry() *PackRegistry {
	defaultPackRegistryOnce.Do(func() {
		defaultPackRegistry = mustDefaultPackRegistry()
	})
	return defaultPackRegistry
}

func BuiltinTool(provider, toolID string) (runtimecontracts.ToolSchemaEntry, bool) {
	pack, ok := DefaultPackRegistry().Lookup(provider, toolID)
	if !ok {
		return runtimecontracts.ToolSchemaEntry{}, false
	}
	tool, ok := pack.Manifest.Tools[strings.TrimSpace(toolID)]
	return runtimecontracts.CloneToolSchemaEntry(tool), ok
}

func mustDefaultPackRegistry() *PackRegistry {
	runningVersion, err := platform.PlatformVersion()
	if err != nil {
		panic(err)
	}
	registry, err := LoadBuiltinPackRegistry(runningVersion)
	if err != nil {
		panic(err)
	}
	return registry
}

func LoadBuiltinPackRegistry(runningPlatformVersion string) (*PackRegistry, error) {
	return loadBuiltinPackRegistryFS(builtinConnectorPackFS, runningPlatformVersion)
}

func loadBuiltinPackRegistryFS(fsys fs.FS, runningPlatformVersion string) (*PackRegistry, error) {
	index, err := loadGeneratedPackIndex(fsys)
	if err != nil {
		return nil, err
	}
	expectedGenerated := index.BuiltinByID()
	entries, err := fs.ReadDir(fsys, connectorPackRoot)
	if err != nil {
		return nil, fmt.Errorf("read built-in provider connector packs: %w", err)
	}
	loaded := make([]LoadedPack, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := path.Join(connectorPackRoot, entry.Name())
		pack, err := LoadPackFS(fsys, dir, runningPlatformVersion)
		if err != nil {
			return nil, err
		}
		loaded = append(loaded, pack)
	}
	seenGenerated := map[string]struct{}{}
	for _, pack := range loaded {
		expected, indexed := expectedGenerated[strings.TrimSpace(pack.Envelope.ID)]
		if indexed {
			if err := validateGeneratedPackIdentity(fsys, pack, expected); err != nil {
				return nil, err
			}
			seenGenerated[strings.TrimSpace(pack.Envelope.ID)] = struct{}{}
			continue
		}
		if pack.Manifest.Generation != nil {
			return nil, fmt.Errorf("builtin connector pack %q carries generation evidence but is not in the generated pack index", pack.Envelope.ID)
		}
	}
	for packID := range expectedGenerated {
		if _, exists := seenGenerated[packID]; !exists {
			return nil, fmt.Errorf("generated connector pack index references unknown builtin pack id %q", packID)
		}
	}
	return NewPackRegistry(loaded...)
}

func NewPackRegistry(loaded ...LoadedPack) (*PackRegistry, error) {
	registry := &PackRegistry{byProvider: map[string]map[string]LoadedPack{}}
	for _, pack := range loaded {
		if err := pack.Manifest.Validate(); err != nil {
			return nil, fmt.Errorf("validate connector manifest for pack %q: %w", pack.Envelope.ID, err)
		}
		admittedTools := make(map[string]runtimecontracts.ToolSchemaEntry, len(pack.Manifest.Tools))
		for toolID, tool := range pack.Manifest.Tools {
			admitted, err := runtimecontracts.AdmitToolSchemaEntry(tool)
			if err != nil {
				return nil, fmt.Errorf("admit connector tool %q for pack %q: %w", toolID, pack.Envelope.ID, err)
			}
			admittedTools[toolID] = admitted
		}
		pack.Manifest.Tools = admittedTools
		if pack.Manifest.Generation != nil {
			if err := pack.Manifest.Generation.Validate(pack.Manifest.Provider, pack.Manifest.Tools); err != nil {
				return nil, fmt.Errorf("validate generation evidence for pack %q: %w", pack.Envelope.ID, err)
			}
		}
		pack = cloneLoadedPack(pack)
		provider := normalizeToken(pack.Manifest.Provider)
		if provider == "" {
			return nil, fmt.Errorf("provider connector pack %q has empty provider", pack.Envelope.ID)
		}
		if registry.byProvider[provider] == nil {
			registry.byProvider[provider] = map[string]LoadedPack{}
		}
		source := firstNonEmpty(pack.Source, pack.Envelope.Provenance.Source+":"+pack.Envelope.ID)
		names := manifestToolNames(pack.Manifest)
		for _, toolID := range names {
			if existing, exists := registry.byProvider[provider][toolID]; exists {
				existingSource := firstNonEmpty(existing.Source, existing.Envelope.Provenance.Source+":"+existing.Envelope.ID)
				return nil, fmt.Errorf("duplicate provider connector pack tool %q for provider %q from %s and %s", toolID, provider, existingSource, source)
			}
			pack.Source = source
			registry.byProvider[provider][toolID] = pack
		}
	}
	return registry, nil
}

func (r *PackRegistry) Lookup(provider, toolID string) (LoadedPack, bool) {
	if r == nil {
		return LoadedPack{}, false
	}
	provider = normalizeToken(provider)
	toolID = strings.TrimSpace(toolID)
	if provider == "" || toolID == "" {
		return LoadedPack{}, false
	}
	byTool := r.byProvider[provider]
	if byTool == nil {
		return LoadedPack{}, false
	}
	pack, ok := byTool[toolID]
	return cloneLoadedPack(pack), ok
}

func (r *PackRegistry) Inventory() []InstalledTool {
	if r == nil {
		return nil
	}
	providers := make([]string, 0, len(r.byProvider))
	for provider := range r.byProvider {
		providers = append(providers, provider)
	}
	sort.Strings(providers)
	var out []InstalledTool
	for _, provider := range providers {
		toolIDs := make([]string, 0, len(r.byProvider[provider]))
		for toolID := range r.byProvider[provider] {
			toolIDs = append(toolIDs, toolID)
		}
		sort.Strings(toolIDs)
		for _, toolID := range toolIDs {
			pack := cloneLoadedPack(r.byProvider[provider][toolID])
			out = append(out, InstalledTool{
				Provider: provider,
				ToolID:   toolID,
				Tool:     runtimecontracts.CloneToolSchemaEntry(pack.Manifest.Tools[toolID]),
				Pack:     pack,
			})
		}
	}
	return out
}

func GenerationSurfaceForPackTool(pack LoadedPack, toolID string) (GenerationSurface, bool) {
	if pack.Manifest.Generation == nil {
		return GenerationSurface{}, false
	}
	operation, ok := pack.Manifest.Generation.OperationForTool(strings.TrimSpace(toolID))
	if !ok {
		return GenerationSurface{}, false
	}
	return GenerationSurface{
		GeneratorVersion: pack.Manifest.Generation.GeneratorVersion,
		SourcePath:       pack.Manifest.Generation.Source.Path,
		SourceSHA256:     pack.Manifest.Generation.Source.SHA256,
		ProfilePath:      pack.Manifest.Generation.Profile.Path,
		ProfileSHA256:    pack.Manifest.Generation.Profile.SHA256,
		ManifestSHA256:   pack.Envelope.ManifestHash,
		OperationID:      operation.OperationID,
		Permissions:      append([]GenerationPermission(nil), operation.Permissions...),
		FixtureID:        operation.FixtureID,
		FixtureStatus:    operation.FixtureStatus,
		ReviewStatus:     operation.ReviewStatus,
	}, true
}

func LoadPackFS(fsys fs.FS, dir, runningPlatformVersion string) (LoadedPack, error) {
	loaded, err := packs.Load(fsys, dir, runningPlatformVersion)
	if err != nil {
		return LoadedPack{}, err
	}
	if strings.TrimSpace(loaded.Envelope.Type) != packs.TypeConnector {
		return LoadedPack{}, fmt.Errorf("provider connector pack %q has unsupported type %q", loaded.Envelope.ID, loaded.Envelope.Type)
	}
	manifest, err := parseConnectorManifestStrict(loaded.ManifestBody)
	if err != nil {
		return LoadedPack{}, fmt.Errorf("parse connector manifest for pack %q: %w", loaded.Envelope.ID, err)
	}
	if err := manifest.Validate(); err != nil {
		return LoadedPack{}, fmt.Errorf("validate connector manifest for pack %q: %w", loaded.Envelope.ID, err)
	}
	if manifest.Generation != nil {
		if err := manifest.Generation.Validate(manifest.Provider, manifest.Tools); err != nil {
			return LoadedPack{}, fmt.Errorf("validate generation evidence for pack %q: %w", loaded.Envelope.ID, err)
		}
	}
	expectedCapabilities := DerivedCapabilities(manifest)
	if !packs.CapabilitiesEqual(loaded.Envelope.Capabilities, expectedCapabilities) {
		return LoadedPack{}, fmt.Errorf("pack %q capabilities do not match connector manifest", loaded.Envelope.ID)
	}
	expectedRequires := DerivedRequires(manifest)
	if !packs.RequiresEqual(loaded.Envelope.Requires, expectedRequires) {
		return LoadedPack{}, fmt.Errorf("pack %q requires do not match connector manifest", loaded.Envelope.ID)
	}
	return LoadedPack{
		Envelope:     loaded.Envelope,
		Manifest:     manifest,
		ManifestBody: loaded.ManifestBody,
		Directory:    loaded.Directory,
		Source:       loaded.Envelope.Provenance.Source + ":" + loaded.Envelope.ID,
	}, nil
}

func parseConnectorManifestStrict(body []byte) (ConnectorManifest, error) {
	var manifest ConnectorManifest
	if err := decodeYAMLStrict(body, &manifest); err != nil {
		return ConnectorManifest{}, err
	}
	return manifest, nil
}

func (m ConnectorManifest) Validate() error {
	provider := normalizeToken(m.Provider)
	if provider == "" {
		return fmt.Errorf("connector manifest provider is required")
	}
	names := manifestToolNames(m)
	if len(names) == 0 {
		return fmt.Errorf("connector manifest tools are required")
	}
	for _, toolID := range names {
		tool := m.Tools[toolID]
		if !isProviderConnector(tool) {
			return fmt.Errorf("connector manifest tool %q must declare category %q", toolID, Category)
		}
		toolProvider, _, ok := splitToolID(toolID)
		if !ok {
			return fmt.Errorf("connector manifest tool %q must use <provider>.<action> id form", toolID)
		}
		if toolProvider != provider {
			return fmt.Errorf("connector manifest tool %q provider %q does not match manifest provider %q", toolID, toolProvider, provider)
		}
		if errs := validateTool(toolID, tool); len(errs) > 0 {
			return fmt.Errorf("%s", joinValidationErrors(errs))
		}
	}
	return nil
}

func DerivedCapabilities(manifest ConnectorManifest) packs.Capabilities {
	names := manifestToolNames(manifest)
	return packs.Capabilities{
		Can: packs.CanCapabilities{
			CallProviderActions:     names,
			LowerThroughActivity:    true,
			JournalActivityAttempts: true,
		},
		Cannot: []string{
			"bypass activity_attempts",
			"retry non_idempotent_write automatically",
			"expose credential values",
		},
	}
}

func DerivedRequires(manifest ConnectorManifest) packs.Requires {
	seenSecrets := map[string]struct{}{}
	seenManaged := map[string]struct{}{}
	var secrets []string
	var managedCredentials []string
	for _, toolID := range manifestToolNames(manifest) {
		tool := manifest.Tools[toolID]
		for _, credential := range tool.Credentials {
			credential = strings.TrimSpace(credential)
			if credential == "" {
				continue
			}
			if _, exists := seenSecrets[credential]; exists {
				continue
			}
			seenSecrets[credential] = struct{}{}
			secrets = append(secrets, credential)
		}
		if tool.ManagedCredential != nil {
			credential := strings.TrimSpace(tool.ManagedCredential.Key)
			if credential != "" {
				if _, exists := seenManaged[credential]; !exists {
					seenManaged[credential] = struct{}{}
					managedCredentials = append(managedCredentials, credential)
				}
			}
		}
	}
	sort.Strings(secrets)
	sort.Strings(managedCredentials)
	return packs.Requires{Secrets: secrets, ManagedCredentials: managedCredentials}
}

func SourceWithConnectorPackImports(source semanticview.Source) (semanticview.Source, error) {
	return SourceWithConnectorPackImportsFromRegistry(source, DefaultPackRegistry())
}

func SourceWithConnectorPackImportsFromRegistry(source semanticview.Source, registry *PackRegistry) (semanticview.Source, error) {
	if source == nil {
		return nil, nil
	}
	if connectorPackImportsApplied(source) {
		return source, nil
	}
	imports := connectorPackImportsFromSource(source)
	if len(imports) == 0 {
		return source, nil
	}
	if registry == nil {
		return nil, fmt.Errorf("provider connector pack registry is required")
	}
	existing := existingToolSources(source)
	importedTools := map[string]runtimecontracts.ToolSchemaEntry{}
	importedGeneration := map[string]GenerationSurface{}
	importSources := map[string]string{}
	importedByProjectScope := map[string]map[string]runtimecontracts.ToolSchemaEntry{}
	for _, item := range imports {
		if item.provider == "" {
			return nil, fmt.Errorf("provider connector pack import in %s must declare provider", item.source)
		}
		if item.toolID == "" {
			return nil, fmt.Errorf("provider connector pack import in %s must declare tool", item.source)
		}
		provider, _, ok := splitToolID(item.toolID)
		if !ok {
			return nil, fmt.Errorf("provider connector pack import %s tool %q must use <provider>.<action> id form", item.source, item.toolID)
		}
		if provider != item.provider {
			return nil, fmt.Errorf("provider connector pack import %s tool %q provider %q does not match import provider %q", item.source, item.toolID, provider, item.provider)
		}
		pack, found := registry.Lookup(item.provider, item.toolID)
		if !found {
			return nil, fmt.Errorf("provider connector pack import %s references unknown tool %q for provider %q", item.source, item.toolID, item.provider)
		}
		if existingSources := existing[item.toolID]; len(existingSources) > 0 {
			return nil, fmt.Errorf("provider connector tool %q collision between connector pack import %s and %s; remove one, or rename the flow-local tool", item.toolID, item.source, strings.Join(existingSources, ", "))
		}
		if prior, exists := importSources[item.toolID]; exists {
			return nil, fmt.Errorf("provider connector tool %q collision between connector pack imports %s and %s; remove one import", item.toolID, prior, item.source)
		}
		tool := runtimecontracts.CloneToolSchemaEntry(pack.Manifest.Tools[item.toolID])
		importedTools[item.toolID] = tool
		if generation, exists := GenerationSurfaceForPackTool(pack, item.toolID); exists {
			importedGeneration[item.toolID] = generation
		}
		importSources[item.toolID] = item.source
		if importedByProjectScope[item.projectScopeKey] == nil {
			importedByProjectScope[item.projectScopeKey] = map[string]runtimecontracts.ToolSchemaEntry{}
		}
		importedByProjectScope[item.projectScopeKey][item.toolID] = tool
	}
	return connectorPackSource{
		Source:                 source,
		importedTools:          importedTools,
		importedGeneration:     importedGeneration,
		importSources:          importSources,
		importedByProjectScope: importedByProjectScope,
	}, nil
}

func connectorPackImportsApplied(source semanticview.Source) bool {
	type applied interface {
		ConnectorPackImportsApplied() bool
	}
	type baseSource interface {
		BaseSemanticSource() semanticview.Source
	}
	for depth := 0; source != nil && depth < 64; depth++ {
		if wrapped, ok := source.(applied); ok && wrapped.ConnectorPackImportsApplied() {
			return true
		}
		wrapped, ok := source.(baseSource)
		if !ok {
			return false
		}
		source = wrapped.BaseSemanticSource()
	}
	return false
}

type connectorPackImport struct {
	provider        string
	toolID          string
	projectScopeKey string
	source          string
}

func connectorPackImportsFromSource(source semanticview.Source) []connectorPackImport {
	var out []connectorPackImport
	for _, scope := range source.ProjectScopes() {
		scopeKey := strings.TrimSpace(scope.Key)
		sourceName := projectScopeSourceName(scope)
		for _, item := range scope.Manifest.ConnectorPacks.Imports {
			provider := normalizeToken(item.Provider)
			toolID := strings.TrimSpace(item.Tool)
			if provider == "" && toolID == "" {
				continue
			}
			out = append(out, connectorPackImport{
				provider:        provider,
				toolID:          toolID,
				projectScopeKey: scopeKey,
				source:          sourceName + " connector_packs.imports",
			})
		}
	}
	return out
}

type connectorPackSource struct {
	semanticview.Source
	importedTools          map[string]runtimecontracts.ToolSchemaEntry
	importedGeneration     map[string]GenerationSurface
	importSources          map[string]string
	importedByProjectScope map[string]map[string]runtimecontracts.ToolSchemaEntry
}

func (s connectorPackSource) BaseSemanticSource() semanticview.Source {
	return s.Source
}

func (s connectorPackSource) ConnectorPackImportsApplied() bool {
	return true
}

func (s connectorPackSource) ConnectorGenerationSurface(toolID string) (GenerationSurface, bool) {
	evidence, ok := s.importedGeneration[strings.TrimSpace(toolID)]
	return evidence, ok
}

func (s connectorPackSource) ConnectorPackImportSource(toolID string) (string, bool) {
	source, ok := s.importSources[strings.TrimSpace(toolID)]
	return source, ok
}

func (s connectorPackSource) ToolEntries() map[string]runtimecontracts.ToolSchemaEntry {
	out := map[string]runtimecontracts.ToolSchemaEntry{}
	for key, value := range s.Source.ToolEntries() {
		out[key] = runtimecontracts.CloneToolSchemaEntry(value)
	}
	for key, value := range s.importedTools {
		out[key] = runtimecontracts.CloneToolSchemaEntry(value)
	}
	return out
}

func (s connectorPackSource) ProjectScopes() []semanticview.ProjectScope {
	scopes := s.Source.ProjectScopes()
	out := make([]semanticview.ProjectScope, 0, len(scopes))
	for _, scope := range scopes {
		scope.Tools = cloneToolMap(scope.Tools)
		for toolID, tool := range s.importedByProjectScope[strings.TrimSpace(scope.Key)] {
			scope.Tools[toolID] = runtimecontracts.CloneToolSchemaEntry(tool)
		}
		out = append(out, scope)
	}
	return out
}

func (s connectorPackSource) ToolEntryForAgent(agentID, toolID string) (runtimecontracts.ToolSchemaEntry, bool) {
	if tool, ok := s.Source.ToolEntryForAgent(agentID, toolID); ok {
		return runtimecontracts.CloneToolSchemaEntry(tool), true
	}
	tool, ok := s.importedTools[strings.TrimSpace(toolID)]
	return runtimecontracts.CloneToolSchemaEntry(tool), ok
}

func (s connectorPackSource) ResolvedEventCatalog() map[string]runtimecontracts.EventCatalogEntry {
	out := cloneConnectorEventCatalog(s.Source.ResolvedEventCatalog())
	for eventType, entry := range s.generatedActivityEventEntries() {
		out[eventType] = entry
	}
	return out
}

func (s connectorPackSource) ResolveFlowEventCatalogEntry(flowID, eventType string) (runtimecontracts.EventCatalogEntry, string, bool) {
	if entry, resolved, ok := s.Source.ResolveFlowEventCatalogEntry(flowID, eventType); ok {
		return entry, resolved, true
	}
	eventType = strings.TrimSpace(eventType)
	entry, ok := s.generatedActivityEventEntries()[eventType]
	return entry, eventType, ok
}

func (s connectorPackSource) EventEntries() map[string]runtimecontracts.EventCatalogEntry {
	out := cloneConnectorEventCatalog(s.Source.EventEntries())
	for eventType, entry := range s.generatedActivityEventEntries() {
		out[eventType] = entry
	}
	return out
}

func (s connectorPackSource) EventEntry(eventType string) (runtimecontracts.EventCatalogEntry, bool) {
	if entry, ok := s.Source.EventEntry(eventType); ok {
		return entry, true
	}
	entry, ok := s.generatedActivityEventEntries()[strings.TrimSpace(eventType)]
	return entry, ok
}

func (s connectorPackSource) generatedActivityEventEntries() map[string]runtimecontracts.EventCatalogEntry {
	out := map[string]runtimecontracts.EventCatalogEntry{}
	tools := s.ToolEntries()
	for nodeID := range s.NodeEntries() {
		flowID := ""
		if owner, ok := s.NodeContractSource(nodeID); ok {
			flowID = strings.TrimSpace(owner.FlowID)
		}
		for _, site := range runtimecontracts.ActivitySitesForNode(flowID, nodeID, s.NodeEventHandlers(nodeID)) {
			tool, ok := tools[strings.TrimSpace(site.Spec.Tool)]
			if !ok {
				continue
			}
			events := runtimecontracts.ActivityResultEventsForSite(site)
			out[events.SuccessEvent] = runtimecontracts.ActivityResultEventCatalogEntry(site, tool, runtimecontracts.ActivityResultStatusSucceeded)
			out[events.FailureEvent] = runtimecontracts.ActivityResultEventCatalogEntry(site, tool, runtimecontracts.ActivityResultStatusFailed)
			if site.Spec.Approval != nil {
				out[events.RevisionRequested] = runtimecontracts.ActivityApprovalEventCatalogEntry(site, true)
				out[events.Rejected] = runtimecontracts.ActivityApprovalEventCatalogEntry(site, false)
			}
		}
	}
	return out
}

func cloneConnectorEventCatalog(in map[string]runtimecontracts.EventCatalogEntry) map[string]runtimecontracts.EventCatalogEntry {
	out := make(map[string]runtimecontracts.EventCatalogEntry, len(in))
	for eventType, entry := range in {
		out[eventType] = entry
	}
	return out
}

func existingToolSources(source semanticview.Source) map[string][]string {
	out := map[string][]string{}
	for _, scope := range source.ProjectScopes() {
		sourceName := projectScopeSourceName(scope)
		for toolID := range scope.Tools {
			toolID = strings.TrimSpace(toolID)
			if toolID != "" {
				out[toolID] = appendIfMissing(out[toolID], sourceName)
			}
		}
	}
	for _, scope := range source.FlowScopes() {
		sourceName := flowScopeSourceName(scope)
		for toolID := range scope.Tools {
			toolID = strings.TrimSpace(toolID)
			if toolID != "" {
				out[toolID] = appendIfMissing(out[toolID], sourceName)
			}
		}
	}
	for toolID := range source.ToolEntries() {
		toolID = strings.TrimSpace(toolID)
		if toolID == "" {
			continue
		}
		if _, exists := out[toolID]; exists {
			continue
		}
		out[toolID] = []string{"merged tool source"}
	}
	for key := range out {
		sort.Strings(out[key])
	}
	return out
}

func manifestToolNames(manifest ConnectorManifest) []string {
	names := make([]string, 0, len(manifest.Tools))
	for name := range manifest.Tools {
		name = strings.TrimSpace(name)
		if name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func cloneToolMap(in map[string]runtimecontracts.ToolSchemaEntry) map[string]runtimecontracts.ToolSchemaEntry {
	return runtimecontracts.CloneToolSchemaEntries(in)
}

func cloneLoadedPack(in LoadedPack) LoadedPack {
	out := in
	out.Envelope.Implements = append([]string(nil), in.Envelope.Implements...)
	out.Envelope.Tests = append([]string(nil), in.Envelope.Tests...)
	out.Envelope.Capabilities.Cannot = append([]string(nil), in.Envelope.Capabilities.Cannot...)
	out.Envelope.Capabilities.Can.EmitEvents = append([]string(nil), in.Envelope.Capabilities.Can.EmitEvents...)
	out.Envelope.Capabilities.Can.CallProviderActions = append([]string(nil), in.Envelope.Capabilities.Can.CallProviderActions...)
	out.Envelope.Requires.Secrets = append([]string(nil), in.Envelope.Requires.Secrets...)
	out.Envelope.Requires.ManagedCredentials = append([]string(nil), in.Envelope.Requires.ManagedCredentials...)
	if in.Envelope.Requires.Packs != nil {
		out.Envelope.Requires.Packs = make(map[string]string, len(in.Envelope.Requires.Packs))
		for name, version := range in.Envelope.Requires.Packs {
			out.Envelope.Requires.Packs[name] = version
		}
	}
	out.Manifest.Tools = runtimecontracts.CloneToolSchemaEntries(in.Manifest.Tools)
	if in.Manifest.Generation != nil {
		generation := *in.Manifest.Generation
		if in.Manifest.Generation.Operations != nil {
			generation.Operations = make([]GenerationOperationEvidence, len(in.Manifest.Generation.Operations))
			for index, operation := range in.Manifest.Generation.Operations {
				generation.Operations[index] = operation
				generation.Operations[index].Permissions = append([]GenerationPermission(nil), operation.Permissions...)
			}
		}
		out.Manifest.Generation = &generation
	}
	out.ManifestBody = append([]byte(nil), in.ManifestBody...)
	return out
}

func projectScopeSourceName(scope semanticview.ProjectScope) string {
	key := strings.TrimSpace(scope.Key)
	if key == "" {
		key = "."
	}
	return "package " + key
}

func flowScopeSourceName(scope semanticview.FlowScope) string {
	id := strings.TrimSpace(scope.ID)
	if id == "" {
		id = strings.TrimSpace(scope.Path)
	}
	if id == "" {
		id = strings.TrimSpace(scope.PackageKey)
	}
	if id == "" {
		id = "unknown"
	}
	return "flow " + id
}

func appendIfMissing(values []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func joinValidationErrors(errs []error) string {
	parts := make([]string, 0, len(errs))
	for _, err := range errs {
		if err == nil {
			continue
		}
		parts = append(parts, strings.TrimSpace(err.Error()))
	}
	sort.Strings(parts)
	return strings.Join(parts, "; ")
}
