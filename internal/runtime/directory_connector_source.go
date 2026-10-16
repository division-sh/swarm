package runtime

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type directoryConnectorBinding struct {
	tool       runtimecontracts.ToolSchemaEntry
	identity   packs.PackIdentity
	generation semanticview.ConnectorGenerationSurface
}

type directoryConnectorSource struct {
	semanticview.Source
	scopes       []semanticview.FlowScope
	tools        map[string]runtimecontracts.ToolSchemaEntry
	capabilities semanticview.Capabilities
}

func compileDirectoryConnectorSource(source semanticview.Source, fact runtimecorrelation.SourceArtifactFact) (semanticview.Source, error) {
	bundle, found := semanticview.Bundle(source)
	if !found {
		return nil, fmt.Errorf("directory connector composition requires its compiled rooted source")
	}
	rooted, err := bundle.RootedSource()
	if err != nil {
		return nil, err
	}
	if rooted.Admission().BundleHash() != fact.BundleHash() {
		return nil, fmt.Errorf("directory connector composition differs from its admitted source fact")
	}
	providers, err := packadmission.AdmitProviderSource(rooted, bundle.Platform)
	if err != nil {
		return nil, err
	}
	source, err = sourceWithDirectoryTriggerSchemas(source, providers)
	if err != nil {
		return nil, err
	}
	out := directoryConnectorSource{Source: source, tools: map[string]runtimecontracts.ToolSchemaEntry{},
		capabilities: source.SemanticCapabilities().WithoutConnectorPackImports()}
	for name, tool := range source.ToolEntries() {
		if tool.Category() != runtimecontracts.ToolCategoryProviderConnector && tool.Category() != runtimecontracts.ToolCategoryProviderRegistration {
			out.tools[name] = tool
		}
	}
	aliases := map[string]directoryConnectorBinding{}
	ambiguous := map[string]bool{}
	scopes, err := directoryConnectorScopes(source, providers)
	if err != nil {
		return nil, err
	}
	for _, scope := range scopes {
		selected, err := providers.Scope(scope.ID)
		if err != nil {
			return nil, err
		}
		scope.Tools = bundle.ToolEntriesForFlow(scope.ID)
		bindings, err := directoryConnectorBindings(selected, scope.Tools)
		if err != nil {
			return nil, err
		}
		for name, binding := range bindings {
			scope.Tools[name] = binding.tool
			if prior, exists := aliases[name]; exists && prior.identity != binding.identity {
				ambiguous[name] = true
			}
			aliases[name] = binding
		}
		out.scopes = append(out.scopes, scope)
		out.capabilities, err = directoryConnectorCapabilities(out.capabilities, scope.ID, bindings)
		if err != nil {
			return nil, err
		}
	}
	for name, binding := range aliases {
		if !ambiguous[name] {
			out.tools[name] = binding.tool
		}
	}
	return semanticview.CompileActivityToolBindings(out)
}

func directoryConnectorScopes(source semanticview.Source, providers packadmission.ProviderSource) ([]semanticview.FlowScope, error) {
	byID := map[string]semanticview.FlowScope{}
	for _, scope := range source.FlowScopes() {
		if _, duplicate := byID[scope.ID]; duplicate {
			return nil, fmt.Errorf("directory connector source repeats flow %q", scope.ID)
		}
		if _, err := providers.Scope(scope.ID); err != nil {
			return nil, err
		}
		byID[scope.ID] = scope
	}
	var scopes []semanticview.FlowScope
	for _, flow := range providers.FlowPaths() {
		scope, found := byID[flow]
		if !found {
			return nil, fmt.Errorf("directory connector source omits admitted flow %q", flow)
		}
		scopes = append(scopes, scope)
	}
	return scopes, nil
}

func directoryConnectorBindings(scope packadmission.ProviderScope, authored map[string]runtimecontracts.ToolSchemaEntry) (map[string]directoryConnectorBinding, error) {
	out := map[string]directoryConnectorBinding{}
	for _, definition := range scope.Providers() {
		key := definition.Provider().Key()
		for name := range authored {
			if strings.HasPrefix(name, key+".") {
				return nil, fmt.Errorf("tool %q in flow %q redeclares provider directory %q; declare its short tool name in %s/connector.yaml", name, scope.FlowPath(), key, definition.Provider().Directory())
			}
		}
		descriptor, present := definition.ConnectorDescriptor()
		if !present {
			continue
		}
		for name, tool := range descriptor.Tools {
			generation, _ := definition.ConnectorGeneration(name)
			out[name] = directoryConnectorBinding{tool: tool, identity: descriptor.Identity, generation: generation}
		}
	}
	return out, nil
}

func directoryConnectorCapabilities(capabilities semanticview.Capabilities, flow string, bindings map[string]directoryConnectorBinding) (semanticview.Capabilities, error) {
	if len(bindings) == 0 {
		return capabilities, nil
	}
	generations := map[string]semanticview.ConnectorGenerationSurface{}
	sources := map[string]semanticview.ConnectorImportSource{}
	identities := map[string]semanticview.ConnectorPackProvenance{}
	for name, binding := range bindings {
		label := binding.identity.Source().Path()
		source, err := semanticview.NewConnectorImportSource(label)
		if err != nil {
			return semanticview.Capabilities{}, err
		}
		sources[name] = source
		identities[name] = semanticview.ConnectorPackProvenance{PackID: binding.identity.ID(),
			Version: binding.identity.Version(), ManifestHash: binding.identity.ManifestHash(), Source: label}
		if binding.generation.Valid() {
			generations[name] = binding.generation
		}
	}
	return capabilities.WithConnectorPackImports(flow, generations, sources).WithConnectorPackProvenance(flow, identities), nil
}

func (s directoryConnectorSource) SemanticCapabilities() semanticview.Capabilities {
	return s.capabilities
}

func (s directoryConnectorSource) ToolEntries() map[string]runtimecontracts.ToolSchemaEntry {
	out := make(map[string]runtimecontracts.ToolSchemaEntry, len(s.tools))
	for name, tool := range s.tools {
		out[name] = tool
	}
	return out
}

func (s directoryConnectorSource) FlowScopes() []semanticview.FlowScope {
	out := append([]semanticview.FlowScope(nil), s.scopes...)
	for index := range out {
		out[index].Tools = make(map[string]runtimecontracts.ToolSchemaEntry, len(s.scopes[index].Tools))
		for name, tool := range s.scopes[index].Tools {
			out[index].Tools[name] = tool
		}
	}
	return out
}

func (s directoryConnectorSource) FlowScopeByID(id string) (semanticview.FlowScope, bool) {
	for _, scope := range s.FlowScopes() {
		if scope.ID == id {
			return scope, true
		}
	}
	return semanticview.FlowScope{}, false
}
