package semanticview

import (
	"strings"

	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

type ConnectorGenerationPermission struct {
	ID   string `yaml:"id"`
	Note string `yaml:"note"`
}

type ConnectorGenerationSurface struct {
	GeneratorVersion string
	SourcePath       string
	SourceSHA256     string
	ProfilePath      string
	ProfileSHA256    string
	ManifestSHA256   string
	OperationID      string
	Permissions      []ConnectorGenerationPermission
	FixtureID        string
	FixtureStatus    string
	ReviewStatus     string
}

type providerTriggerCapabilities struct {
	base       Source
	generation triggergeneration.Generation
	targetFree []runtimeprovideroutput.Authorization
}

type connectorPackCapabilities struct {
	generations   map[string]ConnectorGenerationSurface
	importSources map[string]string
}

// Capabilities is the finite compile-visible semantic-source capability set.
// Wrappers preserve it by embedding Source and replace only the field they own.
type Capabilities struct {
	providerTrigger *providerTriggerCapabilities
	connectorPacks  *connectorPackCapabilities
}

func (c Capabilities) WithProviderTriggerEvents(base Source, generation triggergeneration.Generation, targetFree []runtimeprovideroutput.Authorization) Capabilities {
	out := c
	if base == nil || !generation.Valid() {
		out.providerTrigger = nil
		return out
	}
	authorizations := make([]runtimeprovideroutput.Authorization, len(targetFree))
	for index, authorization := range targetFree {
		authorizations[index] = authorization.Normalized()
	}
	out.providerTrigger = &providerTriggerCapabilities{
		base:       base,
		generation: generation,
		targetFree: authorizations,
	}
	return out
}

func (c Capabilities) ProviderTriggerEvents() (generation triggergeneration.Generation, base Source, ok bool) {
	if c.providerTrigger == nil {
		return triggergeneration.Generation{}, nil, false
	}
	return c.providerTrigger.generation, c.providerTrigger.base, true
}

func (c Capabilities) ProviderTriggerTargetFreeAuthorizations() []runtimeprovideroutput.Authorization {
	if c.providerTrigger == nil {
		return nil
	}
	out := make([]runtimeprovideroutput.Authorization, len(c.providerTrigger.targetFree))
	copy(out, c.providerTrigger.targetFree)
	return out
}

func (c Capabilities) WithConnectorPackImports(generations map[string]ConnectorGenerationSurface, importSources map[string]string) Capabilities {
	out := c
	generationCopy := make(map[string]ConnectorGenerationSurface, len(generations))
	for toolID, generation := range generations {
		generation.Permissions = append([]ConnectorGenerationPermission(nil), generation.Permissions...)
		generationCopy[strings.TrimSpace(toolID)] = generation
	}
	sourceCopy := make(map[string]string, len(importSources))
	for toolID, source := range importSources {
		sourceCopy[strings.TrimSpace(toolID)] = strings.TrimSpace(source)
	}
	out.connectorPacks = &connectorPackCapabilities{generations: generationCopy, importSources: sourceCopy}
	return out
}

func (c Capabilities) ConnectorPackImportsApplied() bool {
	return c.connectorPacks != nil
}

func (c Capabilities) ConnectorGeneration(toolID string) (ConnectorGenerationSurface, bool) {
	if c.connectorPacks == nil {
		return ConnectorGenerationSurface{}, false
	}
	generation, ok := c.connectorPacks.generations[strings.TrimSpace(toolID)]
	generation.Permissions = append([]ConnectorGenerationPermission(nil), generation.Permissions...)
	return generation, ok
}

func (c Capabilities) ConnectorImportSource(toolID string) (string, bool) {
	if c.connectorPacks == nil {
		return "", false
	}
	source, ok := c.connectorPacks.importSources[strings.TrimSpace(toolID)]
	return source, ok
}
