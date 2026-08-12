package semanticview

import (
	"fmt"
	"regexp"
	"strings"

	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

var connectorCapabilitySHA256Pattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

type ConnectorGenerationPermission struct {
	id   string
	note string
}

func NewConnectorGenerationPermission(id, note string) (ConnectorGenerationPermission, error) {
	id = strings.TrimSpace(id)
	note = strings.TrimSpace(note)
	if id == "" || note == "" {
		return ConnectorGenerationPermission{}, fmt.Errorf("connector generation permission id and note are required")
	}
	return ConnectorGenerationPermission{id: id, note: note}, nil
}

func MustConnectorGenerationPermission(id, note string) ConnectorGenerationPermission {
	permission, err := NewConnectorGenerationPermission(id, note)
	if err != nil {
		panic(err)
	}
	return permission
}

func (p ConnectorGenerationPermission) ID() string   { return p.id }
func (p ConnectorGenerationPermission) Note() string { return p.note }

type ConnectorGenerationSurface struct {
	generatorVersion string
	sourcePath       string
	sourceSHA256     string
	profilePath      string
	profileSHA256    string
	manifestSHA256   string
	operationID      string
	permissions      []ConnectorGenerationPermission
	fixtureID        string
	fixtureStatus    string
	reviewStatus     string
}

func NewConnectorGenerationSurface(
	generatorVersion string,
	sourcePath string,
	sourceSHA256 string,
	profilePath string,
	profileSHA256 string,
	manifestSHA256 string,
	operationID string,
	permissions []ConnectorGenerationPermission,
	fixtureID string,
	fixtureStatus string,
	reviewStatus string,
) (ConnectorGenerationSurface, error) {
	values := []*string{
		&generatorVersion, &sourcePath, &profilePath, &operationID, &fixtureID, &fixtureStatus, &reviewStatus,
	}
	for _, value := range values {
		*value = strings.TrimSpace(*value)
		if *value == "" {
			return ConnectorGenerationSurface{}, fmt.Errorf("connector generation surface fields are required")
		}
	}
	hashes := []*string{&sourceSHA256, &profileSHA256, &manifestSHA256}
	for _, hash := range hashes {
		if !connectorCapabilitySHA256Pattern.MatchString(*hash) {
			return ConnectorGenerationSurface{}, fmt.Errorf("connector generation hash %q must use canonical sha256:<lowercase-hex>", *hash)
		}
	}
	if len(permissions) == 0 {
		return ConnectorGenerationSurface{}, fmt.Errorf("connector generation permissions are required")
	}
	permissionCopy := append([]ConnectorGenerationPermission(nil), permissions...)
	for _, permission := range permissionCopy {
		if permission.id == "" || permission.note == "" {
			return ConnectorGenerationSurface{}, fmt.Errorf("connector generation permissions must be admitted")
		}
	}
	return ConnectorGenerationSurface{
		generatorVersion: generatorVersion,
		sourcePath:       sourcePath,
		sourceSHA256:     sourceSHA256,
		profilePath:      profilePath,
		profileSHA256:    profileSHA256,
		manifestSHA256:   manifestSHA256,
		operationID:      operationID,
		permissions:      permissionCopy,
		fixtureID:        fixtureID,
		fixtureStatus:    fixtureStatus,
		reviewStatus:     reviewStatus,
	}, nil
}

func MustConnectorGenerationSurface(
	generatorVersion string,
	sourcePath string,
	sourceSHA256 string,
	profilePath string,
	profileSHA256 string,
	manifestSHA256 string,
	operationID string,
	permissions []ConnectorGenerationPermission,
	fixtureID string,
	fixtureStatus string,
	reviewStatus string,
) ConnectorGenerationSurface {
	surface, err := NewConnectorGenerationSurface(
		generatorVersion, sourcePath, sourceSHA256, profilePath, profileSHA256, manifestSHA256,
		operationID, permissions, fixtureID, fixtureStatus, reviewStatus,
	)
	if err != nil {
		panic(err)
	}
	return surface
}

func (s ConnectorGenerationSurface) Valid() bool {
	if s.generatorVersion == "" || s.sourcePath == "" || s.profilePath == "" || s.operationID == "" ||
		s.fixtureID == "" || s.fixtureStatus == "" || s.reviewStatus == "" ||
		!connectorCapabilitySHA256Pattern.MatchString(s.sourceSHA256) ||
		!connectorCapabilitySHA256Pattern.MatchString(s.profileSHA256) ||
		!connectorCapabilitySHA256Pattern.MatchString(s.manifestSHA256) ||
		len(s.permissions) == 0 {
		return false
	}
	for _, permission := range s.permissions {
		if permission.id == "" || permission.note == "" {
			return false
		}
	}
	return true
}
func (s ConnectorGenerationSurface) GeneratorVersion() string { return s.generatorVersion }
func (s ConnectorGenerationSurface) SourcePath() string       { return s.sourcePath }
func (s ConnectorGenerationSurface) SourceSHA256() string     { return s.sourceSHA256 }
func (s ConnectorGenerationSurface) ProfilePath() string      { return s.profilePath }
func (s ConnectorGenerationSurface) ProfileSHA256() string    { return s.profileSHA256 }
func (s ConnectorGenerationSurface) ManifestSHA256() string   { return s.manifestSHA256 }
func (s ConnectorGenerationSurface) OperationID() string      { return s.operationID }
func (s ConnectorGenerationSurface) FixtureID() string        { return s.fixtureID }
func (s ConnectorGenerationSurface) FixtureStatus() string    { return s.fixtureStatus }
func (s ConnectorGenerationSurface) ReviewStatus() string     { return s.reviewStatus }
func (s ConnectorGenerationSurface) Permissions() []ConnectorGenerationPermission {
	return append([]ConnectorGenerationPermission(nil), s.permissions...)
}

type ConnectorImportSource struct {
	value string
}

func NewConnectorImportSource(value string) (ConnectorImportSource, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return ConnectorImportSource{}, fmt.Errorf("connector import source is required")
	}
	return ConnectorImportSource{value: value}, nil
}

func MustConnectorImportSource(value string) ConnectorImportSource {
	source, err := NewConnectorImportSource(value)
	if err != nil {
		panic(err)
	}
	return source
}

func (s ConnectorImportSource) URI() string { return s.value }

type providerTriggerCapabilities struct {
	base       Source
	generation triggergeneration.Generation
	targetFree []runtimeprovideroutput.Authorization
	provenance []ProviderTriggerEventProvenance
}

type ProviderTriggerEventProvenance struct {
	Provider         string
	Event            string
	Kind             string
	PackID           string
	PackVersion      string
	ManifestHash     string
	SourceProvenance string
	Generation       triggergeneration.Generation
	ProjectScopes    []string
}

func (p ProviderTriggerEventProvenance) Valid() bool {
	return strings.TrimSpace(p.Provider) != "" && strings.TrimSpace(p.Event) != "" &&
		(strings.TrimSpace(p.Kind) == "raw" || strings.TrimSpace(p.Kind) == "normalized") &&
		strings.TrimSpace(p.PackID) != "" && strings.TrimSpace(p.PackVersion) != "" &&
		strings.TrimSpace(p.ManifestHash) != "" && strings.TrimSpace(p.SourceProvenance) != "" &&
		p.Generation.Valid()
}

type connectorPackCapabilities struct {
	generations   map[string]ConnectorGenerationSurface
	importSources map[string]ConnectorImportSource
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
		if !authorization.Valid() {
			out.providerTrigger = nil
			return out
		}
		authorizations[index] = authorization
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

func (c Capabilities) WithProviderTriggerEventProvenance(provenance []ProviderTriggerEventProvenance) Capabilities {
	out := c
	if out.providerTrigger == nil {
		return out
	}
	providerTrigger := *out.providerTrigger
	items := make([]ProviderTriggerEventProvenance, len(provenance))
	for index, item := range provenance {
		item.Provider = strings.TrimSpace(item.Provider)
		item.Event = strings.TrimSpace(item.Event)
		item.Kind = strings.TrimSpace(item.Kind)
		item.PackID = strings.TrimSpace(item.PackID)
		item.PackVersion = strings.TrimSpace(item.PackVersion)
		item.ManifestHash = strings.TrimSpace(item.ManifestHash)
		item.SourceProvenance = strings.TrimSpace(item.SourceProvenance)
		item.ProjectScopes = append([]string(nil), item.ProjectScopes...)
		if !item.Valid() || !item.Generation.Equal(providerTrigger.generation) {
			out.providerTrigger = nil
			return out
		}
		items[index] = item
	}
	providerTrigger.provenance = items
	out.providerTrigger = &providerTrigger
	return out
}

func (c Capabilities) ProviderTriggerEventProvenance() []ProviderTriggerEventProvenance {
	if c.providerTrigger == nil {
		return nil
	}
	out := make([]ProviderTriggerEventProvenance, len(c.providerTrigger.provenance))
	for index, item := range c.providerTrigger.provenance {
		item.ProjectScopes = append([]string(nil), item.ProjectScopes...)
		out[index] = item
	}
	return out
}

func (c Capabilities) WithConnectorPackImports(generations map[string]ConnectorGenerationSurface, importSources map[string]ConnectorImportSource) Capabilities {
	out := c
	if len(importSources) == 0 {
		out.connectorPacks = nil
		return out
	}
	sourceCopy := make(map[string]ConnectorImportSource, len(importSources))
	for toolID, source := range importSources {
		if toolID == "" || toolID != strings.TrimSpace(toolID) || source.value == "" {
			out.connectorPacks = nil
			return out
		}
		sourceCopy[toolID] = source
	}
	generationCopy := make(map[string]ConnectorGenerationSurface, len(generations))
	for toolID, generation := range generations {
		if toolID == "" || toolID != strings.TrimSpace(toolID) || !generation.Valid() {
			out.connectorPacks = nil
			return out
		}
		if _, imported := sourceCopy[toolID]; !imported {
			out.connectorPacks = nil
			return out
		}
		generationCopy[toolID] = generation
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
	if toolID == "" || toolID != strings.TrimSpace(toolID) {
		return ConnectorGenerationSurface{}, false
	}
	generation, ok := c.connectorPacks.generations[toolID]
	return generation, ok
}

func (c Capabilities) ConnectorImportSource(toolID string) (ConnectorImportSource, bool) {
	if c.connectorPacks == nil {
		return ConnectorImportSource{}, false
	}
	if toolID == "" || toolID != strings.TrimSpace(toolID) {
		return ConnectorImportSource{}, false
	}
	source, ok := c.connectorPacks.importSources[toolID]
	return source, ok
}
