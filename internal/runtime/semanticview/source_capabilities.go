package semanticview

import (
	"fmt"
	"regexp"
	"strings"

	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

var connectorCapabilitySHA256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

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
		&generatorVersion, &sourcePath, &sourceSHA256, &profilePath, &profileSHA256,
		&manifestSHA256, &operationID, &fixtureID, &fixtureStatus, &reviewStatus,
	}
	for _, value := range values {
		*value = strings.TrimSpace(*value)
		if *value == "" {
			return ConnectorGenerationSurface{}, fmt.Errorf("connector generation surface fields are required")
		}
	}
	hashes := []*string{&sourceSHA256, &profileSHA256, &manifestSHA256}
	for _, hash := range hashes {
		raw := strings.TrimPrefix(*hash, "sha256:")
		if !connectorCapabilitySHA256Pattern.MatchString(raw) {
			return ConnectorGenerationSurface{}, fmt.Errorf("connector generation hash %q must use canonical sha256:<lowercase-hex>", *hash)
		}
		*hash = "sha256:" + raw
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

func (s ConnectorGenerationSurface) Valid() bool              { return s.generatorVersion != "" }
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

func (s ConnectorImportSource) String() string { return s.value }

type providerTriggerCapabilities struct {
	base       Source
	generation triggergeneration.Generation
	targetFree []runtimeprovideroutput.Authorization
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

func (c Capabilities) WithConnectorPackImports(generations map[string]ConnectorGenerationSurface, importSources map[string]ConnectorImportSource) Capabilities {
	out := c
	generationCopy := make(map[string]ConnectorGenerationSurface, len(generations))
	for toolID, generation := range generations {
		if toolID == strings.TrimSpace(toolID) && toolID != "" && generation.Valid() {
			generationCopy[toolID] = generation
		}
	}
	sourceCopy := make(map[string]ConnectorImportSource, len(importSources))
	for toolID, source := range importSources {
		if toolID == strings.TrimSpace(toolID) && toolID != "" && source.value != "" {
			sourceCopy[toolID] = source
		}
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
	return generation, ok
}

func (c Capabilities) ConnectorImportSource(toolID string) (ConnectorImportSource, bool) {
	if c.connectorPacks == nil {
		return ConnectorImportSource{}, false
	}
	source, ok := c.connectorPacks.importSources[strings.TrimSpace(toolID)]
	return source, ok
}
