package packadmission

import (
	"fmt"

	"github.com/division-sh/swarm/internal/packartifact"
	platformcontract "github.com/division-sh/swarm/internal/platform"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

// ProviderSource freezes declaration visibility for one exact rooted source.
// Held membership is separate from visibility and grants no route or execution.
type ProviderSource struct {
	rooted      sourceartifact.RootedSource
	definitions map[string]ProviderDefinition
	scopes      map[string][]string
}

func AdmitProviderSource(rooted sourceartifact.RootedSource, platform runtimecontracts.PlatformSpecDocument) (ProviderSource, error) {
	if err := rooted.Validate(); err != nil {
		return ProviderSource{}, fmt.Errorf("provider source requires exact rooted admission: %w", err)
	}
	artifact := rooted.Artifact()
	if _, providerRoot := artifact.ProviderDirectory("."); providerRoot {
		return ProviderSource{}, fmt.Errorf("provider source requires a flow directory, not a standalone provider")
	}
	if err := platformcontract.ValidateProductPlatformVersion("*", platform.Platform.Version); err != nil {
		return ProviderSource{}, err
	}
	if err := artifact.ValidatePlatformVersion(platform.Platform.Version); err != nil {
		return ProviderSource{}, err
	}
	out := ProviderSource{rooted: rooted, definitions: map[string]ProviderDefinition{}, scopes: map[string][]string{}}
	for _, directory := range artifact.ProviderDirectories() {
		provider, err := packartifact.ProviderFromRootedSource(rooted, directory.Path())
		if err != nil {
			return ProviderSource{}, err
		}
		definition, err := AdmitProviderDirectory(provider, platform)
		if err != nil {
			return ProviderSource{}, fmt.Errorf("provider directory %s: %w", directory.Path(), err)
		}
		out.definitions[directory.Path()] = definition
	}
	if err := out.freezeScopes(artifact.Root()); err != nil {
		return ProviderSource{}, err
	}
	return out, nil
}

func (p *ProviderSource) freezeScopes(flow *sourceartifact.FlowNode) error {
	if flow == nil {
		return fmt.Errorf("provider source requires its admitted flow tree")
	}
	selected, err := p.rooted.Artifact().ProviderDirectoriesInScope(flow.Path())
	if err != nil {
		return err
	}
	directories := make([]string, 0, len(selected))
	for _, directory := range selected {
		if !p.definitions[directory.Path()].Valid() {
			return fmt.Errorf("selected provider %s has no admitted definition", directory.Path())
		}
		directories = append(directories, directory.Path())
	}
	p.scopes[flow.Path()] = directories
	for _, child := range flow.Children() {
		if err := p.freezeScopes(child); err != nil {
			return err
		}
	}
	return nil
}

func (p ProviderSource) Valid() bool {
	return p.rooted.Validate() == nil && p.scopes != nil
}

func (p ProviderSource) RootedSource() sourceartifact.RootedSource { return p.rooted }

// Scope accepts an exact admitted flow path, never a directory/key heuristic.
func (p ProviderSource) Scope(flowPath string) (ProviderScope, error) {
	if !p.Valid() {
		return ProviderScope{}, fmt.Errorf("admitted provider source is required")
	}
	directories, present := p.scopes[flowPath]
	if !present {
		return ProviderScope{}, fmt.Errorf("provider scope flow %q is not in the admitted source", flowPath)
	}
	return ProviderScope{source: p, flowPath: flowPath, directories: directories}, nil
}

type ProviderScope struct {
	source      ProviderSource
	flowPath    string
	directories []string
}

func (s ProviderScope) FlowPath() string { return s.flowPath }

func (s ProviderScope) Providers() []ProviderDefinition {
	out := make([]ProviderDefinition, 0, len(s.directories))
	for _, directory := range s.directories {
		out = append(out, s.source.definitions[directory])
	}
	return out
}

func (s ProviderScope) Provider(key string) (ProviderDefinition, bool) {
	for _, directory := range s.directories {
		definition := s.source.definitions[directory]
		if definition.Provider().Key() == key {
			return definition, true
		}
	}
	return ProviderDefinition{}, false
}

// HeldProviders is declaration ownership, not enabled ingress or a claim.
func (s ProviderScope) HeldProviders() []ProviderDefinition {
	var out []ProviderDefinition
	for _, definition := range s.Providers() {
		if definition.Provider().DeclaringFlow() == s.flowPath {
			out = append(out, definition)
		}
	}
	return out
}
