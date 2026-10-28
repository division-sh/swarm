package packadmission

import (
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packartifact"
	platformcontract "github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
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

func (p ProviderSource) FlowPaths() []string {
	paths := make([]string, 0, len(p.scopes))
	for flow := range p.scopes {
		paths = append(paths, flow)
	}
	sort.Strings(paths)
	return paths
}

func (p ProviderSource) VerifyProviderOutputAuthorization(authorization runtimeprovideroutput.Authorization) error {
	if !p.Valid() || !authorization.Valid() {
		return fmt.Errorf("provider output requires its captured source and complete authorization")
	}
	for _, flow := range p.FlowPaths() {
		scope, err := p.Scope(flow)
		if err != nil {
			return err
		}
		catalog, err := scope.TriggerCatalog()
		if err != nil {
			return err
		}
		if catalog.VerifyProviderOutputAuthorization(authorization) == nil {
			return nil
		}
	}
	return fmt.Errorf("provider output does not belong to a captured flow scope")
}

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

// ConnectionAccount resolves authored placement without requiring a linked
// account or a provider-emitted payload during structural verification.
func (s ProviderScope) ConnectionAccount(bundle *runtimecontracts.WorkflowContractBundle) (ProviderDefinition, runtimecontracts.FlowOperatorAccount, error) {
	var selected ProviderDefinition
	var account runtimecontracts.FlowOperatorAccount
	for _, definition := range s.HeldProviders() {
		if _, connection := definition.ChannelPlan(); !connection {
			continue
		}
		if selected.Valid() {
			directory := definition.Provider().Directory()
			return ProviderDefinition{}, account, fmt.Errorf("flow %s holds a second connection-account provider %s; move %s/ into its own child flow", s.flowPath, directory, directory)
		}
		selected = definition
	}
	if !selected.Valid() {
		return selected, account, nil
	}
	rooted, err := bundle.RootedSource()
	if err != nil || !rooted.Admission().Equal(s.source.rooted.Admission()) {
		return ProviderDefinition{}, account, fmt.Errorf("flow %s connection account requires the same captured source", s.flowPath)
	}
	schema, found := bundle.FlowSchemaByID(s.flowPath)
	if !found {
		return ProviderDefinition{}, account, fmt.Errorf("flow %s connection account requires its admitted schema", s.flowPath)
	}
	for _, consumed := range s.ConsumedTriggerProviders(schema.Pins.Inputs.EventTypes()) {
		if consumed.Provider().Directory() == selected.Provider().Directory() {
			account, err := bundle.ResolveFlowOperatorAccount(s.flowPath)
			if err == nil && account.FromPolicy {
				plan, _ := selected.ChannelPlan()
				canonical, canonicalErr := channelonboarding.CanonicalAccount(plan.Provider(), account.Policy)
				if canonicalErr != nil {
					return selected, account, fmt.Errorf("flow %s policy.yaml operator_account: %w", s.flowPath, canonicalErr)
				}
				account.Policy = canonical.Account
			}
			return selected, account, err
		}
	}
	return selected, account, nil
}

// ConsumedTriggerProviders projects held declarations, not route activation.
func (s ProviderScope) ConsumedTriggerProviders(inputs []string) []ProviderDefinition {
	var out []ProviderDefinition
	for _, definition := range s.HeldProviders() {
		catalog, present := definition.TriggerCatalog()
		if !present {
			continue
		}
		entry, _ := catalog.EntryByProvider(definition.Provider().Key())
		used := false
		for _, input := range inputs {
			for _, output := range entry.Manifest.OutputManifest() {
				if output.Event == input || output.Kind == providertriggers.OutputKindRaw && output.EventName.Accepts(input) {
					used = true
				}
			}
		}
		if used {
			out = append(out, definition)
		}
	}
	return out
}

func (s ProviderScope) TriggerCatalog() (*providertriggers.CatalogSnapshot, error) {
	if !s.source.Valid() {
		return nil, fmt.Errorf("trigger catalog requires an admitted provider scope")
	}
	var catalogs []*providertriggers.CatalogSnapshot
	for _, definition := range s.Providers() {
		if catalog, present := definition.TriggerCatalog(); present {
			catalogs = append(catalogs, catalog)
		}
	}
	return providertriggers.ComposeCatalogSnapshots(catalogs...)
}
