package packartifact

import (
	"fmt"

	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/runtime/core/manifesthash"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/yamlsource"
)

// AdmittedProvider is a view of one directory in its immutable source artifact.
type AdmittedProvider struct {
	source    *sourceartifact.AdmittedSourceArtifact
	directory sourceartifact.ProviderDirectory
	key       string
}

func ProviderFromArtifact(source *sourceartifact.AdmittedSourceArtifact, directory string) (AdmittedProvider, error) {
	key, err := source.ProviderKey(directory)
	if err != nil {
		return AdmittedProvider{}, err
	}
	return providerFromArtifact(source, directory, key)
}

// ProviderFromRootedSource retains the selected root's key without minting root
// evidence on its shared, content-only artifact.
func ProviderFromRootedSource(source sourceartifact.RootedSource, directory string) (AdmittedProvider, error) {
	key, err := source.ProviderKey(directory)
	if err != nil {
		return AdmittedProvider{}, err
	}
	return providerFromArtifact(source.Artifact(), directory, key)
}

func providerFromArtifact(source *sourceartifact.AdmittedSourceArtifact, directory, key string) (AdmittedProvider, error) {
	provider, present := source.ProviderDirectory(directory)
	if !present {
		return AdmittedProvider{}, fmt.Errorf("source has no admitted provider directory %q", directory)
	}
	return AdmittedProvider{source: source, directory: provider, key: key}, nil
}

// AdmitProviderMembers consumes captured bytes, not host paths or an import
// receipt. Embedded/development readers retain their existing safe I/O owners.
func AdmitProviderMembers(directory string, members map[string][]byte) (AdmittedProvider, error) {
	if directory == "" {
		return AdmittedProvider{}, fmt.Errorf("provider directory is required")
	}
	if len(members) > sourceartifact.MaxMembers {
		return AdmittedProvider{}, fmt.Errorf("provider directory exceeds the source member bound")
	}
	labels := make(map[string][]byte, len(members))
	for member, body := range members {
		label := member
		if directory != "." {
			label = directory + "/" + member
		}
		labels[label] = body
	}
	source, err := sourceartifact.AdmitMembers(labels)
	if err != nil {
		return AdmittedProvider{}, err
	}
	return ProviderFromArtifact(source, directory)
}

func (p AdmittedProvider) Valid() bool {
	return p.source != nil && p.key != "" && p.directory.Manifest().Kind() == packmodel.DirectoryProvider
}
func (p AdmittedProvider) Key() string                  { return p.key }
func (p AdmittedProvider) Directory() string            { return p.directory.Path() }
func (p AdmittedProvider) DeclaringFlow() string        { return p.directory.FlowPath() }
func (p AdmittedProvider) Metadata() packmodel.Manifest { return p.directory.Manifest() }
func (p AdmittedProvider) ManifestBody() []byte {
	if !p.Valid() {
		return nil
	}
	label := packmodel.ManifestFileName
	if p.Directory() != "." {
		label = p.Directory() + "/" + label
	}
	entry, _ := p.source.Entry(label)
	return entry.Bytes()
}

type ProviderBody struct {
	provider AdmittedProvider
	role     packmodel.BodyRole
	label    string
}

func (p AdmittedProvider) Body(role packmodel.BodyRole) (ProviderBody, bool) {
	if !p.Valid() {
		return ProviderBody{}, false
	}
	label, present := p.directory.Body(role)
	if !present {
		return ProviderBody{}, false
	}
	return ProviderBody{provider: p, role: role, label: label}, true
}

func (b ProviderBody) Valid() bool {
	if !b.provider.Valid() {
		return false
	}
	label, present := b.provider.directory.Body(b.role)
	return present && label == b.label
}
func (b ProviderBody) Role() packmodel.BodyRole     { return b.role }
func (b ProviderBody) Label() string                { return b.label }
func (b ProviderBody) Metadata() packmodel.Manifest { return b.provider.Metadata() }
func (b ProviderBody) ProviderKey() string {
	if !b.Valid() {
		return ""
	}
	return b.provider.Key()
}

func (b ProviderBody) Bytes() []byte {
	if !b.Valid() {
		return nil
	}
	entry, _ := b.provider.source.Entry(b.label)
	return entry.Bytes()
}

func (b ProviderBody) Hash() string {
	if !b.Valid() {
		return ""
	}
	return manifesthash.FromBytes(b.Bytes()).String()
}

func (b ProviderBody) SourceValue() yamlsource.Value {
	if !b.Valid() {
		return yamlsource.Value{}
	}
	document, _ := b.provider.source.YAML(b.label)
	return document.Root()
}
