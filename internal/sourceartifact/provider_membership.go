package sourceartifact

import (
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/yamlsource"
)

// ProviderDirectory retains the exact admitted membership, not an import record.
type ProviderDirectory struct {
	directory string
	flowPath  string
	manifest  Manifest
	bodies    map[packmodel.BodyRole]string
}

func (p ProviderDirectory) Path() string       { return p.directory }
func (p ProviderDirectory) FlowPath() string   { return p.flowPath }
func (p ProviderDirectory) Manifest() Manifest { return p.manifest }
func (p ProviderDirectory) Body(role packmodel.BodyRole) (string, bool) {
	label, ok := p.bodies[role]
	return label, ok
}

func (a *AdmittedSourceArtifact) ProviderDirectories() []ProviderDirectory {
	if a == nil {
		return nil
	}
	directories := sortedProviderDirectories(a.providers)
	out := make([]ProviderDirectory, 0, len(directories))
	for _, directory := range directories {
		out = append(out, a.providers[directory])
	}
	return out
}

func (a *AdmittedSourceArtifact) ProviderDirectory(directory string) (ProviderDirectory, bool) {
	if a == nil {
		return ProviderDirectory{}, false
	}
	provider, present := a.providers[directory]
	return provider, present
}

type sourceMembership struct {
	providers map[string]ProviderDirectory
	manifests map[string]Manifest
	root      *FlowNode
}

func admitDirectoryMembership(entries []Entry, snapshots map[string]yamlsource.Snapshot) (sourceMembership, error) {
	providers := collectProviderDirectories(entries)
	if err := validateProviderNesting(providers); err != nil {
		return sourceMembership{}, err
	}
	if err := validateProviderOnlyMembers(entries, providers); err != nil {
		return sourceMembership{}, err
	}
	root, err := buildFlowTree(entries, providers)
	if err != nil {
		return sourceMembership{}, err
	}
	manifests, err := admitDirectoryManifests(entries, snapshots, providers)
	if err != nil {
		return sourceMembership{}, err
	}
	for _, directory := range sortedProviderDirectories(providers) {
		provider, err := admitProviderDirectory(providers[directory], manifests, snapshots)
		if err != nil {
			return sourceMembership{}, err
		}
		providers[directory] = provider
	}
	return sourceMembership{providers: providers, manifests: manifests, root: root}, nil
}

func validateProviderNesting(providers map[string]ProviderDirectory) error {
	for _, directory := range sortedProviderDirectories(providers) {
		if directory != "." {
			if parent, nested := providerForLabel(directory, providers); nested {
				return fmt.Errorf("provider directory %s contains nested provider %s; move the provider into a flow subfolder", parent.directory, directory)
			}
		}
	}
	return nil
}

func admitDirectoryManifests(entries []Entry, snapshots map[string]yamlsource.Snapshot, providers map[string]ProviderDirectory) (map[string]Manifest, error) {
	manifests := make(map[string]Manifest)
	for _, entry := range entries {
		if entry.disposition != DispositionManifest {
			continue
		}
		kind := packmodel.DirectoryFlow
		if _, provider := providers[path.Dir(entry.label)]; provider {
			kind = packmodel.DirectoryProvider
		}
		manifest, err := packmodel.ProjectManifest(snapshots[entry.label].Document(entry.label).Root(), kind)
		if err != nil {
			return nil, err
		}
		manifests[entry.label] = manifest
	}
	return manifests, nil
}

func admitProviderDirectory(provider ProviderDirectory, manifests map[string]Manifest, snapshots map[string]yamlsource.Snapshot) (ProviderDirectory, error) {
	directory := provider.directory
	manifest, present := manifests[path.Join(directory, packmodel.ManifestFileName)]
	if !present {
		return ProviderDirectory{}, fmt.Errorf("provider directory %s requires manifest.yaml beside its bodies", directory)
	}
	for _, role := range []packmodel.BodyRole{packmodel.BodyTrigger, packmodel.BodyConnector, packmodel.BodyChannel} {
		if label, present := provider.bodies[role]; present {
			if err := manifest.ValidateBodyHeader(snapshots[label].Document(label).Root()); err != nil {
				return ProviderDirectory{}, fmt.Errorf("provider body %s: %w", label, err)
			}
		}
	}
	if _, channel := provider.bodies[packmodel.BodyChannel]; channel {
		_, trigger := provider.bodies[packmodel.BodyTrigger]
		_, connector := provider.bodies[packmodel.BodyConnector]
		if !trigger || !connector {
			return ProviderDirectory{}, fmt.Errorf("%s: channel.yaml requires trigger.yaml and connector.yaml in the same folder", directory)
		}
	}
	provider.manifest = manifest
	return provider, nil
}

func sortedProviderDirectories(providers map[string]ProviderDirectory) []string {
	out := make([]string, 0, len(providers))
	for directory := range providers {
		out = append(out, directory)
	}
	sort.Strings(out)
	return out
}

func collectProviderDirectories(entries []Entry) map[string]ProviderDirectory {
	providers := make(map[string]ProviderDirectory)
	for _, entry := range entries {
		_, local, branch, err := splitLabelTopology(strings.Split(entry.label, "/"))
		if err != nil || branch != "" {
			continue
		}
		role, body := packmodel.BodyRoleForFile(local)
		if !body {
			continue
		}
		directory := path.Dir(entry.label)
		provider, present := providers[directory]
		if !present {
			flowPath := path.Dir(directory)
			if directory == "." {
				flowPath = ""
			}
			provider = ProviderDirectory{directory: directory, flowPath: flowPath, bodies: make(map[packmodel.BodyRole]string)}
		}
		provider.bodies[role] = entry.label
		providers[directory] = provider
	}
	return providers
}

func providerForLabel(label string, providers map[string]ProviderDirectory) (ProviderDirectory, bool) {
	for directory := path.Dir(label); ; directory = path.Dir(directory) {
		if provider, present := providers[directory]; present {
			return provider, true
		}
		if directory == "." {
			return ProviderDirectory{}, false
		}
	}
}

func validateProviderOnlyMembers(entries []Entry, providers map[string]ProviderDirectory) error {
	for _, entry := range entries {
		provider, present := providerForLabel(entry.label, providers)
		if !present {
			continue
		}
		relative := entry.label
		if provider.directory != "." {
			relative = strings.TrimPrefix(relative, provider.directory+"/")
		}
		if relative == packmodel.ManifestFileName {
			continue
		}
		if _, body := packmodel.BodyRoleForFile(relative); body {
			continue
		}
		if strings.HasPrefix(relative, "docs/") {
			continue
		}
		if _, document := inertDocuments[relative]; document {
			continue
		}
		return fmt.Errorf("directory %s holds both flow files and provider bodies; move the provider into a subfolder", provider.directory)
	}
	return nil
}
