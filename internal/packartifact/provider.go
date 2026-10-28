package packartifact

import (
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"

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
	// Standalone readers retain a filesystem location, not a declaring flow.
	capturedDirectory string
}

func LoadProviderFS(fsys fs.FS, directory, runningVersion string) (AdmittedProvider, error) {
	if fsys == nil || cleanRelativePath(directory) == "" {
		return AdmittedProvider{}, fmt.Errorf("provider requires its exact directory name")
	}
	entries, err := fs.ReadDir(fsys, path.Dir(directory))
	if err != nil {
		return AdmittedProvider{}, err
	}
	selected, err := selectProviderDirectory(entries, directory)
	if err != nil {
		return AdmittedProvider{}, err
	}
	members := map[string][]byte{}
	var totalBytes int64
	err = fs.WalkDir(fsys, directory, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("provider member %q must be a regular file", name)
		}
		if len(members) >= sourceartifact.MaxMembers {
			return fmt.Errorf("provider directory exceeds the source member bound")
		}
		file, err := fsys.Open(name)
		if err != nil {
			return err
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("provider member %q must be a regular file", name)
		}
		limit := min(int64(sourceartifact.MaxMemberBytes), int64(sourceartifact.MaxArtifactBytes)-totalBytes)
		if info.Size() < 0 || info.Size() > limit {
			return fmt.Errorf("provider member %q exceeds the source byte bound", name)
		}
		body, err := io.ReadAll(io.LimitReader(file, limit+1))
		if err != nil {
			return err
		}
		if int64(len(body)) > limit {
			return fmt.Errorf("provider member %q exceeds the source byte bound", name)
		}
		totalBytes += int64(len(body))
		members[strings.TrimPrefix(name, directory+"/")] = body
		return nil
	})
	if err != nil {
		return AdmittedProvider{}, err
	}
	return admitCapturedProvider(selected, directory, members, runningVersion)
}

func selectProviderDirectory(entries []fs.DirEntry, directory string) (fs.DirEntry, error) {
	for _, entry := range entries {
		if entry.Name() != path.Base(directory) {
			continue
		}
		if !entry.IsDir() || entry.Type()&fs.ModeSymlink != 0 {
			return nil, fmt.Errorf("provider directory %q must be a real directory", directory)
		}
		if err := sourceartifact.ValidateFlowSegment(entry.Name()); err != nil {
			return nil, fmt.Errorf("provider directory %q: %w", directory, err)
		}
		return entry, nil
	}
	return nil, &fs.PathError{Op: "capture provider", Path: directory, Err: fs.ErrNotExist}
}

// Only filesystem readers supply the selected entry; member bytes cannot mint
// a provider key or a runtime root admission.
func admitCapturedProvider(selected fs.DirEntry, directory string, members map[string][]byte, runningVersion string) (AdmittedProvider, error) {
	source, err := sourceartifact.AdmitMembers(members)
	if err != nil {
		return AdmittedProvider{}, err
	}
	provider, err := providerFromArtifact(source, ".", selected.Name())
	if err != nil {
		return AdmittedProvider{}, err
	}
	if err := provider.Metadata().ValidatePlatformVersion(runningVersion); err != nil {
		return AdmittedProvider{}, err
	}
	provider.capturedDirectory = directory
	return provider, nil
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

func (p AdmittedProvider) Valid() bool {
	return p.source != nil && p.key != "" && p.directory.Manifest().Kind() == packmodel.DirectoryProvider
}
func (p AdmittedProvider) Key() string { return p.key }
func (p AdmittedProvider) Directory() string {
	if p.capturedDirectory != "" {
		return p.capturedDirectory
	}
	return p.directory.Path()
}
func (p AdmittedProvider) DeclaringFlow() string {
	if p.capturedDirectory != "" {
		return ""
	}
	return p.directory.FlowPath()
}
func (p AdmittedProvider) Metadata() packmodel.Manifest { return p.directory.Manifest() }
func (p AdmittedProvider) ManifestBody() []byte {
	if !p.Valid() {
		return nil
	}
	label := packmodel.ManifestFileName
	if p.directory.Path() != "." {
		label = p.directory.Path() + "/" + label
	}
	entry, _ := p.source.Entry(label)
	return entry.Bytes()
}

func (p AdmittedProvider) Members() map[string][]byte {
	out := map[string][]byte{}
	if !p.Valid() {
		return out
	}
	prefix := ""
	if p.directory.Path() != "." {
		prefix = p.directory.Path() + "/"
	}
	for _, entry := range p.source.Entries() {
		if strings.HasPrefix(entry.Label(), prefix) {
			out[strings.TrimPrefix(entry.Label(), prefix)] = entry.Bytes()
		}
	}
	return out
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
