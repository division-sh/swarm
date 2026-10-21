package packartifact

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/manifesthash"
	"github.com/division-sh/swarm/internal/yamlsource"
)

const (
	InventoryManifestFileName                   = "inventory.yaml"
	SelectionEmbedded             SelectionMode = "embedded"
	SelectionDevelopmentOverride  SelectionMode = "development_override"
	ProvenanceEmbedded                          = "embedded"
	ProvenanceDevelopmentOverride               = "development_override"
)

type SelectionMode string

func (m SelectionMode) Valid() bool {
	return m == SelectionEmbedded || m == SelectionDevelopmentOverride
}

type InventoryManifest struct {
	source  yamlsource.Value
	Version int                     `yaml:"version"`
	Packs   []InventoryManifestPack `yaml:"packs"`
}

type InventoryManifestPack struct {
	Path string `yaml:"path"`
}

func LoadInventoryManifest(fsys fs.FS, manifestPath string) (InventoryManifest, error) {
	if fsys == nil || cleanRelativePath(manifestPath) == "" {
		return InventoryManifest{}, fmt.Errorf("platform inventory requires a filesystem and exact relative manifest path")
	}
	body, err := fs.ReadFile(fsys, manifestPath)
	if err != nil {
		return InventoryManifest{}, err
	}
	manifest, err := admitInventoryManifest(body, manifestPath)
	if err != nil {
		return InventoryManifest{}, err
	}
	if len(manifest.Packs) == 0 {
		return InventoryManifest{}, fmt.Errorf("platform inventory requires provider directories")
	}
	seen := map[string]bool{}
	for _, item := range manifest.Packs {
		if cleanRelativePath(item.Path) == "" || seen[item.Path] {
			return InventoryManifest{}, fmt.Errorf("platform inventory provider path %q is invalid or repeated", item.Path)
		}
		seen[item.Path] = true
	}
	return manifest, nil
}

// Entry retains one captured provider directory, including all its bodies.
type Entry struct {
	provider  AdmittedProvider
	directory string
	selection string
}

func (e Entry) ID() string                 { return e.provider.Key() }
func (e Entry) Version() string            { return e.provider.Metadata().Version }
func (e Entry) Directory() string          { return e.directory }
func (e Entry) Source() string             { return e.selection }
func (e Entry) Provider() AdmittedProvider { return e.provider }
func (e Entry) ManifestBody() []byte       { return e.provider.ManifestBody() }
func (e Entry) ManifestHash() string       { return manifesthash.FromBytes(e.ManifestBody()).String() }

type PlatformPackInventory struct {
	source                 yamlsource.Value
	mode                   SelectionMode
	digest                 string
	runningPlatformVersion string
	sourceDirectories      []string
	entries                map[string]Entry
}

func (i *PlatformPackInventory) SourceValue() yamlsource.Value {
	if i == nil {
		return yamlsource.Value{}
	}
	return i.source
}
func (i *PlatformPackInventory) SelectionMode() SelectionMode {
	if i == nil {
		return ""
	}
	return i.mode
}
func (i *PlatformPackInventory) Digest() string {
	if i == nil {
		return ""
	}
	return i.digest
}
func (i *PlatformPackInventory) Lookup(key string) (Entry, bool) {
	if i == nil {
		return Entry{}, false
	}
	entry, ok := i.entries[key]
	return entry, ok
}
func (i *PlatformPackInventory) Entries() []Entry {
	if i == nil {
		return nil
	}
	return sortedEntries(i.entries)
}
func (i *PlatformPackInventory) SourceDirectories() []string {
	if i == nil {
		return nil
	}
	return append([]string(nil), i.sourceDirectories...)
}

func LoadPlatformPackInventoryFS(fsys fs.FS, manifestPath, runningVersion string, mode SelectionMode) (*PlatformPackInventory, error) {
	if !mode.Valid() {
		return nil, fmt.Errorf("invalid platform inventory selection %q", mode)
	}
	manifest, err := LoadInventoryManifest(fsys, manifestPath)
	if err != nil {
		return nil, err
	}
	entries := map[string]Entry{}
	expected := map[string]struct{}{manifestPath: {}}
	for _, declared := range manifest.Packs {
		provider, err := LoadProviderFS(fsys, declared.Path, runningVersion)
		if err != nil {
			return nil, err
		}
		for name := range provider.Members() {
			expected[path.Join(declared.Path, name)] = struct{}{}
		}
		if _, duplicate := entries[provider.Key()]; duplicate {
			return nil, fmt.Errorf("duplicate provider directory key %q", provider.Key())
		}
		entries[provider.Key()] = Entry{provider: provider, directory: declared.Path, selection: string(mode)}
	}
	if err := rejectUnexpectedInventoryFiles(fsys, expected); err != nil {
		return nil, err
	}
	digest, err := digestInventoryFiles(fsys, expected)
	if err != nil {
		return nil, err
	}
	return &PlatformPackInventory{source: manifest.source, mode: mode, digest: digest, runningPlatformVersion: runningVersion, entries: entries}, nil
}

type EffectivePackInventory struct{ base *PlatformPackInventory }

func NewEffectivePackInventory(base *PlatformPackInventory) (*EffectivePackInventory, error) {
	if base == nil || base.Digest() == "" || !base.SelectionMode().Valid() {
		return nil, fmt.Errorf("selected platform provider inventory is required")
	}
	return &EffectivePackInventory{base: base}, nil
}
func (i *EffectivePackInventory) SourceValue() yamlsource.Value {
	if i == nil {
		return yamlsource.Value{}
	}
	return i.base.SourceValue()
}
func (i *EffectivePackInventory) BaseDigest() string {
	if i == nil {
		return ""
	}
	return i.base.Digest()
}
func (i *EffectivePackInventory) Digest() string { return i.BaseDigest() }
func (i *EffectivePackInventory) BaseSelectionMode() SelectionMode {
	if i == nil {
		return ""
	}
	return i.base.SelectionMode()
}
func (i *EffectivePackInventory) BaseDirectories() []string {
	if i == nil {
		return nil
	}
	return i.base.SourceDirectories()
}
func (i *EffectivePackInventory) Lookup(key string) (Entry, bool) {
	if i == nil {
		return Entry{}, false
	}
	return i.base.Lookup(key)
}
func (i *EffectivePackInventory) Entries() []Entry {
	if i == nil {
		return nil
	}
	return i.base.Entries()
}

func sortedEntries(entries map[string]Entry) []Entry {
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]Entry, 0, len(keys))
	for _, key := range keys {
		out = append(out, entries[key])
	}
	return out
}

func cleanRelativePath(value string) string {
	if value == "" || strings.Contains(value, "\\") || !fs.ValidPath(value) || value == "." {
		return ""
	}
	return value
}

func rejectUnexpectedInventoryFiles(fsys fs.FS, expected map[string]struct{}) error {
	directories := map[string]bool{".": true}
	for name := range expected {
		for dir := path.Dir(name); dir != "."; dir = path.Dir(dir) {
			directories[dir] = true
		}
	}
	return fs.WalkDir(fsys, ".", func(name string, item fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if item.IsDir() {
			if !directories[name] {
				return fmt.Errorf("platform inventory contains unlisted directory %q", name)
			}
			return nil
		}
		if _, ok := expected[name]; !ok || !item.Type().IsRegular() {
			return fmt.Errorf("platform inventory contains unlisted or non-regular file %q", name)
		}
		return nil
	})
}

func digestInventoryFiles(fsys fs.FS, expected map[string]struct{}) (string, error) {
	members := make(map[string][]byte, len(expected))
	for name := range expected {
		body, err := fs.ReadFile(fsys, name)
		if err != nil {
			return "", err
		}
		members[name] = body
	}
	return digestInventoryMembers(members), nil
}

func digestInventoryMembers(members map[string][]byte) string {
	names := make([]string, 0, len(members))
	for name := range members {
		names = append(names, name)
	}
	sort.Strings(names)
	hash := sha256.New()
	_, _ = hash.Write([]byte("swarm-platform-pack-inventory-v1\n"))
	for _, name := range names {
		writeDigestField(hash, []byte(name))
		writeDigestField(hash, members[name])
	}
	return fmt.Sprintf("sha256:%x", hash.Sum(nil))
}

type digestWriter interface{ Write([]byte) (int, error) }

func writeDigestField(writer digestWriter, value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = writer.Write(size[:])
	_, _ = writer.Write(value)
}
