package packartifact

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"
	"strings"
	"testing/fstest"

	basepacks "github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/runtime/core/manifesthash"
	"github.com/division-sh/swarm/internal/runtime/core/packidentity"
	"gopkg.in/yaml.v3"
)

const (
	InventoryManifestFileName = "inventory.yaml"
	ManifestHashDerived       = "derived"

	SelectionEmbedded            SelectionMode = "embedded"
	SelectionDevelopmentOverride SelectionMode = "development_override"

	ProvenancePlatform            = basepacks.ProvenancePlatform
	ProvenanceExternal            = basepacks.ProvenanceExternal
	ProvenanceProject             = basepacks.ProvenanceProject
	ProvenanceEmbedded            = "embedded"
	ProvenanceDevelopmentOverride = "development_override"

	EnvelopeFileName          = basepacks.EnvelopeFileName
	TriggerManifestFileName   = basepacks.TriggerManifestFileName
	ConnectorManifestFileName = basepacks.ConnectorManifestFileName
	ChannelManifestFileName   = basepacks.ChannelManifestFileName

	TypeTrigger   = basepacks.TypeTrigger
	TypeConnector = basepacks.TypeConnector
	TypeChannel   = basepacks.TypeChannel
)

type (
	Envelope        = basepacks.Envelope
	Provenance      = basepacks.Provenance
	Capabilities    = basepacks.Capabilities
	CanCapabilities = basepacks.CanCapabilities
	Requires        = basepacks.Requires
)

var (
	StampEnvelope           = basepacks.StampEnvelope
	ManifestFileNameForType = basepacks.ManifestFileNameForType
)

type SelectionMode string

func (m SelectionMode) Valid() bool {
	return m == SelectionEmbedded || m == SelectionDevelopmentOverride
}

type InventoryManifest struct {
	Version int                     `yaml:"version"`
	Packs   []InventoryManifestPack `yaml:"packs"`
}

type InventoryManifestPack struct {
	ID   string `yaml:"id"`
	Type string `yaml:"type"`
	Path string `yaml:"path"`
}

func LoadInventoryManifest(fsys fs.FS, manifestPath string) (InventoryManifest, error) {
	if fsys == nil {
		return InventoryManifest{}, fmt.Errorf("platform pack inventory filesystem is required")
	}
	manifestPath = cleanRelativePath(manifestPath)
	if manifestPath == "" {
		return InventoryManifest{}, fmt.Errorf("platform pack inventory manifest path is required")
	}
	manifestBody, err := fs.ReadFile(fsys, manifestPath)
	if err != nil {
		return InventoryManifest{}, fmt.Errorf("read platform pack inventory manifest %q: %w", manifestPath, err)
	}
	var manifest InventoryManifest
	decoder := yaml.NewDecoder(bytes.NewReader(manifestBody))
	decoder.KnownFields(true)
	if err := decoder.Decode(&manifest); err != nil {
		return InventoryManifest{}, fmt.Errorf("parse platform pack inventory manifest %q: %w", manifestPath, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return InventoryManifest{}, fmt.Errorf("platform pack inventory manifest %q contains multiple YAML documents", manifestPath)
		}
		return InventoryManifest{}, fmt.Errorf("parse platform pack inventory manifest %q trailing document: %w", manifestPath, err)
	}
	if manifest.Version != 1 {
		return InventoryManifest{}, fmt.Errorf("platform pack inventory version %d is unsupported", manifest.Version)
	}
	if len(manifest.Packs) == 0 {
		return InventoryManifest{}, fmt.Errorf("platform pack inventory packs are required")
	}
	seenIDs := make(map[string]struct{}, len(manifest.Packs))
	seenPaths := make(map[string]string, len(manifest.Packs))
	for index := range manifest.Packs {
		declared := &manifest.Packs[index]
		declared.ID = strings.TrimSpace(declared.ID)
		declared.Type = strings.TrimSpace(declared.Type)
		declared.Path = cleanRelativePath(declared.Path)
		if declared.ID == "" || declared.Path == "" || ManifestFileNameForType(declared.Type) == "" {
			return InventoryManifest{}, fmt.Errorf("platform pack inventory packs[%d] requires valid id, type, and relative path", index)
		}
		if _, duplicate := seenIDs[declared.ID]; duplicate {
			return InventoryManifest{}, fmt.Errorf("duplicate platform pack inventory id %q", declared.ID)
		}
		if previous, duplicate := seenPaths[declared.Path]; duplicate {
			return InventoryManifest{}, fmt.Errorf("platform pack inventory path %q is shared by %q and %q", declared.Path, previous, declared.ID)
		}
		seenIDs[declared.ID] = struct{}{}
		seenPaths[declared.Path] = declared.ID
	}
	return manifest, nil
}

type ImportOrigin struct {
	Source       string `yaml:"source" json:"source"`
	ID           string `yaml:"id" json:"id"`
	Version      string `yaml:"version" json:"version"`
	ManifestHash string `yaml:"manifest_hash" json:"manifest_hash"`
	EnvelopeHash string `yaml:"envelope_hash" json:"envelope_hash"`
}

func (o ImportOrigin) Valid() bool {
	if strings.TrimSpace(o.Source) != ProvenanceEmbedded {
		return false
	}
	if _, err := packidentity.ParseID(strings.TrimSpace(o.ID)); err != nil {
		return false
	}
	if _, err := packidentity.ParseVersion(strings.TrimSpace(o.Version)); err != nil {
		return false
	}
	if _, err := manifesthash.Parse(strings.TrimSpace(o.ManifestHash)); err != nil {
		return false
	}
	_, err := manifesthash.Parse(strings.TrimSpace(o.EnvelopeHash))
	return err == nil
}

type ProjectPackSource struct {
	Path         string
	EnvelopeBody []byte
	ManifestBody []byte
	Origin       ImportOrigin
}

type Entry struct {
	envelope     Envelope
	envelopeBody []byte
	manifestBody []byte
	directory    string
	selection    string
	origin       ImportOrigin
	shadowsBase  bool
}

func (e Entry) ID() string           { return e.envelope.ID }
func (e Entry) Version() string      { return e.envelope.Version }
func (e Entry) Type() string         { return e.envelope.Type }
func (e Entry) ManifestHash() string { return e.envelope.ManifestHash }
func (e Entry) Directory() string    { return e.directory }
func (e Entry) Source() string       { return e.selection }
func (e Entry) Origin() ImportOrigin { return e.origin }
func (e Entry) ShadowsBase() bool    { return e.shadowsBase }
func (e Entry) Modified() bool {
	return e.selection == ProvenanceProject && e.origin.Valid() &&
		(e.Version() != strings.TrimSpace(e.origin.Version) ||
			e.ManifestHash() != strings.TrimSpace(e.origin.ManifestHash) ||
			importedEnvelopeHash(e.envelopeBody) != strings.TrimSpace(e.origin.EnvelopeHash))
}
func (e Entry) Envelope() Envelope   { return cloneEnvelope(e.envelope) }
func (e Entry) EnvelopeBody() []byte { return append([]byte(nil), e.envelopeBody...) }
func (e Entry) ManifestBody() []byte { return append([]byte(nil), e.manifestBody...) }

func (e Entry) FileSystem() fs.FS {
	envelopeBody, err := yaml.Marshal(e.envelope)
	if err != nil {
		panic(fmt.Sprintf("marshal admitted pack %q envelope: %v", e.ID(), err))
	}
	return fstest.MapFS{
		EnvelopeFileName:                         &fstest.MapFile{Data: envelopeBody, Mode: 0o444},
		ManifestFileNameForType(e.envelope.Type): &fstest.MapFile{Data: e.ManifestBody(), Mode: 0o444},
	}
}

func cloneEntry(e Entry) Entry {
	e.envelope = cloneEnvelope(e.envelope)
	e.envelopeBody = append([]byte(nil), e.envelopeBody...)
	e.manifestBody = append([]byte(nil), e.manifestBody...)
	return e
}

func cloneEnvelope(envelope Envelope) Envelope {
	envelope.Implements = append([]string(nil), envelope.Implements...)
	envelope.Capabilities.Can.EmitEvents = append([]string(nil), envelope.Capabilities.Can.EmitEvents...)
	envelope.Capabilities.Can.CallProviderActions = append([]string(nil), envelope.Capabilities.Can.CallProviderActions...)
	envelope.Capabilities.Cannot = append([]string(nil), envelope.Capabilities.Cannot...)
	envelope.Requires.Secrets = append([]string(nil), envelope.Requires.Secrets...)
	envelope.Requires.ManagedCredentials = append([]string(nil), envelope.Requires.ManagedCredentials...)
	if envelope.Requires.Packs != nil {
		requiredPacks := envelope.Requires.Packs
		envelope.Requires.Packs = make(map[string]string, len(requiredPacks))
		for id, version := range requiredPacks {
			envelope.Requires.Packs[id] = version
		}
	}
	envelope.Tests = append([]string(nil), envelope.Tests...)
	return envelope
}

type PlatformPackInventory struct {
	mode                   SelectionMode
	digest                 string
	runningPlatformVersion string
	sourceDirectories      []string
	entries                map[string]Entry
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

func (i *PlatformPackInventory) Lookup(id string) (Entry, bool) {
	if i == nil {
		return Entry{}, false
	}
	entry, ok := i.entries[strings.TrimSpace(id)]
	return cloneEntry(entry), ok
}

func (i *PlatformPackInventory) Entries() []Entry {
	if i == nil {
		return nil
	}
	ids := sortedEntryIDs(i.entries)
	out := make([]Entry, 0, len(ids))
	for _, id := range ids {
		out = append(out, cloneEntry(i.entries[id]))
	}
	return out
}

func (i *PlatformPackInventory) SourceDirectories() []string {
	if i == nil {
		return nil
	}
	return append([]string(nil), i.sourceDirectories...)
}

func LoadPlatformPackInventoryFS(fsys fs.FS, manifestPath, runningPlatformVersion string, mode SelectionMode) (*PlatformPackInventory, error) {
	if fsys == nil {
		return nil, fmt.Errorf("platform pack inventory filesystem is required")
	}
	if !mode.Valid() {
		return nil, fmt.Errorf("platform pack inventory selection mode %q is invalid", mode)
	}
	manifestPath = cleanRelativePath(manifestPath)
	if manifestPath == "" {
		return nil, fmt.Errorf("platform pack inventory manifest path is required")
	}
	manifest, err := LoadInventoryManifest(fsys, manifestPath)
	if err != nil {
		return nil, err
	}

	selection := ProvenanceEmbedded
	if mode == SelectionDevelopmentOverride {
		selection = ProvenanceDevelopmentOverride
	}
	entries := make(map[string]Entry, len(manifest.Packs))
	expectedFiles := map[string]struct{}{manifestPath: {}}
	for _, declared := range manifest.Packs {
		loaded, err := basepacks.Load(fsys, declared.Path, runningPlatformVersion)
		if err != nil {
			return nil, fmt.Errorf("load platform inventory pack %q: %w", declared.ID, err)
		}
		if loaded.Envelope.ID != declared.ID {
			return nil, fmt.Errorf("platform inventory path %q declares id %q but envelope owns %q", declared.Path, declared.ID, loaded.Envelope.ID)
		}
		if loaded.Envelope.Type != declared.Type {
			return nil, fmt.Errorf("platform inventory pack %q declares type %q but envelope owns %q", declared.ID, declared.Type, loaded.Envelope.Type)
		}
		if loaded.Envelope.Provenance.Source != ProvenancePlatform {
			return nil, fmt.Errorf("platform inventory pack %q provenance %q must be %q", declared.ID, loaded.Envelope.Provenance.Source, ProvenancePlatform)
		}
		envelopePath := path.Join(declared.Path, EnvelopeFileName)
		manifestFilePath := path.Join(declared.Path, ManifestFileNameForType(declared.Type))
		envelopeBody, err := fs.ReadFile(fsys, envelopePath)
		if err != nil {
			return nil, fmt.Errorf("read platform inventory envelope %q: %w", envelopePath, err)
		}
		expectedFiles[envelopePath] = struct{}{}
		expectedFiles[manifestFilePath] = struct{}{}
		entries[declared.ID] = Entry{
			envelope: loaded.Envelope, envelopeBody: envelopeBody, manifestBody: loaded.ManifestBody,
			directory: declared.Path, selection: selection,
		}
	}
	if err := rejectUnexpectedInventoryFiles(fsys, expectedFiles); err != nil {
		return nil, err
	}
	digest, err := digestInventoryFiles(fsys, expectedFiles)
	if err != nil {
		return nil, err
	}
	return &PlatformPackInventory{mode: mode, digest: digest, runningPlatformVersion: runningPlatformVersion, entries: entries}, nil
}

type EffectivePackInventory struct {
	baseDigest      string
	digest          string
	baseMode        SelectionMode
	baseDirectories []string
	entries         map[string]Entry
}

func NewEffectivePackInventory(base *PlatformPackInventory, projects []ProjectPackSource) (*EffectivePackInventory, error) {
	if base == nil || base.Digest() == "" || !base.SelectionMode().Valid() {
		return nil, fmt.Errorf("selected platform pack base inventory is required")
	}
	entries := make(map[string]Entry, len(base.entries)+len(projects))
	for id, entry := range base.entries {
		entries[id] = cloneEntry(entry)
	}
	seenPaths := make(map[string]string, len(projects))
	for index, source := range projects {
		projectPath := cleanRelativePath(source.Path)
		if projectPath == "" {
			return nil, fmt.Errorf("project pack source %d path is invalid", index)
		}
		if previous, duplicate := seenPaths[projectPath]; duplicate {
			return nil, fmt.Errorf("duplicate project pack path %q for %q", projectPath, previous)
		}
		if !source.Origin.Valid() {
			return nil, fmt.Errorf("project pack %q import origin is invalid", projectPath)
		}
		envelope, err := basepacks.ParseEnvelope(source.EnvelopeBody)
		if err != nil {
			return nil, fmt.Errorf("parse project pack %q envelope: %w", projectPath, err)
		}
		if envelope.Provenance.Source != ProvenanceProject {
			return nil, fmt.Errorf("project pack %q provenance %q must be %q", projectPath, envelope.Provenance.Source, ProvenanceProject)
		}
		if strings.TrimSpace(envelope.ManifestHash) != ManifestHashDerived {
			return nil, fmt.Errorf("project pack %q manifest_hash must be %q", projectPath, ManifestHashDerived)
		}
		envelope.ManifestHash = basepacks.ManifestHash(source.ManifestBody)
		if err := envelope.ValidateCommon(base.runningPlatformVersion); err != nil {
			return nil, fmt.Errorf("validate project pack %q: %w", projectPath, err)
		}
		if envelope.ID != source.Origin.ID {
			return nil, fmt.Errorf("project pack %q id %q contradicts imported origin id %q", projectPath, envelope.ID, source.Origin.ID)
		}
		seenPaths[projectPath] = envelope.ID
		_, shadowsBase := entries[envelope.ID]
		entries[envelope.ID] = Entry{
			envelope: envelope, envelopeBody: append([]byte(nil), source.EnvelopeBody...),
			manifestBody: append([]byte(nil), source.ManifestBody...), directory: projectPath,
			selection: ProvenanceProject, origin: source.Origin, shadowsBase: shadowsBase,
		}
	}
	digest := digestEffectiveInventory(base.Digest(), entries)
	return &EffectivePackInventory{
		baseDigest:      base.Digest(),
		digest:          digest,
		baseMode:        base.SelectionMode(),
		baseDirectories: base.SourceDirectories(),
		entries:         entries,
	}, nil
}

func (i *EffectivePackInventory) BaseDigest() string {
	if i == nil {
		return ""
	}
	return i.baseDigest
}

func (i *EffectivePackInventory) BaseSelectionMode() SelectionMode {
	if i == nil {
		return ""
	}
	return i.baseMode
}

func (i *EffectivePackInventory) BaseDirectories() []string {
	if i == nil {
		return nil
	}
	return append([]string(nil), i.baseDirectories...)
}

func (i *EffectivePackInventory) Digest() string {
	if i == nil {
		return ""
	}
	return i.digest
}

func (i *EffectivePackInventory) Lookup(id string) (Entry, bool) {
	if i == nil {
		return Entry{}, false
	}
	entry, ok := i.entries[strings.TrimSpace(id)]
	return cloneEntry(entry), ok
}

func (i *EffectivePackInventory) Entries() []Entry {
	if i == nil {
		return nil
	}
	ids := sortedEntryIDs(i.entries)
	out := make([]Entry, 0, len(ids))
	for _, id := range ids {
		out = append(out, cloneEntry(i.entries[id]))
	}
	return out
}

func (i *EffectivePackInventory) EntriesByType(packType string) []Entry {
	out := make([]Entry, 0)
	for _, entry := range i.Entries() {
		if entry.Type() == strings.TrimSpace(packType) {
			out = append(out, entry)
		}
	}
	return out
}

func cleanRelativePath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || strings.Contains(value, "\\") || strings.HasPrefix(value, "/") {
		return ""
	}
	cleaned := path.Clean(value)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") || cleaned != value {
		return ""
	}
	return cleaned
}

func rejectUnexpectedInventoryFiles(fsys fs.FS, expected map[string]struct{}) error {
	expectedDirectories := map[string]struct{}{".": {}}
	for name := range expected {
		for parent := path.Dir(name); parent != "." && parent != "/"; parent = path.Dir(parent) {
			expectedDirectories[parent] = struct{}{}
		}
	}
	return fs.WalkDir(fsys, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if _, ok := expectedDirectories[name]; !ok {
				return fmt.Errorf("platform pack inventory contains unlisted directory %q", name)
			}
			return nil
		}
		if entry.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("platform pack inventory path %q is a symlink", name)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("platform pack inventory path %q is not a regular file", name)
		}
		if _, ok := expected[name]; !ok {
			return fmt.Errorf("platform pack inventory contains unlisted file %q", name)
		}
		return nil
	})
}

func digestInventoryFiles(fsys fs.FS, expected map[string]struct{}) (string, error) {
	names := make([]string, 0, len(expected))
	for name := range expected {
		names = append(names, name)
	}
	sort.Strings(names)
	hash := sha256.New()
	_, _ = hash.Write([]byte("swarm-platform-pack-inventory-v1\n"))
	for _, name := range names {
		body, err := fs.ReadFile(fsys, name)
		if err != nil {
			return "", fmt.Errorf("read platform pack inventory digest input %q: %w", name, err)
		}
		writeDigestField(hash, []byte(name))
		writeDigestField(hash, body)
	}
	return fmt.Sprintf("sha256:%x", hash.Sum(nil)), nil
}

func digestEffectiveInventory(baseDigest string, entries map[string]Entry) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("swarm-effective-pack-inventory-v1\n"))
	writeDigestField(hash, []byte(baseDigest))
	for _, id := range sortedEntryIDs(entries) {
		entry := entries[id]
		for _, value := range []string{
			entry.ID(), entry.Version(), entry.Type(), entry.ManifestHash(), entry.Source(),
			entry.origin.Source, entry.origin.ID, entry.origin.Version, entry.origin.ManifestHash, entry.origin.EnvelopeHash,
		} {
			writeDigestField(hash, []byte(value))
		}
		writeDigestField(hash, entry.envelopeBody)
		writeDigestField(hash, entry.manifestBody)
	}
	return fmt.Sprintf("sha256:%x", hash.Sum(nil))
}

func importedEnvelopeHash(body []byte) string {
	return manifesthash.FromBytes(body).String()
}

type digestWriter interface {
	Write([]byte) (int, error)
}

func writeDigestField(writer digestWriter, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = writer.Write(length[:])
	_, _ = writer.Write(value)
}

func sortedEntryIDs(entries map[string]Entry) []string {
	ids := make([]string, 0, len(entries))
	for id := range entries {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
