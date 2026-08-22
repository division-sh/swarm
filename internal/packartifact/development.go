package packartifact

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing/fstest"

	basepacks "github.com/division-sh/swarm/internal/packmodel"
	"gopkg.in/yaml.v3"
)

func LoadDevelopmentPlatformPackInventory(runningPlatformVersion string, dirs []string, embedded *PlatformPackInventory) (*PlatformPackInventory, error) {
	if embedded == nil || embedded.SelectionMode() != SelectionEmbedded {
		return nil, fmt.Errorf("embedded platform pack inventory is required to validate a complete development override")
	}
	if len(dirs) == 0 {
		return nil, fmt.Errorf("development platform pack directories are required")
	}
	type candidatePack struct {
		directory    string
		envelope     Envelope
		envelopeBody []byte
		manifestBody []byte
	}
	candidates := make([]candidatePack, 0, len(dirs))
	seenIDs := map[string]string{}
	for index, raw := range dirs {
		dir := filepath.Clean(strings.TrimSpace(raw))
		if dir == "" || dir == "." {
			return nil, fmt.Errorf("platform.packs.platform_dirs[%d] is empty", index)
		}
		absoluteDir, err := filepath.Abs(dir)
		if err != nil {
			return nil, fmt.Errorf("resolve development platform pack %q: %w", dir, err)
		}
		dir = absoluteDir
		rootInfo, err := os.Lstat(dir)
		if err != nil {
			return nil, fmt.Errorf("inspect development platform pack %q: %w", dir, err)
		}
		if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
			return nil, fmt.Errorf("development platform pack %q must be a real directory", dir)
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, fmt.Errorf("read development platform pack %q: %w", dir, err)
		}
		if len(entries) != 2 {
			return nil, fmt.Errorf("development platform pack %q must contain exactly pack.yaml and one body manifest", dir)
		}
		envelopeBody, err := os.ReadFile(filepath.Join(dir, EnvelopeFileName))
		if err != nil {
			return nil, fmt.Errorf("read development platform pack envelope %q: %w", dir, err)
		}
		envelope, err := basepacks.ParseEnvelope(envelopeBody)
		if err != nil {
			return nil, fmt.Errorf("parse development platform pack envelope %q: %w", dir, err)
		}
		manifestFile := ManifestFileNameForType(envelope.Type)
		if manifestFile == "" {
			return nil, fmt.Errorf("development platform pack %q has unsupported type %q", envelope.ID, envelope.Type)
		}
		for _, entry := range entries {
			info, err := entry.Info()
			if err != nil {
				return nil, fmt.Errorf("inspect development platform pack %q: %w", dir, err)
			}
			if !info.Mode().IsRegular() || (entry.Name() != EnvelopeFileName && entry.Name() != manifestFile) {
				return nil, fmt.Errorf("development platform pack %q contains unsupported entry %q", dir, entry.Name())
			}
		}
		if previous, duplicate := seenIDs[envelope.ID]; duplicate {
			return nil, fmt.Errorf("duplicate development platform pack id %q from %q and %q", envelope.ID, previous, dir)
		}
		seenIDs[envelope.ID] = dir
		body, err := os.ReadFile(filepath.Join(dir, manifestFile))
		if err != nil {
			return nil, fmt.Errorf("read development platform pack body %q: %w", dir, err)
		}
		candidates = append(candidates, candidatePack{
			directory: dir, envelope: envelope, envelopeBody: envelopeBody, manifestBody: body,
		})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].envelope.ID < candidates[j].envelope.ID })
	files := fstest.MapFS{}
	manifest := InventoryManifest{Version: 1}
	resolvedDirs := make([]string, 0, len(candidates))
	for index, candidate := range candidates {
		inventoryPath := fmt.Sprintf("packs/%03d", index)
		manifestFile := ManifestFileNameForType(candidate.envelope.Type)
		files[inventoryPath+"/"+EnvelopeFileName] = &fstest.MapFile{Data: candidate.envelopeBody, Mode: 0o444}
		files[inventoryPath+"/"+manifestFile] = &fstest.MapFile{Data: candidate.manifestBody, Mode: 0o444}
		manifest.Packs = append(manifest.Packs, InventoryManifestPack{ID: candidate.envelope.ID, Type: candidate.envelope.Type, Path: inventoryPath})
		resolvedDirs = append(resolvedDirs, candidate.directory)
	}
	manifestBody, err := yaml.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("marshal development platform pack inventory: %w", err)
	}
	files[InventoryManifestFileName] = &fstest.MapFile{Data: manifestBody, Mode: 0o444}
	inventory, err := LoadPlatformPackInventoryFS(files, InventoryManifestFileName, runningPlatformVersion, SelectionDevelopmentOverride)
	if err != nil {
		return nil, err
	}
	if err := requireCompleteReplacement(embedded, inventory); err != nil {
		return nil, err
	}
	inventory.sourceDirectories = append([]string(nil), resolvedDirs...)
	return inventory, nil
}

func requireCompleteReplacement(embedded, candidate *PlatformPackInventory) error {
	embeddedEntries := embedded.Entries()
	candidateEntries := candidate.Entries()
	if len(embeddedEntries) != len(candidateEntries) {
		return fmt.Errorf("development override replaces the embedded inventory and must provide all %d packs; got %d", len(embeddedEntries), len(candidateEntries))
	}
	for _, expected := range embeddedEntries {
		actual, ok := candidate.Lookup(expected.ID())
		if !ok {
			return fmt.Errorf("development override replaces the embedded inventory but is missing %q; add its directory or remove platform.packs.platform_dirs", expected.ID())
		}
		if actual.Type() != expected.Type() {
			return fmt.Errorf("development override pack %q type %q contradicts embedded type %q", expected.ID(), actual.Type(), expected.Type())
		}
	}
	return nil
}
