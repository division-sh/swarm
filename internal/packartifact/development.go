package packartifact

import (
	"fmt"
	"io"
	"io/fs"
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
		root, err := openAdmittedArtifactRoot(dir)
		if err != nil {
			return nil, fmt.Errorf("development platform pack %q must be a real directory with no symlinked ancestors: %w", dir, err)
		}
		entries, err := root.readDir(".")
		if err != nil {
			_ = root.close()
			return nil, fmt.Errorf("read development platform pack %q: %w", dir, err)
		}
		if len(entries) != 2 {
			_ = root.close()
			return nil, fmt.Errorf("development platform pack %q must contain exactly pack.yaml and one body manifest", dir)
		}
		hasEnvelope := false
		for _, entry := range entries {
			hasEnvelope = hasEnvelope || entry.Name() == EnvelopeFileName
		}
		if !hasEnvelope {
			_ = root.close()
			return nil, fmt.Errorf("development platform pack %q must contain exactly pack.yaml and one body manifest", dir)
		}
		artifactBodies, err := readDevelopmentPackArtifacts(root, entries)
		if err != nil {
			_ = root.close()
			return nil, fmt.Errorf("inspect development platform pack %q: %w", dir, err)
		}
		envelopeBody := artifactBodies[EnvelopeFileName]
		envelope, err := basepacks.ParseEnvelope(envelopeBody)
		if err != nil {
			_ = root.close()
			return nil, fmt.Errorf("parse development platform pack envelope %q: %w", dir, err)
		}
		manifestFile := ManifestFileNameForType(envelope.Type)
		if manifestFile == "" {
			_ = root.close()
			return nil, fmt.Errorf("development platform pack %q has unsupported type %q", envelope.ID, envelope.Type)
		}
		for _, entry := range entries {
			if entry.Name() != EnvelopeFileName && entry.Name() != manifestFile {
				_ = root.close()
				return nil, fmt.Errorf("development platform pack %q contains unsupported entry %q", dir, entry.Name())
			}
		}
		if previous, duplicate := seenIDs[envelope.ID]; duplicate {
			_ = root.close()
			return nil, fmt.Errorf("duplicate development platform pack id %q from %q and %q", envelope.ID, previous, dir)
		}
		seenIDs[envelope.ID] = dir
		body := artifactBodies[manifestFile]
		if err := root.close(); err != nil {
			return nil, fmt.Errorf("close development platform pack %q: %w", dir, err)
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
	for _, candidate := range candidates {
		entry := inventory.entries[candidate.envelope.ID]
		entry.directory = candidate.directory
		inventory.entries[candidate.envelope.ID] = entry
	}
	if err := requireCompleteReplacement(embedded, inventory); err != nil {
		return nil, err
	}
	inventory.embeddedImportOrigins = cloneImportOrigins(embedded.embeddedImportOrigins)
	inventory.sourceDirectories = append([]string(nil), resolvedDirs...)
	return inventory, nil
}

func readDevelopmentPackArtifacts(root *admittedArtifactRoot, entries []fs.DirEntry) (map[string][]byte, error) {
	opened := make(map[string]*os.File, len(entries))
	defer func() {
		for _, file := range opened {
			_ = file.Close()
		}
	}()
	for _, entry := range entries {
		file, err := root.openRegularFile(entry.Name())
		if err != nil {
			return nil, err
		}
		opened[entry.Name()] = file
	}
	bodies := make(map[string][]byte, len(opened))
	for name, file := range opened {
		body, err := io.ReadAll(file)
		if err != nil {
			return nil, fmt.Errorf("read opened development pack artifact %q: %w", name, err)
		}
		bodies[name] = body
	}
	return bodies, nil
}

func readRegularDevelopmentPackFile(dir, name string) ([]byte, error) {
	root, err := openAdmittedArtifactRoot(dir)
	if err != nil {
		return nil, err
	}
	defer root.close()
	return root.readRegularFile(name)
}

func cloneImportOrigins(origins map[string]ImportOrigin) map[string]ImportOrigin {
	if origins == nil {
		return nil
	}
	cloned := make(map[string]ImportOrigin, len(origins))
	for id, origin := range origins {
		cloned[id] = origin
	}
	return cloned
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
