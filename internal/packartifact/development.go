package packartifact

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing/fstest"

	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"gopkg.in/yaml.v3"
)

func LoadDevelopmentPlatformPackInventory(runningVersion string, dirs []string, embedded *PlatformPackInventory) (*PlatformPackInventory, error) {
	if embedded == nil || embedded.SelectionMode() != SelectionEmbedded || len(dirs) == 0 {
		return nil, fmt.Errorf("complete development provider directories and embedded inventory are required")
	}
	files := fstest.MapFS{}
	manifest := InventoryManifest{Version: 1}
	resolved := make(map[string]string, len(dirs))
	for _, raw := range dirs {
		if raw == "" {
			return nil, fmt.Errorf("development provider directory is required")
		}
		dir, err := filepath.Abs(raw)
		if err != nil {
			return nil, err
		}
		key := filepath.Base(dir)
		if previous, duplicate := resolved[key]; duplicate {
			return nil, fmt.Errorf("duplicate development provider %q from %q and %q", key, previous, dir)
		}
		root, err := openAdmittedArtifactRoot(dir)
		if err != nil {
			return nil, err
		}
		entries, readErr := root.readDir(".")
		var bodies map[string][]byte
		if readErr == nil {
			bodies, readErr = readDevelopmentPackArtifacts(root, ".", entries)
		}
		closeErr := root.close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		for name, body := range bodies {
			files[key+"/"+name] = &fstest.MapFile{Data: body, Mode: 0o444}
		}
		manifest.Packs = append(manifest.Packs, InventoryManifestPack{Path: key})
		resolved[key] = dir
	}
	body, err := yaml.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	files[InventoryManifestFileName] = &fstest.MapFile{Data: body, Mode: 0o444}
	inventory, err := LoadPlatformPackInventoryFS(files, InventoryManifestFileName, runningVersion, SelectionDevelopmentOverride)
	if err != nil {
		return nil, err
	}
	if err := requireCompleteReplacement(embedded, inventory); err != nil {
		return nil, err
	}
	for _, entry := range inventory.Entries() {
		entry.directory = resolved[entry.ID()]
		inventory.entries[entry.ID()] = entry
		inventory.sourceDirectories = append(inventory.sourceDirectories, entry.Directory())
	}
	return inventory, nil
}

func readDevelopmentPackArtifacts(root *admittedArtifactRoot, directory string, entries []fs.DirEntry) (map[string][]byte, error) {
	opened := make(map[string]*os.File, len(entries))
	defer func() {
		for _, file := range opened {
			_ = file.Close()
		}
	}()
	var collect func(string, []fs.DirEntry) error
	collect = func(parent string, entries []fs.DirEntry) error {
		for _, entry := range entries {
			label := filepath.Join(parent, entry.Name())
			if entry.IsDir() {
				children, err := root.readDir(label)
				if err != nil {
					return err
				}
				if err := collect(label, children); err != nil {
					return err
				}
				continue
			}
			if len(opened) >= sourceartifact.MaxMembers {
				return fmt.Errorf("provider directory exceeds source member limit")
			}
			file, err := root.openRegularFile(label)
			if err != nil {
				return err
			}
			opened[filepath.ToSlash(strings.TrimPrefix(label, directory+string(filepath.Separator)))] = file
		}
		return nil
	}
	if err := collect(directory, entries); err != nil {
		return nil, err
	}
	bodies := make(map[string][]byte, len(opened))
	total := 0
	for name, file := range opened {
		body, err := io.ReadAll(io.LimitReader(file, sourceartifact.MaxMemberBytes+1))
		if err != nil {
			return nil, err
		}
		total += len(body)
		if len(body) > sourceartifact.MaxMemberBytes || total > sourceartifact.MaxArtifactBytes {
			return nil, fmt.Errorf("provider directory exceeds source byte limits")
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

func requireCompleteReplacement(embedded, candidate *PlatformPackInventory) error {
	if len(embedded.entries) != len(candidate.entries) {
		return fmt.Errorf("development override must replace all %d embedded provider directories; got %d", len(embedded.entries), len(candidate.entries))
	}
	for _, expected := range embedded.Entries() {
		actual, present := candidate.Lookup(expected.ID())
		if !present {
			return fmt.Errorf("development override is missing provider %q", expected.ID())
		}
		for _, role := range []packmodel.BodyRole{packmodel.BodyTrigger, packmodel.BodyConnector, packmodel.BodyChannel} {
			_, wanted := expected.Provider().Body(role)
			_, got := actual.Provider().Body(role)
			if wanted != got {
				return fmt.Errorf("development provider %q must preserve its %s BODY membership", expected.ID(), role)
			}
		}
	}
	return nil
}
