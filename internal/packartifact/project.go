package packartifact

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/division-sh/swarm/internal/packmodel"
)

// ImportEmbeddedPack copies the captured provider directory. The copy, not a
// separate imports record, is the selected flow's provider declaration.
func ImportEmbeddedPack(projectRoot, key string, embedded *PlatformPackInventory) (bool, error) {
	if projectRoot == "" {
		return false, fmt.Errorf("selected flow directory is required")
	}
	if embedded == nil || embedded.SelectionMode() != SelectionEmbedded {
		return false, fmt.Errorf("embedded provider inventory is required")
	}
	entry, ok := embedded.Lookup(key)
	if !ok {
		return false, fmt.Errorf("provider %q is unavailable; use its directory name from swarm packs list", key)
	}
	root, err := filepath.Abs(projectRoot)
	if err != nil {
		return false, err
	}
	transaction, err := acquireProjectPackTransaction(root, true)
	if err != nil {
		return false, err
	}
	changed, importErr := importEmbeddedPackLocked(root, key, entry, transaction)
	return changed, errors.Join(importErr, transaction.close())
}

func importEmbeddedPackLocked(root, key string, entry Entry, transaction *projectPackTransaction) (bool, error) {
	if transaction == nil || transaction.root == nil || transaction.writerRoot == nil || filepath.Clean(root) != transaction.stateRoot || key != entry.ID() {
		return false, fmt.Errorf("selected flow import transaction is required")
	}
	manifest, err := transaction.root.readRegularFile(packmodel.ManifestFileName)
	if err != nil {
		return false, fmt.Errorf("provider destination requires a flow manifest.yaml: %w", err)
	}
	if _, err := packmodel.ParseManifestAt(manifest, packmodel.ManifestFileName, packmodel.DirectoryFlow); err != nil {
		return false, err
	}
	members := entry.Provider().Members()
	writer := transaction.writerRoot
	if _, err := writer.Lstat(key); err == nil {
		children, err := transaction.root.readDir(key)
		if err != nil {
			return false, err
		}
		entries, err := readDevelopmentPackArtifacts(transaction.root, key, children)
		if err != nil {
			return false, err
		}
		if len(entries) != len(members) {
			return false, fmt.Errorf("provider directory %q exists with edited bytes; import will not overwrite it", key)
		}
		for name, expected := range members {
			if !bytes.Equal(entries[name], expected) {
				return false, fmt.Errorf("provider directory %q exists with edited bytes; import will not overwrite it", key)
			}
		}
		return false, nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return false, err
	}
	if err := writer.Mkdir(".swarm", 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return false, err
	}
	state, err := writer.Lstat(".swarm")
	if err != nil {
		return false, err
	}
	if !state.IsDir() || state.Mode()&os.ModeSymlink != 0 {
		return false, fmt.Errorf("provider staging requires a real .swarm directory")
	}
	temporary, err := createProjectPackTempDirectory(writer, filepath.Join(".swarm", ".pack-import-"))
	if err != nil {
		return false, err
	}
	defer writer.RemoveAll(temporary)
	for name, body := range members {
		if err := writer.MkdirAll(filepath.Join(temporary, filepath.Dir(name)), 0o700); err != nil {
			return false, err
		}
		if err := writer.WriteFile(filepath.Join(temporary, name), body, 0o644); err != nil {
			return false, err
		}
	}
	if err := writer.Rename(temporary, key); err != nil {
		return false, err
	}
	return true, nil
}

func createProjectPackTempDirectory(root *os.Root, prefix string) (string, error) {
	for range 100 {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", err
		}
		name := prefix + hex.EncodeToString(random[:])
		if err := root.Mkdir(name, 0o700); err == nil {
			return name, nil
		} else if !errors.Is(err, fs.ErrExist) {
			return "", err
		}
	}
	return "", fmt.Errorf("exhausted temporary provider directory names")
}
