package sourceartifact

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// RuntimeProjectionCleanup is an exact disposal intent, not source authority
// and not a handle that can reopen a retired runtime projection.
type RuntimeProjectionCleanup struct {
	BundleHash string `json:"bundle_hash"`
	Identity   string `json:"identity"`
	Root       string `json:"root"`
}

func (p *RuntimeProjection) CleanupIntent() (RuntimeProjectionCleanup, error) {
	if p == nil || p.state == nil {
		return RuntimeProjectionCleanup{}, errors.New("runtime projection is unavailable")
	}
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	// Disposal identity remains readable after release for exact durable
	// replay. It grants no source access and cannot retain a released handle.
	intent := RuntimeProjectionCleanup{BundleHash: p.state.bundleHash, Identity: p.state.identity, Root: p.state.storageRoot}
	return intent, intent.Validate()
}

func (i RuntimeProjectionCleanup) Validate() error {
	if err := ValidateHash(i.BundleHash); err != nil {
		return err
	}
	if err := ValidateRuntimeProjectionIdentity(i.Identity); err != nil {
		return err
	}
	if !filepath.IsAbs(i.Root) || filepath.Clean(i.Root) != i.Root || !strings.HasPrefix(filepath.Base(i.Root), "swarm-source-") {
		return errors.New("projection cleanup requires its exact canonical private directory")
	}
	return nil
}

// SettleRuntimeProjectionCleanup is called only after retained process
// authority excludes the predecessor and its planned containers have settled.
// No directory discovery, identity inference, or source restoration is allowed.
func SettleRuntimeProjectionCleanup(intent RuntimeProjectionCleanup) error {
	if err := intent.Validate(); err != nil {
		return err
	}
	info, err := os.Lstat(intent.Root)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("projection cleanup root %s is not its owned directory", intent.Root)
	}
	root, err := os.OpenRoot(intent.Root)
	if err != nil {
		return err
	}
	defer root.Close()
	entries, err := fs.ReadDir(root.FS(), ".")
	if err != nil {
		return err
	}
	// A crash after removing the final marker can leave only an empty envelope.
	// Never recursively remove an unmarked directory or unknown envelope entry.
	if len(entries) == 0 {
		return removeEmptyProjectionEnvelope(intent.Root, info)
	}
	for _, entry := range entries {
		if entry.Name() != "source" && entry.Name() != "identity.json" {
			return fmt.Errorf("projection cleanup envelope contains unknown entry %q", entry.Name())
		}
	}
	markerInfo, err := root.Lstat("identity.json")
	if err != nil || !markerInfo.Mode().IsRegular() || markerInfo.Mode()&os.ModeSymlink != 0 {
		return errors.Join(err, errors.New("projection cleanup identity marker is unavailable or not regular"))
	}
	marker, err := root.ReadFile("identity.json")
	if err != nil {
		return err
	}
	expected, err := json.Marshal(intent)
	if err != nil || !bytes.Equal(marker, expected) {
		return errors.Join(err, errors.New("projection cleanup identity differs from admitted intent"))
	}
	if err := fs.WalkDir(root.FS(), "source", func(path string, entry fs.DirEntry, walkErr error) error {
		if os.IsNotExist(walkErr) {
			return nil
		}
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return root.Chmod(path, 0o700)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := root.RemoveAll("source"); err != nil {
		return err
	}
	// Keep identity evidence through every recursive deletion failure. Only the
	// non-recursive empty-envelope removal may follow removal of the marker.
	entries, err = fs.ReadDir(root.FS(), ".")
	if err != nil {
		return err
	}
	if len(entries) != 1 || entries[0].Name() != "identity.json" {
		return errors.New("projection cleanup envelope changed during source deletion")
	}
	if err := root.Remove("identity.json"); err != nil {
		return err
	}
	return removeEmptyProjectionEnvelope(intent.Root, info)
}

func removeEmptyProjectionEnvelope(path string, expected os.FileInfo) error {
	current, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if !os.SameFile(expected, current) {
		return errors.New("projection cleanup directory changed during settlement")
	}
	return os.Remove(path)
}
