package sourceartifact

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const RuntimeProjectionIdentityPrefix = "runtime-projection-v1:"

// RuntimeProjection is a disposable read-only filesystem projection of one
// exact admitted source generation. The private root is never source authority;
// it exists only so workspace backends can expose the admitted bytes.
type RuntimeProjection struct {
	state    *runtimeProjectionState
	released bool
}

type runtimeProjectionState struct {
	mu         sync.Mutex
	bundleHash string
	identity   string
	root       string
	refs       int
	removed    bool
}

func MaterializeRuntimeProjection(artifact *AdmittedSourceArtifact) (*RuntimeProjection, error) {
	if artifact == nil {
		return nil, errors.New("runtime source projection requires an admitted source artifact")
	}
	if err := ValidateHash(artifact.BundleHash()); err != nil {
		return nil, fmt.Errorf("runtime source projection bundle_hash: %w", err)
	}
	identity, err := newRuntimeProjectionIdentity()
	if err != nil {
		return nil, err
	}
	root, err := os.MkdirTemp("", "swarm-source-")
	if err != nil {
		return nil, fmt.Errorf("create runtime source projection: %w", err)
	}
	cleanup := func(cause error) (*RuntimeProjection, error) {
		return nil, errors.Join(cause, removeProjectionTree(root))
	}
	for _, entry := range artifact.Entries() {
		target := filepath.Join(root, filepath.FromSlash(entry.Label()))
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			return cleanup(fmt.Errorf("create runtime source projection directory for %s: %w", entry.Label(), err))
		}
		file, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o400)
		if err != nil {
			return cleanup(fmt.Errorf("create runtime source projection member %s: %w", entry.Label(), err))
		}
		_, writeErr := file.Write(entry.Bytes())
		closeErr := file.Close()
		if writeErr != nil || closeErr != nil {
			return cleanup(fmt.Errorf("write runtime source projection member %s: %w", entry.Label(), errors.Join(writeErr, closeErr)))
		}
	}
	if err := sealProjectionTree(root); err != nil {
		return cleanup(err)
	}
	return &RuntimeProjection{state: &runtimeProjectionState{bundleHash: artifact.BundleHash(), identity: identity, root: root, refs: 1}}, nil
}

// Identity is the process-local identity of one materialized projection. It is
// shared by retained handles and differs for separate materializations even
// when their admitted bundle hashes are equal.
func (p *RuntimeProjection) Identity() string {
	if p == nil || p.state == nil || p.released {
		return ""
	}
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	if p.state.removed {
		return ""
	}
	return p.state.identity
}

func (p *RuntimeProjection) BundleHash() string {
	if p == nil || p.state == nil || p.released {
		return ""
	}
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	if p.state.removed {
		return ""
	}
	return p.state.bundleHash
}

func (p *RuntimeProjection) PrivateRoot() string {
	if p == nil || p.state == nil || p.released {
		return ""
	}
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	if p.state.removed {
		return ""
	}
	return p.state.root
}

func (p *RuntimeProjection) Retain() (*RuntimeProjection, error) {
	if p == nil || p.state == nil || p.released {
		return nil, errors.New("runtime source projection is released")
	}
	p.state.mu.Lock()
	defer p.state.mu.Unlock()
	if p.state.removed || p.state.refs == 0 {
		return nil, errors.New("runtime source projection is removed")
	}
	p.state.refs++
	return &RuntimeProjection{state: p.state}, nil
}

func (p *RuntimeProjection) Release() error {
	if p == nil || p.state == nil || p.released {
		return nil
	}
	p.state.mu.Lock()
	if p.released {
		p.state.mu.Unlock()
		return nil
	}
	p.released = true
	p.state.refs--
	if p.state.refs > 0 {
		p.state.mu.Unlock()
		return nil
	}
	root := p.state.root
	p.state.removed = true
	p.state.root = ""
	p.state.mu.Unlock()
	return removeProjectionTree(root)
}

func sealProjectionTree(root string) error {
	return filepath.WalkDir(root, func(current string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		mode := os.FileMode(0o444)
		if entry.IsDir() {
			mode = 0o555
		}
		if err := os.Chmod(current, mode); err != nil {
			return fmt.Errorf("seal runtime source projection %s: %w", current, err)
		}
		return nil
	})
}

func removeProjectionTree(root string) error {
	_ = filepath.WalkDir(root, func(current string, entry os.DirEntry, walkErr error) error {
		if walkErr == nil && entry.IsDir() {
			_ = os.Chmod(current, 0o700)
		}
		return nil
	})
	return os.RemoveAll(root)
}

func newRuntimeProjectionIdentity() (string, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", fmt.Errorf("mint runtime source projection identity: %w", err)
	}
	return RuntimeProjectionIdentityPrefix + hex.EncodeToString(entropy[:]), nil
}

func ValidateRuntimeProjectionIdentity(value string) error {
	if len(value) != len(RuntimeProjectionIdentityPrefix)+32 || !strings.HasPrefix(value, RuntimeProjectionIdentityPrefix) {
		return fmt.Errorf("runtime projection identity must be %s<32 lowercase hex>", RuntimeProjectionIdentityPrefix)
	}
	for _, char := range value[len(RuntimeProjectionIdentityPrefix):] {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			return fmt.Errorf("runtime projection identity must be %s<32 lowercase hex>", RuntimeProjectionIdentityPrefix)
		}
	}
	return nil
}
