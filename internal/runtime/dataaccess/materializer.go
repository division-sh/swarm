package dataaccess

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/currentstate"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const AccessManifestPath = "/data/.swarm/access.v1.json"

const projectionIDPrefix = "data-projection-v1:sha256:"

type ProjectionID string

func (id ProjectionID) Validate() error {
	raw := strings.TrimSpace(string(id))
	if raw != string(id) || !strings.HasPrefix(raw, projectionIDPrefix) {
		return fmt.Errorf("data projection identity must use %s", projectionIDPrefix)
	}
	digest := strings.TrimPrefix(raw, projectionIDPrefix)
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != sha256.Size || digest != strings.ToLower(digest) {
		return fmt.Errorf("data projection identity requires one canonical SHA-256 digest")
	}
	return nil
}

type Projection struct {
	ID         ProjectionID
	Root       string
	AccessList durabledata.AccessList
}

func (p Projection) Validate() error {
	if err := p.ID.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(p.Root) == "" || filepath.Clean(p.Root) != p.Root || !filepath.IsAbs(p.Root) {
		return fmt.Errorf("data projection requires one canonical absolute root")
	}
	return nil
}

type Provider interface {
	Materialize(context.Context, models.AgentConfig) (Projection, error)
}

type Materializer struct {
	mu     sync.Mutex
	root   string
	source semanticview.Source
	store  durabledata.ResourceAccessStore
}

func NewMaterializer(root string, source semanticview.Source, store durabledata.ResourceAccessStore) (*Materializer, error) {
	root = strings.TrimSpace(root)
	if root == "" || source == nil {
		return nil, fmt.Errorf("data projection root and semantic source are required")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve data projection root: %w", err)
	}
	return &Materializer{root: filepath.Clean(abs), source: source, store: store}, nil
}

func DefaultProjectionRoot() string {
	return filepath.Join(os.TempDir(), "swarm", "data-projections")
}

func (m *Materializer) Materialize(ctx context.Context, actor models.AgentConfig) (Projection, error) {
	if m == nil {
		return Projection{}, fmt.Errorf("data projection materializer is required")
	}
	runID, err := currentstate.RequireRunID(ctx)
	if err != nil {
		return Projection{}, err
	}
	access, err := Build(ctx, m.source, m.store, runID, actor)
	if err != nil {
		return Projection{}, err
	}
	manifest, err := canonicaljson.Bytes(access)
	if err != nil {
		return Projection{}, fmt.Errorf("encode data access manifest: %w", err)
	}
	digest := sha256.Sum256(manifest)
	projectionID := ProjectionID(projectionIDPrefix + hex.EncodeToString(digest[:]))
	target := filepath.Join(m.root, "a_"+hex.EncodeToString(digest[:]))

	m.mu.Lock()
	defer m.mu.Unlock()
	if err := os.MkdirAll(m.root, 0o700); err != nil {
		return Projection{}, err
	}
	if err := os.Chmod(m.root, 0o700); err != nil {
		return Projection{}, fmt.Errorf("secure data projection root: %w", err)
	}
	manifestFile := append(append([]byte(nil), manifest...), '\n')
	if existing, err := os.ReadFile(projectionHostPath(target, AccessManifestPath)); err == nil {
		if string(existing) != string(manifestFile) {
			return Projection{}, fmt.Errorf("data access projection %s has contradictory manifest bytes", target)
		}
		if err := verifyProjection(target, access, manifestFile); err != nil {
			return Projection{}, err
		}
		return Projection{ID: projectionID, Root: target, AccessList: access}, nil
	} else if !os.IsNotExist(err) {
		return Projection{}, err
	}
	tmp, err := os.MkdirTemp(m.root, ".projection-")
	if err != nil {
		return Projection{}, err
	}
	defer os.RemoveAll(tmp)
	if err := writeProjectionFile(tmp, AccessManifestPath, manifestFile); err != nil {
		return Projection{}, err
	}
	for _, item := range access.Items {
		switch item.Kind {
		case "static_file":
			if item.Static == nil || item.Resource != nil {
				return Projection{}, fmt.Errorf("static data access item is contradictory")
			}
			if err := writeProjectionFile(tmp, item.Static.MountPath, item.Static.Content); err != nil {
				return Projection{}, err
			}
		case "resource":
			if item.Resource == nil || item.Static != nil {
				return Projection{}, fmt.Errorf("resource data access item is contradictory")
			}
			if err := writeProjectionFile(tmp, item.Resource.MountPath, item.Resource.Content); err != nil {
				return Projection{}, err
			}
		default:
			return Projection{}, fmt.Errorf("unknown data access item kind %q", item.Kind)
		}
	}
	if err := makeProjectionReadOnly(tmp); err != nil {
		return Projection{}, err
	}
	if err := os.Rename(tmp, target); err != nil {
		existing, readErr := os.ReadFile(projectionHostPath(target, AccessManifestPath))
		if readErr != nil {
			return Projection{}, fmt.Errorf("commit data access projection: %w", err)
		}
		if string(existing) != string(manifestFile) {
			return Projection{}, fmt.Errorf("data access projection %s has contradictory manifest bytes", target)
		}
		if err := verifyProjection(target, access, manifestFile); err != nil {
			return Projection{}, err
		}
	}
	return Projection{ID: projectionID, Root: target, AccessList: access}, nil
}

func verifyProjection(root string, access durabledata.AccessList, manifest []byte) error {
	expectedFiles := map[string][]byte{
		projectionHostPath(root, AccessManifestPath): manifest,
	}
	for _, item := range access.Items {
		switch item.Kind {
		case "static_file":
			if item.Static == nil || item.Resource != nil {
				return fmt.Errorf("static data access item is contradictory")
			}
			expectedFiles[projectionHostPath(root, item.Static.MountPath)] = item.Static.Content
		case "resource":
			if item.Resource == nil || item.Static != nil {
				return fmt.Errorf("resource data access item is contradictory")
			}
			expectedFiles[projectionHostPath(root, item.Resource.MountPath)] = item.Resource.Content
		default:
			return fmt.Errorf("unknown data access item kind %q", item.Kind)
		}
	}
	expectedDirs := map[string]struct{}{root: {}}
	for path := range expectedFiles {
		if path == "" {
			return fmt.Errorf("data access projection contains an invalid mount path")
		}
		for dir := filepath.Dir(path); strings.HasPrefix(dir, root); dir = filepath.Dir(dir) {
			expectedDirs[dir] = struct{}{}
			if dir == root {
				break
			}
		}
	}
	seen := make(map[string]struct{}, len(expectedFiles))
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if _, ok := expectedDirs[path]; !ok {
				return fmt.Errorf("data access projection %s contains unexpected directory %s", root, path)
			}
			if info.Mode().Perm() != 0o555 {
				return fmt.Errorf("data access projection %s directory mode is mutable", path)
			}
			return nil
		}
		expected, ok := expectedFiles[path]
		if !ok || !info.Mode().IsRegular() {
			return fmt.Errorf("data access projection %s contains unexpected entry %s", root, path)
		}
		if info.Mode().Perm() != 0o444 {
			return fmt.Errorf("data access projection %s file mode is mutable", path)
		}
		actual, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if string(actual) != string(expected) {
			return fmt.Errorf("data access projection %s file bytes contradict canonical content", path)
		}
		seen[path] = struct{}{}
		return nil
	})
	if err != nil {
		return err
	}
	for path := range expectedFiles {
		if _, ok := seen[path]; !ok {
			return fmt.Errorf("data access projection %s is missing canonical file %s", root, path)
		}
	}
	return nil
}

func writeProjectionFile(root, logicalPath string, content []byte) error {
	path := projectionHostPath(root, logicalPath)
	if path == "" {
		return fmt.Errorf("invalid data projection path %q", logicalPath)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(path, content, 0o400); err != nil {
		return fmt.Errorf("write data projection %s: %w", logicalPath, err)
	}
	return nil
}

func projectionHostPath(root, logicalPath string) string {
	clean := filepath.ToSlash(filepath.Clean(strings.TrimSpace(logicalPath)))
	if clean != "/data" && !strings.HasPrefix(clean, "/data/") {
		return ""
	}
	rel := strings.TrimPrefix(clean, "/data/")
	if rel == clean || rel == "" || rel == "." || strings.HasPrefix(rel, "../") {
		return ""
	}
	return filepath.Join(root, filepath.FromSlash(rel))
}

func makeProjectionReadOnly(root string) error {
	return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return os.Chmod(path, 0o555)
		}
		return os.Chmod(path, 0o444)
	})
}
