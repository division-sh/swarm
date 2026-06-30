package platform

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	rootartifacts "github.com/division-sh/swarm"
)

const (
	DefaultPlatformSpecPath        = "platform-spec.yaml"
	DefaultOpenRPCPath             = "openrpc.json"
	DefaultWorkspaceDockerfilePath = "Dockerfile.workspace"
)

const PlatformSpecDisplayPath = "embedded://swarm/platform-spec.yaml"
const WorkspaceDockerfileDisplayPath = "embedded://swarm/Dockerfile.workspace"

func DefaultPlatformSpecFile(repoRoot string) string {
	return filepath.Join(repoRoot, DefaultPlatformSpecPath)
}

func DefaultOpenRPCFile(repoRoot string) string {
	return filepath.Join(repoRoot, DefaultOpenRPCPath)
}

func PlatformSpecYAML() []byte {
	return rootartifacts.EmbeddedPlatformSpecYAML()
}

func WorkspaceDockerfile() []byte {
	return rootartifacts.EmbeddedWorkspaceDockerfile()
}

func MaterializePlatformSpecFile() (string, error) {
	spec := PlatformSpecYAML()
	return materializeEmbeddedAsset("platform spec", "platform-spec-", ".yaml", spec)
}

func MaterializeWorkspaceDockerfile() (string, error) {
	dockerfile := WorkspaceDockerfile()
	return materializeEmbeddedAsset("workspace Dockerfile", "Dockerfile.workspace-", "", dockerfile)
}

func materializeEmbeddedAsset(label, prefix, suffix string, data []byte) (string, error) {
	digest := sha256.Sum256(data)
	name := prefix + hex.EncodeToString(digest[:8]) + suffix
	var attempts []string
	for _, base := range platformSpecCacheBases() {
		path, err := materializeEmbeddedAssetFile(base, name, data)
		if err == nil {
			return path, nil
		}
		attempts = append(attempts, fmt.Sprintf("%s: %v", base, err))
	}
	return "", fmt.Errorf("materialize embedded %s: %s", label, strings.Join(attempts, "; "))
}

func platformSpecCacheBases() []string {
	var bases []string
	if cache, err := os.UserCacheDir(); err == nil && strings.TrimSpace(cache) != "" {
		bases = append(bases, filepath.Join(cache, "swarm", "embedded-assets"))
	}
	bases = append(bases, filepath.Join(os.TempDir(), "swarm-embedded-assets"))
	return bases
}

func materializeEmbeddedAssetFile(dir, name string, data []byte) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(dir, name)
	if existing, err := os.ReadFile(path); err == nil && bytes.Equal(existing, data) {
		return path, nil
	}
	tmp, err := os.CreateTemp(dir, name+".tmp-*")
	if err != nil {
		return "", err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Chmod(0o644); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		if existing, readErr := os.ReadFile(path); readErr == nil && bytes.Equal(existing, data) {
			return path, nil
		}
		return "", err
	}
	return path, nil
}
