package platformcontracts

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

//go:embed platform-spec.yaml
var platformSpecYAML []byte

const PlatformSpecDisplayPath = "embedded://swarm/platform-spec.yaml"

func PlatformSpecYAML() []byte {
	out := make([]byte, len(platformSpecYAML))
	copy(out, platformSpecYAML)
	return out
}

func MaterializePlatformSpecFile() (string, error) {
	digest := sha256.Sum256(platformSpecYAML)
	name := "platform-spec-" + hex.EncodeToString(digest[:8]) + ".yaml"
	var attempts []string
	for _, base := range platformSpecCacheBases() {
		path, err := materializePlatformSpecFile(base, name)
		if err == nil {
			return path, nil
		}
		attempts = append(attempts, fmt.Sprintf("%s: %v", base, err))
	}
	return "", fmt.Errorf("materialize embedded platform spec: %s", strings.Join(attempts, "; "))
}

func platformSpecCacheBases() []string {
	var bases []string
	if cache, err := os.UserCacheDir(); err == nil && strings.TrimSpace(cache) != "" {
		bases = append(bases, filepath.Join(cache, "swarm", "embedded-assets"))
	}
	bases = append(bases, filepath.Join(os.TempDir(), "swarm-embedded-assets"))
	return bases
}

func materializePlatformSpecFile(dir, name string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(dir, name)
	if existing, err := os.ReadFile(path); err == nil && bytes.Equal(existing, platformSpecYAML) {
		return path, nil
	}
	tmp, err := os.CreateTemp(dir, name+".tmp-*")
	if err != nil {
		return "", err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(platformSpecYAML); err != nil {
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
		if existing, readErr := os.ReadFile(path); readErr == nil && bytes.Equal(existing, platformSpecYAML) {
			return path, nil
		}
		return "", err
	}
	return path, nil
}
