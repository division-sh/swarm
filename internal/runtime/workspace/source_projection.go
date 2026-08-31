package workspace

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

func validateSourceProjection(projection *sourceartifact.RuntimeProjection, expectedHash string) (string, error) {
	if projection == nil {
		return "", fmt.Errorf("workspace validation failed: runtime source projection is required")
	}
	bundleHash := projection.BundleHash()
	if err := sourceartifact.ValidateHash(bundleHash); err != nil {
		return "", fmt.Errorf("workspace validation failed: runtime source projection bundle_hash: %w", err)
	}
	if err := sourceartifact.ValidateRuntimeProjectionIdentity(projection.Identity()); err != nil {
		return "", fmt.Errorf("workspace validation failed: %w", err)
	}
	if expectedHash = strings.TrimSpace(expectedHash); expectedHash != "" && expectedHash != bundleHash {
		return "", fmt.Errorf("workspace validation failed: runtime source projection hash %s does not match runtime bundle_hash %s", bundleHash, expectedHash)
	}
	root := projection.PrivateRoot()
	if root == "" {
		return "", fmt.Errorf("workspace validation failed: runtime source projection is released")
	}
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", fmt.Errorf("workspace validation failed: resolve runtime source projection: %w", err)
	}
	info, err := os.Stat(canonical)
	if err != nil || !info.IsDir() {
		return "", fmt.Errorf("workspace validation failed: runtime source projection is unavailable")
	}
	return canonical, nil
}
