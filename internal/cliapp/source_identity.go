package cliapp

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func admitCLIIdentitySource(root InvocationRoot, raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", fmt.Errorf("--source must be a nonempty directory")
	}
	selected, err := ResolveSourceRoot(root.Path(), raw)
	if err != nil {
		return "", err
	}
	artifact, err := sourceartifact.AdmitDirectory(selected)
	if err != nil {
		return "", err
	}
	version, err := platform.PlatformVersion()
	if err != nil {
		return "", err
	}
	if err := artifact.ValidatePlatformVersion(version); err != nil {
		return "", err
	}
	return artifact.BundleHash(), nil
}

func humanSourceIdentity(hash, label string) string {
	if label != "" {
		return cliSanitizeOneLineValue(label)
	}
	return sourceartifact.ShortHashLabel(hash)
}
