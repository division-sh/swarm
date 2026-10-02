package platform

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

func PlatformVersion() (string, error) {
	return PlatformVersionFromYAML(PlatformSpecYAML())
}

func PlatformVersionFromYAML(raw []byte) (string, error) {
	source, err := yamlsource.Load(raw)
	if err != nil {
		return "", fmt.Errorf("parse platform version: %w", err)
	}
	return PlatformVersionFromValue(source.Document("platform-spec.yaml").Root())
}

func PlatformVersionFromValue(root yamlsource.Value) (string, error) {
	owner, err := root.Lookup("platform")
	if err != nil {
		return "", err
	}
	if len(owner.Occurrences) != 1 {
		return "", fmt.Errorf("platform declaration missing or duplicated at %s", root.Location())
	}
	version, err := owner.Value.Lookup("version")
	if err != nil {
		return "", err
	}
	if len(version.Occurrences) != 1 {
		return "", fmt.Errorf("platform.version missing or duplicated at %s", owner.Value.Location())
	}
	scalar, err := version.Value.Scalar()
	if err != nil || scalar.Tag != "!!str" || strings.TrimSpace(scalar.Value) == "" {
		return "", fmt.Errorf("%s at %s must be nonempty text", version.SemanticPath, version.Value.Location())
	}
	return strings.TrimSpace(scalar.Value), nil
}
