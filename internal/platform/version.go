package platform

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/yamlsource"
)

type platformVersionDocument struct {
	Platform struct {
		Version string `yaml:"version"`
	} `yaml:"platform"`
}

func PlatformVersion() (string, error) {
	return PlatformVersionFromYAML(PlatformSpecYAML())
}

func PlatformVersionFromYAML(raw []byte) (string, error) {
	source, err := yamlsource.Load(raw)
	if err != nil {
		return "", fmt.Errorf("parse platform version: %w", err)
	}
	var doc platformVersionDocument
	if err := source.Decode(&doc); err != nil {
		return "", fmt.Errorf("parse platform version: %w", err)
	}
	version := strings.TrimSpace(doc.Platform.Version)
	if version == "" {
		return "", fmt.Errorf("platform.version missing")
	}
	return version, nil
}
