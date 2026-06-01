package platform

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
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
	var doc platformVersionDocument
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return "", fmt.Errorf("parse platform version: %w", err)
	}
	version := strings.TrimSpace(doc.Platform.Version)
	if version == "" {
		return "", fmt.Errorf("platform.version missing")
	}
	return version, nil
}
