package cliapp

import (
	goruntime "runtime"
	"runtime/debug"
	"strings"

	"github.com/division-sh/swarm/internal/platform"
)

const unknownVersionValue = "unknown"

var (
	readVersionBuildInfo       = debug.ReadBuildInfo
	readVersionPlatformVersion = platform.PlatformVersion
	readVersionPlatformDigest  = platform.PlatformSpecDigest
)

type versionMetadata struct {
	BinaryVersion      string
	ModuleVersion      string
	PlatformVersion    string
	PlatformSpecDigest string
	Commit             string
	Built              string
	GoVersion          string
	GOOS               string
	GOARCH             string
}

func resolveLocalVersionMetadata(injected BuildMetadata) (versionMetadata, error) {
	platformVersion, err := readVersionPlatformVersion()
	if err != nil {
		return versionMetadata{}, err
	}
	buildInfo, ok := readVersionBuildInfo()
	if !ok {
		buildInfo = nil
	}
	moduleVersion := buildInfoModuleVersion(buildInfo)
	vcsRevision := buildInfoSetting(buildInfo, "vcs.revision")
	if vcsRevision != "" && buildInfoSetting(buildInfo, "vcs.modified") == "true" {
		vcsRevision += "-modified"
	}
	vcsTime := buildInfoSetting(buildInfo, "vcs.time")
	return versionMetadata{
		BinaryVersion:      resolvedBinaryVersion(injected.Version, moduleVersion),
		ModuleVersion:      resolvedOptionalVersion(moduleVersion),
		PlatformVersion:    strings.TrimSpace(platformVersion),
		PlatformSpecDigest: readVersionPlatformDigest(),
		Commit:             resolvedInjectedOrFallback(injected.Commit, unknownVersionValue, vcsRevision),
		Built:              resolvedInjectedOrFallback(injected.Date, unknownVersionValue, vcsTime),
		GoVersion:          goruntime.Version(),
		GOOS:               goruntime.GOOS,
		GOARCH:             goruntime.GOARCH,
	}, nil
}

func resolvedBinaryVersion(injected, moduleVersion string) string {
	if injected = strings.TrimSpace(injected); injected != "" && injected != "dev" {
		return injected
	}
	if moduleVersion != "" {
		return moduleVersion
	}
	return "dev"
}

func resolvedOptionalVersion(version string) string {
	if version != "" {
		return version
	}
	return unknownVersionValue
}

func resolvedInjectedOrFallback(injected, defaultValue, fallback string) string {
	if value := strings.TrimSpace(injected); value != "" && value != defaultValue {
		return value
	}
	if value := strings.TrimSpace(fallback); value != "" {
		return value
	}
	return defaultValue
}

func buildInfoModuleVersion(info *debug.BuildInfo) string {
	if info == nil {
		return ""
	}
	version := strings.TrimSpace(info.Main.Version)
	if version == "" || version == "(devel)" {
		return ""
	}
	return version
}

func buildInfoSetting(info *debug.BuildInfo, key string) string {
	if info == nil {
		return ""
	}
	for _, setting := range info.Settings {
		if setting.Key == key {
			return strings.TrimSpace(setting.Value)
		}
	}
	return ""
}
