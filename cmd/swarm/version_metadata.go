package main

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
)

type localVersionMetadata struct {
	BinaryVersion   string
	ModuleVersion   string
	PlatformVersion string
	Commit          string
	Built           string
	GoVersion       string
	GOOS            string
	GOARCH          string
}

func resolveLocalVersionMetadata() (localVersionMetadata, error) {
	platformVersion, err := readVersionPlatformVersion()
	if err != nil {
		return localVersionMetadata{}, err
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
	return localVersionMetadata{
		BinaryVersion:   resolvedBinaryVersion(moduleVersion),
		ModuleVersion:   resolvedOptionalVersion(moduleVersion),
		PlatformVersion: strings.TrimSpace(platformVersion),
		Commit:          resolvedInjectedOrFallback(binaryCommit, unknownVersionValue, vcsRevision),
		Built:           resolvedInjectedOrFallback(binaryDate, unknownVersionValue, vcsTime),
		GoVersion:       goruntime.Version(),
		GOOS:            goruntime.GOOS,
		GOARCH:          goruntime.GOARCH,
	}, nil
}

func resolvedBinaryVersion(moduleVersion string) string {
	if injected := strings.TrimSpace(binaryVersion); injected != "" && injected != "dev" {
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
