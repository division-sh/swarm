package versionmetadata

import (
	"errors"
	"runtime/debug"
	"testing"
)

func TestResolveLocalVersionMetadataPrefersInjectedReleaseMetadata(t *testing.T) {
	withVersionMetadataHooks(t, "v1.6.0", "release-commit", "2026-06-01T00:00:00Z", "0.7.0", &debug.BuildInfo{
		Main: debug.Module{Version: "v1.5.0"},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "build-info-commit"},
			{Key: "vcs.time", Value: "2026-05-31T00:00:00Z"},
		},
	}, nil)

	got, err := Resolve(Injected{Version: "v1.6.0", Commit: "release-commit", Date: "2026-06-01T00:00:00Z"})
	if err != nil {
		t.Fatalf("resolveLocalVersionMetadata() error = %v", err)
	}
	if got.BinaryVersion != "v1.6.0" {
		t.Fatalf("BinaryVersion = %q, want v1.6.0", got.BinaryVersion)
	}
	if got.ModuleVersion != "v1.5.0" {
		t.Fatalf("ModuleVersion = %q, want v1.5.0", got.ModuleVersion)
	}
	if got.Commit != "release-commit" {
		t.Fatalf("Commit = %q, want release-commit", got.Commit)
	}
	if got.Built != "2026-06-01T00:00:00Z" {
		t.Fatalf("Built = %q, want injected date", got.Built)
	}
	if got.PlatformVersion != "0.7.0" {
		t.Fatalf("PlatformVersion = %q, want 0.7.0", got.PlatformVersion)
	}
}

func TestResolveLocalVersionMetadataFallsBackToBuildInfo(t *testing.T) {
	withVersionMetadataHooks(t, "dev", "unknown", "unknown", "0.7.0", &debug.BuildInfo{
		Main: debug.Module{Version: "v1.6.0"},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "build-info-commit"},
			{Key: "vcs.time", Value: "2026-06-01T00:00:00Z"},
			{Key: "vcs.modified", Value: "true"},
		},
	}, nil)

	got, err := Resolve(Injected{Version: "dev", Commit: "unknown", Date: "unknown"})
	if err != nil {
		t.Fatalf("resolveLocalVersionMetadata() error = %v", err)
	}
	if got.BinaryVersion != "v1.6.0" {
		t.Fatalf("BinaryVersion = %q, want v1.6.0", got.BinaryVersion)
	}
	if got.ModuleVersion != "v1.6.0" {
		t.Fatalf("ModuleVersion = %q, want v1.6.0", got.ModuleVersion)
	}
	if got.Commit != "build-info-commit-modified" {
		t.Fatalf("Commit = %q, want modified build-info commit", got.Commit)
	}
	if got.Built != "2026-06-01T00:00:00Z" {
		t.Fatalf("Built = %q, want build-info time", got.Built)
	}
}

func TestResolveLocalVersionMetadataUsesDeterministicDevFallback(t *testing.T) {
	withVersionMetadataHooks(t, "dev", "unknown", "unknown", "0.7.0", nil, nil)

	got, err := Resolve(Injected{Version: "dev", Commit: "unknown", Date: "unknown"})
	if err != nil {
		t.Fatalf("resolveLocalVersionMetadata() error = %v", err)
	}
	if got.BinaryVersion != "dev" {
		t.Fatalf("BinaryVersion = %q, want dev", got.BinaryVersion)
	}
	if got.ModuleVersion != "unknown" {
		t.Fatalf("ModuleVersion = %q, want unknown", got.ModuleVersion)
	}
	if got.Commit != "unknown" {
		t.Fatalf("Commit = %q, want unknown", got.Commit)
	}
	if got.Built != "unknown" {
		t.Fatalf("Built = %q, want unknown", got.Built)
	}
}

func TestResolveLocalVersionMetadataFailsWithoutPlatformVersion(t *testing.T) {
	withVersionMetadataHooks(t, "dev", "unknown", "unknown", "", nil, errors.New("platform.version missing"))

	if _, err := Resolve(Injected{Version: "dev", Commit: "unknown", Date: "unknown"}); err == nil {
		t.Fatal("resolveLocalVersionMetadata() error = nil, want platform version error")
	}
}

func withVersionMetadataHooks(t *testing.T, version, commit, date, platformVersion string, info *debug.BuildInfo, platformErr error) {
	t.Helper()
	oldBuildInfo := readVersionBuildInfo
	oldPlatformVersion := readVersionPlatformVersion
	_ = version
	_ = commit
	_ = date
	readVersionBuildInfo = func() (*debug.BuildInfo, bool) {
		if info == nil {
			return nil, false
		}
		return info, true
	}
	readVersionPlatformVersion = func() (string, error) {
		return platformVersion, platformErr
	}
	t.Cleanup(func() {
		readVersionBuildInfo = oldBuildInfo
		readVersionPlatformVersion = oldPlatformVersion
	})
}
