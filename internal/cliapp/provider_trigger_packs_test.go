package cliapp

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
)

// Public ingress behavior is owned by the standing-activation served tests.
// This release-layout test is bounded to pack discovery and provenance.

func TestPlatformSpecDoesNotRestoreHardCodedProviderTriggerInventory(t *testing.T) {
	body, err := os.ReadFile(filepath.Join(RepoRoot(), "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, retired := range []string{"required_platform_inventory", "RequiredPlatformPackIdentities", "complete first-party platform pack inventory"} {
		if strings.Contains(string(body), retired) {
			t.Fatalf("platform spec restored retired inventory authority %q", retired)
		}
	}
}

func TestPlatformPackDirsInIgnoredProjectHaveNoEffect(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	writeRuntimeConfigText(t, filepath.Join(repo, "swarm.yaml"), strings.Join([]string{
		"platform:",
		"  packs:",
		"    platform_dirs:",
		"      - ./packs/provider-triggers/github",
	}, "\n")+"\n")

	loaded, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo})
	if err != nil || len(loaded.Config.Platform.Packs.PlatformDirs) != 0 || loaded.Path != "" {
		t.Fatalf("ignored project supplied development pack selection: error=%v path=%q", err, loaded.Path)
	}
}

func TestPlatformPackDirsResolveFromSelectedFile(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	localDir := filepath.Join(repo, ".swarm")
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		t.Fatalf("mkdir local config dir: %v", err)
	}
	platformConfig := []string{"platform:", "  packs:", "    platform_dirs:"}
	base := packfixture.EmbeddedBase(t)
	wantPlatformDirs := make([]string, 0, len(base.Entries()))
	for index, entry := range base.Entries() {
		relative := filepath.Join(fmt.Sprintf("platform-%02d", index), entry.ID())
		target := filepath.Join(localDir, relative)
		if err := os.MkdirAll(target, 0o755); err != nil {
			t.Fatal(err)
		}
		for name, body := range entry.Provider().Members() {
			if err := os.WriteFile(filepath.Join(target, name), body, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		platformConfig = append(platformConfig, "      - ./"+relative)
		wantPlatformDirs = append(wantPlatformDirs, target)
	}

	writeRuntimeConfigText(t, filepath.Join(localDir, "swarm.yaml"), strings.Join(platformConfig, "\n")+"\n")
	explicitDir := t.TempDir()
	explicitPath := filepath.Join(explicitDir, "explicit.yaml")
	writeRuntimeConfigText(t, explicitPath, "runtime:\n  recovery_on_startup: false\n")

	cfgResult, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo})
	if err != nil {
		t.Fatalf("load deployment config: %v", err)
	}
	loaded, err := LoadConfiguredPlatformPackBase(repo, cfgResult)
	if err != nil {
		t.Fatalf("load configured provider trigger packs: %v", err)
	}
	if loaded.SelectionMode() != packartifact.SelectionDevelopmentOverride || strings.Join(loaded.SourceDirectories(), "\n") != strings.Join(wantPlatformDirs, "\n") {
		t.Fatalf("platform base = mode:%s dirs:%v, want development override paths %v", loaded.SelectionMode(), loaded.SourceDirectories(), wantPlatformDirs)
	}
	if cfgResult.Path != filepath.Join(localDir, "swarm.yaml") {
		t.Fatalf("selected pack config = %q", cfgResult.Path)
	}
	// An explicit document is a complete replacement, not a layer over deployment.
	cfgResult, err = LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: explicitPath})
	if err != nil {
		t.Fatal(err)
	}
	loaded, err = LoadConfiguredPlatformPackBase(repo, cfgResult)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.SourceDirectories()) != 0 || loaded.SelectionMode() == packartifact.SelectionDevelopmentOverride {
		t.Fatal("unselected deployment supplied development packs")
	}
	for _, selection := range []string{"flag", "locator"} {
		t.Run(selection, func(t *testing.T) {
			opts := RuntimeConfigLoadOptions{RepoRoot: repo}
			if selection == "flag" {
				opts.ExplicitPath = filepath.Join(localDir, "swarm.yaml")
			} else {
				t.Setenv("SWARM_CONFIG", filepath.Join(localDir, "swarm.yaml"))
			}
			selected, err := LoadRuntimeConfigWithOptions(opts)
			if err != nil {
				t.Fatal(err)
			}
			packs, err := LoadConfiguredPlatformPackBase(repo, selected)
			if err != nil {
				t.Fatal(err)
			}
			if packs.SelectionMode() != packartifact.SelectionDevelopmentOverride || strings.Join(packs.SourceDirectories(), "\n") != strings.Join(wantPlatformDirs, "\n") {
				t.Fatalf("selected-file relative pack resolution changed: mode=%s paths=%v", packs.SelectionMode(), packs.SourceDirectories())
			}
		})
	}
}
