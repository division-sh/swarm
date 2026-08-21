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

func TestPlatformSpecProviderTriggerTargetAuthorityMatchesStandingIngress(t *testing.T) {
	var spec struct {
		ToolModel struct {
			ProviderTriggerAdapters struct {
				Scope                []string `yaml:"scope"`
				ExistingGatewayOwner string   `yaml:"existing_gateway_owner"`
				ManifestVocabulary   struct {
					Provider string `yaml:"provider"`
				} `yaml:"manifest_vocabulary"`
				SecretBinding string `yaml:"secret_binding"`
			} `yaml:"provider_trigger_adapters"`
		} `yaml:"tool_model"`
	}
	decodeAuthoritativeYAMLFileForTest(t, filepath.Join(RepoRoot(), "platform-spec.yaml"), &spec)
	contract := spec.ToolModel.ProviderTriggerAdapters
	routeAuthority := strings.Join(append(append([]string(nil), contract.Scope...), contract.ExistingGatewayOwner, contract.ManifestVocabulary.Provider), "\n")
	for _, want := range []string{"/webhooks/{alias}/{provider}", "standing ingress target", "RuntimeContextManager"} {
		if !strings.Contains(routeAuthority, want) {
			t.Fatalf("provider trigger route authority lacks %q:\n%s", want, routeAuthority)
		}
	}
	for _, retired := range []string{"/webhooks/{entity}/{provider}", "flow_instances.config.secrets.webhook_signing"} {
		if strings.Contains(routeAuthority+"\n"+contract.SecretBinding, retired) {
			t.Fatalf("provider trigger authority retains retired owner %q", retired)
		}
	}
	for _, want := range []string{"package.yaml flows[].ingress.providers[].signing_secret", "deployment credential-store entry", "Flow-instance config"} {
		if !strings.Contains(contract.SecretBinding, want) {
			t.Fatalf("provider trigger secret authority lacks %q:\n%s", want, contract.SecretBinding)
		}
	}
}

func TestPlatformPackDirsAreElevated(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	writeRuntimeConfigText(t, filepath.Join(repo, "swarm.yaml"), strings.Join([]string{
		"platform:",
		"  packs:",
		"    platform_dirs:",
		"      - ./packs/provider-triggers/github",
	}, "\n")+"\n")

	_, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo})
	if err == nil {
		t.Fatal("project platform_dirs passed elevated trust admission")
	}
	for _, want := range []string{"platform.packs.platform_dirs", "not allowed in project_config", "move this key"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("platform_dirs trust error = %q, want containing %q", err, want)
		}
	}
}

func TestPlatformPackDirsResolveFromEffectiveDeclaringLayer(t *testing.T) {
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
		relative := fmt.Sprintf("platform-%02d", index)
		target := filepath.Join(localDir, relative)
		if err := os.MkdirAll(target, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(target, packartifact.EnvelopeFileName), entry.EnvelopeBody(), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(target, packartifact.ManifestFileNameForType(entry.Type())), entry.ManifestBody(), 0o644); err != nil {
			t.Fatal(err)
		}
		platformConfig = append(platformConfig, "      - ./"+relative)
		wantPlatformDirs = append(wantPlatformDirs, target)
	}

	writeRuntimeConfigText(t, filepath.Join(localDir, "swarm.yaml"), strings.Join(platformConfig, "\n")+"\n")
	explicitDir := t.TempDir()
	explicitPath := filepath.Join(explicitDir, "explicit.yaml")
	writeRuntimeConfigText(t, explicitPath, "runtime:\n  recovery_on_startup: false\n")

	cfgResult, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: explicitPath})
	if err != nil {
		t.Fatalf("load layered config: %v", err)
	}
	loaded, err := LoadConfiguredPlatformPackBase(repo, cfgResult)
	if err != nil {
		t.Fatalf("load configured provider trigger packs: %v", err)
	}
	if loaded.SelectionMode() != packartifact.SelectionDevelopmentOverride || strings.Join(loaded.SourceDirectories(), "\n") != strings.Join(wantPlatformDirs, "\n") {
		t.Fatalf("platform base = mode:%s dirs:%v, want development override paths %v", loaded.SelectionMode(), loaded.SourceDirectories(), wantPlatformDirs)
	}
	if got := cfgResult.KeyOrigins["platform.packs.platform_dirs"]; got.Path != filepath.Join(localDir, "swarm.yaml") || got.Layer != unifiedLayerLocalOperator {
		t.Fatalf("platform key origin = %+v", got)
	}
}
