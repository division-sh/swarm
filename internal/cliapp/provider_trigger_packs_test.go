package cliapp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/providertriggers"
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

func TestProviderTriggerPlatformDirsAreElevated(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	writeRuntimeConfigText(t, filepath.Join(repo, "swarm.yaml"), strings.Join([]string{
		"provider_triggers:",
		"  packs:",
		"    platform_dirs:",
		"      - ./packs/provider-triggers/github",
	}, "\n")+"\n")

	_, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo})
	if err == nil {
		t.Fatal("project platform_dirs passed elevated trust admission")
	}
	for _, want := range []string{"provider_triggers.packs.platform_dirs", "not allowed in project_config", "move this key"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("platform_dirs trust error = %q, want containing %q", err, want)
		}
	}
}

func TestProviderTriggerPackDirsResolveFromEffectiveDeclaringLayers(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()
	projectExternal := filepath.Join(repo, "project-external")
	copyProviderTriggerPackFixture(t, "stripe", projectExternal, true)
	rewriteProviderTriggerPackFixtureAsExternalAcme(t, projectExternal)
	localDir := filepath.Join(repo, ".swarm")
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		t.Fatalf("mkdir local config dir: %v", err)
	}
	platformConfig := []string{"provider_triggers:", "  packs:", "    platform_dirs:"}
	providers := configuredProviderTriggerFixtureProviders()
	wantPlatformDirs := make([]string, 0, len(providers))
	for _, provider := range providers {
		relative := "platform-" + provider
		target := filepath.Join(localDir, relative)
		copyProviderTriggerPackFixture(t, provider, target, false)
		platformConfig = append(platformConfig, "      - ./"+relative)
		wantPlatformDirs = append(wantPlatformDirs, target)
	}

	writeRuntimeConfigText(t, filepath.Join(repo, "swarm.yaml"), strings.Join([]string{
		"provider_triggers:",
		"  packs:",
		"    external_dirs:",
		"      - ./project-external",
	}, "\n")+"\n")
	writeRuntimeConfigText(t, filepath.Join(localDir, "swarm.yaml"), strings.Join(platformConfig, "\n")+"\n")
	explicitDir := t.TempDir()
	explicitPath := filepath.Join(explicitDir, "explicit.yaml")
	writeRuntimeConfigText(t, explicitPath, "runtime:\n  recovery_on_startup: false\n")

	cfgResult, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: repo, ExplicitPath: explicitPath})
	if err != nil {
		t.Fatalf("load layered config: %v", err)
	}
	loaded, err := LoadConfiguredProviderTriggerPacks(repo, cfgResult)
	if err != nil {
		t.Fatalf("load configured provider trigger packs: %v", err)
	}
	if strings.Join(loaded.PlatformDirs, "\n") != strings.Join(wantPlatformDirs, "\n") {
		t.Fatalf("platform dirs = %v, want declaring local layer paths %v", loaded.PlatformDirs, wantPlatformDirs)
	}
	if len(loaded.ExternalDirs) != 1 || loaded.ExternalDirs[0] != projectExternal {
		t.Fatalf("external dirs = %v, want declaring project layer path %s", loaded.ExternalDirs, projectExternal)
	}
	if got := cfgResult.KeyOrigins["provider_triggers.packs.platform_dirs"]; got.Path != filepath.Join(localDir, "swarm.yaml") || got.Layer != unifiedLayerLocalOperator {
		t.Fatalf("platform key origin = %+v", got)
	}
	if got := cfgResult.KeyOrigins["provider_triggers.packs.external_dirs"]; got.Path != filepath.Join(repo, "swarm.yaml") || got.Layer != unifiedLayerProject {
		t.Fatalf("external key origin = %+v", got)
	}
}

func configuredProviderTriggerFixtureProviders() []string {
	return []string{"github", "intercom", "shopify", "slack", "stripe", "telegram", "twilio", "typeform"}
}

func rewriteProviderTriggerPackFixtureAsExternalAcme(t *testing.T, dir string) {
	t.Helper()
	manifestPath := filepath.Join(dir, "trigger.yaml")
	manifestBody, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read external trigger fixture: %v", err)
	}
	manifestBody = []byte(strings.ReplaceAll(string(manifestBody), "stripe", "acme"))
	if err := os.WriteFile(manifestPath, manifestBody, 0o644); err != nil {
		t.Fatalf("write external trigger fixture: %v", err)
	}

	envelopePath := filepath.Join(dir, "pack.yaml")
	envelopeBody, err := os.ReadFile(envelopePath)
	if err != nil {
		t.Fatalf("read external pack fixture: %v", err)
	}
	envelopeBody = []byte(strings.ReplaceAll(string(envelopeBody), "stripe", "acme"))
	_, stamped, err := providertriggers.StampPackEnvelope(envelopeBody, manifestBody)
	if err != nil {
		t.Fatalf("stamp external Acme fixture: %v", err)
	}
	if err := os.WriteFile(envelopePath, stamped, 0o644); err != nil {
		t.Fatalf("write external pack fixture: %v", err)
	}
}

func copyProviderTriggerPackFixture(t *testing.T, provider, target string, external bool) {
	t.Helper()
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatalf("mkdir pack fixture: %v", err)
	}
	source := filepath.Join(RepoRoot(), "packs", "provider-triggers", provider)
	for _, name := range []string{"pack.yaml", "trigger.yaml"} {
		body, err := os.ReadFile(filepath.Join(source, name))
		if err != nil {
			t.Fatalf("read %s fixture: %v", name, err)
		}
		if external && name == "pack.yaml" {
			body = []byte(strings.Replace(string(body), "source: platform", "source: external", 1))
		}
		if err := os.WriteFile(filepath.Join(target, name), body, 0o644); err != nil {
			t.Fatalf("write %s fixture: %v", name, err)
		}
	}
}
