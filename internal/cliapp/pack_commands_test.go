package cliapp

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
)

func TestPacksListAndShowUseEmbeddedInventoryOutsideProject(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)

	code, stdout, stderr := runPacksCommand(t, repo, "packs", "list", repo, "--json")
	if code != 0 || stderr != "" {
		t.Fatalf("packs list code=%d stderr=%q stdout=%s", code, stderr, stdout)
	}
	list := decodeOutputJSON[packInventoryReadback](t, stdout)
	if list.BaseMode != "embedded" || list.BaseDigest == "" || list.EffectiveDigest == "" {
		t.Fatalf("bare embedded list = %#v", list)
	}

	code, stdout, stderr = runPacksCommand(t, repo, "packs", "show", "telegram", repo, "--json")
	if code != 0 || stderr != "" {
		t.Fatalf("packs show code=%d stderr=%q stdout=%s", code, stderr, stdout)
	}
	show := decodeOutputJSON[packShowReadback](t, stdout)
	if show.Pack.ID != "telegram" || show.Pack.Source != "embedded" ||
		!strings.Contains(show.ManifestYAML, "version: 0.1.0") || !strings.Contains(show.Bodies["trigger.yaml"], "event_name:\n  literal: raw") {
		t.Fatalf("bare embedded show = %#v", show)
	}

	code, stdout, stderr = runPacksCommand(t, repo, "packs", "show", "telegram", repo)
	if code != 0 || stderr != "" || !strings.Contains(stdout, "manifest.yaml:") || !strings.Contains(stdout, "trigger.yaml:") || !strings.Contains(stdout, "event_name:\n  literal: raw") {
		t.Fatalf("human packs show code=%d stderr=%q stdout=%s", code, stderr, stdout)
	}
}

func TestPacksReadbackReportsConfiguredDevelopmentDirectories(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	_, dirs := packfixture.DevelopmentBase(t, nil)
	configPath := filepath.Join(t.TempDir(), "swarm.yaml")
	configLines := []string{"platform:", "  packs:", "    platform_dirs:"}
	wantByID := make(map[string]string, len(dirs))
	for _, dir := range dirs {
		configLines = append(configLines, "      - "+dir)
		wantByID[filepath.Base(dir)] = dir
	}
	writeRuntimeConfigText(t, configPath, strings.Join(configLines, "\n")+"\n")

	sourceRoot := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
	code, stdout, stderr := runPacksCommand(t, sourceRoot, "packs", "list", sourceRoot, "--config", configPath, "--json")
	if code != 0 || stderr != "" {
		t.Fatalf("development list code=%d stderr=%q stdout=%s", code, stderr, stdout)
	}
	list := decodeOutputJSON[packInventoryReadback](t, stdout)
	for _, entry := range list.Packs {
		if got, want := entry.Directory, wantByID[entry.ID]; got != want {
			t.Errorf("list pack %q directory = %q, want %q", entry.ID, got, want)
		}
	}

	code, stdout, stderr = runPacksCommand(t, sourceRoot, "packs", "show", "telegram", sourceRoot, "--config", configPath, "--json")
	if code != 0 || stderr != "" {
		t.Fatalf("development show code=%d stderr=%q stdout=%s", code, stderr, stdout)
	}
	show := decodeOutputJSON[packShowReadback](t, stdout)
	if got, want := show.Pack.Directory, wantByID[show.Pack.ID]; got != want {
		t.Fatalf("show pack %q directory = %q, want %q", show.Pack.ID, got, want)
	}
}

func TestCLIWorkflowModuleConsumersUseConfiguredDevelopmentBase(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	development, dirs := packfixture.DevelopmentBase(t, nil)
	configPath := filepath.Join(t.TempDir(), "swarm.yaml")
	configLines := []string{"llm:", "  backend: anthropic", "platform:", "  packs:", "    platform_dirs:"}
	for _, dir := range dirs {
		configLines = append(configLines, "      - "+dir)
	}
	writeRuntimeConfigText(t, configPath, strings.Join(configLines, "\n")+"\n")

	sourceRoot := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)

	_, bundle, paths, err := loadConfiguredCLIWorkflowModule(RepoRoot(), CLISourcePathOptions{
		SourceRoot: sourceRoot, ConfigPath: configPath,
	})
	if err != nil {
		t.Fatalf("load configured CLI workflow module: %v", err)
	}
	if bundle.PackInventory.BaseDigest() != development.Digest() {
		t.Fatalf("CLI workflow base digest = %q, want development %q", bundle.PackInventory.BaseDigest(), development.Digest())
	}
	if paths.SourceRoot != sourceRoot {
		t.Fatalf("normalized source path = %q, want %q", paths.SourceRoot, sourceRoot)
	}

	commands := []struct {
		name string
		args []string
	}{
		{name: "describe", args: []string{"describe", sourceRoot, "--config", configPath, "--json"}},
		{name: "describe routes", args: []string{"describe", "routes", sourceRoot, "--config", configPath, "--json"}},
	}
	for _, tc := range commands {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := runPacksCommand(t, RepoRoot(), tc.args...)
			if code != 0 || stderr != "" {
				t.Fatalf("%s code=%d stderr=%q stdout=%s", tc.name, code, stderr, stdout)
			}
		})
	}

}

func TestImportEmbeddedPackOwnsProjectBytesAndBundleIdentity(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
	specPath := runtimecontracts.DefaultPlatformSpecFile(RepoRoot())
	beforeHash := loadPackCommandBundleHash(t, project, specPath)

	code, stdout, stderr := runPacksCommand(t, RepoRoot(), "import", "telegram", project, "--json")
	if code != 0 || stderr != "" {
		t.Fatalf("first import code=%d stderr=%q stdout=%s", code, stderr, stdout)
	}
	first := decodeOutputJSON[packImportReadback](t, stdout)
	if !first.Changed || first.ID != "telegram" || first.Source != "local" {
		t.Fatalf("first import = %#v", first)
	}

	code, stdout, stderr = runPacksCommand(t, RepoRoot(), "import", "telegram", project, "--json")
	if code != 0 || stderr != "" || decodeOutputJSON[packImportReadback](t, stdout).Changed {
		t.Fatalf("idempotent import code=%d stderr=%q stdout=%s", code, stderr, stdout)
	}

	afterImportHash := loadPackCommandBundleHash(t, project, specPath)
	if afterImportHash == beforeHash {
		t.Fatalf("project import did not enter bundle identity: %s", beforeHash)
	}
	bodyPath := filepath.Join(project, "telegram", "trigger.yaml")
	body, err := os.ReadFile(bodyPath)
	if err != nil {
		t.Fatal(err)
	}
	edited := strings.Replace(string(body), "telegram update object is required", "project telegram update object is required", 1)
	if edited == string(body) {
		t.Fatal("telegram test edit found no canonical field")
	}
	if err := os.WriteFile(bodyPath, []byte(edited), 0o644); err != nil {
		t.Fatal(err)
	}
	afterEditHash := loadPackCommandBundleHash(t, project, specPath)
	if afterEditHash == afterImportHash {
		t.Fatalf("edited project pack did not change bundle identity: %s", afterImportHash)
	}

	code, _, stderr = runPacksCommand(t, RepoRoot(), "import", "telegram", project)
	if code == 0 || !strings.Contains(stderr, "will not overwrite") {
		t.Fatalf("edited reimport code=%d stderr=%q", code, stderr)
	}
	code, _, stderr = runPacksCommand(t, RepoRoot(), "import", "unknown", project)
	if code == 0 || !strings.Contains(stderr, "use its directory name from swarm packs list") {
		t.Fatalf("unknown import code=%d stderr=%q", code, stderr)
	}
}

func runPacksCommand(t testing.TB, repo string, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := executeRootCommand(context.Background(), repo, args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func loadPackCommandBundleHash(t testing.TB, project, specPath string) string {
	t.Helper()
	_, bundle, err := NewSwarmWorkflowModule(RepoRoot(), project)
	if err != nil {
		t.Fatalf("load project bundle: %v", err)
	}
	hash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		t.Fatalf("hash project bundle: %v", err)
	}
	return hash
}
