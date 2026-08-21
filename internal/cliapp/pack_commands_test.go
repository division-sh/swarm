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
)

func TestPacksListAndShowUseEmbeddedInventoryOutsideProject(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	repo := t.TempDir()

	code, stdout, stderr := runPacksCommand(t, repo, "packs", "list", "--json")
	if code != 0 || stderr != "" {
		t.Fatalf("packs list code=%d stderr=%q stdout=%s", code, stderr, stdout)
	}
	list := decodeOutputJSON[packInventoryReadback](t, stdout)
	if list.BaseMode != "embedded" || len(list.Packs) != 14 || list.BaseDigest == "" || list.EffectiveDigest == "" {
		t.Fatalf("bare embedded list = %#v", list)
	}

	code, stdout, stderr = runPacksCommand(t, repo, "packs", "show", "provider.telegram", "--json")
	if code != 0 || stderr != "" {
		t.Fatalf("packs show code=%d stderr=%q stdout=%s", code, stderr, stdout)
	}
	show := decodeOutputJSON[packShowReadback](t, stdout)
	if show.Pack.ID != "provider.telegram" || show.Pack.Source != "embedded" ||
		!strings.Contains(show.EnvelopeYAML, "id: provider.telegram") || !strings.Contains(show.ManifestYAML, "provider: telegram") {
		t.Fatalf("bare embedded show = %#v", show)
	}

	code, stdout, stderr = runPacksCommand(t, repo, "packs", "show", "provider.telegram")
	if code != 0 || stderr != "" || !strings.Contains(stdout, "pack.yaml:") || !strings.Contains(stdout, "trigger.yaml:") || !strings.Contains(stdout, "provider: telegram") {
		t.Fatalf("human packs show code=%d stderr=%q stdout=%s", code, stderr, stdout)
	}
}

func TestPacksListDoesNotRequireRuntimeExecutionPosture(t *testing.T) {
	t.Setenv("SWARM_CONFIG", "")
	t.Setenv("SWARM_API_SERVER", "")
	t.Setenv("SWARM_API_TOKEN", "")
	t.Setenv("SWARM_API_TOKEN_FILE", "")
	t.Setenv("SWARM_CONTRACTS_PATH", "")
	t.Setenv("SWARM_CONTRACTS_DIR", "")
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("HOME", t.TempDir())

	code, stdout, stderr := runPacksCommand(t, t.TempDir(), "packs", "list", "--json")
	if code != 0 || stderr != "" {
		t.Fatalf("packs list without runtime posture code=%d stderr=%q stdout=%s", code, stderr, stdout)
	}
	list := decodeOutputJSON[packInventoryReadback](t, stdout)
	if list.BaseMode != "embedded" || len(list.Packs) != 14 {
		t.Fatalf("bare embedded list = %#v", list)
	}
}

func TestImportEmbeddedPackOwnsProjectBytesAndBundleIdentity(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
	specPath := runtimecontracts.DefaultPlatformSpecFile(RepoRoot())
	beforeHash := loadPackCommandBundleHash(t, project, specPath)

	code, stdout, stderr := runPacksCommand(t, RepoRoot(), "import", "provider.telegram", "--contracts", project, "--json")
	if code != 0 || stderr != "" {
		t.Fatalf("first import code=%d stderr=%q stdout=%s", code, stderr, stdout)
	}
	first := decodeOutputJSON[packImportReadback](t, stdout)
	if !first.Changed || first.ID != "provider.telegram" || first.Source != "project" {
		t.Fatalf("first import = %#v", first)
	}

	code, stdout, stderr = runPacksCommand(t, RepoRoot(), "import", "provider.telegram", "--contracts", project, "--json")
	if code != 0 || stderr != "" || decodeOutputJSON[packImportReadback](t, stdout).Changed {
		t.Fatalf("idempotent import code=%d stderr=%q stdout=%s", code, stderr, stdout)
	}

	afterImportHash := loadPackCommandBundleHash(t, project, specPath)
	if afterImportHash == beforeHash {
		t.Fatalf("project import did not enter bundle identity: %s", beforeHash)
	}
	bodyPath := filepath.Join(project, "packs", "provider.telegram", "trigger.yaml")
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

	code, stdout, stderr = runPacksCommand(t, RepoRoot(), "packs", "list", "--contracts", project, "--json")
	if code != 0 || stderr != "" {
		t.Fatalf("project packs list code=%d stderr=%q stdout=%s", code, stderr, stdout)
	}
	list := decodeOutputJSON[packInventoryReadback](t, stdout)
	var telegram packInventoryEntryReadback
	for _, entry := range list.Packs {
		if entry.ID == "provider.telegram" {
			telegram = entry
			break
		}
	}
	if telegram.Source != "project" || !telegram.ShadowsBase || !telegram.Modified || !telegram.Origin.Valid() {
		t.Fatalf("project telegram readback = %#v", telegram)
	}

	code, _, stderr = runPacksCommand(t, RepoRoot(), "import", "provider.telegram", "--contracts", project)
	if code == 0 || !strings.Contains(stderr, "will not overwrite") {
		t.Fatalf("edited reimport code=%d stderr=%q", code, stderr)
	}
	code, _, stderr = runPacksCommand(t, RepoRoot(), "import", "provider.unknown", "--contracts", project)
	if code == 0 || !strings.Contains(stderr, "available embedded packs:") || !strings.Contains(stderr, "provider.telegram") {
		t.Fatalf("unknown import code=%d stderr=%q", code, stderr)
	}
}

func runPacksCommand(t testing.TB, repo string, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := Execute(context.Background(), repo, args, &stdout, &stderr, nil)
	return code, stdout.String(), stderr.String()
}

func loadPackCommandBundleHash(t testing.TB, project, specPath string) string {
	t.Helper()
	_, bundle, err := NewSwarmWorkflowModule(RepoRoot(), project, specPath)
	if err != nil {
		t.Fatalf("load project bundle: %v", err)
	}
	hash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		t.Fatalf("hash project bundle: %v", err)
	}
	return hash
}
