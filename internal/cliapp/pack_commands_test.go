package cliapp

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packartifact"
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

func TestPackReadbackMarksVersionOnlyProjectEditModified(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
	base, err := packartifact.LoadEmbeddedPlatformPackInventory("0.7.0")
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := packartifact.ImportEmbeddedPack(project, "provider.telegram", base); err != nil || !changed {
		t.Fatalf("import Telegram changed=%t err=%v", changed, err)
	}
	envelopePath := filepath.Join(project, "packs", "provider.telegram", packartifact.EnvelopeFileName)
	body, err := os.ReadFile(envelopePath)
	if err != nil {
		t.Fatal(err)
	}
	edited := strings.Replace(string(body), "version: 0.1.0", "version: 0.1.1", 1)
	if edited == string(body) {
		t.Fatal("Telegram envelope version fixture changed")
	}
	if err := os.WriteFile(envelopePath, []byte(edited), 0o644); err != nil {
		t.Fatal(err)
	}

	code, stdout, stderr := runPacksCommand(t, RepoRoot(), "packs", "show", "provider.telegram", "--contracts", project, "--json")
	if code != 0 || stderr != "" {
		t.Fatalf("JSON show code=%d stderr=%q stdout=%s", code, stderr, stdout)
	}
	show := decodeOutputJSON[packShowReadback](t, stdout)
	if show.Pack.Version != "0.1.1" || !show.Pack.Modified || show.Pack.Origin.Version != "0.1.0" {
		t.Fatalf("version-only JSON readback = %#v", show.Pack)
	}
	code, stdout, stderr = runPacksCommand(t, RepoRoot(), "packs", "show", "provider.telegram", "--contracts", project)
	if code != 0 || stderr != "" || !strings.Contains(stdout, "version=0.1.1") || !strings.Contains(stdout, "modified=true") || !strings.Contains(stdout, "origin=provider.telegram@0.1.0") {
		t.Fatalf("version-only human readback code=%d stderr=%q stdout=%s", code, stderr, stdout)
	}
}

func TestMalformedPackBodiesFailBeforeEveryCLIPublishingSurface(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	configPath := writeTestVerifyRuntimeConfig(t)
	base, err := packartifact.LoadEmbeddedPlatformPackInventory("0.7.0")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name, id, bodyFile, wantErr string
	}{
		{name: "trigger", id: "provider.telegram", bodyFile: packartifact.TriggerManifestFileName, wantErr: "admit provider trigger packs"},
		{name: "connector", id: "provider.telegram.connector", bodyFile: packartifact.ConnectorManifestFileName, wantErr: "admit provider connector packs"},
		{name: "channel", id: "provider.telegram.hitl_channel", bodyFile: packartifact.ChannelManifestFileName, wantErr: "admit channel packs"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			project := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
			if changed, err := packartifact.ImportEmbeddedPack(project, tc.id, base); err != nil || !changed {
				t.Fatalf("import %s changed=%t err=%v", tc.id, changed, err)
			}
			if err := os.WriteFile(filepath.Join(project, "packs", tc.id, tc.bodyFile), []byte("unknown_field: true\n"), 0o644); err != nil {
				t.Fatal(err)
			}

			assertRejected := func(surface string, args ...string) string {
				t.Helper()
				var stdout, stderr bytes.Buffer
				code := executeRootCommandWithOptions(context.Background(), RepoRoot(), args, &stdout, &stderr, defaultRootCommandOptions())
				combined := stdout.String() + stderr.String()
				if code == 0 || !strings.Contains(combined, tc.wantErr) {
					t.Fatalf("%s code=%d stdout=%s stderr=%s, want %q", surface, code, stdout.String(), stderr.String(), tc.wantErr)
				}
				return stdout.String()
			}

			assertRejected("packs list", "packs", "list", "--contracts", project, "--json")
			assertRejected("packs show", "packs", "show", tc.id, "--contracts", project, "--json")
			if _, _, err := NewSwarmWorkflowModule(RepoRoot(), project, runtimecontracts.DefaultPlatformSpecFile(RepoRoot())); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("bundle hash admission error = %v, want %q", err, tc.wantErr)
			}
			assertRejected("bundle build", "bundle", "build", "--contracts", project, "--output", t.TempDir(), "--config", configPath)
			assertRejected("bundle register", "bundle", "register", "--contracts", project, "--config", configPath)
			assertRejected("verify", "verify", "--contracts", project, "--config", configPath, "--json")
			doctorOutput := assertRejected("doctor", "doctor", "--contracts", project, "--config", configPath, "--json")
			report := decodeOutputJSON[LocalPreflightReport](t, doctorOutput)
			if report.PackInventory != nil {
				t.Fatalf("doctor published malformed %s inventory: %#v", tc.name, report.PackInventory)
			}
		})
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
