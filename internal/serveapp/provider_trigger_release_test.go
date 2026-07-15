package serveapp

import (
	"context"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/store"
)

func TestProviderTriggerReleaseLayoutLoadsCompleteFilesystemInventory(t *testing.T) {
	releaseRoot := t.TempDir()
	binaryPath := filepath.Join(releaseRoot, "swarm")
	build := exec.Command("go", "build", "-o", binaryPath, "./cmd/swarm")
	build.Dir = cliapp.RepoRoot()
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build release-shaped swarm binary: %v\n%s", err, output)
	}
	copyProviderTriggerReleaseTree(t, filepath.Join(cliapp.RepoRoot(), "packs", "provider-triggers"), filepath.Join(releaseRoot, "packs", "provider-triggers"))
	copyProviderTriggerReleaseTree(t, filepath.Join(cliapp.RepoRoot(), "tests", "tier8-boot-verification", "test-boot-success"), filepath.Join(releaseRoot, "contracts"))
	platformSpecBody, err := os.ReadFile(filepath.Join(cliapp.RepoRoot(), "platform-spec.yaml"))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	if err := os.WriteFile(filepath.Join(releaseRoot, "platform-spec.yaml"), platformSpecBody, 0o644); err != nil {
		t.Fatalf("write release platform spec: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(releaseRoot, "data"), 0o755); err != nil {
		t.Fatalf("mkdir release data: %v", err)
	}
	sqlitePath := filepath.Join(releaseRoot, "runtime.db")
	seedReleaseProviderTriggerStore(t, platformSpecBody, sqlitePath)
	configPath := filepath.Join(releaseRoot, "platform-config.yaml")
	writeReleaseProviderTriggerConfig(t, configPath, sqlitePath)

	runDoctor := func() (string, error) {
		cmd := exec.Command(binaryPath,
			"doctor",
			"--config", configPath,
			"--contracts", filepath.Join(releaseRoot, "contracts"),
			"--platform-spec", filepath.Join(releaseRoot, "platform-spec.yaml"),
			"--data", filepath.Join(releaseRoot, "data"),
			"--workspace-backend", "host",
			"--api-listen-addr", "127.0.0.1:0",
			"--mcp-listen-addr", "127.0.0.1:0",
			"--json",
		)
		cmd.Dir = releaseRoot
		cmd.Env = releaseProviderTriggerProcessEnv()
		output, err := cmd.CombinedOutput()
		return string(output), err
	}

	output, err := runDoctor()
	if err != nil {
		t.Fatalf("release-shaped doctor failed: %v\n%s", err, output)
	}
	for _, provider := range []string{"github", "intercom", "shopify", "slack", "stripe", "telegram", "twilio", "typeform"} {
		for _, want := range []string{"provider_trigger_pack_" + provider, "provenance=platform", filepath.Join("packs", "provider-triggers", provider)} {
			if !strings.Contains(output, want) {
				t.Fatalf("release doctor missing %q for %s:\n%s", want, provider, output)
			}
		}
	}

	if err := os.RemoveAll(filepath.Join(releaseRoot, "packs", "provider-triggers", "stripe")); err != nil {
		t.Fatalf("remove declared Stripe pack: %v", err)
	}
	output, err = runDoctor()
	if err == nil {
		t.Fatalf("release doctor accepted missing declared Stripe pack:\n%s", output)
	}
	for _, want := range []string{"provider_trigger_pack_load_failed", "stripe", "pack.yaml"} {
		if !strings.Contains(output, want) {
			t.Fatalf("missing-pack release failure lacks %q:\n%s", want, output)
		}
	}
}

func writeReleaseProviderTriggerConfig(t *testing.T, path, sqlitePath string) {
	t.Helper()
	lines := []string{
		"store:",
		"  backend: sqlite",
		"  sqlite:",
		"    path: " + strconv.Quote(sqlitePath),
		"provider_triggers:",
		"  packs:",
		"    platform_dirs:",
	}
	for _, provider := range []string{"github", "intercom", "shopify", "slack", "stripe", "telegram", "twilio", "typeform"} {
		lines = append(lines, "      - packs/provider-triggers/"+provider)
	}
	writeRuntimeConfigText(t, path, strings.Join(lines, "\n")+"\n")
}

func seedReleaseProviderTriggerStore(t *testing.T, platformSpecBody []byte, sqlitePath string) {
	t.Helper()
	var platformSpec runtimecontracts.PlatformSpecDocument
	decodeAuthoritativeYAMLBytesForTest(t, platformSpecBody, &platformSpec)
	plans, err := store.GeneratePlatformTableDDLs(platformSpec)
	if err != nil {
		t.Fatalf("generate release platform tables: %v", err)
	}
	sqliteStore, err := store.NewSQLiteRuntimeStore(sqlitePath)
	if err != nil {
		t.Fatalf("create release SQLite store: %v", err)
	}
	ctx := context.Background()
	bootstrapSQLiteSchemaForTest(t, ctx, sqliteStore, plans)
	seedProviderTriggerSmokeRuntime(t, runtimecorrelation.WithRunID(ctx, "76000000-0000-0000-0000-000000000001"), sqliteStore,
		"76000000-0000-0000-0000-000000000001", "76000000-0000-0000-0000-000000000002", "release-stripe", "stripe-customer", "stripe", "stripe-release-secret", "release-stripe-agent")
	seedProviderTriggerSmokeRuntime(t, runtimecorrelation.WithRunID(ctx, "76000000-0000-0000-0000-000000000003"), sqliteStore,
		"76000000-0000-0000-0000-000000000003", "76000000-0000-0000-0000-000000000004", "release-slack", "slack-customer", "slack", "slack-release-secret", "release-slack-agent")
	if err := sqliteStore.Close(); err != nil {
		t.Fatalf("close seeded release SQLite store: %v", err)
	}
}

func releaseProviderTriggerProcessEnv() []string {
	env := make([]string, 0, len(os.Environ())+5)
	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, "SWARM_TEST_") {
			continue
		}
		env = append(env, entry)
	}
	return append(env,
		"SWARM_CONFIG=",
		"SWARM_CREDENTIALS_FILE=",
		"SWARM_MANAGED_CREDENTIALS_FILE=",
		"PGPASSWORD=",
		"CLAUDE_CODE_OAUTH_TOKEN=",
	)
}

func copyProviderTriggerReleaseTree(t *testing.T, source, target string) {
	t.Helper()
	err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		destination := filepath.Join(target, relative)
		if entry.IsDir() {
			return os.MkdirAll(destination, 0o755)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(destination, body, 0o644)
	})
	if err != nil {
		t.Fatalf("copy release tree %s: %v", source, err)
	}
}
