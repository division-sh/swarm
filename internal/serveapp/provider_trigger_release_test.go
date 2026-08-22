package serveapp

import (
	"context"
	"encoding/json"
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

func TestReleaseBinaryUsesEmbeddedPackInventoryWithoutAdjacentPackTree(t *testing.T) {
	releaseRoot := t.TempDir()
	binaryPath := filepath.Join(releaseRoot, "swarm")
	build := exec.Command("go", "build", "-o", binaryPath, "./cmd/swarm")
	build.Dir = cliapp.RepoRoot()
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build release-shaped swarm binary: %v\n%s", err, output)
	}
	copyReleaseFixtureTree(t, filepath.Join(cliapp.RepoRoot(), "tests", "tier8-boot-verification", "test-boot-success"), filepath.Join(releaseRoot, "contracts"))
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

	runDoctor := func(executable, cwd string) (string, error) {
		cmd := exec.Command(executable,
			"doctor",
			"--config", configPath,
			"--contracts", filepath.Join(releaseRoot, "contracts"),
			"--data", filepath.Join(releaseRoot, "data"),
			"--workspace-backend", "host",
			"--api-listen-addr", "127.0.0.1:0",
			"--mcp-listen-addr", "127.0.0.1:0",
			"--json",
		)
		cmd.Dir = cwd
		cmd.Env = releaseProviderTriggerProcessEnv()
		output, err := cmd.CombinedOutput()
		return string(output), err
	}

	output, err := runDoctor(binaryPath, releaseRoot)
	if err != nil {
		t.Fatalf("bare-binary doctor failed: %v\n%s", err, output)
	}
	doctor := decodeReleasePackInventory(t, output)
	if doctor.BaseMode != "embedded" || doctor.EffectiveDigest == "" {
		t.Fatalf("release doctor pack inventory = %#v", doctor)
	}
	for _, provider := range []string{"github", "intercom", "shopify", "slack", "stripe", "telegram", "twilio", "typeform"} {
		if !strings.Contains(output, "provider_trigger_pack_"+provider) {
			t.Fatalf("release doctor missing provider trigger %s:\n%s", provider, output)
		}
		entry, ok := doctor.Packs["provider."+provider]
		if !ok || entry.Source != "embedded" || entry.ManifestHash == "" {
			t.Fatalf("release doctor provider.%s = %#v, present=%t", provider, entry, ok)
		}
	}

	relocatedRoot := t.TempDir()
	relocatedBinary := filepath.Join(relocatedRoot, "renamed-swarm")
	if err := os.Rename(binaryPath, relocatedBinary); err != nil {
		t.Fatalf("relocate release binary: %v", err)
	}
	output, err = runDoctor(relocatedBinary, relocatedRoot)
	if err != nil {
		t.Fatalf("relocated bare-binary doctor failed: %v\n%s", err, output)
	}
	doctor = decodeReleasePackInventory(t, output)
	if doctor.BaseMode != "embedded" || !strings.HasPrefix(doctor.EffectiveDigest, "sha256:") {
		t.Fatalf("relocated release doctor pack inventory = %#v", doctor)
	}
	for _, id := range []string{"provider.telegram", "provider.telegram.connector", "provider.telegram.hitl_channel"} {
		entry, ok := doctor.Packs[id]
		if !ok || entry.Source != "embedded" || !strings.HasPrefix(entry.ManifestHash, "sha256:") {
			t.Fatalf("relocated release doctor %s = %#v, present=%t", id, entry, ok)
		}
	}
	for _, command := range [][]string{{"packs", "list", "--json"}, {"packs", "show", "provider.telegram", "--json"}} {
		cmd := exec.Command(relocatedBinary, command...)
		cmd.Dir = relocatedRoot
		cmd.Env = releaseProviderTriggerProcessEnv()
		body, commandErr := cmd.CombinedOutput()
		if commandErr != nil {
			t.Fatalf("relocated bare-binary %s failed: %v\n%s", strings.Join(command, " "), commandErr, body)
		}
		if command[1] == "list" {
			inventory := decodeReleasePackInventory(t, string(body))
			entry, ok := inventory.Packs["provider.telegram"]
			if !ok || entry.Source != "embedded" || !strings.HasPrefix(entry.ManifestHash, "sha256:") {
				t.Fatalf("relocated bare-binary packs list telegram = %#v, present=%t", entry, ok)
			}
			continue
		}
		var show struct {
			Pack         releasePackInventoryEntry `json:"pack"`
			EnvelopeYAML string                    `json:"envelope_yaml"`
			ManifestYAML string                    `json:"manifest_yaml"`
		}
		if err := json.Unmarshal(body, &show); err != nil {
			t.Fatalf("decode relocated bare-binary packs show: %v\n%s", err, body)
		}
		if show.Pack.ID != "provider.telegram" || show.Pack.Source != "embedded" || !strings.HasPrefix(show.Pack.ManifestHash, "sha256:") ||
			!strings.Contains(show.EnvelopeYAML, "id: provider.telegram") || !strings.Contains(show.ManifestYAML, "provider: telegram") {
			t.Fatalf("relocated bare-binary packs show = %#v", show)
		}
	}
}

type releasePackInventoryEntry struct {
	ID           string `json:"id"`
	Source       string `json:"source"`
	ManifestHash string `json:"manifest_hash"`
}

type releasePackInventory struct {
	BaseMode        string                               `json:"base_mode"`
	EffectiveDigest string                               `json:"effective_digest"`
	Packs           map[string]releasePackInventoryEntry `json:"-"`
}

func decodeReleasePackInventory(t *testing.T, body string) releasePackInventory {
	t.Helper()
	var envelope struct {
		BaseMode        string                      `json:"base_mode"`
		EffectiveDigest string                      `json:"effective_digest"`
		Packs           []releasePackInventoryEntry `json:"packs"`
		PackInventory   *struct {
			BaseMode        string                      `json:"base_mode"`
			EffectiveDigest string                      `json:"effective_digest"`
			Packs           []releasePackInventoryEntry `json:"packs"`
		} `json:"pack_inventory"`
	}
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		t.Fatalf("decode release pack inventory: %v\n%s", err, body)
	}
	if envelope.PackInventory != nil {
		envelope.BaseMode = envelope.PackInventory.BaseMode
		envelope.EffectiveDigest = envelope.PackInventory.EffectiveDigest
		envelope.Packs = envelope.PackInventory.Packs
	}
	result := releasePackInventory{
		BaseMode:        envelope.BaseMode,
		EffectiveDigest: envelope.EffectiveDigest,
		Packs:           make(map[string]releasePackInventoryEntry, len(envelope.Packs)),
	}
	for _, entry := range envelope.Packs {
		result.Packs[entry.ID] = entry
	}
	return result
}

func writeReleaseProviderTriggerConfig(t *testing.T, path, sqlitePath string) {
	t.Helper()
	lines := []string{
		"runtime:",
		"  execution_posture: live",
		"store:",
		"  backend: sqlite",
		"  sqlite:",
		"    path: " + strconv.Quote(sqlitePath),
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

func copyReleaseFixtureTree(t *testing.T, source, target string) {
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
