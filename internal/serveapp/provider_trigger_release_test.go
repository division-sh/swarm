package serveapp

import (
	"encoding/json"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestReleaseBinaryUsesEmbeddedPackInventoryWithoutAdjacentPackTree(t *testing.T) {
	releaseRoot := t.TempDir()
	binaryPath := filepath.Join(releaseRoot, "swarm")
	build := exec.Command("go", "build", "-o", binaryPath, "./cmd/swarm")
	build.Dir = repoRootForTest()
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build release-shaped swarm binary: %v\n%s", err, output)
	}
	sourceRoot := filepath.Join(releaseRoot, "source")
	copyReleaseFixtureTree(t, filepath.Join(repoRootForTest(), "tests", "tier8-boot-verification", "test-boot-success"), sourceRoot)
	runPackList := func(executable, cwd string) (string, error) {
		cmd := exec.Command(executable, "packs", "list", sourceRoot, "--json")
		cmd.Dir = cwd
		cmd.Env = releaseProviderTriggerProcessEnv()
		output, err := cmd.CombinedOutput()
		return string(output), err
	}

	output, err := runPackList(binaryPath, releaseRoot)
	if err != nil {
		t.Fatalf("bare-binary packs list failed: %v\n%s", err, output)
	}
	inventory := decodeReleasePackInventory(t, output)
	if inventory.BaseMode != "embedded" || inventory.EffectiveDigest == "" {
		t.Fatalf("release pack inventory = %#v\n%s", inventory, output)
	}
	for _, provider := range []string{"github", "intercom", "shopify", "slack", "stripe", "telegram", "twilio", "typeform"} {
		entry, ok := inventory.Packs["provider."+provider]
		if !ok || entry.Source != "embedded" || entry.ManifestHash == "" {
			t.Fatalf("release pack inventory provider.%s = %#v, present=%t", provider, entry, ok)
		}
	}

	relocatedRoot := t.TempDir()
	relocatedBinary := filepath.Join(relocatedRoot, "renamed-swarm")
	if err := os.Rename(binaryPath, relocatedBinary); err != nil {
		t.Fatalf("relocate release binary: %v", err)
	}
	output, err = runPackList(relocatedBinary, relocatedRoot)
	if err != nil {
		t.Fatalf("relocated bare-binary packs list failed: %v\n%s", err, output)
	}
	inventory = decodeReleasePackInventory(t, output)
	if inventory.BaseMode != "embedded" || !strings.HasPrefix(inventory.EffectiveDigest, "sha256:") {
		t.Fatalf("relocated release pack inventory = %#v", inventory)
	}
	for _, id := range []string{"provider.telegram", "provider.telegram.connector", "provider.telegram.hitl_channel"} {
		entry, ok := inventory.Packs[id]
		if !ok || entry.Source != "embedded" || !strings.HasPrefix(entry.ManifestHash, "sha256:") {
			t.Fatalf("relocated release pack inventory %s = %#v, present=%t", id, entry, ok)
		}
	}
	for _, command := range [][]string{{"packs", "show", "provider.telegram", sourceRoot, "--json"}} {
		cmd := exec.Command(relocatedBinary, command...)
		cmd.Dir = relocatedRoot
		cmd.Env = releaseProviderTriggerProcessEnv()
		body, commandErr := cmd.CombinedOutput()
		if commandErr != nil {
			t.Fatalf("relocated bare-binary %s failed: %v\n%s", strings.Join(command, " "), commandErr, body)
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
