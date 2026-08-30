package canonicalrouting

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTelegramAgentConsumesEmbeddedPackInventory(t *testing.T) {
	exampleRoot := ExampleRoot(t, TelegramAgent)
	if _, err := os.Stat(filepath.Join(exampleRoot, "provider-triggers")); !os.IsNotExist(err) {
		t.Fatalf("Telegram example must not own a provider-trigger snapshot: %v", err)
	}
	for _, relative := range []string{"swarm.yaml", "swarm.live.yaml"} {
		body, err := os.ReadFile(filepath.Join(exampleRoot, relative))
		if err != nil {
			t.Fatalf("read Telegram example config %s: %v", relative, err)
		}
		if strings.Contains(string(body), "platform_dirs") || strings.Contains(string(body), "provider_triggers:") {
			t.Fatalf("Telegram example config %s bypasses the embedded effective pack inventory", relative)
		}
	}
}

func TestTelegramAgentHasNoGeneratedPositiveOwner(t *testing.T) {
	repoRoot := RepoRoot(t)
	retired := "CopyStanding" + "Telegram"
	err := filepath.WalkDir(repoRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(body), retired) {
			rel, _ := filepath.Rel(repoRoot, path)
			t.Fatalf("retired generated Telegram owner survives in %s", filepath.ToSlash(rel))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan generated Telegram owners: %v", err)
	}
	if _, err := os.Stat(filepath.Join(repoRoot, "internal", "cliapp", "archetypes", "webhook-responder")); !os.IsNotExist(err) {
		t.Fatalf("retired private webhook-responder owner still exists: %v", err)
	}
}
