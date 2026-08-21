package canonicalrouting

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTelegramAgentTriggerPackSnapshotMatchesPlatformOwner(t *testing.T) {
	repoRoot := RepoRoot(t)
	exampleRoot := ExampleRoot(t, TelegramAgent)
	for _, name := range []string{"pack.yaml", "trigger.yaml"} {
		want, err := os.ReadFile(filepath.Join(repoRoot, "packs", "provider-triggers", "telegram", name))
		if err != nil {
			t.Fatalf("read platform Telegram %s: %v", name, err)
		}
		got, err := os.ReadFile(filepath.Join(exampleRoot, "provider-triggers", "telegram", name))
		if err != nil {
			t.Fatalf("read example Telegram %s: %v", name, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("example provider-triggers/telegram/%s drifted from packs/provider-triggers/telegram/%s", name, name)
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
