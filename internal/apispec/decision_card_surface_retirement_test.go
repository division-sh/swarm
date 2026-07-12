package apispec

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDecisionCardMigrationHasNoRetiredPublicSurfaceSurvivors(t *testing.T) {
	repoRoot := filepath.Clean(filepath.Join("..", ".."))
	retired := []string{
		"mailbox." + "approve",
		"mailbox." + "reject",
		"approve_" + "mailbox",
		"reject_" + "mailbox",
		"mailbox_" + "approve",
		"mailbox_" + "reject",
		"approve:" + "mailbox",
		"mailbox.item_" + "decided",
		"mailbox.item_" + "deferred",
		"Decide" + "MailboxItem",
		"DecideV1" + "MailboxItem",
	}
	var survivors []string
	err := filepath.WalkDir(repoRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".swarm", "vendor", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		ext := strings.ToLower(filepath.Ext(path))
		if ext != ".go" && ext != ".yaml" && ext != ".yml" && ext != ".json" {
			return nil
		}
		if strings.HasSuffix(path, "decision_card_surface_retirement_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, token := range retired {
			if strings.Contains(string(raw), token) {
				rel, _ := filepath.Rel(repoRoot, path)
				survivors = append(survivors, filepath.ToSlash(rel)+": "+token)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan retired decision-card surface: %v", err)
	}
	if len(survivors) > 0 {
		t.Fatalf("retired decision-card surface survived migration:\n%s", strings.Join(survivors, "\n"))
	}
}
