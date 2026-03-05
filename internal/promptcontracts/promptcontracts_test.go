package promptcontracts

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFromDir_DefaultAndModeVariant(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "market-research-agent.md"), []byte("default prompt"), 0o644); err != nil {
		t.Fatalf("write default prompt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "market-research-agent.corpus.md"), []byte("corpus prompt"), 0o644); err != nil {
		t.Fatalf("write mode prompt: %v", err)
	}

	got, found, err := LoadFromDir(dir, "market-research-agent", "corpus")
	if err != nil {
		t.Fatalf("load mode prompt: %v", err)
	}
	if !found || got != "corpus prompt" {
		t.Fatalf("expected mode prompt, found=%v got=%q", found, got)
	}

	got, found, err = LoadFromDir(dir, "market-research-agent", "saas_gap")
	if err != nil {
		t.Fatalf("load fallback prompt: %v", err)
	}
	if !found || got != "default prompt" {
		t.Fatalf("expected default fallback, found=%v got=%q", found, got)
	}
}

func TestLoadFromDir_RejectsInvalidPathTokens(t *testing.T) {
	dir := t.TempDir()
	if _, _, err := LoadFromDir(dir, "../bad", ""); err == nil {
		t.Fatal("expected invalid agent id error")
	}
	if _, _, err := LoadFromDir(dir, "good-agent", "../bad"); err == nil {
		t.Fatal("expected invalid mode error")
	}
}
