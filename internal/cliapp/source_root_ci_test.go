package cliapp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestShippedCISmokeUsesPositionalSourceRoot(t *testing.T) {
	body, err := os.ReadFile(filepath.Join(RepoRoot(), ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(body)
	if strings.Contains(source, "--contracts") {
		t.Fatal("CI restores the retired --contracts source-root interpreter")
	}
	if !strings.Contains(source, "./swarm run start examples/routing/root-ingress") {
		t.Fatal("SQLite smoke does not exercise the canonical positional source root")
	}
}
