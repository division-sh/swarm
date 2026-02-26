package templateops

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCompileTemplateFromYAML_SmokeAndSkipsRoutesFile(t *testing.T) {
	dir := t.TempDir()
	// Agent template YAMLs.
	if err := os.WriteFile(filepath.Join(dir, "a.yaml"), []byte(strings.Join([]string{
		"role: opco-ceo",
		"type: sonnet",
		"system_prompt: CEO",
		"tools: [tool_a, tool_a]", // dup to exercise normalize
		"subscriptions: [system.started]",
	}, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("write agent yaml: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.yaml"), []byte(strings.Join([]string{
		"role: vp-product",
		"parent_role: opco-ceo",
		"type: haiku",
		"system_prompt: VP",
		"tools: [tool_b]",
		"subscriptions: [board.*]",
	}, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("write agent yaml: %v", err)
	}
	// Colocated routes yaml should be ignored by the agent loader.
	if err := os.WriteFile(filepath.Join(dir, "routes.yaml"), []byte("bootstrap_routes: []\nseeded_routes: []\n"), 0o644); err != nil {
		t.Fatalf("write colocated routes yaml: %v", err)
	}

	routesPath := filepath.Join(t.TempDir(), "routes.yaml")
	if err := os.WriteFile(routesPath, []byte(strings.Join([]string{
		"bootstrap_routes:",
		"  - event_pattern: system.started",
		"    subscriber_role: opco-ceo",
		"    reason: startup",
		"seeded_routes:",
		"  - event_pattern: board.*",
		"    subscriber_role: vp-product",
		"    reason: tests",
	}, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("write routes yaml: %v", err)
	}

	agentsJSON, bootstrapJSON, seededJSON, err := CompileTemplateFromYAML(dir, routesPath)
	if err != nil {
		t.Fatalf("CompileTemplateFromYAML: %v", err)
	}
	var agents []map[string]any
	if err := json.Unmarshal(agentsJSON, &agents); err != nil || len(agents) != 2 {
		t.Fatalf("agents json invalid len=%d err=%v", len(agents), err)
	}
	var br []map[string]any
	_ = json.Unmarshal(bootstrapJSON, &br)
	if len(br) != 1 {
		t.Fatalf("expected 1 bootstrap route, got %d", len(br))
	}
	var sr []map[string]any
	_ = json.Unmarshal(seededJSON, &sr)
	if len(sr) != 1 {
		t.Fatalf("expected 1 seeded route, got %d", len(sr))
	}
}

func TestCompileTemplateFromYAML_Errors(t *testing.T) {
	if _, _, _, err := CompileTemplateFromYAML("", "x"); err == nil {
		t.Fatal("expected agentsDir required error")
	}
	if _, _, _, err := CompileTemplateFromYAML("x", ""); err == nil {
		t.Fatal("expected routesPath required error")
	}
	// No yaml files.
	dir := t.TempDir()
	routesPath := filepath.Join(t.TempDir(), "routes.yaml")
	_ = os.WriteFile(routesPath, []byte("bootstrap_routes: []\n"), 0o644)
	if _, _, _, err := CompileTemplateFromYAML(dir, routesPath); err == nil {
		t.Fatal("expected no agent yaml error")
	}
	// Duplicate roles.
	dir2 := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir2, "a.yaml"), []byte("role: x\nsystem_prompt: a\n"), 0o644)
	_ = os.WriteFile(filepath.Join(dir2, "b.yaml"), []byte("role: x\nsystem_prompt: b\n"), 0o644)
	if _, _, _, err := CompileTemplateFromYAML(dir2, routesPath); err == nil {
		t.Fatal("expected duplicate role error")
	}
}

func TestDefaultParentRole_TableCoverage(t *testing.T) {
	_ = defaultParentRole("opco-ceo")
	_ = defaultParentRole("vp-product")
	_ = defaultParentRole("backend-agent")
	_ = defaultParentRole("unknown")
}

