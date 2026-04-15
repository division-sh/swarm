package swarmflowtest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestValidateCatalogExpectedDocument_AllowsUnsupportedNonExecutableExpectations(t *testing.T) {
	var expected catalogExpectedDocument
	expected.Trigger.Event = "spawn.requested"
	expected.Expected.HandlerOutcome = "success"
	expected.Expected.FlowInstanceCreated = map[string]any{
		"template":    "worker",
		"instance_id": "w-001",
	}

	err := validateCatalogExpectedDocument("tier5-flow-lifecycle/test-create-flow-instance", expected)
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	if catalogCaseExecutableNowForDir("tier5-flow-lifecycle/test-create-flow-instance", expected) {
		t.Fatal("expected unsupported expectation case to be treated as non-executable")
	}
}

func TestValidateCatalogExpectedDocument_RuntimeOnlyCaseIsNonExecutable(t *testing.T) {
	var expected catalogExpectedDocument
	expected.Trigger.Event = "task.started"
	expected.Expected.RuntimeOnly = true
	expected.Expected.HandlerOutcome = "kill"
	expected.Expected.ChainDepthExceeded = true

	err := validateCatalogExpectedDocument("tier6-event-loop/test-chain-depth-limit", expected)
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
	if catalogCaseExecutableNowForDir("tier6-event-loop/test-chain-depth-limit", expected) {
		t.Fatal("expected runtime-only case to be treated as non-executable")
	}
	if catalogCaseSimpleHarnessEligible(expected) {
		t.Fatal("expected runtime-only case to be ineligible for the simple harness")
	}
}

func TestCatalogFixtures_UseCanonicalCreateFlowInstanceAuthoring(t *testing.T) {
	repoRoot := repoRootForTest(t)
	var matches []string
	err := filepath.WalkDir(filepath.Join(repoRoot, "tests"), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if filepath.Base(path) != "nodes.yaml" {
			return nil
		}
		matches = append(matches, path)
		return nil
	})
	if err != nil {
		t.Fatalf("walk catalog nodes.yaml: %v", err)
	}
	for _, path := range matches {
		t.Run(strings.TrimPrefix(filepath.ToSlash(path), filepath.ToSlash(filepath.Join(repoRoot, "tests"))+"/"), func(t *testing.T) {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			var root yaml.Node
			if err := yaml.Unmarshal(raw, &root); err != nil {
				t.Fatalf("parse %s: %v", path, err)
			}
			assertCanonicalCreateFlowInstanceAuthoring(t, path, &root)
		})
	}
}

func assertCanonicalCreateFlowInstanceAuthoring(t *testing.T, path string, node *yaml.Node) {
	t.Helper()
	if node == nil {
		return
	}
	if node.Kind == yaml.DocumentNode && len(node.Content) > 0 {
		assertCanonicalCreateFlowInstanceAuthoring(t, path, node.Content[0])
		return
	}
	if node.Kind != yaml.MappingNode {
		return
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		if key != "event_handlers" || value.Kind != yaml.MappingNode {
			assertCanonicalCreateFlowInstanceAuthoring(t, path, value)
			continue
		}
		for j := 0; j+1 < len(value.Content); j += 2 {
			handlerKey := strings.TrimSpace(value.Content[j].Value)
			handler := value.Content[j+1]
			assertCanonicalCreateFlowInstanceHandler(t, path, handlerKey, handler)
		}
	}
}

func assertCanonicalCreateFlowInstanceHandler(t *testing.T, path, handlerKey string, node *yaml.Node) {
	t.Helper()
	if node == nil || node.Kind != yaml.MappingNode {
		return
	}
	var actionNode *yaml.Node
	var template string
	var instanceIDFrom string
	var configFrom *yaml.Node
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := strings.TrimSpace(node.Content[i].Value)
		value := node.Content[i+1]
		switch key {
		case "action":
			actionNode = value
		case "template":
			template = strings.TrimSpace(value.Value)
		case "instance_id_from":
			instanceIDFrom = strings.TrimSpace(value.Value)
		case "config_from":
			configFrom = value
		}
	}
	if actionNode == nil {
		return
	}
	switch actionNode.Kind {
	case yaml.ScalarNode:
		if strings.TrimSpace(actionNode.Value) != "create_flow_instance" {
			return
		}
		if template == "" {
			t.Fatalf("%s handler %q uses create_flow_instance without canonical template field", path, handlerKey)
		}
		if instanceIDFrom == "" {
			t.Fatalf("%s handler %q uses create_flow_instance without canonical instance_id_from field", path, handlerKey)
		}
		if configFrom != nil {
			assertCanonicalConfigFromBindings(t, path, handlerKey, configFrom)
		}
	case yaml.MappingNode:
		var legacyType string
		for i := 0; i+1 < len(actionNode.Content); i += 2 {
			if strings.TrimSpace(actionNode.Content[i].Value) != "type" {
				continue
			}
			legacyType = strings.TrimSpace(actionNode.Content[i+1].Value)
			break
		}
		if legacyType == "create_flow_instance" {
			t.Fatalf("%s handler %q still uses legacy mapping-shaped create_flow_instance authoring", path, handlerKey)
		}
	}
}

func assertCanonicalConfigFromBindings(t *testing.T, path, handlerKey string, node *yaml.Node) {
	t.Helper()
	if node.Kind != yaml.MappingNode {
		return
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		raw := strings.TrimSpace(node.Content[i+1].Value)
		if strings.Contains(raw, "{{") {
			t.Fatalf("%s handler %q uses templated config_from binding %q instead of canonical payload-path binding", path, handlerKey, raw)
		}
	}
}
