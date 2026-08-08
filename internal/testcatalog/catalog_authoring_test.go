package testcatalog

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

func TestCatalogFixturesUseCanonicalCreateFlowInstanceAuthoring(t *testing.T) {
	repoRoot := catalogRepoRoot(t)
	err := filepath.WalkDir(filepath.Join(repoRoot, "tests"), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Base(path) != "nodes.yaml" {
			return nil
		}
		relativePath, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		t.Run(filepath.ToSlash(relativePath), func(t *testing.T) {
			source, err := yamlsource.LoadFile(path)
			if err != nil {
				t.Fatalf("load canonical YAML: %v", err)
			}
			root := source.NodeCopy()
			assertCanonicalCreateFlowInstanceAuthoring(t, path, &root)
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk catalog nodes.yaml: %v", err)
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
	for index := 0; index+1 < len(node.Content); index += 2 {
		key := strings.TrimSpace(node.Content[index].Value)
		value := node.Content[index+1]
		if key != "event_handlers" || value.Kind != yaml.MappingNode {
			assertCanonicalCreateFlowInstanceAuthoring(t, path, value)
			continue
		}
		for handlerIndex := 0; handlerIndex+1 < len(value.Content); handlerIndex += 2 {
			assertCanonicalCreateFlowInstanceHandler(t, path, strings.TrimSpace(value.Content[handlerIndex].Value), value.Content[handlerIndex+1])
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
	for index := 0; index+1 < len(node.Content); index += 2 {
		key := strings.TrimSpace(node.Content[index].Value)
		value := node.Content[index+1]
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
		for index := 0; index+1 < len(actionNode.Content); index += 2 {
			if strings.TrimSpace(actionNode.Content[index].Value) == "type" && strings.TrimSpace(actionNode.Content[index+1].Value) == "create_flow_instance" {
				t.Fatalf("%s handler %q still uses legacy mapping-shaped create_flow_instance authoring", path, handlerKey)
			}
		}
	}
}

func assertCanonicalConfigFromBindings(t *testing.T, path, handlerKey string, node *yaml.Node) {
	t.Helper()
	if node.Kind != yaml.MappingNode {
		return
	}
	for index := 0; index+1 < len(node.Content); index += 2 {
		raw := strings.TrimSpace(node.Content[index+1].Value)
		if strings.Contains(raw, "{{") {
			t.Fatalf("%s handler %q uses templated config_from binding %q instead of canonical payload-path binding", path, handlerKey, raw)
		}
	}
}
