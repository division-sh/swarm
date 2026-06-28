package apispec

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestPlatformSpecStaticAnalyzerSpecBasisRefsResolve(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	staticAnalyzer := mustMappingValue(t, root, "static_analyzer")

	missing := collectMissingSpecBasisRefs(root, staticAnalyzer, []string{"static_analyzer"})
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Fatalf("static_analyzer spec_basis refs must resolve to parsed platform-spec.yaml owners:\n%s", strings.Join(missing, "\n"))
	}
}

func TestPlatformSpecHandlerSpecificationHierarchy(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	handlerSpec := mustMappingValue(t, root, "handler_specification")
	handlerFields := mustMappingValue(t, handlerSpec, "handler_fields")
	expressionContext := mustMappingValue(t, handlerSpec, "expression_context")

	expectedHandlerFields := []string{
		"description",
		"description_field",
		"create_entity",
		"select_entity",
		"select_or_create_entity",
		"guard",
		"accumulate",
		"compute",
		"on_complete",
		"advances_to",
		"sets_gate",
		"data_accumulation",
		"emit",
		"rules",
		"fan_out",
		"query",
		"reduce",
		"filter",
		"count",
		"clear",
		"action",
		"retired_fields",
		"clear_gates",
		"evidence_target",
	}
	for _, key := range expectedHandlerFields {
		if !hasMappingKey(handlerFields, key) {
			t.Fatalf("handler_specification.handler_fields.%s missing", key)
		}
		if key != "description" && hasMappingKey(expressionContext, key) {
			t.Fatalf("handler field %s is still parsed under handler_specification.expression_context", key)
		}
	}

	expectedExpressionContext := map[string]bool{
		"description":     true,
		"namespaces":      true,
		"boot_validation": true,
	}
	for key := range expectedExpressionContext {
		if !hasMappingKey(expressionContext, key) {
			t.Fatalf("handler_specification.expression_context.%s missing", key)
		}
	}
	for _, key := range mappingKeys(expressionContext) {
		if !expectedExpressionContext[key] {
			t.Fatalf("handler_specification.expression_context.%s is not an expression-context owner", key)
		}
	}
}

func collectMissingSpecBasisRefs(root, node *yaml.Node, path []string) []string {
	if node == nil {
		return nil
	}
	var missing []string
	switch node.Kind {
	case yaml.MappingNode:
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := node.Content[i].Value
			value := node.Content[i+1]
			nextPath := append(append([]string{}, path...), key)
			if key == "spec_basis" {
				missing = append(missing, unresolvedSpecBasisRefs(root, value, nextPath)...)
				continue
			}
			missing = append(missing, collectMissingSpecBasisRefs(root, value, nextPath)...)
		}
	case yaml.SequenceNode:
		for i, child := range node.Content {
			nextPath := append(append([]string{}, path...), fmt.Sprintf("[%d]", i))
			missing = append(missing, collectMissingSpecBasisRefs(root, child, nextPath)...)
		}
	}
	return missing
}

func unresolvedSpecBasisRefs(root, node *yaml.Node, path []string) []string {
	if node == nil || node.Kind != yaml.SequenceNode {
		return []string{fmt.Sprintf("%s is kind %v, want sequence", strings.Join(path, "."), nodeKind(node))}
	}
	var missing []string
	for i, item := range node.Content {
		if item.Kind != yaml.ScalarNode {
			missing = append(missing, fmt.Sprintf("%s[%d] is kind %v, want scalar", strings.Join(path, "."), i, nodeKind(item)))
			continue
		}
		ref := scalarValue(item)
		if ref == "" {
			missing = append(missing, fmt.Sprintf("%s[%d] is empty", strings.Join(path, "."), i))
			continue
		}
		if !yamlPathExists(root, ref) {
			missing = append(missing, fmt.Sprintf("%s[%d] -> %s", strings.Join(path, "."), i, ref))
		}
	}
	return missing
}

func yamlPathExists(root *yaml.Node, path string) bool {
	current := root
	for _, key := range strings.Split(path, ".") {
		current = mappingValue(current, key)
		if current == nil {
			return false
		}
	}
	return true
}

func mappingKeys(node *yaml.Node) []string {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	keys := make([]string, 0, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		keys = append(keys, node.Content[i].Value)
	}
	sort.Strings(keys)
	return keys
}
