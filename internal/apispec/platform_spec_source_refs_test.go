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

func TestPlatformEventsCatalogSchemaAuthorityRefsResolve(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	catalog := mustMappingValue(t, mustMappingValue(t, root, "platform_events"), "catalog")

	const prefix = "platform-spec.yaml#"
	expectedSimpleRefs := map[string]string{
		"platform.dead_letter": "platform-spec.yaml#engine.error_model.dead_letter_schema",
		"platform.runtime_log": "platform-spec.yaml#platform_tables.diagnostics_encoding",
		"mailbox.item_decided": "platform-spec.yaml#api_specification.conventions.mailbox.approval_event_payload_contract",
	}

	var checked []string
	for i := 0; i+1 < len(catalog.Content); i += 2 {
		eventName := catalog.Content[i].Value
		entry := catalog.Content[i+1]
		ref := scalarValue(mappingValue(entry, "schema_authority"))
		if ref == "" {
			t.Fatalf("%s missing schema_authority", eventName)
		}
		if !strings.HasPrefix(ref, prefix) {
			t.Fatalf("%s schema_authority = %q, want %s-prefixed ref", eventName, ref, prefix)
		}
		if !yamlPathExists(root, strings.TrimPrefix(ref, prefix)) {
			t.Fatalf("%s schema_authority does not resolve to parsed platform-spec.yaml owner: %s", eventName, ref)
		}
		if expected, ok := expectedSimpleRefs[eventName]; ok && ref != expected {
			t.Fatalf("%s schema_authority = %q, want existing simple owner guard %q", eventName, ref, expected)
		}
		checked = append(checked, eventName)
	}
	if len(checked) != len(mappingKeys(catalog)) {
		t.Fatalf("checked %d schema_authority refs, catalog has %d entries", len(checked), len(mappingKeys(catalog)))
	}
	if len(checked) < 18 {
		t.Fatalf("checked %d schema_authority refs, want at least the 18 refs present when #1518 was gated", len(checked))
	}
}

func TestPlatformSpecRefPathLiteralSegments(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)

	for _, tc := range []struct {
		name string
		path string
		want bool
	}{
		{
			name: "simple dot path",
			path: "engine.error_model.dead_letter_schema",
			want: true,
		},
		{
			name: "bracketed dotted event key",
			path: `platform_events.catalog["platform.boot"].payload`,
			want: true,
		},
		{
			name: "old unbracketed dotted event key remains invalid",
			path: "platform_events.catalog.platform.boot.payload",
			want: false,
		},
		{
			name: "missing bracket quotes fail closed",
			path: "platform_events.catalog[platform.boot].payload",
			want: false,
		},
		{
			name: "unterminated bracket fails closed",
			path: `platform_events.catalog["platform.boot".payload`,
			want: false,
		},
		{
			name: "empty segment fails closed",
			path: "platform_events.catalog..payload",
			want: false,
		},
		{
			name: "empty literal segment fails closed",
			path: `platform_events.catalog[""].payload`,
			want: false,
		},
		{
			name: "missing dot after bracketed literal fails closed",
			path: `platform_events.catalog["platform.boot"]payload`,
			want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := yamlPathExists(root, tc.path); got != tc.want {
				t.Fatalf("yamlPathExists(%q) = %v, want %v", tc.path, got, tc.want)
			}
		})
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
	segments, ok := parseYAMLPathSegments(path)
	if !ok {
		return false
	}
	current := root
	for _, key := range segments {
		current = mappingValue(current, key)
		if current == nil {
			return false
		}
	}
	return true
}

func parseYAMLPathSegments(path string) ([]string, bool) {
	if path == "" {
		return nil, false
	}
	var segments []string
	expectSegment := true
	for i := 0; i < len(path); {
		switch path[i] {
		case '.':
			if expectSegment {
				return nil, false
			}
			expectSegment = true
			i++
			if i == len(path) {
				return nil, false
			}
		case '[':
			segment, next, ok := parseBracketedLiteralSegment(path, i)
			if !ok {
				return nil, false
			}
			segments = append(segments, segment)
			i = next
			expectSegment = false
			if i < len(path) && path[i] != '.' && path[i] != '[' {
				return nil, false
			}
		default:
			start := i
			for i < len(path) && path[i] != '.' && path[i] != '[' && path[i] != ']' {
				i++
			}
			if start == i {
				return nil, false
			}
			segments = append(segments, path[start:i])
			expectSegment = false
		}
	}
	if expectSegment {
		return nil, false
	}
	return segments, true
}

func parseBracketedLiteralSegment(path string, start int) (string, int, bool) {
	if start+3 >= len(path) || path[start] != '[' || path[start+1] != '"' {
		return "", 0, false
	}
	var b strings.Builder
	for i := start + 2; i < len(path); {
		switch path[i] {
		case '\\':
			if i+1 >= len(path) {
				return "", 0, false
			}
			next := path[i+1]
			if next != '\\' && next != '"' {
				return "", 0, false
			}
			b.WriteByte(next)
			i += 2
		case '"':
			if i+1 >= len(path) || path[i+1] != ']' || b.Len() == 0 {
				return "", 0, false
			}
			return b.String(), i + 2, true
		case '[':
			return "", 0, false
		case ']':
			return "", 0, false
		default:
			b.WriteByte(path[i])
			i++
		}
	}
	return "", 0, false
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
