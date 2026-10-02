package apispec

import (
	"fmt"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

const apiAdmissionFixture = `api_specification:
  description: note
  components: {schemas: {Sample: {type: object}}, errors: {bad: note}, error_catalog_metadata: {description: note}}
  method_catalog_metadata: {description: note}
  examples_policy: {status: deferred, owner: policy, applies_to: all, openrpc_method_examples: omitted, runtime_probe_fixtures: fixtures, reason: note, requirements: [explicit], future_source_model_required: false}
  service_discovery_policy: {status: deferred, owner: policy, applies_to: all, rpc_discover: omitted, publication_artifact: openrpc, runtime_behavior: reject, reason: note, requirements: [explicit]}
  method_catalog:
    sample:
      tier: control
      description: note
      deprecated: false
      scope: {required: [read]}
      params: [{name: id, required: false, description: note, schema: {$ref: '#/components/schemas/Sample', default: null, example: false, oneOf: [{type: 'null'}, {type: object}]}}]
      result: {name: result, required: true, schema: {type: object}}
      idempotency: {key: null}
      notification_schema: {type: object}
      errors: [bad]
  conventions:
    idempotency: {mutating_methods: [sample]}
    scopes: {catalog: [read]}
    mailbox: {status_storage_model: explicit, decision_event_routes: [{item_type: approval, terminal_event_name: approved, deferred_event_name: deferred}]}
`

func admitAPIText(body []byte) (*APISpecification, error) {
	snapshot, err := yamlsource.Load(body)
	if err != nil {
		return nil, err
	}
	return AdmitPlatformAPIValue(snapshot.Document("platform-spec.yaml").Root())
}

func TestAPISpecAdmissionPresenceMatrix(t *testing.T) {
	rows := []struct {
		path                                              string
		optional, emptyText, emptyMap, emptyList, boolean bool
	}{
		{"api_specification", false, false, false, false, false},
		{"api_specification.description", true, true, false, false, false},
		{"api_specification.components", true, false, true, false, false},
		{"api_specification.components.schemas", true, false, true, false, false},
		{"api_specification.components.errors", true, false, true, false, false},
		{"api_specification.components.error_catalog_metadata", true, false, true, false, false},
		{"api_specification.method_catalog_metadata", true, false, true, false, false},
		{"api_specification.examples_policy", true, false, true, false, false},
		{"api_specification.examples_policy.reason", true, true, false, false, false},
		{"api_specification.examples_policy.requirements", true, false, false, true, false},
		{"api_specification.examples_policy.future_source_model_required", true, false, false, false, true},
		{"api_specification.service_discovery_policy", true, false, true, false, false},
		{"api_specification.service_discovery_policy.reason", true, true, false, false, false},
		{"api_specification.service_discovery_policy.requirements", true, false, false, true, false},
		{"api_specification.method_catalog", false, false, false, false, false},
		{"api_specification.method_catalog.sample", false, false, true, false, false},
		{"api_specification.method_catalog.sample.tier", true, false, false, false, false},
		{"api_specification.method_catalog.sample.description", true, true, false, false, false},
		{"api_specification.method_catalog.sample.deprecated", true, false, false, false, true},
		{"api_specification.method_catalog.sample.scope", true, false, true, false, false},
		{"api_specification.method_catalog.sample.scope.required", true, false, false, true, false},
		{"api_specification.method_catalog.sample.params", true, false, false, true, false},
		{"api_specification.method_catalog.sample.params.0.name", false, false, false, false, false},
		{"api_specification.method_catalog.sample.params.0.required", false, false, false, false, true},
		{"api_specification.method_catalog.sample.params.0.description", true, true, false, false, false},
		{"api_specification.method_catalog.sample.params.0.schema", false, false, true, false, false},
		{"api_specification.method_catalog.sample.result", true, false, false, false, false},
		{"api_specification.method_catalog.sample.errors", true, false, false, true, false},
		{"api_specification.conventions", true, false, true, false, false},
		{"api_specification.conventions.idempotency", true, false, true, false, false},
		{"api_specification.conventions.idempotency.mutating_methods", true, false, false, true, false},
		{"api_specification.conventions.scopes.catalog", true, false, false, true, false},
		{"api_specification.conventions.mailbox", true, false, true, false, false},
		{"api_specification.conventions.mailbox.status_storage_model", true, false, false, false, false},
		{"api_specification.conventions.mailbox.decision_event_routes", true, false, false, true, false},
	}
	for _, group := range []string{"examples_policy", "service_discovery_policy"} {
		fields := []string{"status", "owner", "applies_to"}
		if group == "examples_policy" {
			fields = append(fields, "openrpc_method_examples", "runtime_probe_fixtures")
		} else {
			fields = append(fields, "rpc_discover", "publication_artifact", "runtime_behavior")
		}
		for _, field := range fields {
			rows = append(rows, struct {
				path                                              string
				optional, emptyText, emptyMap, emptyList, boolean bool
			}{"api_specification." + group + "." + field, true, false, false, false, false})
		}
	}
	for _, field := range []string{"item_type", "terminal_event_name", "deferred_event_name"} {
		rows = append(rows, struct {
			path                                              string
			optional, emptyText, emptyMap, emptyList, boolean bool
		}{"api_specification.conventions.mailbox.decision_event_routes.0." + field, false, false, false, false, false})
	}
	for _, row := range rows {
		for _, state := range []string{"missing", "null", "empty_text", "empty_list", "empty_map", "wrong_kind", "valid", "merge"} {
			t.Run(row.path+"/"+state, func(t *testing.T) {
				body := apiPresenceBody(t, row.path, state)
				_, err := admitAPIText(body)
				want := state == "valid" || state == "merge" || state == "missing" && row.optional || state == "empty_text" && row.emptyText || state == "empty_map" && row.emptyMap || state == "empty_list" && row.emptyList || state == "wrong_kind" && row.boolean
				if (err == nil) != want {
					t.Fatalf("want admission=%t, got %v", want, err)
				}
			})
		}
	}
}

func TestAPISpecAdmissionPreservesExtendedLiteralSchemaAndSource(t *testing.T) {
	api, err := admitAPIText([]byte(apiAdmissionFixture))
	if err != nil {
		t.Fatal(err)
	}
	descriptor := api.MethodCatalog["sample"].Params[0]
	schema := descriptor.Schema.(map[string]any)
	if descriptor.Required || schema["default"] != nil || schema["example"] != false || schema["$ref"] == nil || len(schema["oneOf"].([]any)) != 2 {
		t.Fatalf("API schema dialect changed: %#v", descriptor)
	}
	if api.ExamplesPolicy.FutureSourceModelRequired || api.MethodCatalog["sample"].Deprecated {
		t.Fatal("false was lost")
	}
	var projected map[string]any
	if err := api.SourceValue().Project(&projected); err != nil {
		t.Fatal(err)
	}
	delete(projected, "method_catalog")
	var fresh map[string]any
	if err := api.SourceValue().Project(&fresh); err != nil {
		t.Fatal(err)
	}
	if len(fresh["method_catalog"].(map[string]any)) != 1 {
		t.Fatal("source mutated")
	}
	for _, body := range []string{
		"api_specification: {method_catalog: {sample: {params: [{name: id, required: false, schema: {properties: {x: {type: text, type: integer}}}}]}}}",
		"api_specification: {method_catalog: {sample: {idempotency: {key: a, key: b}}}}",
	} {
		if _, err := admitAPIText([]byte(body)); err == nil || !strings.Contains(err.Error(), "platform-spec.yaml:") {
			t.Fatalf("literal duplicate escaped source admission: %v", err)
		}
	}
}

func apiPresenceBody(t testing.TB, path, state string) []byte {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(apiAdmissionFixture), &doc); err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(path, ".")
	parent := doc.Content[0]
	for _, name := range parts[:len(parts)-1] {
		if parent.Kind == yaml.SequenceNode {
			parent = parent.Content[0]
			continue
		}
		var next *yaml.Node
		for i := 0; i < len(parent.Content); i += 2 {
			if parent.Content[i].Value == name {
				next = parent.Content[i+1]
				break
			}
		}
		if next == nil {
			t.Fatalf("missing fixture path %s", path)
		}
		parent = next
	}
	index := -1
	for i := 0; i < len(parent.Content); i += 2 {
		if parent.Content[i].Value == parts[len(parts)-1] {
			index = i
			break
		}
	}
	if index < 0 {
		t.Fatalf("missing fixture field %s", path)
	}
	switch state {
	case "missing":
		parent.Content = append(parent.Content[:index], parent.Content[index+2:]...)
	case "merge":
		key, value := parent.Content[index], parent.Content[index+1]
		parent.Content = append(parent.Content[:index], parent.Content[index+2:]...)
		parent.Content = append(parent.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!merge", Value: "<<"}, &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Content: []*yaml.Node{key, value}})
	case "valid":
	default:
		var replacement yaml.Node
		if err := yaml.Unmarshal([]byte(map[string]string{"null": "null", "empty_text": "''", "empty_list": "[]", "empty_map": "{}", "wrong_kind": "true"}[state]), &replacement); err != nil {
			t.Fatal(err)
		}
		parent.Content[index+1] = replacement.Content[0]
	}
	result, err := yaml.Marshal(&doc)
	if err != nil {
		t.Fatal(fmt.Errorf("marshal presence witness: %w", err))
	}
	return result
}
