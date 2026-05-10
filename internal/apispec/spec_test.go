package apispec

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestPlatformAPISpecValidationCoverage(t *testing.T) {
	api := loadRepoAPISpec(t)
	report, err := Validate(api)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if report.MethodCount != 36 {
		t.Fatalf("method count = %d, want 36", report.MethodCount)
	}
	if report.SchemaCount != 49 {
		t.Fatalf("schema count = %d, want 49", report.SchemaCount)
	}
	if report.ErrorCodeCount != 19 {
		t.Fatalf("error code count = %d, want 19", report.ErrorCodeCount)
	}
	if report.MutatingMethodCount != 12 {
		t.Fatalf("mutating method count = %d, want 12", report.MutatingMethodCount)
	}
	if report.SubscriptionMethodCnt != 3 {
		t.Fatalf("subscription method count = %d, want 3", report.SubscriptionMethodCnt)
	}
	if _, ok := api.MethodCatalog["rpc.unsubscribe"]; !ok {
		t.Fatal("rpc.unsubscribe missing from method catalog")
	}
	if _, ok := api.MethodCatalog["description"]; ok {
		t.Fatal("method_catalog.description must not be a generated method")
	}
	if _, ok := api.Components.Errors["description"]; ok {
		t.Fatal("components.errors.description must not be a concrete error code")
	}
}

func TestGeneratedOpenRPCArtifactMatchesPlatformSpec(t *testing.T) {
	api := loadRepoAPISpec(t)
	generated, err := GenerateOpenRPC(api)
	if err != nil {
		t.Fatalf("GenerateOpenRPC() error = %v", err)
	}
	artifactPath := filepath.Join(repoRoot(t), "docs", "specs", "swarm-platform", "platform", "contracts", "openrpc.json")
	artifact, err := os.ReadFile(artifactPath)
	if err != nil {
		t.Fatalf("read openrpc artifact: %v", err)
	}
	if !EqualJSON(artifact, generated) {
		t.Fatalf("openrpc artifact drifted from platform-spec.yaml; run go run ./cmd/swarm-openrpc-gen")
	}

	var doc OpenRPCDocument
	if err := json.Unmarshal(artifact, &doc); err != nil {
		t.Fatalf("unmarshal openrpc artifact: %v", err)
	}
	if len(doc.Methods) != 36 {
		t.Fatalf("generated OpenRPC methods = %d, want 36", len(doc.Methods))
	}
	if len(doc.Components.Schemas) != 49 {
		t.Fatalf("generated OpenRPC schemas = %d, want 49", len(doc.Components.Schemas))
	}
	if len(doc.Components.Errors) != 19 {
		t.Fatalf("generated OpenRPC errors = %d, want 19", len(doc.Components.Errors))
	}
}

func TestMutatingMethodsDeclareIdempotencyKey(t *testing.T) {
	api := loadRepoAPISpec(t)
	for _, methodName := range api.Conventions.Idempotency.MutatingMethods {
		method, ok := api.MethodCatalog[methodName]
		if !ok {
			t.Fatalf("mutating method %s missing from catalog", methodName)
		}
		if !methodHasParam(method, "idempotency_key") {
			t.Fatalf("mutating method %s missing idempotency_key", methodName)
		}
	}
}

func TestEventListSubscribeFilterParity(t *testing.T) {
	api := loadRepoAPISpec(t)
	listRef, listOK := paramSchemaRef(api.MethodCatalog["event.list"], "filter")
	subscribeRef, subscribeOK := paramSchemaRef(api.MethodCatalog["event.subscribe"], "filter")
	if !listOK || !subscribeOK {
		t.Fatalf("event list/subscribe filter params must both exist")
	}
	if listRef != subscribeRef {
		t.Fatalf("event.subscribe filter ref = %q, want event.list filter ref %q", subscribeRef, listRef)
	}
}

func TestContentDescriptorsDeclareRequiredFlag(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	api := mustMappingValue(t, root, "api_specification")
	methodCatalog := mustMappingValue(t, api, "method_catalog")
	for i := 0; i+1 < len(methodCatalog.Content); i += 2 {
		methodName := methodCatalog.Content[i].Value
		method := methodCatalog.Content[i+1]
		params := mappingValue(method, "params")
		if params != nil {
			if params.Kind != yaml.SequenceNode {
				t.Fatalf("%s params kind = %v, want sequence", methodName, params.Kind)
			}
			for idx, param := range params.Content {
				if !hasMappingKey(param, "required") {
					t.Fatalf("%s params[%d] missing required flag", methodName, idx)
				}
			}
		}
		result := mappingValue(method, "result")
		if result != nil && !hasMappingKey(result, "required") {
			t.Fatalf("%s result missing required flag", methodName)
		}
	}
}

func loadRepoAPISpec(t *testing.T) *APISpecification {
	t.Helper()
	api, err := LoadPlatformSpec(filepath.Join(repoRoot(t), "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml"))
	if err != nil {
		t.Fatalf("LoadPlatformSpec() error = %v", err)
	}
	return api
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("repo root with go.mod not found")
		}
		dir = parent
	}
}

func loadPlatformSpecYAMLNode(t *testing.T) *yaml.Node {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml"))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse platform spec yaml: %v", err)
	}
	if len(doc.Content) != 1 {
		t.Fatalf("platform spec yaml document content count = %d, want 1", len(doc.Content))
	}
	return doc.Content[0]
}

func mustMappingValue(t *testing.T, node *yaml.Node, key string) *yaml.Node {
	t.Helper()
	value := mappingValue(node, key)
	if value == nil {
		t.Fatalf("mapping key %q not found", key)
	}
	return value
}

func mappingValue(node *yaml.Node, key string) *yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1]
		}
	}
	return nil
}

func hasMappingKey(node *yaml.Node, key string) bool {
	return mappingValue(node, key) != nil
}
