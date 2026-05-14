package apispec

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestPlatformAPISpecValidationCoverage(t *testing.T) {
	api := loadRepoAPISpec(t)
	report, err := Validate(api)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if report.MethodCount != 40 {
		t.Fatalf("method count = %d, want 40", report.MethodCount)
	}
	if report.SchemaCount != 57 {
		t.Fatalf("schema count = %d, want 57", report.SchemaCount)
	}
	if report.ErrorCodeCount != 27 {
		t.Fatalf("error code count = %d, want 27", report.ErrorCodeCount)
	}
	if report.MutatingMethodCount != 14 {
		t.Fatalf("mutating method count = %d, want 14", report.MutatingMethodCount)
	}
	if report.SubscriptionMethodCnt != 4 {
		t.Fatalf("subscription method count = %d, want 4", report.SubscriptionMethodCnt)
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
	if len(doc.Methods) != 40 {
		t.Fatalf("generated OpenRPC methods = %d, want 40", len(doc.Methods))
	}
	if len(doc.Components.Schemas) != 57 {
		t.Fatalf("generated OpenRPC schemas = %d, want 57", len(doc.Components.Schemas))
	}
	if len(doc.Components.Errors) != 27 {
		t.Fatalf("generated OpenRPC errors = %d, want 27", len(doc.Components.Errors))
	}
	methods := map[string]OpenRPCMethod{}
	for _, method := range doc.Methods {
		methods[method.Name] = method
	}
	if _, ok := methods["event.publish"]; !ok {
		t.Fatal("generated OpenRPC missing event.publish")
	}
	if _, ok := methods["event.replay"]; !ok {
		t.Fatal("generated OpenRPC missing event.replay")
	}
	if _, ok := methods["runtime.subscribe_logs"]; !ok {
		t.Fatal("generated OpenRPC missing runtime.subscribe_logs")
	}
	if !methods["run.start"].Deprecated {
		t.Fatal("generated OpenRPC run.start deprecated flag = false, want true")
	}
}

func TestGeneratedOpenRPCApplicationErrorCodesAreUnique(t *testing.T) {
	api := loadRepoAPISpec(t)
	generated, err := GenerateOpenRPC(api)
	if err != nil {
		t.Fatalf("GenerateOpenRPC() error = %v", err)
	}
	var doc OpenRPCDocument
	if err := json.Unmarshal(generated, &doc); err != nil {
		t.Fatalf("unmarshal generated openrpc: %v", err)
	}
	componentCodes := make(map[int]string, len(doc.Components.Errors))
	for name, errDef := range doc.Components.Errors {
		if errDef.Code > OpenRPCApplicationErrorCodeStart || errDef.Code < OpenRPCApplicationErrorCodeMinimum {
			t.Fatalf("component error %s numeric code = %d, want %d..%d", name, errDef.Code, OpenRPCApplicationErrorCodeMinimum, OpenRPCApplicationErrorCodeStart)
		}
		if existing, ok := componentCodes[errDef.Code]; ok {
			t.Fatalf("component errors %s and %s share numeric code %d", existing, name, errDef.Code)
		}
		componentCodes[errDef.Code] = name
	}
	if len(componentCodes) != len(api.Components.Errors) {
		t.Fatalf("unique OpenRPC component error codes = %d, want %d", len(componentCodes), len(api.Components.Errors))
	}
	for _, method := range doc.Methods {
		methodCodes := make(map[int]struct{}, len(method.Errors))
		for _, errDef := range method.Errors {
			if _, ok := componentCodes[errDef.Code]; !ok {
				t.Fatalf("method %s references numeric error code %d absent from components.errors", method.Name, errDef.Code)
			}
			if _, ok := methodCodes[errDef.Code]; ok {
				t.Fatalf("method %s has duplicate numeric error code %d", method.Name, errDef.Code)
			}
			methodCodes[errDef.Code] = struct{}{}
		}
	}
}

func TestMutatingMethodsDeclareIdempotencyKey(t *testing.T) {
	api := loadRepoAPISpec(t)
	for _, methodName := range api.Conventions.Idempotency.MutatingMethods {
		method, ok := api.MethodCatalog[methodName]
		if !ok {
			t.Fatalf("mutating method %s missing from catalog", methodName)
		}
		param, ok := methodParam(method, "idempotency_key")
		if !ok {
			t.Fatalf("mutating method %s missing idempotency_key", methodName)
		}
		if param.Required {
			t.Fatalf("mutating method %s idempotency_key required = true, want optional", methodName)
		}
	}
}

func TestValidateRejectsRequiredMutatingIdempotencyKey(t *testing.T) {
	api := loadRepoAPISpec(t)
	const methodName = "run.start"
	method := api.MethodCatalog[methodName]
	for i := range method.Params {
		if method.Params[i].Name == "idempotency_key" {
			method.Params[i].Required = true
		}
	}
	api.MethodCatalog[methodName] = method

	_, err := Validate(api)
	if err == nil {
		t.Fatal("Validate() error = nil, want required idempotency_key rejection")
	}
	if got, want := err.Error(), "mutating method run.start idempotency_key param must be optional"; !strings.Contains(got, want) {
		t.Fatalf("Validate() error = %q, want substring %q", got, want)
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

func TestRuntimeIngressConventionsRatifyQueueAndRejectSemantics(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	apiNode := mustMappingValue(t, root, "api_specification")
	conventions := mustMappingValue(t, apiNode, "conventions")
	ingress := mustMappingValue(t, conventions, "runtime_ingress")

	assertScalarValue(t, mappingValue(ingress, "state_storage"), "runtime_ingress_state")
	assertScalarContains(t, mappingValue(ingress, "owner"), "canonical runtime ingress controller")
	assertScalarContains(t, mappingValue(ingress, "owner"), "Low-level runtimebus flags")
	assertSurfaceListed(t, ingress, "queueable_ingress", "inbound.webhook")
	assertSurfaceListed(t, ingress, "queueable_ingress", "api.event_producing_methods")
	assertSurfaceListed(t, ingress, "queueable_ingress", "timer.fire")
	assertSurfaceListed(t, ingress, "non_queueable_ingress", "mcp.tool_call")
	assertSurfaceListed(t, ingress, "non_queueable_ingress", "mcp.json_rpc_call")
	assertSurfaceListed(t, ingress, "non_queueable_ingress", "read_only_operator_api")
	assertScalarContains(t, mappingValue(ingress, "queued_not_dispatched"), "events table")
	assertScalarContains(t, mappingValue(ingress, "queued_not_dispatched"), "runtime_ingress_state.status")
	assertScalarContains(t, mappingValue(ingress, "queued_not_dispatched"), "must not transition to in_progress")
	assertScalarContains(t, mappingValue(ingress, "resume_release"), "exactly once")

	transitions := mustMappingValue(t, ingress, "transitions")
	pause := mustMappingValue(t, transitions, "pause")
	assertScalarValue(t, mappingValue(pause, "method"), "runtime.pause")
	assertScalarValue(t, mappingValue(pause, "from"), "running")
	assertScalarValue(t, mappingValue(pause, "to"), "paused")
	assertScalarValue(t, mappingValue(pause, "already_in_state_error"), "RUNTIME_ALREADY_PAUSED")
	assertScalarValue(t, mappingValue(pause, "emits"), "platform.paused")
	assertScalarContains(t, mappingValue(pause, "idempotency"), "replay with the same request body returns the original success response")

	resume := mustMappingValue(t, transitions, "resume")
	assertScalarValue(t, mappingValue(resume, "method"), "runtime.resume")
	assertScalarValue(t, mappingValue(resume, "from"), "paused")
	assertScalarValue(t, mappingValue(resume, "to"), "running")
	assertScalarValue(t, mappingValue(resume, "already_in_state_error"), "RUNTIME_NOT_PAUSED")
	assertScalarValue(t, mappingValue(resume, "emits"), "platform.resumed")
	assertScalarContains(t, mappingValue(resume, "idempotency"), "replay with the same request body returns the original success response")

	consumers := mustMappingValue(t, ingress, "consumers")
	for _, consumer := range []string{
		"v1_runtime_methods",
		"dashboard_runtime_actions",
		"inbound_webhook",
		"mcp_tool_gateway",
		"auth_breaker",
		"reset",
	} {
		if mappingValue(consumers, consumer) == nil {
			t.Fatalf("runtime_ingress.consumers missing %s", consumer)
		}
	}

	tables := mustMappingValue(t, mustMappingValue(t, root, "platform_tables"), "tables")
	runtimeIngressState := mustMappingValue(t, tables, "runtime_ingress_state")
	assertScalarContains(t, mappingValue(runtimeIngressState, "ddl"), "CHECK (status IN ('running', 'paused'))")

	events := mustMappingValue(t, mustMappingValue(t, root, "platform_events"), "catalog")
	paused := mustMappingValue(t, events, "platform.paused")
	resumed := mustMappingValue(t, events, "platform.resumed")
	assertScalarContains(t, mappingValue(paused, "description"), "Queueable event-producing ingress is accepted")
	assertScalarContains(t, mappingValue(paused, "description"), "MCP/tool execution is rejected fail-closed")
	assertScalarContains(t, mappingValue(resumed, "description"), "exactly once")

	api := loadRepoAPISpec(t)
	assertMethodDescriptionContains(t, api, "runtime.pause", "Queueable event-producing ingress is persisted")
	assertMethodDescriptionContains(t, api, "runtime.pause", "MCP/tool request-response ingress fails closed")
	assertMethodDescriptionContains(t, api, "runtime.resume", "exactly once")
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

func scalarValue(node *yaml.Node) string {
	if node == nil {
		return ""
	}
	return strings.TrimSpace(node.Value)
}

func assertScalarValue(t *testing.T, node *yaml.Node, want string) {
	t.Helper()
	if got := scalarValue(node); got != want {
		t.Fatalf("scalar value = %q, want %q", got, want)
	}
}

func assertScalarContains(t *testing.T, node *yaml.Node, want string) {
	t.Helper()
	if got := scalarValue(node); !strings.Contains(got, want) {
		t.Fatalf("scalar value = %q, want substring %q", got, want)
	}
}

func assertMethodDescriptionContains(t *testing.T, api *APISpecification, methodName, want string) {
	t.Helper()
	method, ok := api.MethodCatalog[methodName]
	if !ok {
		t.Fatalf("method %s missing from catalog", methodName)
	}
	if !strings.Contains(method.Description, want) {
		t.Fatalf("method %s description = %q, want substring %q", methodName, method.Description, want)
	}
}

func assertSurfaceListed(t *testing.T, ingress *yaml.Node, listName, surface string) {
	t.Helper()
	list := mustMappingValue(t, ingress, listName)
	if list.Kind != yaml.SequenceNode {
		t.Fatalf("runtime_ingress.%s kind = %v, want sequence", listName, list.Kind)
	}
	for _, entry := range list.Content {
		if scalarValue(mappingValue(entry, "surface")) == surface {
			return
		}
	}
	t.Fatalf("runtime_ingress.%s missing surface %s", listName, surface)
}
