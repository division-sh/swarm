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
	if report.MethodCount != 43 {
		t.Fatalf("method count = %d, want 43", report.MethodCount)
	}
	if report.SchemaCount != 72 {
		t.Fatalf("schema count = %d, want 72", report.SchemaCount)
	}
	if report.ErrorCodeCount != 28 {
		t.Fatalf("error code count = %d, want 28", report.ErrorCodeCount)
	}
	if report.MutatingMethodCount != 16 {
		t.Fatalf("mutating method count = %d, want 16", report.MutatingMethodCount)
	}
	if report.SubscriptionMethodCnt != 4 {
		t.Fatalf("subscription method count = %d, want 4", report.SubscriptionMethodCnt)
	}
	if _, ok := api.MethodCatalog["rpc.unsubscribe"]; !ok {
		t.Fatal("rpc.unsubscribe missing from method catalog")
	}
	if _, ok := api.MethodCatalog["runtime.nuke"]; !ok {
		t.Fatal("runtime.nuke missing from method catalog")
	}
	if _, ok := api.MethodCatalog["description"]; ok {
		t.Fatal("method_catalog.description must not be a generated method")
	}
	if _, ok := api.Components.Errors["description"]; ok {
		t.Fatal("components.errors.description must not be a concrete error code")
	}
	assertExamplesPolicyDeferred(t, api.ExamplesPolicy)
	assertServiceDiscoveryPolicyNotPublished(t, api.ServiceDiscoveryPolicy)
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
	if len(doc.Methods) != 43 {
		t.Fatalf("generated OpenRPC methods = %d, want 43", len(doc.Methods))
	}
	if len(doc.Components.Schemas) != 72 {
		t.Fatalf("generated OpenRPC schemas = %d, want 72", len(doc.Components.Schemas))
	}
	if len(doc.Components.Errors) != 28 {
		t.Fatalf("generated OpenRPC errors = %d, want 28", len(doc.Components.Errors))
	}
	assertGeneratedMethodsOmitExamplesUnderPolicy(t, api, artifact)
	assertGeneratedMethodsOmitRPCDiscoverUnderPolicy(t, api, doc)
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
	if _, ok := methods["agent.replay"]; !ok {
		t.Fatal("generated OpenRPC missing agent.replay")
	}
	if _, ok := methods["agent.diagnose"]; !ok {
		t.Fatal("generated OpenRPC missing agent.diagnose")
	}
	if _, ok := doc.Components.Schemas["AgentPendingDelivery"]; !ok {
		t.Fatal("generated OpenRPC missing AgentPendingDelivery")
	}
	if _, ok := doc.Components.Schemas["AgentDiagnosisRuntimeState"]; !ok {
		t.Fatal("generated OpenRPC missing AgentDiagnosisRuntimeState")
	}
	if _, ok := doc.Components.Schemas["AgentDiagnosisWatchdog"]; !ok {
		t.Fatal("generated OpenRPC missing AgentDiagnosisWatchdog")
	}
	if _, ok := doc.Components.Schemas["AgentDiagnosisActive"]; !ok {
		t.Fatal("generated OpenRPC missing AgentDiagnosisActive")
	}
	if _, ok := doc.Components.Schemas["AgentDiagnosisLastToolOutcome"]; !ok {
		t.Fatal("generated OpenRPC missing AgentDiagnosisLastToolOutcome")
	}
	for _, schemaName := range []string{"MailboxDecisionSheet", "MailboxEntityContext", "MailboxDownstreamPreview"} {
		if _, ok := doc.Components.Schemas[schemaName]; !ok {
			t.Fatalf("generated OpenRPC missing %s", schemaName)
		}
	}
	agentDiagnosisSchema, ok := doc.Components.Schemas["AgentDiagnosis"].(map[string]any)
	if !ok {
		t.Fatalf("generated AgentDiagnosis schema = %#v, want object", doc.Components.Schemas["AgentDiagnosis"])
	}
	agentDiagnosisProperties, ok := agentDiagnosisSchema["properties"].(map[string]any)
	if !ok {
		t.Fatalf("generated AgentDiagnosis properties = %#v, want object", agentDiagnosisSchema["properties"])
	}
	runtimeStateSchema, ok := agentDiagnosisProperties["runtime_state"].(map[string]any)
	if !ok {
		t.Fatalf("generated AgentDiagnosis.runtime_state = %#v, want object", agentDiagnosisProperties["runtime_state"])
	}
	if got, want := runtimeStateSchema["$ref"], "#/components/schemas/AgentDiagnosisRuntimeState"; got != want {
		t.Fatalf("generated AgentDiagnosis.runtime_state ref = %#v, want %q", got, want)
	}
	activeSchema, ok := agentDiagnosisProperties["active"].(map[string]any)
	if !ok {
		t.Fatalf("generated AgentDiagnosis.active = %#v, want object", agentDiagnosisProperties["active"])
	}
	if got, want := activeSchema["$ref"], "#/components/schemas/AgentDiagnosisActive"; got != want {
		t.Fatalf("generated AgentDiagnosis.active ref = %#v, want %q", got, want)
	}
	lastToolOutcomeSchema, ok := agentDiagnosisProperties["last_tool_outcome"].(map[string]any)
	if !ok {
		t.Fatalf("generated AgentDiagnosis.last_tool_outcome = %#v, want object", agentDiagnosisProperties["last_tool_outcome"])
	}
	if got, want := lastToolOutcomeSchema["$ref"], "#/components/schemas/AgentDiagnosisLastToolOutcome"; got != want {
		t.Fatalf("generated AgentDiagnosis.last_tool_outcome ref = %#v, want %q", got, want)
	}
	if _, ok := methods["runtime.subscribe_logs"]; !ok {
		t.Fatal("generated OpenRPC missing runtime.subscribe_logs")
	}
	if _, ok := methods["runtime.nuke"]; !ok {
		t.Fatal("generated OpenRPC missing runtime.nuke")
	}
	if !methods["run.start"].Deprecated {
		t.Fatal("generated OpenRPC run.start deprecated flag = false, want true")
	}
	expectedNotifications := map[string]string{
		"event.subscribe":        "#/components/schemas/EventFull",
		"health.subscribe":       "#/components/schemas/HealthCheckResult",
		"run.subscribe_trace":    "#/components/schemas/RunTraceRow",
		"runtime.subscribe_logs": "#/components/schemas/LogEntry",
	}
	for methodName, wantRef := range expectedNotifications {
		if got := notificationSchemaRef(t, methodName, methods[methodName].NotificationSchema); got != wantRef {
			t.Fatalf("%s notification_schema ref = %q, want %q", methodName, got, wantRef)
		}
	}
	for methodName, method := range methods {
		if _, ok := expectedNotifications[methodName]; ok {
			continue
		}
		if method.NotificationSchema != nil {
			t.Fatalf("%s unexpectedly publishes notification_schema: %#v", methodName, method.NotificationSchema)
		}
	}
}

func TestEntityFullAccumulatedSchemaPublishesRuntimeAccumulatorState(t *testing.T) {
	api := loadRepoAPISpec(t)
	entityFull, ok := api.Components.Schemas["EntityFull"].(map[string]any)
	if !ok {
		t.Fatalf("EntityFull schema = %#v, want object", api.Components.Schemas["EntityFull"])
	}
	properties, ok := entityFull["properties"].(map[string]any)
	if !ok {
		t.Fatalf("EntityFull.properties = %#v, want object", entityFull["properties"])
	}
	accumulated, ok := properties["accumulated"].(map[string]any)
	if !ok {
		t.Fatalf("EntityFull.accumulated = %#v, want object schema", properties["accumulated"])
	}
	if got, want := accumulated["type"], "object"; got != want {
		t.Fatalf("EntityFull.accumulated.type = %#v, want %q", got, want)
	}
	if got, ok := accumulated["additionalProperties"].(bool); !ok || !got {
		t.Fatalf("EntityFull.accumulated.additionalProperties = %#v, want true", accumulated["additionalProperties"])
	}
	if _, ok := accumulated["items"]; ok {
		t.Fatalf("EntityFull.accumulated must not use array items at the map boundary: %#v", accumulated)
	}
	method := api.MethodCatalog["entity.get"]
	if method.Result == nil {
		t.Fatal("entity.get missing result descriptor")
	}
	resultSchema, ok := method.Result.Schema.(map[string]any)
	if !ok {
		t.Fatalf("entity.get result schema = %#v, want object", method.Result.Schema)
	}
	if got, want := resultSchema["$ref"], "#/components/schemas/EntityFull"; got != want {
		t.Fatalf("entity.get result ref = %#v, want %q", got, want)
	}

	generated, err := GenerateOpenRPC(api)
	if err != nil {
		t.Fatalf("GenerateOpenRPC() error = %v", err)
	}
	var doc OpenRPCDocument
	if err := json.Unmarshal(generated, &doc); err != nil {
		t.Fatalf("unmarshal generated openrpc: %v", err)
	}
	generatedEntityFull, ok := doc.Components.Schemas["EntityFull"].(map[string]any)
	if !ok {
		t.Fatalf("generated EntityFull schema = %#v, want object", doc.Components.Schemas["EntityFull"])
	}
	generatedProperties, ok := generatedEntityFull["properties"].(map[string]any)
	if !ok {
		t.Fatalf("generated EntityFull.properties = %#v, want object", generatedEntityFull["properties"])
	}
	generatedAccumulated, ok := generatedProperties["accumulated"].(map[string]any)
	if !ok {
		t.Fatalf("generated EntityFull.accumulated = %#v, want object schema", generatedProperties["accumulated"])
	}
	if got, ok := generatedAccumulated["additionalProperties"].(bool); !ok || !got {
		t.Fatalf("generated EntityFull.accumulated.additionalProperties = %#v, want true", generatedAccumulated["additionalProperties"])
	}
}

func TestValidateRejectsMissingExamplesPolicy(t *testing.T) {
	api := loadRepoAPISpec(t)
	api.ExamplesPolicy = ExamplesPolicy{}

	_, err := Validate(api)
	if err == nil {
		t.Fatal("Validate() error = nil, want examples_policy rejection")
	}
	if got, want := err.Error(), "api_specification.examples_policy missing status"; !strings.Contains(got, want) {
		t.Fatalf("Validate() error = %q, want substring %q", got, want)
	}
}

func TestValidateRejectsUnsupportedExamplesPolicy(t *testing.T) {
	api := loadRepoAPISpec(t)
	api.ExamplesPolicy.Status = "authored"

	_, err := Validate(api)
	if err == nil {
		t.Fatal("Validate() error = nil, want unsupported examples_policy status rejection")
	}
	if got, want := err.Error(), `api_specification.examples_policy.status = "authored", want "deferred"`; !strings.Contains(got, want) {
		t.Fatalf("Validate() error = %q, want substring %q", got, want)
	}
}

func TestValidateRejectsMissingServiceDiscoveryPolicy(t *testing.T) {
	api := loadRepoAPISpec(t)
	api.ServiceDiscoveryPolicy = ServiceDiscoveryPolicy{}

	_, err := Validate(api)
	if err == nil {
		t.Fatal("Validate() error = nil, want service_discovery_policy rejection")
	}
	if got, want := err.Error(), "api_specification.service_discovery_policy missing status"; !strings.Contains(got, want) {
		t.Fatalf("Validate() error = %q, want substring %q", got, want)
	}
}

func TestValidateRejectsUnsupportedServiceDiscoveryPolicy(t *testing.T) {
	api := loadRepoAPISpec(t)
	api.ServiceDiscoveryPolicy.Status = "published"

	_, err := Validate(api)
	if err == nil {
		t.Fatal("Validate() error = nil, want unsupported service_discovery_policy status rejection")
	}
	if got, want := err.Error(), `api_specification.service_discovery_policy.status = "published", want "not_published"`; !strings.Contains(got, want) {
		t.Fatalf("Validate() error = %q, want substring %q", got, want)
	}
}

func TestValidateRejectsRPCDiscoverUnderNonPublicationPolicy(t *testing.T) {
	api := loadRepoAPISpec(t)
	api.MethodCatalog["rpc.discover"] = api.MethodCatalog["rpc.unsubscribe"]

	_, err := Validate(api)
	if err == nil {
		t.Fatal("Validate() error = nil, want rpc.discover non-publication rejection")
	}
	if got, want := err.Error(), "method_catalog must not include rpc.discover while api_specification.service_discovery_policy.status is not_published"; !strings.Contains(got, want) {
		t.Fatalf("Validate() error = %q, want substring %q", got, want)
	}
}

func notificationSchemaRef(t *testing.T, methodName string, schema any) string {
	t.Helper()
	schemaMap, ok := schema.(map[string]any)
	if !ok {
		t.Fatalf("%s notification_schema = %#v, want object", methodName, schema)
	}
	ref, ok := schemaMap["$ref"].(string)
	if !ok || strings.TrimSpace(ref) == "" {
		t.Fatalf("%s notification_schema = %#v, want $ref", methodName, schema)
	}
	return ref
}

func assertExamplesPolicyDeferred(t *testing.T, policy ExamplesPolicy) {
	t.Helper()
	if policy.Status != ExamplesPolicyStatusDeferred {
		t.Fatalf("examples_policy.status = %q, want %q", policy.Status, ExamplesPolicyStatusDeferred)
	}
	if policy.Owner != ExamplesPolicyOwner {
		t.Fatalf("examples_policy.owner = %q, want %q", policy.Owner, ExamplesPolicyOwner)
	}
	if policy.AppliesTo != ExamplesPolicyAppliesToAllGenerated {
		t.Fatalf("examples_policy.applies_to = %q, want %q", policy.AppliesTo, ExamplesPolicyAppliesToAllGenerated)
	}
	if policy.OpenRPCMethodExamples != ExamplesPolicyOpenRPCExamplesOmitted {
		t.Fatalf("examples_policy.openrpc_method_examples = %q, want %q", policy.OpenRPCMethodExamples, ExamplesPolicyOpenRPCExamplesOmitted)
	}
	if policy.RuntimeProbeFixtures != ExamplesPolicyRuntimeFixturesNotExamples {
		t.Fatalf("examples_policy.runtime_probe_fixtures = %q, want %q", policy.RuntimeProbeFixtures, ExamplesPolicyRuntimeFixturesNotExamples)
	}
	if !policy.FutureSourceModelRequired {
		t.Fatal("examples_policy.future_source_model_required = false, want true")
	}
	if strings.TrimSpace(policy.Reason) == "" {
		t.Fatal("examples_policy.reason must explain examples deferral")
	}
	if len(policy.Requirements) == 0 {
		t.Fatal("examples_policy.requirements must list enforcement requirements")
	}
}

func assertServiceDiscoveryPolicyNotPublished(t *testing.T, policy ServiceDiscoveryPolicy) {
	t.Helper()
	if policy.Status != ServiceDiscoveryPolicyStatusNotPublished {
		t.Fatalf("service_discovery_policy.status = %q, want %q", policy.Status, ServiceDiscoveryPolicyStatusNotPublished)
	}
	if policy.Owner != ServiceDiscoveryPolicyOwner {
		t.Fatalf("service_discovery_policy.owner = %q, want %q", policy.Owner, ServiceDiscoveryPolicyOwner)
	}
	if policy.AppliesTo != ServiceDiscoveryPolicyAppliesToGeneratedCatalog {
		t.Fatalf("service_discovery_policy.applies_to = %q, want %q", policy.AppliesTo, ServiceDiscoveryPolicyAppliesToGeneratedCatalog)
	}
	if policy.RPCDiscover != ServiceDiscoveryPolicyRPCDiscoverOmitted {
		t.Fatalf("service_discovery_policy.rpc_discover = %q, want %q", policy.RPCDiscover, ServiceDiscoveryPolicyRPCDiscoverOmitted)
	}
	if policy.PublicationArtifact != ServiceDiscoveryPolicyPublicationArtifactOpenRPC {
		t.Fatalf("service_discovery_policy.publication_artifact = %q, want %q", policy.PublicationArtifact, ServiceDiscoveryPolicyPublicationArtifactOpenRPC)
	}
	if policy.RuntimeBehavior != ServiceDiscoveryPolicyRuntimeBehaviorMethodNotFound {
		t.Fatalf("service_discovery_policy.runtime_behavior = %q, want %q", policy.RuntimeBehavior, ServiceDiscoveryPolicyRuntimeBehaviorMethodNotFound)
	}
	if strings.TrimSpace(policy.Reason) == "" {
		t.Fatal("service_discovery_policy.reason must explain rpc.discover non-publication")
	}
	if len(policy.Requirements) == 0 {
		t.Fatal("service_discovery_policy.requirements must list enforcement requirements")
	}
}

func assertGeneratedMethodsOmitExamplesUnderPolicy(t *testing.T, api *APISpecification, artifact []byte) {
	t.Helper()
	assertExamplesPolicyDeferred(t, api.ExamplesPolicy)

	var rawDoc struct {
		Methods []map[string]json.RawMessage `json:"methods"`
	}
	if err := json.Unmarshal(artifact, &rawDoc); err != nil {
		t.Fatalf("unmarshal raw openrpc methods: %v", err)
	}
	for _, method := range rawDoc.Methods {
		var name string
		if err := json.Unmarshal(method["name"], &name); err != nil {
			t.Fatalf("parse raw openrpc method name: %v", err)
		}
		if rawJSONHasContent(method["examples"]) {
			t.Fatalf("%s publishes OpenRPC examples while examples_policy is deferred", name)
		}
		if rawJSONHasContent(method["example"]) {
			t.Fatalf("%s publishes OpenRPC example while examples_policy is deferred", name)
		}
	}
}

func assertGeneratedMethodsOmitRPCDiscoverUnderPolicy(t *testing.T, api *APISpecification, doc OpenRPCDocument) {
	t.Helper()
	assertServiceDiscoveryPolicyNotPublished(t, api.ServiceDiscoveryPolicy)
	if _, ok := api.MethodCatalog["rpc.discover"]; ok {
		t.Fatal("api_specification.method_catalog includes rpc.discover while service_discovery_policy is not_published")
	}
	for _, method := range doc.Methods {
		if method.Name == "rpc.discover" {
			t.Fatal("generated OpenRPC publishes rpc.discover while service_discovery_policy is not_published")
		}
	}
}

func rawJSONHasContent(raw json.RawMessage) bool {
	trimmed := strings.TrimSpace(string(raw))
	return trimmed != "" && trimmed != "null" && trimmed != "[]"
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
