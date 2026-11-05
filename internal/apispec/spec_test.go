package apispec

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

func TestPlatformAPISpecValidationCoverage(t *testing.T) {
	api := loadRepoAPISpec(t)
	report, err := Validate(api)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if report.MethodCount != 70 {
		t.Fatalf("method count = %d, want 70", report.MethodCount)
	}
	// #2720/6245827935 adds the per-instance status and frozen turn attribution.
	for _, name := range []string{"RunInstanceOperationalStatus", "RunStallAttribution"} {
		if _, ok := api.Components.Schemas[name]; !ok {
			t.Fatalf("N19 schema %s missing from catalog", name)
		}
	}
	if _, ok := api.Components.Schemas["ChannelCapabilityVector"]; !ok {
		t.Fatal("ChannelCapabilityVector missing from schema catalog")
	}
	if report.MutatingMethodCount != 28 {
		t.Fatalf("mutating method count = %d, want 28", report.MutatingMethodCount)
	}
	if report.SubscriptionMethodCnt != 5 {
		t.Fatalf("subscription method count = %d, want 5", report.SubscriptionMethodCnt)
	}
	if _, ok := api.MethodCatalog["rpc.unsubscribe"]; !ok {
		t.Fatal("rpc.unsubscribe missing from method catalog")
	}
	if _, ok := api.MethodCatalog["runtime.nuke"]; !ok {
		t.Fatal("runtime.nuke missing from method catalog")
	}
	if _, ok := api.MethodCatalog["agent.frame"]; !ok {
		t.Fatal("agent.frame missing from method catalog")
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
	artifactPath := platform.DefaultOpenRPCFile(repoRoot(t))
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
	if len(doc.Methods) != 70 {
		t.Fatalf("generated OpenRPC methods = %d, want 70", len(doc.Methods))
	}
	for _, name := range []string{"RunInstanceOperationalStatus", "RunStallAttribution"} {
		if _, ok := doc.Components.Schemas[name]; !ok {
			t.Fatalf("generated OpenRPC missing N19 schema %s", name)
		}
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
	if _, ok := methods["test.setup_entities"]; !ok {
		t.Fatal("generated OpenRPC missing test.setup_entities")
	}
	if _, ok := methods["event.replay"]; !ok {
		t.Fatal("generated OpenRPC missing event.replay")
	}
	if _, ok := methods["agent.frame"]; !ok {
		t.Fatal("generated OpenRPC missing agent.frame")
	}
	if _, ok := methods["agent.replay"]; !ok {
		t.Fatal("generated OpenRPC missing agent.replay")
	}
	if _, ok := methods["agent.diagnose"]; !ok {
		t.Fatal("generated OpenRPC missing agent.diagnose")
	}
	if _, ok := methods["agent.usage"]; !ok {
		t.Fatal("generated OpenRPC missing agent.usage")
	}
	if _, ok := methods["agent.delivery_diagnostics"]; !ok {
		t.Fatal("generated OpenRPC missing agent.delivery_diagnostics")
	}
	if _, ok := methods["agent.delivery_lifecycle"]; !ok {
		t.Fatal("generated OpenRPC missing agent.delivery_lifecycle")
	}
	for _, methodName := range []string{"bundle.list", "bundle.get", "bundle.agents", "bundle.register", "bundle.delete"} {
		if _, ok := methods[methodName]; ok {
			t.Fatalf("generated OpenRPC retains retired %s", methodName)
		}
	}
	for _, schemaName := range []string{"BundleSummary", "BundleListResult", "BundleDetail", "BundleAgentDefinition", "BundleAgentsResult", "BundleRegistrationEnvelopeV1", "BundleRegistrationFile", "BundleRegisterDataBlobV1", "BundleRegistrationResult", "BundleDeleteResult"} {
		if _, ok := doc.Components.Schemas[schemaName]; ok {
			t.Fatalf("generated OpenRPC retains retired %s", schemaName)
		}
	}
	if _, ok := methods["run.fork"]; !ok {
		t.Fatal("generated OpenRPC missing run.fork")
	}
	for _, methodName := range []string{"conversation.fork", "conversation.fork_chat", "conversation.fork_list", "conversation.fork_view", "conversation.fork_delete"} {
		if _, ok := methods[methodName]; !ok {
			t.Fatalf("generated OpenRPC missing %s", methodName)
		}
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
	for _, schemaName := range []string{"AgentDeliveryDiagnostics", "AgentDeliveryDiagnosticsSummary", "AgentDeliveryFailure", "AgentDeadLetterDelivery", "AgentDeliveryLifecycleList", "AgentDeliveryLifecycleRow"} {
		if _, ok := doc.Components.Schemas[schemaName]; !ok {
			t.Fatalf("generated OpenRPC missing %s", schemaName)
		}
	}
	for _, schemaName := range []string{"UsageAccounting", "AgentUsageWindow", "AgentUsageTotals", "AgentUsageByAccounting", "AgentUsageBreakdown", "AgentUsage"} {
		if _, ok := doc.Components.Schemas[schemaName]; !ok {
			t.Fatalf("generated OpenRPC missing %s", schemaName)
		}
	}
	for _, schemaName := range []string{"MailboxDecisionSheet", "MailboxEntityContext", "MailboxDownstreamPreview"} {
		if _, ok := doc.Components.Schemas[schemaName]; !ok {
			t.Fatalf("generated OpenRPC missing %s", schemaName)
		}
	}
	for _, schemaName := range []string{"TestSetupEntityResult", "TestSetupEntitiesResult", "ClockScheduleReadback"} {
		if _, ok := doc.Components.Schemas[schemaName]; !ok {
			t.Fatalf("generated OpenRPC missing %s", schemaName)
		}
	}
	for _, schemaName := range []string{
		"ConversationForkPointSelector",
		"ConversationForkPointDescriptor",
		"ConversationForkSession",
		"ConversationForkTurn",
		"ConversationForkEntitySnapshot",
		"ConversationForkSnapshot",
		"ConversationForkSandboxPolicy",
		"ConversationForkChatResult",
		"ConversationForkCreateResult",
		"ConversationForkDeleteResult",
	} {
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
	if methods["run.start"].Deprecated {
		t.Fatal("generated OpenRPC run.start deprecated flag = true, want false")
	}
	expectedNotifications := map[string]string{
		"event.subscribe":        "#/components/schemas/EventFull",
		"health.subscribe":       "#/components/schemas/HealthCheckResult",
		"mailbox.subscribe":      "#/components/schemas/DecisionCardChange",
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

func TestGeneratedOpenRPCBundleIdentityDescriptionsPreserveConstraints(t *testing.T) {
	artifactPath := platform.DefaultOpenRPCFile(repoRoot(t))
	artifact, err := os.ReadFile(artifactPath)
	if err != nil {
		t.Fatalf("read openrpc artifact: %v", err)
	}
	var doc OpenRPCDocument
	if err := json.Unmarshal(artifact, &doc); err != nil {
		t.Fatalf("unmarshal openrpc artifact: %v", err)
	}
	methods := map[string]OpenRPCMethod{}
	for _, method := range doc.Methods {
		methods[method.Name] = method
	}
	for _, methodName := range []string{"event.publish", "run.start"} {
		method, ok := methods[methodName]
		if !ok {
			t.Fatalf("generated OpenRPC missing %s", methodName)
		}
		params := map[string]ContentDescriptor{}
		for _, param := range method.Params {
			params[param.Name] = param
		}
		assertOpenRPCParamDescriptionContains(t, methodName, params, "bundle_hash",
			"bundle-v2:sha256:<64 lowercase hex>",
			"UNSUPPORTED_BUNDLE_HASH",
		)
		if _, ok := params["bundle_ref"]; ok {
			t.Fatalf("generated OpenRPC %s still publishes retired bundle_ref", methodName)
		}
	}
	runFork, ok := methods["run.fork"]
	if !ok {
		t.Fatal("generated OpenRPC missing run.fork")
	}
	runForkParams := map[string]ContentDescriptor{}
	for _, param := range runFork.Params {
		runForkParams[param.Name] = param
	}
	assertOpenRPCParamDescriptionContains(t, "run.fork", runForkParams, "bundle_hash",
		"exact retained target artifact",
		"independent process-owned selected preparation",
		"Loaded normal runtime membership is irrelevant",
		"BUNDLE_UNAVAILABLE",
		"BUNDLE_DATA_INTEGRITY_ERROR",
	)
	for _, retired := range []string{"confirm_source_freeze", "allow_source_freeze"} {
		if _, ok := runForkParams[retired]; ok {
			t.Fatalf("generated run.fork still accepts retired freeze parameter %q", retired)
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
	accumulated = requireNullableOpenObject(t, "EntityFull.accumulated", accumulated)
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
	generatedAccumulated = requireNullableOpenObject(t, "generated EntityFull.accumulated", generatedAccumulated)
	if got, ok := generatedAccumulated["additionalProperties"].(bool); !ok || !got {
		t.Fatalf("generated EntityFull.accumulated.additionalProperties = %#v, want true", generatedAccumulated["additionalProperties"])
	}
}

func TestEntityFullBookkeepingSchemaPublishesIndependentPlatformFacts(t *testing.T) {
	api := loadRepoAPISpec(t)
	entityFull, ok := api.Components.Schemas["EntityFull"].(map[string]any)
	if !ok {
		t.Fatalf("EntityFull schema = %#v, want object", api.Components.Schemas["EntityFull"])
	}
	properties, ok := entityFull["properties"].(map[string]any)
	if !ok {
		t.Fatalf("EntityFull.properties = %#v, want object", entityFull["properties"])
	}
	bookkeeping, ok := properties["bookkeeping"].(map[string]any)
	if !ok {
		t.Fatalf("EntityFull.bookkeeping = %#v, want nullable open object schema", properties["bookkeeping"])
	}
	requireNullableOpenObject(t, "EntityFull.bookkeeping", bookkeeping)
	required, ok := entityFull["required"].([]any)
	if !ok {
		t.Fatalf("EntityFull.required = %#v, want list", entityFull["required"])
	}
	found := false
	for _, item := range required {
		if item == "bookkeeping" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("EntityFull.required = %#v, want bookkeeping", required)
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
	generatedBookkeeping, ok := generatedProperties["bookkeeping"].(map[string]any)
	if !ok {
		t.Fatalf("generated EntityFull.bookkeeping = %#v, want nullable open object schema", generatedProperties["bookkeeping"])
	}
	requireNullableOpenObject(t, "generated EntityFull.bookkeeping", generatedBookkeeping)
}

func requireNullableOpenObject(t *testing.T, name string, schema map[string]any) map[string]any {
	t.Helper()
	branches, ok := schema["oneOf"].([]any)
	if !ok || len(branches) != 2 {
		t.Fatalf("%s = %#v, want exactly constructed-object and imported-null branches", name, schema)
	}
	object, objectOK := branches[0].(map[string]any)
	null, nullOK := branches[1].(map[string]any)
	if !objectOK || object["type"] != "object" || object["additionalProperties"] != true ||
		!nullOK || len(null) != 1 || null["type"] != "null" {
		t.Fatalf("%s branches = %#v, want open object and null", name, branches)
	}
	if _, exists := object["items"]; exists {
		t.Fatalf("%s object branch must not use array items: %#v", name, object)
	}
	if _, exists := schema["type"]; exists {
		t.Fatalf("%s must select its type through oneOf: %#v", name, schema)
	}
	return object
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

func loadRepoAPISpec(t *testing.T) *APISpecification {
	t.Helper()
	api, err := LoadPlatformSpec(platform.DefaultPlatformSpecFile(repoRoot(t)))
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
	source, err := yamlsource.LoadFile(platform.DefaultPlatformSpecFile(repoRoot(t)))
	if err != nil {
		t.Fatalf("read platform spec: %v", err)
	}
	doc := source.NodeCopy()
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

func sequenceContainsScalar(node *yaml.Node, want string) bool {
	if node == nil || node.Kind != yaml.SequenceNode {
		return false
	}
	for _, item := range node.Content {
		if scalarValue(item) == want {
			return true
		}
	}
	return false
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

func assertOpenRPCParamDescriptionContains(t *testing.T, methodName string, params map[string]ContentDescriptor, paramName string, wants ...string) {
	t.Helper()
	param, ok := params[paramName]
	if !ok {
		t.Fatalf("generated OpenRPC %s missing param %s", methodName, paramName)
	}
	for _, want := range wants {
		if !strings.Contains(param.Description, want) {
			t.Fatalf("generated OpenRPC %s.%s description = %q, want substring %q", methodName, paramName, param.Description, want)
		}
	}
}
