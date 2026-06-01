package apispec

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
	"swarm/internal/platform"
)

func TestPlatformAPISpecValidationCoverage(t *testing.T) {
	api := loadRepoAPISpec(t)
	report, err := Validate(api)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if report.MethodCount != 56 {
		t.Fatalf("method count = %d, want 56", report.MethodCount)
	}
	if report.SchemaCount != 104 {
		t.Fatalf("schema count = %d, want 104", report.SchemaCount)
	}
	if report.ErrorCodeCount != 39 {
		t.Fatalf("error code count = %d, want 39", report.ErrorCodeCount)
	}
	if report.MutatingMethodCount != 22 {
		t.Fatalf("mutating method count = %d, want 22", report.MutatingMethodCount)
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
	if len(doc.Methods) != 56 {
		t.Fatalf("generated OpenRPC methods = %d, want 56", len(doc.Methods))
	}
	if len(doc.Components.Schemas) != 104 {
		t.Fatalf("generated OpenRPC schemas = %d, want 104", len(doc.Components.Schemas))
	}
	if len(doc.Components.Errors) != 39 {
		t.Fatalf("generated OpenRPC errors = %d, want 39", len(doc.Components.Errors))
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
	if _, ok := methods["agent.usage"]; !ok {
		t.Fatal("generated OpenRPC missing agent.usage")
	}
	if _, ok := methods["agent.delivery_diagnostics"]; !ok {
		t.Fatal("generated OpenRPC missing agent.delivery_diagnostics")
	}
	for _, methodName := range []string{"bundle.list", "bundle.get", "bundle.agents", "bundle.register", "bundle.delete"} {
		if _, ok := methods[methodName]; !ok {
			t.Fatalf("generated OpenRPC missing %s", methodName)
		}
	}
	for _, schemaName := range []string{"BundleSummary", "BundleListResult", "BundleDetail", "BundleAgentDefinition", "BundleAgentsResult", "BundleRegistrationEnvelopeV1", "BundleRegistrationFile", "BundleRegisterDataBlobV1", "BundleRegistrationResult", "BundleDeleteResult"} {
		if _, ok := doc.Components.Schemas[schemaName]; !ok {
			t.Fatalf("generated OpenRPC missing %s", schemaName)
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
	for _, schemaName := range []string{"AgentDeliveryDiagnostics", "AgentDeliveryDiagnosticsSummary", "AgentDeliveryFailure", "AgentDeadLetterDelivery"} {
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
			"#1001",
			"bundle-v1:sha256:<64 lowercase hex>",
			"cannot be combined with legacy bundle_ref",
			"UNSUPPORTED_BUNDLE_HASH",
		)
		assertOpenRPCParamDescriptionContains(t, methodName, params, "bundle_ref",
			"#1001",
			"bundle_ref.fingerprint",
			"not authoritative for create-new-work scope or routing",
			"cannot be combined with bundle_hash",
		)
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
		"#976",
		"loaded/boot-pinned RuntimeContextManager BundleContext",
		"BUNDLE_UNAVAILABLE",
		"BUNDLE_DATA_INTEGRITY_ERROR",
	)
}

func TestMultiBundleSourceAuthorityPublishesOnlyImplementedBundleReadAndRunForkMethods(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	multi := mustMappingValue(t, root, "multi_bundle_persistence")
	assertScalarValue(t, mustMappingValue(t, multi, "status"), "promoted_source_authority_with_partial_runtime_behavior")

	sourceEvidence := mustMappingValue(t, multi, "source_evidence")
	assertScalarValue(t, mustMappingValue(t, sourceEvidence, "run_fork_cli_authority_absorbed_from"), "#1038")
	assertScalarValue(t, mustMappingValue(t, sourceEvidence, "boot_pinned_runtime_context_manager"), "#1175")

	generatedPolicy := mustMappingValue(t, multi, "generated_artifact_policy")
	assertScalarValue(t, mustMappingValue(t, generatedPolicy, "current_openrpc_status"), "bundle_read_catalog_register_run_fork_and_delete_methods_published")
	assertScalarContains(t, mustMappingValue(t, generatedPolicy, "rule"), "bundle.list")
	assertScalarContains(t, mustMappingValue(t, generatedPolicy, "rule"), "run.fork")
	assertScalarContains(t, mustMappingValue(t, generatedPolicy, "rule"), "bundle.register")
	assertScalarContains(t, mustMappingValue(t, generatedPolicy, "rule"), "now published")

	identity := mustMappingValue(t, multi, "bundle_identity")
	assertScalarValue(t, mustMappingValue(t, identity, "canonical_name"), "bundle_hash")
	assertScalarContains(t, mustMappingValue(t, identity, "hash_rule_owner"), "platform-spec.yaml#multi_bundle_persistence.bundle_identity.canonicalization_v1")
	canonicalization := mustMappingValue(t, identity, "canonicalization_v1")
	assertScalarValue(t, mustMappingValue(t, canonicalization, "status"), "promoted_merge_bearing_authority")
	assertScalarValue(t, mustMappingValue(t, canonicalization, "implementation_owner"), "internal/runtime/contracts.BundleHash")
	assertScalarContains(t, mustMappingValue(t, canonicalization, "runtime_behavior_boundary"), "Serve ingest")
	preimage := mustMappingValue(t, canonicalization, "preimage_stream")
	assertScalarContains(t, mustMappingValue(t, preimage, "entry_encoding"), "uint64 big-endian")
	labels := mustMappingValue(t, canonicalization, "labels")
	assertScalarContains(t, mustMappingValue(t, labels, "bundle_label_rule"), "bundle/<relative path from contracts root>")
	entries := mustMappingValue(t, canonicalization, "canonical_entries")
	if !sequenceContainsScalar(mustMappingValue(t, entries, "root_optional_yaml"), "schema.yaml") {
		t.Fatal("canonicalization_v1 must include root schema.yaml as an optional YAML input")
	}
	contentPolicies := mustMappingValue(t, canonicalization, "content_policies")
	yamlPolicy := mustMappingValue(t, contentPolicies, "yaml")
	assertScalarContains(t, mustMappingValue(t, yamlPolicy, "tags"), "quoted explicit non-string scalar tags fail closed")
	assertScalarContains(t, mustMappingValue(t, yamlPolicy, "json_emission"), "including `+` on positive exponents")
	assertScalarContains(t, mustMappingValue(t, mustMappingValue(t, contentPolicies, "prompt_text"), "canonicalization"), "normalize CRLF and CR to LF")
	renamePolicy := mustMappingValue(t, identity, "rename_policy")
	assertScalarValue(t, mustMappingValue(t, renamePolicy, "bundle_hash"), "promoted_name")
	assertScalarContains(t, mustMappingValue(t, renamePolicy, "dual_accept_transition"), "#1001")

	persistence := mustMappingValue(t, multi, "persistence_model")
	assertScalarContains(t, mustMappingValue(t, persistence, "live_schema_boundary"), "#1013 promotes the bundles table")
	serveIngest := mustMappingValue(t, persistence, "serve_ingest_projection")
	assertScalarValue(t, mustMappingValue(t, serveIngest, "status"), "implemented_for_local_postgres_serve_contracts")
	assertScalarValue(t, mustMappingValue(t, serveIngest, "projection_owner"), "internal/runtime/contracts.BuildBundleCatalogProjection")
	assertScalarValue(t, mustMappingValue(t, serveIngest, "store_owner"), "internal/store.PostgresStore.UpsertBundleCatalog")
	assertScalarValue(t, mustMappingValue(t, serveIngest, "source_fact_owner"), "cmd/swarm.prepareServeBundleSource")
	assertScalarContains(t, mustMappingValue(t, serveIngest, "content_yaml"), "canonical content bytes as base64")
	assertScalarContains(t, mustMappingValue(t, serveIngest, "data_blob"), "raw bytes as base64")
	assertScalarContains(t, mustMappingValue(t, serveIngest, "parsed_json"), "Runtime-owned fields")
	assertScalarContains(t, mustMappingValue(t, serveIngest, "idempotency"), "fails closed before runtime construction")
	serveDBLoaded := mustMappingValue(t, persistence, "serve_db_loaded_runtime_source")
	assertScalarValue(t, mustMappingValue(t, serveDBLoaded, "status"), "implemented_for_boot_pinned_postgres_serve_bundle_hash_contexts")
	assertScalarContains(t, mustMappingValue(t, serveDBLoaded, "cli_flag"), "[--bundle-hash")
	assertScalarValue(t, mustMappingValue(t, serveDBLoaded, "runtime_context_manager_owner"), "internal/runtime.RuntimeContextManager")
	assertScalarValue(t, mustMappingValue(t, serveDBLoaded, "bundle_context_owner"), "internal/runtime.BundleContext")
	platformTables := mustMappingValue(t, mustMappingValue(t, root, "platform_tables"), "tables")
	if mappingValue(platformTables, "bundles") == nil {
		t.Fatal("bundles table must be live in platform_tables after the DB migration child lands")
	}
	runsDDL := mustMappingValue(t, mustMappingValue(t, platformTables, "runs"), "ddl")
	assertScalarContains(t, runsDDL, "bundle_hash")
	assertScalarContains(t, runsDDL, "bundle_source")
	assertScalarContains(t, runsDDL, "bundle_fingerprint TEXT")

	cliSurface := mustMappingValue(t, multi, "cli_surface")
	runFork := mustMappingValue(t, cliSurface, "run_fork")
	const runForkCommand = "swarm fork <source-run-id> [--bundle-hash <bundle_hash>] [--at-event <event-id>] [--idempotency-key <key>]"
	assertScalarValue(t, mustMappingValue(t, runFork, "command"), runForkCommand)
	if strings.Contains(runForkCommand, "--bundle ") {
		t.Fatal("run fork command promoted legacy --bundle spelling")
	}
	if strings.Contains(runForkCommand, "swarm control run fork") {
		t.Fatal("run fork command promoted control-run fork spelling")
	}

	apiSurface := mustMappingValue(t, multi, "api_surface")
	assertScalarValue(t, mustMappingValue(t, apiSurface, "publication_status"), "bundle_read_catalog_register_run_fork_and_delete_generated_openrpc")

	bundleDelete := mustMappingValue(t, multi, "bundle_delete")
	phaseFive := mustMappingValue(t, bundleDelete, "phase_5_atomicity")
	assertScalarValue(t, mustMappingValue(t, phaseFive, "tracker"), "#1009")
	assertScalarValue(t, mustMappingValue(t, phaseFive, "implementation_status"), "implemented_for_force_and_non_force_delete_runtime")
	assertScalarValue(t, mustMappingValue(t, phaseFive, "canonical_runtime_owner"), "internal/store.PostgresStore.ApplyBundleDeleteFinalMutation")
	appliesTo := mustMappingValue(t, phaseFive, "applies_to")
	if !sequenceContainsScalar(appliesTo, "#1018 non-force bundle.delete final bundle availability mutation") {
		t.Fatal("phase_5_atomicity must bind non-force bundle.delete")
	}
	if !sequenceContainsScalar(appliesTo, "#1019 bundle.delete --force final bundle availability mutation after preservation cleanup succeeds") {
		t.Fatal("phase_5_atomicity must bind force bundle.delete")
	}
	assertScalarContains(t, mustMappingValue(t, phaseFive, "transaction_rule"), "one database transaction")
	assertScalarContains(t, mustMappingValue(t, phaseFive, "transaction_rule"), "serializes run creation")
	assertScalarContains(t, mustMappingValue(t, phaseFive, "transaction_rule"), "before deleting the matching bundles")
	assertScalarContains(t, mustMappingValue(t, phaseFive, "transaction_rule"), "shared store transaction owner")
	order := mustMappingValue(t, phaseFive, "transaction_order")
	if len(order.Content) != 3 {
		t.Fatalf("phase_5_atomicity transaction_order has %d items, want 3", len(order.Content))
	}
	if got := scalarValue(order.Content[0]); got != "lock_runs_table_against_new_persisted_bundle_references" {
		t.Fatalf("phase_5_atomicity transaction_order[0] = %q, want run-creation lock first", got)
	}
	if got := scalarValue(order.Content[1]); got != "update_eligible_runs_bundle_source_to_deleted" {
		t.Fatalf("phase_5_atomicity transaction_order[1] = %q, want update second", got)
	}
	if got := scalarValue(order.Content[2]); got != "delete_matching_bundles_row" {
		t.Fatalf("phase_5_atomicity transaction_order[2] = %q, want delete last", got)
	}
	assertScalarContains(t, mustMappingValue(t, phaseFive, "reader_invariant"), "read runs.bundle_source before consulting bundles")
	assertScalarContains(t, mustMappingValue(t, phaseFive, "reader_invariant"), "BUNDLE_UNAVAILABLE")
	assertScalarContains(t, mustMappingValue(t, phaseFive, "reader_invariant"), "BUNDLE_DATA_INTEGRITY_ERROR")
	postDeleteAdmission := mustMappingValue(t, phaseFive, "post_delete_new_work_admission")
	assertScalarValue(t, mustMappingValue(t, postDeleteAdmission, "status"), "implemented_for_force_and_non_force_delete_runtime")
	assertScalarValue(t, mustMappingValue(t, postDeleteAdmission, "canonical_owner"), "internal/store/runlifecycle.EnsureActive")
	admissionConsumers := mustMappingValue(t, postDeleteAdmission, "consumed_by")
	if !sequenceContainsScalar(admissionConsumers, "/v1/rpc event.publish") {
		t.Fatal("post_delete_new_work_admission must bind event.publish")
	}
	if !sequenceContainsScalar(admissionConsumers, "/v1/rpc run.start") {
		t.Fatal("post_delete_new_work_admission must bind run.start")
	}
	if !sequenceContainsScalar(admissionConsumers, "internal/runtime.RuntimeLogger runtime-log run-row persistence") {
		t.Fatal("post_delete_new_work_admission must bind runtime-log persistence")
	}
	if !sequenceContainsScalar(admissionConsumers, "internal/runtime/mutationlog mutation-log run-row persistence") {
		t.Fatal("post_delete_new_work_admission must bind mutation-log persistence")
	}
	assertScalarContains(t, mustMappingValue(t, postDeleteAdmission, "rule"), "event.publish, run.start, runtime-log, and mutation-log attempts fail closed")
	invalid := mustMappingValue(t, phaseFive, "invalid_implementations")
	if !sequenceContainsScalar(invalid, "deleting the bundles row before marking eligible runs deleted") {
		t.Fatal("phase_5_atomicity must reject delete-before-update implementations")
	}

	apiRunFork := mustMappingValue(t, apiSurface, "run_fork")
	assertScalarValue(t, mustMappingValue(t, apiRunFork, "method"), "run.fork")
	apiRunForkParams := mustMappingValue(t, apiRunFork, "params")
	bundleHashParam := mustMappingValue(t, apiRunForkParams, "bundle_hash")
	assertScalarValue(t, mustMappingValue(t, bundleHashParam, "cli_flag"), "--bundle-hash <bundle_hash>")

	splits := mustMappingValue(t, multi, "explicit_splits")
	for split, tracker := range map[string]string{
		"schema_foundation":                   "#1013",
		"reader_migration":                    "#1014",
		"bundle_ingest_and_run_stamping":      "#1015",
		"startup_recovery_unavailable_bundle": "#1016",
		"bundle_api_catalog":                  "#1017",
		"bundle_delete_non_force":             "#1018",
		"bundle_delete_force":                 "#1019",
		"serve_db_loaded_bundle":              "#1020",
		"bundle_safe_list_read_scoping":       "#1021",
		"create_new_work_bundle_hash":         "#1022",
		"cross_bundle_fork":                   "#976",
		"db_loaded_same_bundle_source":        "#1024",
		"run_fork_cli_consumer":               "#1023",
		"multi_bundle_cli_inventory":          "#1023",
		"bundle_hash_dual_accept_migration":   "#1001",
	} {
		assertScalarValue(t, mustMappingValue(t, mustMappingValue(t, splits, split), "tracker"), tracker)
	}

	cli := mustMappingValue(t, root, "cli_specification")
	commandCatalog := mustMappingValue(t, cli, "command_catalog")
	bundleCommand := mustMappingValue(t, commandCatalog, "bundle")
	assertScalarValue(t, mustMappingValue(t, bundleCommand, "command"), "swarm bundle list|show|agents|register|delete")
	assertScalarValue(t, mustMappingValue(t, bundleCommand, "implementation_status"), "implemented_inventory_prepared_envelope_directory_register_and_delete")
	assertScalarContains(t, mustMappingValue(t, bundleCommand, "blocker_state"), "local directory swarm bundle register --contracts")
	assertScalarContains(t, mustMappingValue(t, bundleCommand, "blocker_state"), "Destructive swarm bundle delete is implemented")
	assertScalarContains(t, mustMappingValue(t, bundleCommand, "blocker_state"), "Archive packaging remains split")
	runForkCatalog := mustMappingValue(t, commandCatalog, "run_fork")
	assertScalarValue(t, mustMappingValue(t, runForkCatalog, "command"), runForkCommand)
	assertScalarValue(t, mustMappingValue(t, runForkCatalog, "implementation_status"), "implemented_public_cli_consumer")
	retired := mustMappingValue(t, cli, "retired_namespaces")
	if mappingValue(retired, "fork") != nil {
		t.Fatal("bare top-level swarm fork must not remain classified as a retired namespace")
	}
	legacyHarness := mustMappingValue(t, retired, "fork_legacy_harness_forms")
	assertScalarContains(t, mustMappingValue(t, legacyHarness, "command"), "--contracts")

	parentTail := mustMappingValue(t, cli, "parent_tail")
	retiredOrFailClosed := mustMappingValue(t, parentTail, "retired_or_fail_closed")
	for _, item := range retiredOrFailClosed.Content {
		if scalarValue(item) == "swarm fork" {
			t.Fatal("bare top-level swarm fork must not remain in retired_or_fail_closed")
		}
	}
	remaining := mustMappingValue(t, parentTail, "remaining_should_have_not_implemented")
	if sequenceContainsScalar(remaining, runForkCommand) {
		t.Fatalf("remaining_should_have_not_implemented still includes implemented %q", runForkCommand)
	}
	if sequenceContainsScalar(remaining, "swarm bundle list|show|agents|register|delete") {
		t.Fatal("remaining_should_have_not_implemented still includes implemented swarm bundle inventory commands")
	}
	if !sequenceContainsScalar(remaining, "swarm bundle register --archive <archive-file>") {
		t.Fatal("remaining_should_have_not_implemented missing archive register split")
	}
	if sequenceContainsScalar(remaining, "swarm bundle delete <bundle-hash> [--force] [--dry-run] [--idempotency-key <key>]") {
		t.Fatal("remaining_should_have_not_implemented still includes implemented swarm bundle delete")
	}

	apiBoundary := mustMappingValue(t, mustMappingValue(t, root, "api_specification"), "multi_bundle_publication_boundary")
	assertScalarValue(t, mustMappingValue(t, apiBoundary, "status"), "partial_bundle_read_catalog_register_run_fork_and_delete_method_catalog")
	assertScalarContains(t, mustMappingValue(t, apiBoundary, "rule"), "bundle.register is also published")
	assertScalarContains(t, mustMappingValue(t, apiBoundary, "rule"), "prepared-envelope swarm")
	assertScalarContains(t, mustMappingValue(t, apiBoundary, "rule"), "local contracts-directory packaging is implemented")
	assertScalarContains(t, mustMappingValue(t, apiBoundary, "rule"), "archive packaging remains split")
	assertScalarContains(t, mustMappingValue(t, apiBoundary, "rule"), "bundle.delete is promoted")
	assertScalarContains(t, mustMappingValue(t, apiBoundary, "rule"), "omitted or false force performs non-force deletion")

	for _, relPath := range []string{
		filepath.Join("cmd", "swarm", "main.go"),
		filepath.Join("cmd", "swarm", "fork.go"),
	} {
		raw, err := os.ReadFile(filepath.Join(repoRoot(t), relPath))
		if err != nil {
			t.Fatalf("read %s: %v", relPath, err)
		}
		content := string(raw)
		if strings.Contains(content, "swarm control run fork") {
			t.Fatalf("%s still promotes stale swarm control run fork authority", relPath)
		}
		if strings.Contains(content, "retires top-level `swarm fork`") || strings.Contains(content, "top-level `swarm fork` is retired") {
			t.Fatalf("%s still says top-level swarm fork is retired", relPath)
		}
		if strings.Contains(content, "retired by the Cobra command tree") {
			t.Fatalf("%s still carries stale retired-by-Cobra swarm fork authority", relPath)
		}
		if !strings.Contains(content, runForkCommand) {
			t.Fatalf("%s missing promoted run fork command shape", relPath)
		}
	}

	api := loadRepoAPISpec(t)
	for _, methodName := range []string{"bundle.list", "bundle.get", "bundle.agents", "bundle.register", "bundle.delete"} {
		if _, ok := api.MethodCatalog[methodName]; !ok {
			t.Fatalf("%s must be in live method_catalog after bundle catalog implementation lands", methodName)
		}
	}
	if _, ok := api.MethodCatalog["run.fork"]; !ok {
		t.Fatal("run.fork missing from live method_catalog after API/runtime handler promotion")
	}
	generated, err := GenerateOpenRPC(api)
	if err != nil {
		t.Fatalf("GenerateOpenRPC() error = %v", err)
	}
	var doc OpenRPCDocument
	if err := json.Unmarshal(generated, &doc); err != nil {
		t.Fatalf("unmarshal generated openrpc: %v", err)
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

func TestAgentIdentityModelRecordsCurrentSlugConstraint(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	identity := mustMappingValue(t, root, "agent_identity_model")
	assertScalarValue(t, mappingValue(identity, "status"), "current_global_slug_constraint_promoted_source_authority")
	assertScalarValue(t, mappingValue(identity, "promoted_by"), "#977")
	assertScalarValue(t, mappingValue(identity, "current_supported_posture"), "global_live_slug_identity")
	assertScalarContains(t, mappingValue(identity, "authority_rule"), "agent_id as the authored contract slug")
	assertScalarContains(t, mappingValue(identity, "authority_rule"), "globally unique live runtime identity")

	constraint := mustMappingValue(t, identity, "current_multi_bundle_constraint")
	assertScalarContains(t, mappingValue(constraint, "rule"), "globally unique authored agent_id values")
	assertScalarContains(t, mappingValue(constraint, "rule"), "Duplicate authored agent slugs across live loaded BundleContexts are unsupported")
	assertScalarContains(t, mappingValue(constraint, "enforcement_status"), "does not add runtime/store enforcement")

	migration := mustMappingValue(t, identity, "uuid_internal_migration_target")
	assertScalarValue(t, mappingValue(migration, "tracker"), "#977")
	assertScalarValue(t, mappingValue(migration, "status"), "staged_parent_only_not_current_runtime_behavior")
	assertSurfaceListedValue(t, mustMappingValue(t, migration, "not_promoted_in_this_slice"), "agents.id UUID schema migration")
	assertSurfaceListedValue(t, mustMappingValue(t, migration, "not_promoted_in_this_slice"), "duplicate slug support across loaded bundles")

	disposition := mustMappingValue(t, identity, "consumer_disposition")
	for _, key := range []string{
		"uuid_internal_candidates",
		"slug_display_aliases",
		"bundle_scoped_lookup_candidates",
		"historical_immutable_records",
		"split_or_escalate",
	} {
		if mappingValue(disposition, key) == nil {
			t.Fatalf("agent_identity_model.consumer_disposition missing %s", key)
		}
	}

	multiBundle := mustMappingValue(t, root, "multi_bundle_persistence")
	assertScalarValue(t, mappingValue(mustMappingValue(t, multiBundle, "agent_identity_boundary"), "owner"), "platform-spec.yaml#agent_identity_model")
	sourceEvidence := mustMappingValue(t, multiBundle, "source_evidence")
	assertScalarValue(t, mappingValue(sourceEvidence, "agent_identity_source_authority"), "#977")

	api := mustMappingValue(t, root, "api_specification")
	assertScalarValue(t, mappingValue(mustMappingValue(t, api, "agent_identity_boundary"), "owner"), "platform-spec.yaml#agent_identity_model")
	blocked := mustMappingValue(t, mustMappingValue(t, api, "multi_bundle_publication_boundary"), "blocked_children")
	assertScalarValue(t, mappingValue(blocked, "agent_identity_duplicate_slug_migration"), "#977")

	cli := mustMappingValue(t, root, "cli_specification")
	assertScalarValue(t, mappingValue(mustMappingValue(t, cli, "agent_identity_boundary"), "owner"), "platform-spec.yaml#agent_identity_model")
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
	raw, err := os.ReadFile(platform.DefaultPlatformSpecFile(repoRoot(t)))
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

func assertSurfaceListedValue(t *testing.T, list *yaml.Node, want string) {
	t.Helper()
	if list == nil || list.Kind != yaml.SequenceNode {
		t.Fatalf("list kind = %v, want sequence", listKind(list))
	}
	for _, entry := range list.Content {
		if scalarValue(entry) == want {
			return
		}
	}
	t.Fatalf("list missing value %s", want)
}

func listKind(node *yaml.Node) yaml.Kind {
	if node == nil {
		return 0
	}
	return node.Kind
}
