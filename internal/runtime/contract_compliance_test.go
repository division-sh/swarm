package runtime

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"

	"empireai/internal/commgraph"
	runtimecontracts "empireai/internal/runtime/contracts"
	llm "empireai/internal/runtime/llm"
	"gopkg.in/yaml.v3"
)

type contractComplianceAgent struct {
	ID                     string   `yaml:"id"`
	Type                   string   `yaml:"type"`
	Role                   string   `yaml:"role"`
	NodeType               string   `yaml:"node_type"`
	ModelTier              string   `yaml:"model_tier"`
	ConversationMode       string   `yaml:"conversation_mode"`
	MaxTurnsPerTask        int      `yaml:"max_turns_per_task"`
	Subscriptions          []string `yaml:"subscriptions"`
	SubscriptionsBootstrap []string `yaml:"subscriptions_bootstrap"`
	SubscribesTo           []string `yaml:"subscribes_to"`
	ToolsTier2             []string `yaml:"tools_tier2"`
	EmitEvents             []string `yaml:"emit_events"`
	Implementation         string   `yaml:"implementation"`
}

type contractComplianceAgentConfigMap struct {
	Agents map[string]struct {
		ConfigPath *string `yaml:"config_path"`
	} `yaml:"agents"`
}

type contractComplianceAgentConfig struct {
	ID            string   `yaml:"id"`
	Role          string   `yaml:"role"`
	ModelTier     string   `yaml:"model_tier"`
	Subscriptions []string `yaml:"subscriptions"`
	Tools         []string `yaml:"tools"`
	Constraints   struct {
		MaxTurnsPerTask  int    `yaml:"max_turns_per_task"`
		ConversationMode string `yaml:"conversation_mode"`
	} `yaml:"constraints"`
}

type contractComplianceCatalogEvent struct {
	Emitter           string   `yaml:"emitter"`
	EmitterType       string   `yaml:"emitter_type"`
	AlternateEmitters []string `yaml:"alternate_emitters"`
	Payload           any      `yaml:"payload"`
}

type contractComplianceSystemNode struct {
	ID             string   `yaml:"id"`
	SubscribesTo   []string `yaml:"subscribes_to"`
	Produces       []string `yaml:"produces"`
	Implementation string   `yaml:"implementation"`
}

type contractComplianceRoutes struct {
	BootstrapRoutes []contractComplianceRoute `yaml:"bootstrap_routes"`
}

type contractComplianceRoute struct {
	EventPattern   string `yaml:"event_pattern"`
	SubscriberRole string `yaml:"subscriber_role"`
}

type contractComplianceVerificationGates struct {
	SpecVersion string `yaml:"spec_version"`
}

type contractComplianceToolingLock struct {
	ContractFormatVersion string `yaml:"contract_format_version"`
}

type contractComplianceToolSchema struct {
	InputSchema map[string]any `yaml:"input_schema"`
}

func TestContractCompliance(t *testing.T) {
	t.Helper()
	ensureEventSchemaRegistry()

	repoRoot := contractComplianceRepoRoot(t)
	contractAgents := contractComplianceLoadAgentTools(t, repoRoot)
	agentConfigMap := contractComplianceLoadAgentConfigMap(t, repoRoot)
	eventCatalog := contractComplianceLoadEventCatalog(t, repoRoot)
	systemNodes := contractComplianceLoadSystemNodes(t, repoRoot)
	routes := contractComplianceLoadRoutes(t, repoRoot)
	specVersion := contractComplianceLoadSpecVersion(t, repoRoot)
	toolingLockVersion := contractComplianceLoadToolingLockVersion(t, repoRoot)
	toolSchemas := contractComplianceLoadToolSchemas(t, repoRoot)

	t.Run("gate1_agent_config_fields_match_contract", func(t *testing.T) {
		ids := make([]string, 0, len(contractAgents))
		for id := range contractAgents {
			ids = append(ids, id)
		}
		sort.Strings(ids)

		for _, id := range ids {
			ag := contractAgents[id]
			if strings.EqualFold(strings.TrimSpace(ag.NodeType), "system") {
				continue
			}
			path, ok := contractComplianceConfigPathForAgent(id, agentConfigMap)
			if !ok {
				t.Fatalf("agent %s missing in contracts/agent-config-map.yaml", id)
			}
			if strings.TrimSpace(path) == "" {
				continue
			}
			cfg := contractComplianceLoadAgentConfig(t, repoRoot, path)
			if got, want := strings.TrimSpace(cfg.ModelTier), strings.TrimSpace(ag.ModelTier); got != want {
				t.Fatalf("agent %s model_tier mismatch: got=%q want=%q", id, got, want)
			}
			if got, want := cfg.Constraints.MaxTurnsPerTask, ag.MaxTurnsPerTask; got != want {
				t.Fatalf("agent %s max_turns_per_task mismatch: got=%d want=%d", id, got, want)
			}
			if got, want := strings.TrimSpace(cfg.Constraints.ConversationMode), strings.TrimSpace(ag.ConversationMode); got != want {
				t.Fatalf("agent %s conversation_mode mismatch: got=%q want=%q", id, got, want)
			}

			actualTools := make([]string, 0, len(cfg.Tools))
			for _, tool := range cfg.Tools {
				tool = strings.TrimSpace(tool)
				if tool == "" || tool == "agent_message" {
					continue
				}
				actualTools = append(actualTools, tool)
			}
			expectedTools := ag.ToolsTier2
			if diff := contractComplianceDiffSet(expectedTools, actualTools); diff != "" {
				t.Fatalf("agent %s tools mismatch (%s)", id, diff)
			}
		}
	})

	t.Run("gate2_subscriptions_match_contract", func(t *testing.T) {
		// 2a) Holding/factory + leadership static subscriptions: config/default roster must match contract.
		ids := make([]string, 0, len(contractAgents))
		for id := range contractAgents {
			ids = append(ids, id)
		}
		sort.Strings(ids)

		for _, id := range ids {
			ag := contractAgents[id]
			if strings.EqualFold(strings.TrimSpace(ag.NodeType), "system") {
				continue
			}
			path, ok := contractComplianceConfigPathForAgent(id, agentConfigMap)
			if !ok || strings.TrimSpace(path) == "" {
				continue
			}
			// Contract static subscriptions are authoritative for holding/factory and OpCo leadership.
			if len(ag.Subscriptions) == 0 {
				continue
			}
			cfg := contractComplianceLoadAgentConfig(t, repoRoot, path)
			if diff := contractComplianceDiffSet(ag.Subscriptions, cfg.Subscriptions); diff != "" {
				t.Fatalf("agent %s static subscriptions mismatch (%s)", id, diff)
			}
		}

		// 2b) OpCo worker bootstrap subscriptions: routes.yaml must match contract subscriptions_bootstrap.
		routeSubs := contractComplianceRouteBootstrapByRole(routes)
		for id, ag := range contractAgents {
			if strings.EqualFold(strings.TrimSpace(ag.NodeType), "system") || strings.TrimSpace(ag.Type) != "operating" {
				continue
			}
			role, ok := contractComplianceContractIDToBootstrapRole(id)
			if !ok {
				continue
			}
			actual := routeSubs[role]
			if diff := contractComplianceDiffSet(ag.SubscriptionsBootstrap, actual); diff != "" {
				t.Fatalf("agent %s bootstrap subscriptions mismatch (%s)", id, diff)
			}
		}

		// 2c) OpCo leadership subscriptions in defaultOpCoRoster() must match contract.
		roster := defaultOpCoRoster("contract-compliance")
		rosterSubs := map[string][]string{}
		for _, rec := range roster {
			rosterSubs[strings.TrimSpace(rec.Config.Role)] = rec.Config.Subscriptions
		}
		leadership := map[string]string{
			"opco-ceo":             "opco-ceo",
			"opco-chief-of-staff":  "chief-of-staff",
			"opco-head-of-product": "vp-product",
			"opco-head-of-growth":  "vp-growth",
		}
		for contractID, runtimeRole := range leadership {
			ag, ok := contractAgents[contractID]
			if !ok {
				t.Fatalf("contract missing leadership agent %s", contractID)
			}
			actual := rosterSubs[runtimeRole]
			if diff := contractComplianceDiffSet(ag.Subscriptions, actual); diff != "" {
				t.Fatalf("defaultOpCoRoster leadership subscriptions mismatch for %s (%s)", contractID, diff)
			}
		}
	})

	t.Run("gate3_commgraph_emit_events_match_contract", func(t *testing.T) {
		errs := make([]string, 0, 16)
		ids := make([]string, 0, len(contractAgents))
		for id := range contractAgents {
			ids = append(ids, id)
		}
		sort.Strings(ids)

		for _, id := range ids {
			ag := contractAgents[id]
			if strings.EqualFold(strings.TrimSpace(ag.NodeType), "system") {
				continue
			}
			role := contractComplianceContractIDToCommgraphRole(id, ag.Role)
			actual := commgraph.ProducerEventsForRole(role)
			if diff := contractComplianceDiffSet(ag.EmitEvents, actual); diff != "" {
				errs = append(errs, fmt.Sprintf("commgraph producer mismatch for %s (role=%s) (%s)", id, role, diff))
			}
		}

		scoringNode, ok := systemNodes["scoring-node"]
		if !ok {
			t.Fatalf("contracts/system-nodes.yaml missing scoring-node")
		}
		if diff := contractComplianceDiffSet(scoringNode.Produces, commgraph.ProducerEventsForRole("scoring-node")); diff != "" {
			errs = append(errs, fmt.Sprintf("scoring-node produces mismatch vs system-nodes.yaml (%s)", diff))
		}

		// Validate event-catalog emitter ownership only for Gate 3 scoped emitter types.
		eventTypes := make([]string, 0, len(eventCatalog))
		for eventType := range eventCatalog {
			eventTypes = append(eventTypes, eventType)
		}
		sort.Strings(eventTypes)
		for _, eventType := range eventTypes {
			cat := eventCatalog[eventType]
			emitterType := strings.ToLower(strings.TrimSpace(cat.EmitterType))
			// Gate 3 scope:
			// - agent/opco_agent: enforce against commgraph producer events.
			// - system_node: enforce against contracts/system-nodes.yaml produces.
			// - runtime, human: intentionally skipped here.
			if emitterType == "runtime" || emitterType == "human" {
				continue
			}
			if emitterType != "agent" && emitterType != "system_node" && emitterType != "opco_agent" {
				errs = append(errs, fmt.Sprintf("event-catalog emitter_type unsupported for %s (emitter_type=%q)", eventType, cat.EmitterType))
				continue
			}
			emitters := append([]string{cat.Emitter}, cat.AlternateEmitters...)
			match := false
			for _, emitter := range emitters {
				if (emitterType == "agent" || emitterType == "opco_agent") && contractComplianceAgentEmitterProduces(eventType, emitter) {
					match = true
					break
				}
				if emitterType == "system_node" && contractComplianceSystemNodeProduces(systemNodes, eventType, emitter) {
					match = true
					break
				}
			}
			if !match {
				errs = append(errs, fmt.Sprintf("event-catalog emitter mismatch for %s (emitter_type=%s emitters=%v)", eventType, emitterType, contractComplianceNormalizeList(emitters)))
			}
		}

		if len(errs) > 0 {
			t.Fatalf("gate3 failures (%d):\n- %s", len(errs), strings.Join(errs, "\n- "))
		}
	})

	t.Run("gate4_event_schema_registry_payload_coverage", func(t *testing.T) {
		errs := make([]string, 0, 32)
		schemas := EventSchemaSnapshot()
		events := make([]string, 0, len(eventCatalog))
		for evt := range eventCatalog {
			events = append(events, evt)
		}
		sort.Strings(events)

		for _, evt := range events {
			catalog := eventCatalog[evt]
			schema, ok := schemas[evt]
			if !ok {
				continue
			}
			props := schemaProperties(schema.Schema["properties"])
			schemaFields := make([]string, 0, len(props))
			for k := range props {
				schemaFields = append(schemaFields, strings.TrimSpace(k))
			}

			expectedFields := contractComplianceCatalogPayloadFields(catalog.Payload)
			if diff := contractComplianceMissingFrom(expectedFields, schemaFields); diff != "" {
				errs = append(errs, fmt.Sprintf("event %s schema missing catalog payload fields (%s)", evt, diff))
			}

			if !contractComplianceBool(schema.Schema["additionalProperties"], true) {
				if diff := contractComplianceDiffSet(expectedFields, schemaFields); diff != "" {
					errs = append(errs, fmt.Sprintf("event %s has additionalProperties=false but payload fields diverge (%s)", evt, diff))
				}
			}

			if evt == "trend.identified" {
				requiredStructured := []string{"opportunity_name", "preliminary_icp", "build_sketch", "evidence"}
				if diff := contractComplianceMissingFrom(requiredStructured, expectedFields); diff != "" {
					errs = append(errs, fmt.Sprintf("event %s catalog missing required structured fields (%s)", evt, diff))
				}
				if diff := contractComplianceMissingFrom(requiredStructured, schemaFields); diff != "" {
					errs = append(errs, fmt.Sprintf("event %s schema missing required structured fields (%s)", evt, diff))
				}
				for _, field := range []string{"build_sketch", "evidence"} {
					prop, ok := props[field]
					if !ok {
						errs = append(errs, fmt.Sprintf("event %s schema missing %s object", evt, field))
						continue
					}
					if got, _ := prop["type"].(string); strings.TrimSpace(got) != "object" {
						errs = append(errs, fmt.Sprintf("event %s %s must be object, got=%q", evt, field, got))
						continue
					}
					if len(schemaProperties(prop["properties"])) == 0 {
						errs = append(errs, fmt.Sprintf("event %s %s must define structured nested properties", evt, field))
					}
				}
			}
		}
		if len(errs) > 0 {
			t.Fatalf("gate4 failures (%d):\n- %s", len(errs), contractComplianceFormatErrs(errs, 40))
		}
	})

	t.Run("gate5_ddl_table_count_and_runtime_table_spotcheck", func(t *testing.T) {
		errs := make([]string, 0, 8)
		ddlPath := filepath.Join(repoRoot, "contracts", "ddl-canonical.sql")
		raw, err := os.ReadFile(ddlPath)
		if err != nil {
			t.Fatalf("read %s: %v", ddlPath, err)
		}
		tables, err := contractComplianceParseDDLTables(string(raw))
		if err != nil {
			t.Fatalf("parse canonical ddl: %v", err)
		}
		if got, want := len(tables), 37; got != want {
			errs = append(errs, fmt.Sprintf("canonical DDL table count mismatch: got=%d want=%d", got, want))
		}

		expected := map[string][]string{
			"runtime_config":            {"id", "config_yaml", "config_path", "applied_at", "created_at"},
			"pipeline_receipts":         {"event_id", "status", "error", "processed_at"},
			"scan_accumulators":         {"scan_id", "campaign_id", "mode", "geography", "expected", "complete", "completed_by", "reports", "discovered", "skipped", "pending_dedup", "timeout_at", "started_at", "completed_at", "created_at", "updated_at"},
			"pending_dedup_candidates":  {"dedup_event_id", "scan_id", "campaign_id", "mode", "name", "geography", "discovery_mode", "signal_strength", "payload", "existing_id", "status", "created_at", "resolved_at"},
			"validation_pipelines":      {"vertical_id", "status", "g1_research", "g2_spec", "g3_cto", "g4_brand", "research_payload", "spec_payload", "cto_payload", "brand_payload", "scoring_payload", "revision_count", "inner_revision_count", "spec_version", "packaging_requested", "packaging_requested_at", "packaging_retries", "created_at", "updated_at"},
			"pipeline_processed_events": {"event_id", "processed_at"},
			"template_prompt_drafts":    {"role", "prompt", "source", "notes", "created_at", "updated_at"},
		}
		for table, wantCols := range expected {
			gotCols, ok := tables[table]
			if !ok {
				errs = append(errs, fmt.Sprintf("canonical DDL missing table %s", table))
				continue
			}
			if diff := contractComplianceDiffSet(wantCols, gotCols); diff != "" {
				errs = append(errs, fmt.Sprintf("table %s columns mismatch (%s)", table, diff))
			}
		}
		if len(errs) > 0 {
			t.Fatalf("gate5 failures (%d):\n- %s", len(errs), contractComplianceFormatErrs(errs, 20))
		}
	})

	t.Run("gate6_runtime_and_template_versions", func(t *testing.T) {
		if got, want := contractComplianceNormVersion(runtimeSpecVersion), contractComplianceNormVersion(specVersion); got != want {
			t.Fatalf("runtimeSpecVersion mismatch: got=%q want=%q", runtimeSpecVersion, specVersion)
		}
		if got, want := contractComplianceNormVersion(toolingLockVersion), contractComplianceNormVersion(specVersion); got != want {
			t.Fatalf("tooling.lock contract_format_version mismatch: got=%q want=%q", toolingLockVersion, specVersion)
		}
		roster := defaultOpCoRoster("contract-version-check")
		for _, rec := range roster {
			if got, want := contractComplianceNormVersion(rec.TemplateVersion), contractComplianceNormVersion(specVersion); got != want {
				t.Fatalf("template version mismatch for role=%s agent=%s: got=%q want=%q", rec.Config.Role, rec.Config.ID, rec.TemplateVersion, specVersion)
			}
		}
	})

	t.Run("gate7_prefilter_contract_vectors", func(t *testing.T) {
		runPrefilterContractVectorChecks(t, repoRoot)
	})

	t.Run("gate8_subscription_handler_coverage", func(t *testing.T) {
		errs := make([]string, 0, 16)
		subscriberEvents, implementationBySubscriber := contractComplianceDeterministicSubscriptions(contractAgents, systemNodes)
		handledEventsCache := map[string]map[string]struct{}{}
		handlerTests, testErr := contractComplianceCollectRuntimeTestNames(repoRoot)
		if testErr != nil {
			errs = append(errs, fmt.Sprintf("collect runtime test names: %v", testErr))
		}

		for _, sub := range subscriberEvents {
			implPath := strings.TrimSpace(implementationBySubscriber[sub.Subscriber])
			if implPath == "" {
				errs = append(errs, fmt.Sprintf("subscriber %s missing implementation path for %s", sub.Subscriber, sub.EventType))
				continue
			}
			handledEvents, ok := handledEventsCache[implPath]
			if !ok {
				var err error
				handledEvents, err = contractComplianceParseHandledEventsFromFile(repoRoot, implPath)
				if err != nil {
					errs = append(errs, fmt.Sprintf("parse handler coverage %s: %v", implPath, err))
					continue
				}
				handledEventsCache[implPath] = handledEvents
			}
			if _, ok := handledEvents[sub.EventType]; !ok {
				errs = append(errs, fmt.Sprintf("subscription has no handler case: subscriber=%s event=%s implementation=%s", sub.Subscriber, sub.EventType, implPath))
			}
			expectedTest := contractComplianceExpectedHandlerTestName(sub.Subscriber, sub.EventType)
			if expectedTest != "" {
				if _, ok := handlerTests[expectedTest]; !ok {
					errs = append(errs, fmt.Sprintf("subscription missing handler test: want=%s (subscriber=%s event=%s)", expectedTest, sub.Subscriber, sub.EventType))
				}
			}
		}

		// Interceptor parity check for consumed runtime events:
		// any event listed in interceptPolicy must have a corresponding handleEvent case
		// (except spec.revision_needed, handled in Intercept special-case branch).
		pipelinePath := filepath.Join(repoRoot, "internal", "runtime", "pipeline_coordinator.go")
		interceptEvents, handleEvents, _, err := parsePipelineInterceptorCoverage(pipelinePath)
		if err != nil {
			errs = append(errs, fmt.Sprintf("parse interceptor coverage: %v", err))
		} else {
			for evt := range interceptEvents {
				if strings.TrimSpace(evt) == "spec.revision_needed" {
					continue
				}
				if _, ok := handleEvents[evt]; !ok {
					errs = append(errs, fmt.Sprintf("interceptor event %s has no handleEvent case", evt))
				}
			}
		}

		if len(errs) > 0 {
			t.Fatalf("gate8 failures (%d):\n- %s", len(errs), contractComplianceFormatErrs(errs, 40))
		}
	})

	t.Run("gate9_tool_schemas_match_contract", func(t *testing.T) {
		errs := make([]string, 0, 32)
		expectedTools := map[string]struct{}{
			"agent_message": {},
			"mailbox_send":  {},
		}
		for _, ag := range contractAgents {
			for _, tool := range ag.ToolsTier2 {
				tool = strings.TrimSpace(tool)
				if tool == "" {
					continue
				}
				expectedTools[tool] = struct{}{}
			}
		}

		runtimeDefs := NewRuntimeToolExecutor(nil, nil, nil).ToolDefinitions()
		runtimeByName := make(map[string]llm.ToolDefinition, len(runtimeDefs))
		for _, def := range runtimeDefs {
			runtimeByName[strings.TrimSpace(def.Name)] = def
		}

		for toolName := range expectedTools {
			entry, ok := toolSchemas[toolName]
			if !ok {
				errs = append(errs, fmt.Sprintf("tool %s missing from contracts/tool-schemas.yaml", toolName))
				continue
			}
			def, ok := runtimeByName[toolName]
			if !ok {
				errs = append(errs, fmt.Sprintf("tool %s missing from runtime ToolDefinitions()", toolName))
				continue
			}
			runtimeSchema, ok := def.Schema.(map[string]any)
			if !ok {
				errs = append(errs, fmt.Sprintf("tool %s runtime schema is not object map", toolName))
				continue
			}
			contractProps := schemaProperties(entry.InputSchema["properties"])
			runtimeProps := schemaProperties(runtimeSchema["properties"])
			contractFields := make([]string, 0, len(contractProps))
			runtimeFields := make([]string, 0, len(runtimeProps))
			for field := range contractProps {
				contractFields = append(contractFields, field)
			}
			for field := range runtimeProps {
				runtimeFields = append(runtimeFields, field)
			}
			if diff := contractComplianceDiffSet(contractFields, runtimeFields); diff != "" {
				errs = append(errs, fmt.Sprintf("tool %s schema properties mismatch (%s)", toolName, diff))
			}
			if diff := contractComplianceDiffSet(contractComplianceRequiredFields(entry.InputSchema["required"]), contractComplianceRequiredFields(runtimeSchema["required"])); diff != "" {
				errs = append(errs, fmt.Sprintf("tool %s required fields mismatch (%s)", toolName, diff))
			}

			validPayload, err := contractComplianceBuildValidToolPayload(entry.InputSchema)
			if err != nil {
				errs = append(errs, fmt.Sprintf("tool %s valid payload generation failed: %v", toolName, err))
				continue
			}
			exec := NewRuntimeToolExecutor(nil, nil, nil)
			if err := exec.validateRuntimeToolInput(toolName, validPayload); err != nil {
				errs = append(errs, fmt.Sprintf("tool %s rejected valid contract payload: %v", toolName, err))
			}

			invalidPayload, err := contractComplianceBuildInvalidToolPayload(entry.InputSchema)
			if err != nil {
				errs = append(errs, fmt.Sprintf("tool %s invalid payload generation failed: %v", toolName, err))
				continue
			}
			if err := exec.validateRuntimeToolInput(toolName, invalidPayload); err == nil {
				errs = append(errs, fmt.Sprintf("tool %s accepted invalid contract payload", toolName))
			}
		}
		if len(errs) > 0 {
			t.Fatalf("gate9 failures (%d):\n- %s", len(errs), contractComplianceFormatErrs(errs, 40))
		}
	})

	t.Run("gate10_prompt_schema_guard", func(t *testing.T) {
		if err := runtimecontracts.ValidatePromptSchemaGuards(repoRoot); err != nil {
			t.Fatal(err)
		}
	})
}

type contractComplianceSubscription struct {
	Subscriber string
	EventType  string
}

func contractComplianceRepoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatalf("resolve current file path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
}

func contractComplianceLoadAgentTools(t *testing.T, repoRoot string) map[string]contractComplianceAgent {
	t.Helper()
	path := filepath.Join(repoRoot, "contracts", "agent-tools.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	all := map[string]contractComplianceAgent{}
	if err := yaml.Unmarshal(raw, &all); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	out := map[string]contractComplianceAgent{}
	for key, v := range all {
		if strings.TrimSpace(v.ID) == "" {
			continue
		}
		out[strings.TrimSpace(key)] = v
	}
	return out
}

func contractComplianceLoadAgentConfigMap(t *testing.T, repoRoot string) contractComplianceAgentConfigMap {
	t.Helper()
	path := filepath.Join(repoRoot, "contracts", "agent-config-map.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var out contractComplianceAgentConfigMap
	if err := yaml.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return out
}

func contractComplianceLoadEventCatalog(t *testing.T, repoRoot string) map[string]contractComplianceCatalogEvent {
	t.Helper()
	path := filepath.Join(repoRoot, "contracts", "event-catalog.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	all := map[string]contractComplianceCatalogEvent{}
	if err := yaml.Unmarshal(raw, &all); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	out := map[string]contractComplianceCatalogEvent{}
	for key, v := range all {
		if strings.TrimSpace(v.Emitter) == "" {
			continue
		}
		out[strings.TrimSpace(key)] = v
	}
	return out
}

func contractComplianceLoadSystemNodes(t *testing.T, repoRoot string) map[string]contractComplianceSystemNode {
	t.Helper()
	path := filepath.Join(repoRoot, "contracts", "system-nodes.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	all := map[string]contractComplianceSystemNode{}
	if err := yaml.Unmarshal(raw, &all); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	out := map[string]contractComplianceSystemNode{}
	for key, v := range all {
		if strings.TrimSpace(v.ID) == "" {
			continue
		}
		out[strings.TrimSpace(key)] = v
	}
	return out
}

func contractComplianceLoadRoutes(t *testing.T, repoRoot string) contractComplianceRoutes {
	t.Helper()
	path := filepath.Join(repoRoot, "configs", "agents", "templates", "routes.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var out contractComplianceRoutes
	if err := yaml.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return out
}

func contractComplianceLoadSpecVersion(t *testing.T, repoRoot string) string {
	t.Helper()
	path := filepath.Join(repoRoot, "contracts", "verification-gates.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var gates contractComplianceVerificationGates
	if err := yaml.Unmarshal(raw, &gates); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if strings.TrimSpace(gates.SpecVersion) == "" {
		t.Fatalf("spec_version missing in %s", path)
	}
	return strings.TrimSpace(gates.SpecVersion)
}

func contractComplianceLoadToolingLockVersion(t *testing.T, repoRoot string) string {
	t.Helper()
	path := filepath.Join(repoRoot, "contracts", "tooling.lock")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var lock contractComplianceToolingLock
	if err := yaml.Unmarshal(raw, &lock); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if strings.TrimSpace(lock.ContractFormatVersion) == "" {
		t.Fatalf("contract_format_version missing in %s", path)
	}
	return strings.TrimSpace(lock.ContractFormatVersion)
}

func contractComplianceLoadToolSchemas(t *testing.T, repoRoot string) map[string]contractComplianceToolSchema {
	t.Helper()
	path := filepath.Join(repoRoot, "contracts", "tool-schemas.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	all := map[string]contractComplianceToolSchema{}
	if err := yaml.Unmarshal(raw, &all); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return all
}

func contractComplianceRequiredFields(raw any) []string {
	arr, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		if field, ok := item.(string); ok {
			out = append(out, strings.TrimSpace(field))
		}
	}
	return out
}

func contractComplianceBuildValidToolPayload(schema map[string]any) (map[string]any, error) {
	v, err := contractComplianceBuildSchemaValue(schema)
	if err != nil {
		return nil, err
	}
	payload, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("schema root is not object")
	}
	return payload, nil
}

func contractComplianceBuildInvalidToolPayload(schema map[string]any) (map[string]any, error) {
	valid, err := contractComplianceBuildValidToolPayload(schema)
	if err != nil {
		return nil, err
	}
	required := contractComplianceRequiredFields(schema["required"])
	if len(required) > 0 {
		valid[required[0]] = []any{"wrong_type"}
		return valid, nil
	}
	props := schemaProperties(schema["properties"])
	keys := make([]string, 0, len(props))
	for key := range props {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		return nil, fmt.Errorf("schema has no fields to invalidate")
	}
	valid[keys[0]] = []any{"wrong_type"}
	return valid, nil
}

func contractComplianceBuildSchemaValue(schema map[string]any) (any, error) {
	if schema == nil {
		return map[string]any{}, nil
	}
	if enumRaw, ok := schema["enum"]; ok {
		switch enum := enumRaw.(type) {
		case []any:
			if len(enum) > 0 {
				return enum[0], nil
			}
		case []string:
			if len(enum) > 0 {
				return enum[0], nil
			}
		}
	}
	switch strings.TrimSpace(asString(schema["type"])) {
	case "", "object":
		payload := map[string]any{}
		props := schemaProperties(schema["properties"])
		for _, field := range contractComplianceRequiredFields(schema["required"]) {
			propSchema, ok := props[field]
			if !ok {
				return nil, fmt.Errorf("required field %s missing property schema", field)
			}
			value, err := contractComplianceBuildSchemaValue(propSchema)
			if err != nil {
				return nil, fmt.Errorf("field %s: %w", field, err)
			}
			payload[field] = value
		}
		return payload, nil
	case "string":
		return "x", nil
	case "number":
		return 1.0, nil
	case "integer":
		return 1, nil
	case "boolean":
		return true, nil
	case "array":
		items, _ := schema["items"].(map[string]any)
		if items == nil {
			return []any{}, nil
		}
		value, err := contractComplianceBuildSchemaValue(items)
		if err != nil {
			return nil, err
		}
		return []any{value}, nil
	case "null":
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported schema type %q", schema["type"])
	}
}

func contractComplianceConfigPathForAgent(id string, cfgMap contractComplianceAgentConfigMap) (string, bool) {
	entry, ok := cfgMap.Agents[strings.TrimSpace(id)]
	if !ok {
		return "", false
	}
	if entry.ConfigPath == nil {
		return "", true
	}
	return strings.TrimSpace(*entry.ConfigPath), true
}

func contractComplianceLoadAgentConfig(t *testing.T, repoRoot, relPath string) contractComplianceAgentConfig {
	t.Helper()
	path := filepath.Clean(filepath.Join(repoRoot, relPath))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var cfg contractComplianceAgentConfig
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return cfg
}

func contractComplianceRouteBootstrapByRole(routes contractComplianceRoutes) map[string][]string {
	out := map[string][]string{}
	for _, r := range routes.BootstrapRoutes {
		role := strings.TrimSpace(r.SubscriberRole)
		evt := strings.TrimSpace(r.EventPattern)
		if role == "" || evt == "" {
			continue
		}
		out[role] = append(out[role], evt)
	}
	return out
}

func contractComplianceContractIDToBootstrapRole(contractID string) (string, bool) {
	m := map[string]string{
		"opco-pm":          "pm-agent",
		"opco-cto":         "cto-agent",
		"opco-tech-writer": "tech-writer",
		"opco-backend":     "backend-agent",
		"opco-frontend":    "frontend-agent",
		"opco-qa":          "qa-agent",
		"opco-devops":      "devops-agent",
		"opco-marketing":   "marketing-agent",
		"opco-support":     "support-agent",
	}
	role, ok := m[strings.TrimSpace(contractID)]
	return role, ok
}

func contractComplianceContractIDToCommgraphRole(contractID, contractRole string) string {
	id := strings.TrimSpace(contractID)
	r := strings.TrimSpace(strings.ReplaceAll(contractRole, "_", "-"))
	if v, ok := map[string]string{
		"empire-coordinator":      "empire-coordinator",
		"factory-cto":             "factory-cto",
		"holding-devops":          "holding-devops",
		"operations-analyst":      "operations-analyst",
		"spec-auditor":            "spec-auditor",
		"discovery-coordinator":   "discovery-coordinator",
		"analysis-agent":          "analysis-agent",
		"validation-coordinator":  "validation-coordinator",
		"business-research-agent": "business-research-agent",
		"lightweight-spec-agent":  "lightweight-spec-agent",
		"spec-reviewer":           "spec-reviewer",
		"market-research-agent":   "market-research-agent",
		"trend-research-agent":    "trend-research-agent",
		"pre-brand-agent":         "pre-brand-agent",
		"scanner-agent":           "scanner-agent",
		"opco-head-of-product":    "vp-product",
		"opco-head-of-growth":     "vp-growth",
		"opco-pm":                 "pm-agent",
		"opco-cto":                "cto-agent",
		"opco-tech-writer":        "tech-writer",
		"opco-backend":            "backend-agent",
		"opco-frontend":           "frontend-agent",
		"opco-qa":                 "qa-agent",
		"opco-devops":             "devops-agent",
		"opco-marketing":          "marketing-agent",
		"opco-support":            "support-agent",
	}[id]; ok {
		return v
	}
	if v, ok := map[string]string{
		"empire-coordinator":     "empire-coordinator",
		"factory-cto":            "factory-cto",
		"operations-analyst":     "operations-analyst",
		"spec-auditor":           "spec-auditor",
		"discovery-coordinator":  "discovery-coordinator",
		"analysis-agent":         "analysis-agent",
		"validation-coordinator": "validation-coordinator",
		"business-research":      "business-research-agent",
		"lightweight-spec":       "lightweight-spec-agent",
		"spec-reviewer":          "spec-reviewer",
		"market-research":        "market-research-agent",
		"trend-research":         "trend-research-agent",
		"pre-brand":              "pre-brand-agent",
		"scanner":                "scanner-agent",
		"opco-ceo":               "opco-ceo",
		"chief-of-staff":         "chief-of-staff",
		"head-of-product":        "vp-product",
		"head-of-growth":         "vp-growth",
		"pm":                     "pm-agent",
		"cto":                    "cto-agent",
		"tech-writer":            "tech-writer",
		"backend":                "backend-agent",
		"frontend":               "frontend-agent",
		"qa":                     "qa-agent",
		"marketing":              "marketing-agent",
		"support":                "support-agent",
	}[r]; ok {
		if r == "devops" {
			// Disambiguate by contract ID.
			if strings.HasPrefix(id, "opco-") {
				return "devops-agent"
			}
			return "holding-devops"
		}
		return v
	}
	if r == "devops" {
		if strings.HasPrefix(id, "opco-") {
			return "devops-agent"
		}
		return "holding-devops"
	}
	if r != "" {
		return r
	}
	return id
}

func contractComplianceDiffSet(expected, actual []string) string {
	exp := contractComplianceNormalizeList(expected)
	act := contractComplianceNormalizeList(actual)
	missing := contractComplianceSetSubtract(exp, act)
	extra := contractComplianceSetSubtract(act, exp)
	if len(missing) == 0 && len(extra) == 0 {
		return ""
	}
	return fmt.Sprintf("missing=%v extra=%v", missing, extra)
}

func contractComplianceMissingFrom(expected, actual []string) string {
	miss := contractComplianceSetSubtract(contractComplianceNormalizeList(expected), contractComplianceNormalizeList(actual))
	if len(miss) == 0 {
		return ""
	}
	return fmt.Sprintf("missing=%v", miss)
}

func contractComplianceNormalizeList(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func contractComplianceSetSubtract(a, b []string) []string {
	bm := make(map[string]struct{}, len(b))
	for _, v := range b {
		bm[v] = struct{}{}
	}
	out := make([]string, 0)
	for _, v := range a {
		if _, ok := bm[v]; ok {
			continue
		}
		out = append(out, v)
	}
	return out
}

func contractComplianceBool(v any, fallback bool) bool {
	b, ok := v.(bool)
	if !ok {
		return fallback
	}
	return b
}

func contractComplianceParseDDLTables(sqlText string) (map[string][]string, error) {
	tables := map[string][]string{}
	re := regexp.MustCompile(`(?is)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_."-]+)\s*\((.*?)\);`)
	matches := re.FindAllStringSubmatch(sqlText, -1)
	for _, m := range matches {
		if len(m) < 3 {
			continue
		}
		tableName := strings.TrimSpace(m[1])
		tableName = strings.Trim(tableName, `"`)
		if dot := strings.LastIndex(tableName, "."); dot >= 0 {
			tableName = tableName[dot+1:]
		}
		if tableName == "" {
			return nil, fmt.Errorf("extract table name from match %q", m[0])
		}
		cols := contractComplianceExtractColumns(m[2])
		tables[tableName] = cols
	}
	return tables, nil
}

func contractComplianceExtractColumns(block string) []string {
	block = contractComplianceStripLineComments(block)
	parts := contractComplianceSplitSQLTopLevel(block)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		upper := strings.ToUpper(p)
		if strings.HasPrefix(upper, "CONSTRAINT ") || strings.HasPrefix(upper, "PRIMARY KEY") || strings.HasPrefix(upper, "FOREIGN KEY") || strings.HasPrefix(upper, "UNIQUE ") || strings.HasPrefix(upper, "CHECK ") {
			continue
		}
		fields := strings.Fields(p)
		if len(fields) == 0 {
			continue
		}
		col := strings.TrimSpace(fields[0])
		col = strings.Trim(col, "\"")
		if col == "" {
			continue
		}
		out = append(out, col)
	}
	return contractComplianceNormalizeList(out)
}

func contractComplianceStripLineComments(in string) string {
	lines := strings.Split(in, "\n")
	for i, line := range lines {
		if idx := strings.Index(line, "--"); idx >= 0 {
			line = line[:idx]
		}
		lines[i] = line
	}
	return strings.Join(lines, "\n")
}

func contractComplianceSplitSQLTopLevel(in string) []string {
	parts := make([]string, 0, 64)
	var b strings.Builder
	depth := 0
	inSingleQuote := false
	for _, r := range in {
		switch r {
		case '\'':
			inSingleQuote = !inSingleQuote
			b.WriteRune(r)
		case '(':
			if !inSingleQuote {
				depth++
			}
			b.WriteRune(r)
		case ')':
			if !inSingleQuote && depth > 0 {
				depth--
			}
			b.WriteRune(r)
		case ',':
			if !inSingleQuote && depth == 0 {
				parts = append(parts, b.String())
				b.Reset()
				continue
			}
			b.WriteRune(r)
		default:
			b.WriteRune(r)
		}
	}
	if strings.TrimSpace(b.String()) != "" {
		parts = append(parts, b.String())
	}
	return parts
}

func contractComplianceDeterministicSubscriptions(
	contractAgents map[string]contractComplianceAgent,
	systemNodes map[string]contractComplianceSystemNode,
) ([]contractComplianceSubscription, map[string]string) {
	out := make([]contractComplianceSubscription, 0, 16)
	implementationBySubscriber := map[string]string{}
	seen := map[string]struct{}{}

	for id, ag := range contractAgents {
		if !strings.EqualFold(strings.TrimSpace(ag.NodeType), "system") {
			continue
		}
		subscriber := strings.TrimSpace(id)
		if subscriber == "" {
			continue
		}
		impl := strings.TrimSpace(ag.Implementation)
		if impl != "" {
			implementationBySubscriber[subscriber] = impl
		}
		for _, evt := range append([]string{}, ag.SubscribesTo...) {
			eventType := strings.TrimSpace(evt)
			if eventType == "" {
				continue
			}
			key := subscriber + "|" + eventType
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, contractComplianceSubscription{Subscriber: subscriber, EventType: eventType})
		}
	}

	for id, node := range systemNodes {
		subscriber := strings.TrimSpace(id)
		if subscriber == "" {
			continue
		}
		impl := strings.TrimSpace(node.Implementation)
		if impl != "" {
			implementationBySubscriber[subscriber] = impl
		}
		for _, evt := range node.SubscribesTo {
			eventType := strings.TrimSpace(evt)
			if eventType == "" {
				continue
			}
			key := subscriber + "|" + eventType
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, contractComplianceSubscription{Subscriber: subscriber, EventType: eventType})
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Subscriber == out[j].Subscriber {
			return out[i].EventType < out[j].EventType
		}
		return out[i].Subscriber < out[j].Subscriber
	})
	return out, implementationBySubscriber
}

func contractComplianceParseHandledEventsFromFile(repoRoot, relPath string) (map[string]struct{}, error) {
	path := strings.TrimSpace(relPath)
	if path == "" {
		return map[string]struct{}{}, nil
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(repoRoot, path)
	}
	fset := token.NewFileSet()
	fileNode, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return nil, err
	}

	events := map[string]struct{}{}
	ast.Inspect(fileNode, func(n ast.Node) bool {
		switch typed := n.(type) {
		case *ast.CaseClause:
			for _, expr := range typed.List {
				if lit, ok := contractComplianceStringLiteral(expr); ok {
					events[strings.TrimSpace(lit)] = struct{}{}
				}
			}
		case *ast.BinaryExpr:
			if typed.Op != token.EQL {
				return true
			}
			if lit, ok := contractComplianceStringLiteral(typed.X); ok {
				events[strings.TrimSpace(lit)] = struct{}{}
			}
			if lit, ok := contractComplianceStringLiteral(typed.Y); ok {
				events[strings.TrimSpace(lit)] = struct{}{}
			}
		}
		return true
	})
	return events, nil
}

func contractComplianceStringLiteral(expr ast.Expr) (string, bool) {
	bl, ok := expr.(*ast.BasicLit)
	if !ok || bl.Kind != token.STRING {
		return "", false
	}
	v := strings.TrimSpace(bl.Value)
	if len(v) < 2 {
		return "", false
	}
	if (strings.HasPrefix(v, `"`) && strings.HasSuffix(v, `"`)) || (strings.HasPrefix(v, "`") && strings.HasSuffix(v, "`")) {
		return strings.TrimSpace(v[1 : len(v)-1]), true
	}
	return "", false
}

func contractComplianceCollectRuntimeTestNames(repoRoot string) (map[string]struct{}, error) {
	files, err := filepath.Glob(filepath.Join(repoRoot, "internal", "runtime", "*_test.go"))
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	out := map[string]struct{}{}
	for _, path := range files {
		fileNode, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return nil, parseErr
		}
		for _, decl := range fileNode.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil {
				continue
			}
			name := strings.TrimSpace(fn.Name.Name)
			if strings.HasPrefix(name, "Test") {
				out[name] = struct{}{}
			}
		}
	}
	return out, nil
}

func contractComplianceExpectedHandlerTestName(subscriber, eventType string) string {
	sub := contractComplianceSanitizeTestToken(subscriber)
	evt := contractComplianceSanitizeTestToken(eventType)
	if sub == "" || evt == "" {
		return ""
	}
	return "TestHandler_" + sub + "_" + evt
}

func contractComplianceSanitizeTestToken(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return ""
	}
	var b strings.Builder
	lastUnderscore := false
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	out := strings.TrimSpace(b.String())
	out = strings.Trim(out, "_")
	return out
}

func contractComplianceNormVersion(v string) string {
	v = strings.TrimSpace(v)
	v = strings.TrimPrefix(strings.ToLower(v), "v")
	return strings.TrimSpace(v)
}

func contractComplianceFormatErrs(errs []string, limit int) string {
	if len(errs) == 0 {
		return ""
	}
	if limit <= 0 || len(errs) <= limit {
		return strings.Join(errs, "\n- ")
	}
	trimmed := append([]string(nil), errs[:limit]...)
	trimmed = append(trimmed, fmt.Sprintf("... (%d more)", len(errs)-limit))
	return strings.Join(trimmed, "\n- ")
}

func contractComplianceAgentEmitterProduces(eventType, emitter string) bool {
	eventType = strings.TrimSpace(eventType)
	emitter = strings.TrimSpace(emitter)
	if eventType == "" || emitter == "" {
		return false
	}
	role := contractComplianceCatalogEmitterToCommgraphRole(emitter)
	if role == "" {
		return false
	}
	return containsString(commgraph.ProducerEventsForRole(role), eventType)
}

func contractComplianceSystemNodeProduces(systemNodes map[string]contractComplianceSystemNode, eventType, emitter string) bool {
	eventType = strings.TrimSpace(eventType)
	emitter = strings.TrimSpace(emitter)
	if eventType == "" || emitter == "" {
		return false
	}
	node, ok := systemNodes[emitter]
	if !ok {
		return false
	}
	return containsString(node.Produces, eventType)
}

func contractComplianceCatalogEmitterToCommgraphRole(emitter string) string {
	emitter = strings.TrimSpace(emitter)
	if emitter == "" {
		return ""
	}
	if v, ok := map[string]string{
		"opco-head-of-product": "vp-product",
		"opco-head-of-growth":  "vp-growth",
		"opco-chief-of-staff":  "chief-of-staff",
		"opco-support":         "support-agent",
		"opco-marketing":       "marketing-agent",
		"opco-cto":             "cto-agent",
		"opco-pm":              "pm-agent",
		"opco-qa":              "qa-agent",
		"opco-tech-writer":     "tech-writer",
		"opco-devops":          "devops-agent",
	}[emitter]; ok {
		return v
	}
	return emitter
}

func containsString(values []string, target string) bool {
	target = strings.TrimSpace(target)
	for _, v := range values {
		if strings.TrimSpace(v) == target {
			return true
		}
	}
	return false
}

func contractComplianceCatalogPayloadFields(payload any) []string {
	switch v := payload.(type) {
	case nil:
		return nil
	case []string:
		return contractComplianceNormalizeList(v)
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return contractComplianceNormalizeList(out)
	case map[string]any:
		out := make([]string, 0, len(v))
		for key := range v {
			out = append(out, strings.TrimSpace(key))
		}
		return contractComplianceNormalizeList(out)
	default:
		return nil
	}
}
