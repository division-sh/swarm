package runtime

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
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
	runtimemanager "empireai/internal/runtime/manager"
	runtimetools "empireai/internal/runtime/tools"
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
	RuntimeHandling   string   `yaml:"runtime_handling"`
	OwningNode        string   `yaml:"owning_node"`
	Payload           any      `yaml:"payload"`
}

type contractComplianceSystemNode struct {
	ID               string   `yaml:"id"`
	SubscribesTo     []string `yaml:"subscribes_to"`
	Produces         []string `yaml:"produces"`
	OwnedTransitions []string `yaml:"owned_transitions"`
	Implementation   string   `yaml:"implementation"`
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

type contractComplianceWorkflowSchema struct {
	Workflow struct {
		Name         string `yaml:"name"`
		Version      string `yaml:"version"`
		Entity       string `yaml:"entity"`
		EntityTable  string `yaml:"entity_table"`
		StateField   string `yaml:"state_field"`
		InitialStage string `yaml:"initial_stage"`
		Stages       []struct {
			ID string `yaml:"id"`
		} `yaml:"stages"`
		TerminalStages []string                                     `yaml:"terminal_stages"`
		Transitions    []contractComplianceWorkflowSchemaTransition `yaml:"transitions"`
		Timers         []struct {
			ID    string `yaml:"id"`
			Stage string `yaml:"stage"`
			Event string `yaml:"event"`
			Owner string `yaml:"owner"`
		} `yaml:"timers"`
	} `yaml:"workflow"`
}

type contractComplianceWorkflowSchemaTransition struct {
	ID      string   `yaml:"id"`
	From    any      `yaml:"from"`
	To      string   `yaml:"to"`
	Trigger string   `yaml:"trigger"`
	Node    string   `yaml:"node"`
	Guards  []string `yaml:"guards"`
	Actions []string `yaml:"actions"`
}

type contractComplianceGuardActionRegistry struct {
	Guards  map[string]contractComplianceGuardActionEntry
	Actions map[string]contractComplianceGuardActionEntry
}

type contractComplianceGuardActionEntry struct {
	ID              string `yaml:"id"`
	Category        string `yaml:"category"`
	PlatformBuiltin string `yaml:"platform_builtin"`
	Emits           string `yaml:"emits"`
}

type contractCompliancePlatformSpec struct {
	Platform struct {
		Name    string `yaml:"name"`
		Version string `yaml:"version"`
	} `yaml:"platform"`
	Vocabulary struct {
		Participant struct {
			Types map[string]struct {
				Execution string `yaml:"execution"`
			} `yaml:"types"`
		} `yaml:"participant"`
	} `yaml:"vocabulary"`
	ContractFormats map[string]any `yaml:"contract_formats"`
	WorkflowState   struct {
		Fields map[string]struct {
			Type string `yaml:"type"`
		} `yaml:"fields"`
	} `yaml:"workflow_state"`
	BuiltinHooks struct {
		Guards  []contractComplianceBuiltinHook `yaml:"guards"`
		Actions []contractComplianceBuiltinHook `yaml:"actions"`
	} `yaml:"builtin_hooks"`
	ComplianceRules map[string][]struct {
		ID string `yaml:"id"`
	} `yaml:"compliance_rules"`
	FileLayout struct {
		MigrationNote string `yaml:"migration_note"`
	} `yaml:"file_layout"`
}

type contractComplianceBuiltinHook struct {
	ID string `yaml:"id"`
}

func TestContractCompliance(t *testing.T) {
	t.Helper()
	_ = runtimetools.EventSchemaSnapshot()

	repoRoot := contractComplianceRepoRoot(t)
	contractAgents := contractComplianceLoadAgentTools(t, repoRoot)
	agentConfigMap := contractComplianceLoadAgentConfigMap(t, repoRoot)
	eventCatalog := contractComplianceLoadEventCatalog(t, repoRoot)
	systemNodes := contractComplianceLoadSystemNodes(t, repoRoot)
	routes := contractComplianceLoadRoutes(t, repoRoot)
	specVersion := contractComplianceLoadSpecVersion(t, repoRoot)
	toolingLockVersion := contractComplianceLoadToolingLockVersion(t, repoRoot)
	toolSchemas := contractComplianceLoadToolSchemas(t, repoRoot)
	platformSpec := contractComplianceLoadPlatformSpec(t, repoRoot)

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

		// 2c) OpCo leadership subscriptions in DefaultOpCoRoster() must match contract.
		roster := runtimemanager.DefaultOpCoRoster("contract-compliance")
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
				t.Fatalf("DefaultOpCoRoster leadership subscriptions mismatch for %s (%s)", contractID, diff)
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
		schemas := runtimetools.EventSchemaSnapshot()
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
		if got, want := len(tables), 38; got != want {
			errs = append(errs, fmt.Sprintf("canonical DDL table count mismatch: got=%d want=%d", got, want))
		}

		expected := map[string][]string{
			"runtime_config":            {"id", "config_yaml", "config_path", "applied_at", "created_at"},
			"pipeline_receipts":         {"event_id", "status", "error", "processed_at"},
			"scan_accumulators":         {"scan_id", "campaign_id", "mode", "geography", "expected", "complete", "completed_by", "reports", "discovered", "skipped", "pending_dedup", "timeout_at", "started_at", "completed_at", "created_at", "updated_at"},
			"pending_dedup_candidates":  {"dedup_event_id", "scan_id", "campaign_id", "mode", "name", "geography", "discovery_mode", "signal_strength", "payload", "existing_id", "status", "created_at", "resolved_at"},
			"validation_pipelines":      {"vertical_id", "status", "g1_research", "g2_spec", "g3_cto", "g4_brand", "research_payload", "spec_payload", "cto_payload", "brand_payload", "scoring_payload", "revision_count", "inner_revision_count", "spec_version", "packaging_requested", "packaging_requested_at", "packaging_retries", "created_at", "updated_at"},
			"workflow_instances":        {"instance_id", "workflow_name", "workflow_version", "current_stage", "entered_stage_at", "transition_history", "accumulator_state", "timer_state", "metadata", "created_at", "updated_at"},
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
		if got, want := contractComplianceNormVersion("v2.2.1"), contractComplianceNormVersion(specVersion); got != want {
			t.Fatalf("runtime spec version mismatch: got=%q want=%q", "v2.2.1", specVersion)
		}
		if got := contractComplianceNormVersion(toolingLockVersion); got == "" {
			t.Fatal("tooling.lock contract_format_version missing")
		}
		roster := runtimemanager.DefaultOpCoRoster("contract-version-check")
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
				switch strings.TrimSpace(sub.Subscriber) {
				case "pipeline-coordinator":
					extraPaths, extraErr := filepath.Glob(filepath.Join(repoRoot, "internal", "runtime", "pipeline", "workflow_node*.go"))
					if extraErr != nil {
						errs = append(errs, fmt.Sprintf("glob workflow node coverage: %v", extraErr))
						continue
					}
					for _, extraPath := range extraPaths {
						extraEvents, parseErr := contractComplianceParseHandledEventsFromFile(repoRoot, extraPath)
						if parseErr != nil {
							errs = append(errs, fmt.Sprintf("parse handler coverage %s: %v", extraPath, parseErr))
							continue
						}
						for evt := range extraEvents {
							handledEvents[evt] = struct{}{}
						}
					}
				case "scoring-node":
					extraPath := filepath.Join(repoRoot, "internal", "runtime", "pipeline", "workflow_node_scoring.go")
					extraEvents, parseErr := contractComplianceParseHandledEventsFromFile(repoRoot, extraPath)
					if parseErr != nil {
						errs = append(errs, fmt.Sprintf("parse handler coverage %s: %v", extraPath, parseErr))
						continue
					}
					for evt := range extraEvents {
						handledEvents[evt] = struct{}{}
					}
				}
				handledEventsCache[implPath] = handledEvents
			}
			if _, ok := handledEvents[sub.EventType]; !ok {
				errs = append(errs, fmt.Sprintf("subscription has no handler case: subscriber=%s event=%s implementation=%s", sub.Subscriber, sub.EventType, implPath))
			}
			expectedTest := contractComplianceExpectedHandlerTestName(sub.Subscriber, sub.EventType)
			if expectedTest != "" {
				if _, ok := handlerTests[expectedTest]; !ok && !contractCompliancePackageTestMentionsEvent(repoRoot, implPath, sub.EventType) {
					errs = append(errs, fmt.Sprintf("subscription missing handler test: want=%s (subscriber=%s event=%s)", expectedTest, sub.Subscriber, sub.EventType))
				}
			}
		}

		// Interceptor parity check for consumed runtime events:
		// any event listed in interceptPolicy must have a corresponding handleEvent case
		// (except spec.revision_needed, handled in Intercept special-case branch).
		pipelinePath := filepath.Join(repoRoot, "internal", "runtime", "pipeline", "coordinator.go")
		interceptEvents, handleEvents, _, err := parsePipelineInterceptorCoverage(pipelinePath)
		if err != nil {
			errs = append(errs, fmt.Sprintf("parse interceptor coverage: %v", err))
		} else {
			workflowNodePaths, nodeErr := filepath.Glob(filepath.Join(repoRoot, "internal", "runtime", "pipeline", "workflow_node*.go"))
			if nodeErr != nil {
				errs = append(errs, fmt.Sprintf("glob workflow node coverage: %v", nodeErr))
			} else {
				for _, workflowNodePath := range workflowNodePaths {
					nodeHandledEvents, parseErr := contractComplianceParseHandledEventsFromFile(repoRoot, workflowNodePath)
					if parseErr != nil {
						errs = append(errs, fmt.Sprintf("parse workflow node coverage %s: %v", workflowNodePath, parseErr))
						continue
					}
					for evt := range nodeHandledEvents {
						handleEvents[evt] = struct{}{}
					}
				}
			}
			nonLocalIntercepts := map[string]struct{}{}
			for id, node := range systemNodes {
				if strings.TrimSpace(id) == "pipeline-coordinator" {
					continue
				}
				for _, evt := range node.SubscribesTo {
					evt = strings.TrimSpace(evt)
					if evt == "" {
						continue
					}
					nonLocalIntercepts[evt] = struct{}{}
				}
			}
			for evt := range interceptEvents {
				if strings.TrimSpace(evt) == "spec.revision_needed" {
					continue
				}
				if _, ok := nonLocalIntercepts[evt]; ok {
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

		runtimeDefs := runtimetools.NewExecutor(nil, nil, nil).ToolDefinitions()
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
			exec := runtimetools.NewExecutor(nil, nil, nil)
			if err := exec.ValidateRuntimeToolInputForTest(toolName, validPayload); err != nil {
				errs = append(errs, fmt.Sprintf("tool %s rejected valid contract payload: %v", toolName, err))
			}

			invalidPayload, err := contractComplianceBuildInvalidToolPayload(entry.InputSchema)
			if err != nil {
				errs = append(errs, fmt.Sprintf("tool %s invalid payload generation failed: %v", toolName, err))
				continue
			}
			if err := exec.ValidateRuntimeToolInputForTest(toolName, invalidPayload); err == nil {
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

	t.Run("platform_spec", func(t *testing.T) {
		if strings.TrimSpace(platformSpec.Platform.Name) == "" {
			t.Fatal("platform.name missing")
		}
		if strings.TrimSpace(platformSpec.Platform.Version) == "" {
			t.Fatal("platform.version missing")
		}
		if len(platformSpec.Vocabulary.Participant.Types) == 0 {
			t.Fatal("vocabulary.participant.types missing")
		}
		for _, participant := range []string{"system_node", "agent", "runtime"} {
			if _, ok := platformSpec.Vocabulary.Participant.Types[participant]; !ok {
				t.Fatalf("participant type %q missing from platform spec", participant)
			}
		}
		if len(platformSpec.ContractFormats) == 0 {
			t.Fatal("contract_formats missing")
		}
		if len(platformSpec.WorkflowState.Fields) == 0 {
			t.Fatal("workflow_state.fields missing")
		}
		for _, field := range []string{"instance_id", "workflow_name", "workflow_version", "current_stage", "transition_history", "accumulator_state", "timer_state", "metadata"} {
			if _, ok := platformSpec.WorkflowState.Fields[field]; !ok {
				t.Fatalf("workflow_state field %q missing", field)
			}
		}
		if len(platformSpec.BuiltinHooks.Guards) == 0 {
			t.Fatal("builtin_hooks.guards missing")
		}
		if len(platformSpec.BuiltinHooks.Actions) == 0 {
			t.Fatal("builtin_hooks.actions missing")
		}
		if len(platformSpec.ComplianceRules) == 0 {
			t.Fatal("compliance_rules missing")
		}
		if strings.TrimSpace(platformSpec.FileLayout.MigrationNote) == "" {
			t.Fatal("file_layout.migration_note missing")
		}
	})

	t.Run("workflow_schema", func(t *testing.T) {
		workflow := contractComplianceLoadWorkflowSchema(t, repoRoot)
		registry := contractComplianceLoadGuardActionRegistry(t, repoRoot)
		builtinGuards := contractComplianceBuiltinIDs(platformSpec.BuiltinHooks.Guards)
		builtinActions := contractComplianceBuiltinIDs(platformSpec.BuiltinHooks.Actions)
		implicitPlatformActions := map[string]struct{}{
			"record_transition":   {},
			"update_stage":        {},
			"cancel_stage_timers": {},
			"start_stage_timers":  {},
		}
		errs := make([]string, 0, 16)
		guardAliases := map[string]string{}
		for id, entry := range registry.Guards {
			if builtin := strings.TrimSpace(entry.PlatformBuiltin); builtin != "" {
				if _, ok := builtinGuards[builtin]; !ok {
					errs = append(errs, fmt.Sprintf("guard %s aliases unknown platform builtin %s", id, builtin))
				}
				if prev, ok := guardAliases[builtin]; ok && prev != id {
					errs = append(errs, fmt.Sprintf("platform guard builtin %s is aliased by both %s and %s", builtin, prev, id))
				} else {
					guardAliases[builtin] = id
				}
			}
		}
		actionAliases := map[string]string{}
		for id, entry := range registry.Actions {
			if builtin := strings.TrimSpace(entry.PlatformBuiltin); builtin != "" {
				if _, ok := builtinActions[builtin]; !ok {
					errs = append(errs, fmt.Sprintf("action %s aliases unknown platform builtin %s", id, builtin))
				}
				if _, implicit := implicitPlatformActions[builtin]; implicit {
					errs = append(errs, fmt.Sprintf("action %s aliases implicit platform action %s", id, builtin))
				}
				if prev, ok := actionAliases[builtin]; ok && prev != id {
					errs = append(errs, fmt.Sprintf("platform action builtin %s is aliased by both %s and %s", builtin, prev, id))
				} else {
					actionAliases[builtin] = id
				}
			}
		}
		for _, tr := range workflow.Workflow.Transitions {
			transitionID := strings.TrimSpace(tr.ID)
			for _, guard := range tr.Guards {
				guard = strings.TrimSpace(guard)
				if guard == "" {
					continue
				}
				if _, ok := registry.Guards[guard]; !ok {
					if _, builtin := builtinGuards[guard]; builtin {
						continue
					}
					errs = append(errs, fmt.Sprintf("transition %s references unknown guard %s", transitionID, guard))
				}
			}
			for _, action := range tr.Actions {
				action = strings.TrimSpace(action)
				if action == "" {
					continue
				}
				if _, ok := registry.Actions[action]; !ok {
					if _, builtin := builtinActions[action]; builtin {
						if _, implicit := implicitPlatformActions[action]; implicit {
							errs = append(errs, fmt.Sprintf("transition %s references implicit platform action %s", transitionID, action))
						}
						continue
					}
					errs = append(errs, fmt.Sprintf("transition %s references unknown action %s", transitionID, action))
				}
			}
		}
		if len(errs) > 0 {
			t.Fatalf("workflow_schema failures (%d):\n- %s", len(errs), contractComplianceFormatErrs(errs, 40))
		}
	})

	t.Run("workflow_nodes", func(t *testing.T) {
		workflow := contractComplianceLoadWorkflowSchema(t, repoRoot)
		validNodes := map[string]struct{}{
			"runtime": {},
			"human":   {},
		}
		participantTypes := map[string]string{
			"runtime": "runtime",
			"human":   "runtime",
		}
		for id, ag := range contractAgents {
			validNodes[strings.TrimSpace(id)] = struct{}{}
			participantTypes[strings.TrimSpace(id)] = "agent"
			if role := strings.TrimSpace(ag.Role); role != "" {
				validNodes[role] = struct{}{}
				participantTypes[role] = "agent"
			}
		}
		for id := range systemNodes {
			validNodes[strings.TrimSpace(id)] = struct{}{}
			participantTypes[strings.TrimSpace(id)] = "system_node"
		}
		errs := make([]string, 0, 16)
		for _, tr := range workflow.Workflow.Transitions {
			node := strings.TrimSpace(tr.Node)
			if node == "" {
				errs = append(errs, fmt.Sprintf("transition %s missing node", tr.ID))
				continue
			}
			if _, ok := validNodes[node]; !ok {
				errs = append(errs, fmt.Sprintf("transition %s references unknown node %s", tr.ID, node))
				continue
			}
			if participantType, ok := participantTypes[node]; !ok {
				errs = append(errs, fmt.Sprintf("transition %s node %s has unknown participant type", tr.ID, node))
			} else if _, allowed := platformSpec.Vocabulary.Participant.Types[participantType]; !allowed {
				errs = append(errs, fmt.Sprintf("transition %s node %s resolves to unsupported participant type %s", tr.ID, node, participantType))
			}
		}
		for _, timer := range workflow.Workflow.Timers {
			owner := strings.TrimSpace(timer.Owner)
			if owner == "" {
				errs = append(errs, fmt.Sprintf("timer %s missing owner", timer.ID))
				continue
			}
			if _, ok := validNodes[owner]; !ok {
				errs = append(errs, fmt.Sprintf("timer %s references unknown owner %s", timer.ID, owner))
			}
		}
		if len(errs) > 0 {
			t.Fatalf("workflow_nodes failures (%d):\n- %s", len(errs), contractComplianceFormatErrs(errs, 40))
		}
	})

	t.Run("workflow_node_coverage", func(t *testing.T) {
		workflow := contractComplianceLoadWorkflowSchema(t, repoRoot)
		transitionByID := make(map[string]contractComplianceWorkflowSchemaTransition, len(workflow.Workflow.Transitions))
		for _, tr := range workflow.Workflow.Transitions {
			if id := strings.TrimSpace(tr.ID); id != "" {
				transitionByID[id] = tr
			}
		}
		errs := make([]string, 0, 16)
		for nodeID, node := range systemNodes {
			nodeID = strings.TrimSpace(nodeID)
			subs := contractComplianceNormalizeSet(node.SubscribesTo)
			produces := contractComplianceNormalizeSet(node.Produces)
			for _, transitionID := range node.OwnedTransitions {
				transitionID = strings.TrimSpace(transitionID)
				if transitionID == "" {
					continue
				}
				tr, ok := transitionByID[transitionID]
				if !ok {
					errs = append(errs, fmt.Sprintf("system node %s owns unknown transition %s", nodeID, transitionID))
					continue
				}
				if owner := strings.TrimSpace(tr.Node); owner != nodeID {
					errs = append(errs, fmt.Sprintf("system node %s owns transition %s but workflow owner is %s", nodeID, transitionID, owner))
				}
				trigger := strings.TrimSpace(tr.Trigger)
				if trigger == "" {
					errs = append(errs, fmt.Sprintf("owned transition %s for node %s is missing trigger", transitionID, nodeID))
					continue
				}
				if !contractComplianceUsesOwningNodeModel(eventCatalog) {
					if _, ok := subs[trigger]; !ok {
						if _, emitted := produces[trigger]; !emitted {
							errs = append(errs, fmt.Sprintf("system node %s cannot see owned transition trigger %s for %s", nodeID, trigger, transitionID))
						}
					}
				}
			}
		}
		if len(errs) > 0 {
			t.Fatalf("workflow_node_coverage failures (%d):\n- %s", len(errs), contractComplianceFormatErrs(errs, 40))
		}
	})

	t.Run("workflow_graph", func(t *testing.T) {
		workflow := contractComplianceLoadWorkflowSchema(t, repoRoot)
		registry := contractComplianceLoadGuardActionRegistry(t, repoRoot)
		stageIDs := map[string]struct{}{}
		for _, stage := range workflow.Workflow.Stages {
			if id := strings.TrimSpace(stage.ID); id != "" {
				stageIDs[id] = struct{}{}
			}
		}
		errs := make([]string, 0, 32)
		initialStage := strings.TrimSpace(workflow.Workflow.InitialStage)
		if _, ok := stageIDs[initialStage]; !ok {
			errs = append(errs, fmt.Sprintf("initial_stage %s is not declared in workflow.stages", initialStage))
		}
		terminal := map[string]struct{}{}
		for _, stageID := range workflow.Workflow.TerminalStages {
			stageID = strings.TrimSpace(stageID)
			if _, ok := stageIDs[stageID]; !ok {
				errs = append(errs, fmt.Sprintf("terminal stage %s is not declared in workflow.stages", stageID))
			}
			terminal[stageID] = struct{}{}
		}
		outbound := map[string]int{}
		adj := map[string][]string{}
		validParticipants := map[string]struct{}{"runtime": {}, "human": {}}
		for id := range systemNodes {
			validParticipants[strings.TrimSpace(id)] = struct{}{}
		}
		for id, ag := range contractAgents {
			validParticipants[strings.TrimSpace(id)] = struct{}{}
			if role := strings.TrimSpace(ag.Role); role != "" {
				validParticipants[role] = struct{}{}
			}
		}
		for _, tr := range workflow.Workflow.Transitions {
			to := strings.TrimSpace(tr.To)
			if _, ok := stageIDs[to]; !ok {
				errs = append(errs, fmt.Sprintf("transition %s references unknown to-stage %s", tr.ID, to))
			}
			for _, from := range contractComplianceWorkflowFromStages(tr.From) {
				if from != "*" {
					if _, ok := stageIDs[from]; !ok {
						errs = append(errs, fmt.Sprintf("transition %s references unknown from-stage %s", tr.ID, from))
						continue
					}
					outbound[from]++
					if _, terminalStage := terminal[from]; terminalStage {
						errs = append(errs, fmt.Sprintf("terminal stage %s has outgoing transition %s", from, tr.ID))
					}
					adj[from] = append(adj[from], to)
				}
			}
			if _, ok := eventCatalog[strings.TrimSpace(tr.Trigger)]; !ok {
				errs = append(errs, fmt.Sprintf("transition %s trigger %s missing from event catalog", tr.ID, tr.Trigger))
			}
			if _, ok := validParticipants[strings.TrimSpace(tr.Node)]; !ok {
				errs = append(errs, fmt.Sprintf("transition %s node %s missing from participants", tr.ID, tr.Node))
			} else if !contractComplianceUsesOwningNodeModel(eventCatalog) &&
				!contractComplianceTransitionNodeCanSeeTrigger(strings.TrimSpace(tr.Node), strings.TrimSpace(tr.Trigger), contractAgents, systemNodes, eventCatalog) {
				errs = append(errs, fmt.Sprintf("transition %s node %s is neither subscribed to nor emitter of %s", tr.ID, tr.Node, tr.Trigger))
			}
			for _, actionID := range tr.Actions {
				entry, ok := registry.Actions[strings.TrimSpace(actionID)]
				if !ok {
					continue
				}
				if emits := strings.TrimSpace(entry.Emits); emits != "" {
					if _, ok := eventCatalog[emits]; !ok {
						errs = append(errs, fmt.Sprintf("transition %s action %s emits missing catalog event %s", tr.ID, actionID, emits))
					}
				}
			}
		}
		for _, stage := range workflow.Workflow.Stages {
			id := strings.TrimSpace(stage.ID)
			if id == "" || id == initialStage {
				continue
			}
			if _, terminalStage := terminal[id]; terminalStage {
				continue
			}
			if !contractComplianceStageReachable(initialStage, id, adj) {
				errs = append(errs, fmt.Sprintf("stage %s is unreachable from initial stage %s", id, initialStage))
			}
			if outbound[id] == 0 && id != "winding_down" {
				errs = append(errs, fmt.Sprintf("non-terminal stage %s has no outbound transitions", id))
			}
		}
		for _, timer := range workflow.Workflow.Timers {
			stage := strings.TrimSpace(timer.Stage)
			if stage != "" && stage != "*" {
				if _, ok := stageIDs[stage]; !ok {
					errs = append(errs, fmt.Sprintf("timer %s references unknown stage %s", timer.ID, stage))
				}
			}
			if _, ok := eventCatalog[strings.TrimSpace(timer.Event)]; !ok {
				errs = append(errs, fmt.Sprintf("timer %s event %s missing from event catalog", timer.ID, timer.Event))
			}
		}
		covered := map[string]string{}
		for nodeID, node := range systemNodes {
			nodeSubs := make(map[string]struct{}, len(node.SubscribesTo))
			nodeProduces := make(map[string]struct{}, len(node.Produces))
			for _, evt := range node.SubscribesTo {
				if evt = strings.TrimSpace(evt); evt != "" {
					nodeSubs[evt] = struct{}{}
				}
			}
			for _, evt := range node.Produces {
				if evt = strings.TrimSpace(evt); evt != "" {
					nodeProduces[evt] = struct{}{}
				}
			}
			for _, transitionID := range node.OwnedTransitions {
				transitionID = strings.TrimSpace(transitionID)
				if transitionID == "" {
					continue
				}
				tr, ok := contractComplianceWorkflowTransitionByID(workflow.Workflow.Transitions, transitionID)
				if !ok {
					errs = append(errs, fmt.Sprintf("system node %s owns unknown transition %s", nodeID, transitionID))
					continue
				}
				if owner := strings.TrimSpace(tr.Node); owner != strings.TrimSpace(nodeID) {
					errs = append(errs, fmt.Sprintf("transition %s is owned by %s in workflow but listed under %s", transitionID, owner, nodeID))
				}
				trigger := strings.TrimSpace(tr.Trigger)
				if !contractComplianceUsesOwningNodeModel(eventCatalog) {
					if _, ok := nodeSubs[trigger]; !ok {
						if _, ok := nodeProduces[trigger]; !ok {
							errs = append(errs, fmt.Sprintf("system node %s owns transition %s but neither subscribes to nor emits %s", nodeID, transitionID, trigger))
						}
					}
				}
				covered[transitionID] = nodeID
			}
		}
		for _, tr := range workflow.Workflow.Transitions {
			transitionID := strings.TrimSpace(tr.ID)
			owner := strings.TrimSpace(tr.Node)
			if owner == "runtime" {
				continue
			}
			if _, systemNode := systemNodes[owner]; !systemNode {
				continue
			}
			if _, ok := covered[transitionID]; !ok {
				errs = append(errs, fmt.Sprintf("system-node transition %s missing owned_transitions coverage", transitionID))
			}
		}
		if len(errs) > 0 {
			t.Fatalf("workflow_graph failures (%d):\n- %s", len(errs), contractComplianceFormatErrs(errs, 60))
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
	path := runtimecontracts.ResolveWorkflowContractPaths(repoRoot).AgentRegistryFile
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
	path := runtimecontracts.ResolveWorkflowContractPaths(repoRoot).EventCatalogFile
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
	path := runtimecontracts.ResolveWorkflowContractPaths(repoRoot).SystemNodesFile
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
	path := runtimecontracts.ResolveWorkflowContractPaths(repoRoot).ToolSchemasFile
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

func contractComplianceLoadWorkflowSchema(t *testing.T, repoRoot string) contractComplianceWorkflowSchema {
	t.Helper()
	path := runtimecontracts.ResolveWorkflowContractPaths(repoRoot).WorkflowSchemaFile
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var out contractComplianceWorkflowSchema
	if err := yaml.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return out
}

func contractComplianceLoadGuardActionRegistry(t *testing.T, repoRoot string) contractComplianceGuardActionRegistry {
	t.Helper()
	path := runtimecontracts.ResolveWorkflowContractPaths(repoRoot).GuardRegistryFile
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var doc struct {
		Guards  []contractComplianceGuardActionEntry `yaml:"guards"`
		Actions []contractComplianceGuardActionEntry `yaml:"actions"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	out := contractComplianceGuardActionRegistry{
		Guards:  make(map[string]contractComplianceGuardActionEntry, len(doc.Guards)),
		Actions: make(map[string]contractComplianceGuardActionEntry, len(doc.Actions)),
	}
	for _, item := range doc.Guards {
		if id := strings.TrimSpace(item.ID); id != "" {
			out.Guards[id] = item
		}
	}
	for _, item := range doc.Actions {
		if id := strings.TrimSpace(item.ID); id != "" {
			out.Actions[id] = item
		}
	}
	return out
}

func contractComplianceLoadPlatformSpec(t *testing.T, repoRoot string) contractCompliancePlatformSpec {
	t.Helper()
	path := runtimecontracts.ResolveWorkflowContractPaths(repoRoot).PlatformSpecFile
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var out contractCompliancePlatformSpec
	if err := yaml.Unmarshal(raw, &out); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return out
}

func contractComplianceBuiltinIDs(items []contractComplianceBuiltinHook) map[string]struct{} {
	out := make(map[string]struct{}, len(items))
	for _, item := range items {
		if id := strings.TrimSpace(item.ID); id != "" {
			out[id] = struct{}{}
		}
	}
	return out
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

func contractComplianceWorkflowFromStages(raw any) []string {
	switch typed := raw.(type) {
	case string:
		if v := strings.TrimSpace(typed); v != "" {
			return []string{v}
		}
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if s, ok := item.(string); ok {
				if v := strings.TrimSpace(s); v != "" {
					out = append(out, v)
				}
			}
		}
		return out
	case []string:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if v := strings.TrimSpace(item); v != "" {
				out = append(out, v)
			}
		}
		return out
	}
	return nil
}

func contractComplianceTransitionNodeCanSeeTrigger(node, trigger string, contractAgents map[string]contractComplianceAgent, systemNodes map[string]contractComplianceSystemNode, catalog map[string]contractComplianceCatalogEvent) bool {
	if node == "" || trigger == "" {
		return false
	}
	if node == "runtime" || node == "human" {
		return true
	}
	if evt, ok := catalog[trigger]; ok {
		emitter := strings.TrimSpace(evt.Emitter)
		if emitter == node {
			return true
		}
		if owner := strings.TrimSpace(evt.OwningNode); owner != "" && owner == node {
			return true
		}
		switch strings.TrimSpace(evt.RuntimeHandling) {
		case "consuming", "dual_delivery", "projection", "stage_projection":
			if strings.TrimSpace(evt.OwningNode) != "" {
				return true
			}
		}
		if ag, ok := contractAgents[emitter]; ok {
			if strings.TrimSpace(ag.Role) == node {
				return true
			}
		}
	}
	if sn, ok := systemNodes[node]; ok {
		for _, sub := range sn.SubscribesTo {
			if strings.TrimSpace(sub) == trigger {
				return true
			}
		}
		return false
	}
	if ag, ok := contractAgents[node]; ok {
		for _, sub := range append(append([]string{}, ag.Subscriptions...), append(ag.SubscriptionsBootstrap, ag.SubscribesTo...)...) {
			if strings.TrimSpace(sub) == trigger {
				return true
			}
		}
		return false
	}
	for _, ag := range contractAgents {
		if strings.TrimSpace(ag.Role) != node {
			continue
		}
		for _, sub := range append(append([]string{}, ag.Subscriptions...), append(ag.SubscriptionsBootstrap, ag.SubscribesTo...)...) {
			if strings.TrimSpace(sub) == trigger {
				return true
			}
		}
	}
	return false
}

func contractComplianceUsesOwningNodeModel(catalog map[string]contractComplianceCatalogEvent) bool {
	for _, evt := range catalog {
		if strings.TrimSpace(evt.OwningNode) != "" {
			return true
		}
	}
	return false
}

func contractComplianceStageReachable(initial, target string, adj map[string][]string) bool {
	if initial == "" || target == "" {
		return false
	}
	if initial == target {
		return true
	}
	seen := map[string]struct{}{initial: {}}
	queue := []string{initial}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, next := range adj[cur] {
			if _, ok := seen[next]; ok {
				continue
			}
			if next == target {
				return true
			}
			seen[next] = struct{}{}
			queue = append(queue, next)
		}
	}
	return false
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

func contractComplianceNormalizeSet(in []string) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v != "" {
			out[v] = struct{}{}
		}
	}
	return out
}

func contractComplianceWorkflowTransitionByID(in []contractComplianceWorkflowSchemaTransition, wantID string) (contractComplianceWorkflowSchemaTransition, bool) {
	wantID = strings.TrimSpace(wantID)
	for _, item := range in {
		if strings.TrimSpace(item.ID) == wantID {
			return item, true
		}
	}
	return contractComplianceWorkflowSchemaTransition{}, false
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
	fset := token.NewFileSet()
	out := map[string]struct{}{}
	root := filepath.Join(repoRoot, "internal", "runtime")
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fileNode, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return parseErr
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
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func contractCompliancePackageTestMentionsEvent(repoRoot, relPath, eventType string) bool {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return false
	}
	path := strings.TrimSpace(relPath)
	if path == "" {
		return false
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(repoRoot, path)
	}
	dir := filepath.Dir(path)
	found := false
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(p, "_test.go") {
			return nil
		}
		raw, readErr := os.ReadFile(p)
		if readErr != nil {
			return nil
		}
		if strings.Contains(string(raw), eventType) {
			found = true
			return fs.SkipAll
		}
		return nil
	})
	return found
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
