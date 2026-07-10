package apispec

import (
	"fmt"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// #1654 source-authority proofs for cli_specification.topology_revision_v2_2.
// These tests bind the promoted CLI v2.2 topology target to structural rules:
// contract-bearing groups on every catalog row, complete per-spelling
// dispositions, and target rows that never claim implemented behavior.

var cliGroupAllowedValues = map[string]bool{
	"getting_started": true,
	"author_validate": true,
	"run_operate":     true,
	"observe_debug":   true,
	"utilities":       true,
}

// Rows exempt from the required group field per
// cli_specification.topology_revision_v2_2.group_field: the root row renders
// the groups; retired hidden stubs never render in help.
var cliGroupExemptRows = map[string]bool{
	"root":                      true,
	"investigate":               true,
	"control_mailbox":           true,
	"fork_legacy_harness_forms": true,
	"unpromoted_review_only_legacy_spellings": true,
}

func cliSpecification(t *testing.T) *yaml.Node {
	t.Helper()
	return mustMappingValue(t, loadPlatformSpecYAMLNode(t), "cli_specification")
}

func forEachMappingEntry(t *testing.T, node *yaml.Node, visit func(key string, value *yaml.Node)) {
	t.Helper()
	if node.Kind != yaml.MappingNode {
		t.Fatalf("node kind = %d, want mapping", node.Kind)
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		visit(node.Content[i].Value, node.Content[i+1])
	}
}

func TestCLICommandCatalogRowsDeclareContractBearingGroups(t *testing.T) {
	catalog := mustMappingValue(t, cliSpecification(t), "command_catalog")
	rows := 0
	forEachMappingEntry(t, catalog, func(row string, value *yaml.Node) {
		if cliGroupExemptRows[row] {
			return
		}
		if value.Kind != yaml.MappingNode || mappingValue(value, "command") == nil {
			return // policy/ledger sub-blocks, not command rows
		}
		rows++
		group := mappingValue(value, "group")
		if group == nil {
			t.Errorf("command_catalog.%s: missing required contract-bearing group field", row)
			return
		}
		if !cliGroupAllowedValues[group.Value] {
			t.Errorf("command_catalog.%s: group %q not in allowed vocabulary", row, group.Value)
		}
	})
	if rows < 40 {
		t.Fatalf("command rows visited = %d, want >= 40; row detection is broken", rows)
	}
}

func TestCLIVerifyJSONShapeIncludesStructuredFailureFindings(t *testing.T) {
	outputContract := mustMappingValue(t, mustMappingValue(t, cliSpecification(t), "foundations"), "output_contract")
	commandSupport := mustMappingValue(t, outputContract, "command_support")
	registry := mustMappingValue(t, commandSupport, "output_conformance_registry")
	rows := mustMappingValue(t, registry, "rows")
	verify := mustMappingValue(t, rows, "verify")
	assertScalarValue(t, mustMappingValue(t, verify, "classification"), "shared_output")
	jsonShape := mustMappingValue(t, verify, "json_shape")

	for _, want := range []string{
		"errors",
		"workspace_backend",
		"warnings",
		"lint_evidence",
		"check_id",
		"severity",
		"location",
		"message",
		"remediation",
		"evidence",
		"ok: false",
		"human stderr",
	} {
		assertScalarContains(t, jsonShape, want)
	}

	humanStreams := mustMappingValue(t, verify, "human_text_streams")
	for _, want := range []string{
		"[BLOCKER]",
		"[WARN]",
		"[INFO]",
		"ERROR:",
		"WARNING:",
	} {
		assertScalarContains(t, mustMappingValue(t, humanStreams, "stderr"), want)
	}
}

func TestCLIOutputConformanceRegistryPromotedAsCurrentStateOwner(t *testing.T) {
	outputContract := mustMappingValue(t, mustMappingValue(t, cliSpecification(t), "foundations"), "output_contract")
	commandSupport := mustMappingValue(t, outputContract, "command_support")
	if mappingValue(commandSupport, "currently_implemented_consumers") != nil {
		t.Fatal("command_support.currently_implemented_consumers must not remain as a second output-conformance registry")
	}
	if mappingValue(commandSupport, "implemented_output_mode_first_slice") != nil {
		t.Fatal("command_support.implemented_output_mode_first_slice must not remain as a second output-conformance registry")
	}
	if mappingValue(commandSupport, "implemented_color_policy_first_slice") != nil {
		t.Fatal("command_support.implemented_color_policy_first_slice must not remain as a second output-conformance registry")
	}

	registry := mustMappingValue(t, commandSupport, "output_conformance_registry")
	assertScalarValue(t, mustMappingValue(t, registry, "promoted_by"), "#1821")
	assertScalarValue(t, mustMappingValue(t, registry, "canonical_owner"), "platform-spec.yaml#cli_specification.foundations.output_contract.command_support.output_conformance_registry")
	assertScalarValue(t, mustMappingValue(t, registry, "implementation_owner"), "cmd/swarm/cli_output_conformance_registry_test.go")
	assertScalarContains(t, mustMappingValue(t, registry, "rule"), "single living current-state authority")
	assertScalarContains(t, mustMappingValue(t, mustMappingValue(t, registry, "ratchet_rules"), "full_catalog_coverage"), "Unclassified rows fail closed")
	assertScalarContains(t, mustMappingValue(t, mustMappingValue(t, registry, "ratchet_rules"), "no_output_byte_changes"), "does not change command output bytes")

	values := mustMappingValue(t, registry, "classification_values")
	for _, key := range []string{"shared_output", "exception", "split"} {
		if mappingValue(values, key) == nil {
			t.Fatalf("output_conformance_registry.classification_values missing %s", key)
		}
	}

	color := mustMappingValue(t, outputContract, "color_control")
	assertScalarContains(t, mustMappingValue(t, color, "consumer_registry"), "output_conformance_registry")
}

func TestCLIHumanCodeProjectionOwnsCurrentProducerTuples(t *testing.T) {
	outputContract := mustMappingValue(t, mustMappingValue(t, cliSpecification(t), "foundations"), "output_contract")
	sharedRenderer := mustMappingValue(t, outputContract, "shared_renderer_contract")
	projection := mustMappingValue(t, sharedRenderer, "human_code_projection")

	assertScalarValue(t, mustMappingValue(t, projection, "promoted_by"), "#1817")
	assertScalarValue(t, mustMappingValue(t, projection, "canonical_owner"), "platform-spec.yaml#cli_specification.foundations.output_contract.shared_renderer_contract.human_code_projection")
	assertScalarContains(t, mustMappingValue(t, projection, "implementation_owner"), "formatCLIHumanCode")
	assertScalarContains(t, mustMappingValue(t, projection, "machine_surface_rule"), "retain canonical machine field names and values exactly")
	assertScalarContains(t, mustMappingValue(t, projection, "unknown_value_rule"), "exact original machine shape")

	families := mustMappingValue(t, projection, "families")
	providerFamilies := map[string]string{
		"provider_subject_kind":       "internal/packs",
		"provider_subject_status":     "internal/packs",
		"provider_capability":         "internal/packs",
		"provider_guarantee":          "tool_model.provider_capability_surface",
		"provider_requirement_status": "internal/packs",
	}
	for family, owner := range providerFamilies {
		row := mustMappingValue(t, families, family)
		assertScalarContains(t, mustMappingValue(t, row, "machine_owner"), owner)
		if phrases := mustMappingValue(t, row, "phrases"); len(phrases.Content) == 0 {
			t.Fatalf("human_code_projection family %s has no reviewed phrases", family)
		}
	}
	lifecycle := mustMappingValue(t, families, "agent_lifecycle_tuples")
	assertScalarContains(t, mustMappingValue(t, lifecycle, "machine_owner"), "agentLifecycleBlockingLayer")
	assertScalarContains(t, mustMappingValue(t, lifecycle, "machine_owner"), "StateFromDelivery")
	wantLifecycle := map[string]string{
		"queued":    "delivery_queue",
		"launching": "session_launch",
		"active":    "session_execution",
		"retrying":  "delivery_retry",
		"exhausted": "delivery_terminal",
	}
	assertCurrentCodePairs(t, mustMappingValue(t, lifecycle, "current"), "state", "blocking_layer", wantLifecycle)
	assertScalarContains(t, mustMappingValue(t, lifecycle, "delivered_exclusion"), "not an AgentDeliveryLifecycleState")

	wantRunBlocking := map[string]string{
		"scoring_terminal_outcome": "terminal_scoring_outcome_missing",
		"delivery_lifecycle":       "no_active_deliveries",
	}
	assertCurrentCodePairs(t, mustMappingValue(t, mustMappingValue(t, families, "run_blocking_tuples"), "current_non_empty"), "blocking_layer", "blocking_reason", wantRunBlocking)

	watchdog := mustMappingValue(t, mustMappingValue(t, families, "watchdog_tuples"), "current")
	if len(watchdog.Content) != 2 {
		t.Fatalf("watchdog current tuple count = %d, want 2", len(watchdog.Content))
	}
	wantWatchdog := map[string]string{
		"healthy_long_running": "turn_long_running",
		"no_output":            "session_no_output",
	}
	assertCurrentCodePairs(t, watchdog, "state", "action", wantWatchdog)

	vocabulary := mustMappingValue(t, projection, "vocabulary_guard")
	assertSequenceContainsSubstring(t, mustMappingValue(t, vocabulary, "global_terms"), "Wave 1")
	assertSequenceContainsSubstring(t, mustMappingValue(t, vocabulary, "global_terms"), "unified")
	assertScalarContains(t, mustMappingValue(t, vocabulary, "consumption_rule"), "generated public")

	rows := mustMappingValue(t, mustMappingValue(t, mustMappingValue(t, outputContract, "command_support"), "output_conformance_registry"), "rows")
	for _, key := range []string{"agent_view", "agent_diagnose", "agent_deliveries"} {
		row := mustMappingValue(t, rows, key)
		assertScalarValue(t, mustMappingValue(t, row, "classification"), "shared_output")
		if mappingValue(row, "fact_owner") == nil || mappingValue(row, "json_shape") == nil || mappingValue(row, "quiet_values") == nil {
			t.Errorf("%s shared output row is missing fact_owner/json_shape/quiet_values", key)
		}
	}
}

func assertCurrentCodePairs(t *testing.T, sequence *yaml.Node, keyField, valueField string, want map[string]string) {
	t.Helper()
	if sequence.Kind != yaml.SequenceNode {
		t.Fatalf("current tuple node kind = %d, want sequence", sequence.Kind)
	}
	got := map[string]string{}
	for _, row := range sequence.Content {
		got[mustMappingValue(t, row, keyField).Value] = mustMappingValue(t, row, valueField).Value
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("current tuples = %v, want %v", got, want)
	}
}

func TestCLIDiagnosticConventionPromotedToOutputContract(t *testing.T) {
	outputContract := mustMappingValue(t, mustMappingValue(t, cliSpecification(t), "foundations"), "output_contract")
	convention := mustMappingValue(t, outputContract, "diagnostic_convention")

	assertScalarValue(t, mustMappingValue(t, convention, "promoted_by"), "#1812")
	assertScalarValue(t, mustMappingValue(t, convention, "canonical_owner"), "platform-spec.yaml#cli_specification.foundations.output_contract.diagnostic_convention")
	assertScalarContains(t, mustMappingValue(t, convention, "scope"), "user-facing CLI diagnostic text")
	assertScalarContains(t, mustMappingValue(t, convention, "scope"), "does not by itself sweep existing strings")

	surface := mustMappingValue(t, convention, "user_facing_surface")
	assertScalarContains(t, mustMappingValue(t, surface, "human_text"), "stdout/stderr")
	assertScalarContains(t, mustMappingValue(t, surface, "public_diagnostic_json_fields"), "`message`")
	assertScalarContains(t, mustMappingValue(t, surface, "public_diagnostic_json_fields"), "`remediation`")
	assertScalarContains(t, mustMappingValue(t, surface, "public_diagnostic_json_fields"), "diagnostic `detail`")
	assertScalarContains(t, mustMappingValue(t, surface, "internal_metadata_exemption"), "implementation trackers")

	severity := mustMappingValue(t, convention, "severity_vocabulary")
	assertScalarContains(t, mustMappingValue(t, severity, "command_outcome_failures"), "ERROR:")
	assertScalarContains(t, mustMappingValue(t, severity, "command_outcome_failures"), "WARNING:")
	assertScalarContains(t, mustMappingValue(t, severity, "typed_finding_lists"), "[BLOCKER]")
	assertScalarContains(t, mustMappingValue(t, severity, "typed_finding_lists"), "[WARN]")
	assertScalarContains(t, mustMappingValue(t, severity, "typed_finding_lists"), "[INFO]")
	assertScalarContains(t, mustMappingValue(t, severity, "typed_finding_lists"), "engine.boot_verification.severity_behavior.surface_fatality.text_rendering")

	rules := mustMappingValue(t, convention, "diagnostic_content_rules")
	assertScalarContains(t, mustMappingValue(t, rules, "translate_never_dump"), "Raw Go errors")
	assertScalarContains(t, mustMappingValue(t, rules, "author_facing_terms"), "test-only")
	assertScalarContains(t, mustMappingValue(t, rules, "no_internal_issue_or_tracker_refs"), "must not reference internal GitHub issue numbers")
	assertScalarContains(t, mustMappingValue(t, rules, "structure"), "first line")
	grammar := mustMappingValue(t, rules, "capitalization_and_grammar")
	assertScalarContains(t, grammar, "uppercase with a colon")
	assertScalarContains(t, grammar, "`ERROR:`")
	assertScalarContains(t, grammar, "`WARNING:`")
	assertScalarContains(t, grammar, "lowercase initial")
	assertScalarContains(t, grammar, "Remediation lines are capitalized, imperative")
	assertScalarContains(t, grammar, "exclamation marks")
	assertScalarContains(t, grammar, "decorative emoji/glyph")
	assertScalarContains(t, mustMappingValue(t, rules, "remediation"), "actionable remediation")
	assertScalarContains(t, mustMappingValue(t, rules, "evidence"), "user-facing")

	exitCodes := mustMappingValue(t, convention, "exit_code_taxonomy")
	assertScalarValue(t, mustMappingValue(t, exitCodes, "success"), "0")
	assertScalarValue(t, mustMappingValue(t, exitCodes, "unexpected_or_internal_failure"), "1")
	assertScalarValue(t, mustMappingValue(t, exitCodes, "usage_argument_configuration_or_contract_validation_before_runtime_action"), "2")
	assertScalarValue(t, mustMappingValue(t, exitCodes, "transport_api_or_runtime_state_failure"), "3")
	assertScalarValue(t, mustMappingValue(t, exitCodes, "authentication_or_authorization_failure"), "4")
	assertScalarValue(t, mustMappingValue(t, exitCodes, "addressed_resource_not_found"), "5")
	assertScalarContains(t, mustMappingValue(t, exitCodes, "explicit_exception_rule"), "explicit command row")

	examples := mustMappingValue(t, convention, "examples")
	assertSequenceContainsSubstring(t, mustMappingValue(t, examples, "good"), "ERROR: cannot reach the Swarm runtime")
	assertSequenceContainsSubstring(t, mustMappingValue(t, examples, "good"), "[BLOCKER] accumulator_input_producer_path")
	assertSequenceContainsSubstring(t, mustMappingValue(t, examples, "bad"), "v1 RPC request failed")
	assertSequenceContainsSubstring(t, mustMappingValue(t, examples, "bad"), "test_quiescence_ready")

	argCount := mustMappingValue(t, convention, "arg_count_diagnostics")
	assertScalarValue(t, mustMappingValue(t, argCount, "promoted_by"), "#1818")
	assertScalarValue(t, mustMappingValue(t, argCount, "canonical_owner"), "platform-spec.yaml#cli_specification.foundations.output_contract.diagnostic_convention.arg_count_diagnostics")
	assertScalarValue(t, mustMappingValue(t, argCount, "implementation_owner"), "cmd/swarm/cli_arg_count.go")
	assertScalarContains(t, mustMappingValue(t, argCount, "scope"), "raw Cobra")
	assertScalarContains(t, mustMappingValue(t, argCount, "scope"), "command-local `len(args)` checks")
	assertScalarContains(t, mustMappingValue(t, argCount, "structural_rule"), "shared arg-count validator/formatter")
	assertScalarContains(t, mustMappingValue(t, argCount, "structural_rule"), "`Use` metadata")
	assertScalarContains(t, mustMappingValue(t, argCount, "rendering_rule"), "quote the received positional")
	assertScalarContains(t, mustMappingValue(t, argCount, "secret_material_split"), "MUST NOT use the generic quoted-token evidence path")
	assertScalarContains(t, mustMappingValue(t, argCount, "conformance_guard"), "cobra.ExactArgs")
}

func TestCLIIdentifierResolutionPromotedToOutputContract(t *testing.T) {
	outputContract := mustMappingValue(t, mustMappingValue(t, cliSpecification(t), "foundations"), "output_contract")
	resolution := mustMappingValue(t, outputContract, "identifier_resolution")

	assertScalarValue(t, mustMappingValue(t, resolution, "promoted_by"), "#1815")
	assertScalarValue(t, mustMappingValue(t, resolution, "canonical_owner"), "platform-spec.yaml#cli_specification.foundations.output_contract.identifier_resolution")
	assertScalarContains(t, mustMappingValue(t, resolution, "implementation_owner"), "cmd/swarm/cli_identifier_registry.go")
	assertScalarContains(t, mustMappingValue(t, resolution, "implementation_owner"), "cmd/swarm/cli_identifier_resolver.go")
	assertScalarContains(t, mustMappingValue(t, resolution, "implementation_owner"), "cmd/swarm/cli_output.go")
	assertScalarContains(t, mustMappingValue(t, resolution, "round_trip_law"), "any full-only, unresolved, or split input row")
	assertScalarContains(t, mustMappingValue(t, resolution, "display_enforcement"), "MUST declare that family")
	assertScalarContains(t, mustMappingValue(t, resolution, "display_enforcement"), "Command-local identifier slicing")

	matching := mustMappingValue(t, resolution, "matching")
	assertScalarContains(t, mustMappingValue(t, matching, "exact_precedence"), "exact canonical identifier wins")
	assertScalarContains(t, mustMappingValue(t, matching, "bounded_enumeration"), "page to completion")
	assertScalarContains(t, mustMappingValue(t, matching, "bounded_enumeration"), "reject repeated cursors")
	assertScalarContains(t, mustMappingValue(t, matching, "unbounded_enumeration"), "MUST NOT be globally paged")
	assertScalarContains(t, mustMappingValue(t, matching, "mutation_safety"), "never act silently on a prefix")

	families := mustMappingValue(t, resolution, "family_registry")
	expectedFamilies := map[string]struct {
		candidateSource   string
		scopeMode         string
		normalizationMode string
	}{
		"agent":         {candidateSource: "/v1/rpc agent.list", scopeMode: "global_bounded", normalizationMode: "trim_case_sensitive"},
		"bundle":        {candidateSource: "/v1/rpc bundle.list", scopeMode: "bounded_catalog", normalizationMode: "bundle_digest_hex_case_fold"},
		"run":           {candidateSource: "/v1/rpc run.list", scopeMode: "unbounded_full_only", normalizationMode: "trim_case_sensitive"},
		"entity":        {candidateSource: "/v1/rpc entity.list", scopeMode: "full_run_required", normalizationMode: "trim_case_sensitive"},
		"event":         {candidateSource: "/v1/rpc event.list", scopeMode: "unbounded_full_only", normalizationMode: "trim_case_sensitive"},
		"session":       {candidateSource: "/v1/rpc conversation.list", scopeMode: "unpromoted_full_only", normalizationMode: "trim_case_sensitive"},
		"fork":          {candidateSource: "/v1/rpc conversation.fork_list", scopeMode: "unpromoted_full_only", normalizationMode: "trim_case_sensitive"},
		"mailbox":       {candidateSource: "/v1/rpc mailbox.list", scopeMode: "unpromoted_full_only", normalizationMode: "trim_case_sensitive"},
		"flow_instance": {candidateSource: "unpromoted", scopeMode: "unpromoted_full_only", normalizationMode: "existing_flow_path"},
		"context":       {candidateSource: "local_context_registry", scopeMode: "local_bounded", normalizationMode: "trim_case_sensitive"},
		"subscriber":    {candidateSource: "polymorphic_subscriber_identity", scopeMode: "polymorphic_full_only", normalizationMode: "trim_case_sensitive"},
	}
	familyCount := 0
	forEachMappingEntry(t, families, func(name string, family *yaml.Node) {
		familyCount++
		expected, ok := expectedFamilies[name]
		if !ok {
			t.Errorf("identifier family registry has unsupported family %q", name)
			return
		}
		assertScalarValue(t, mustMappingValue(t, family, "display_shortening_eligible"), "false")
		for _, field := range []string{"candidate_source", "scope_mode", "scope_rule", "normalization_mode", "normalization", "display_projection"} {
			if mappingValue(family, field) == nil {
				t.Errorf("identifier family %s must declare %s", name, field)
			}
		}
		assertScalarValue(t, mustMappingValue(t, family, "candidate_source"), expected.candidateSource)
		assertScalarValue(t, mustMappingValue(t, family, "scope_mode"), expected.scopeMode)
		assertScalarValue(t, mustMappingValue(t, family, "normalization_mode"), expected.normalizationMode)
		assertScalarValue(t, mustMappingValue(t, family, "display_projection"), "full")
	})
	if familyCount != len(expectedFamilies) {
		t.Fatalf("identifier family count=%d, want %d", familyCount, len(expectedFamilies))
	}

	allowedModes := map[string]bool{"resolver_bounded": true, "resolver_scoped": true, "full_only": true, "different_concept": true, "split": true}
	rows := mustMappingValue(t, resolution, "input_rows")
	rowCount := 0
	forEachMappingEntry(t, rows, func(name string, row *yaml.Node) {
		rowCount++
		for _, field := range []string{"command", "selector", "family", "mode"} {
			if mappingValue(row, field) == nil {
				t.Errorf("identifier input row %s missing %s", name, field)
			}
		}
		if mode := mappingValue(row, "mode"); mode != nil && !allowedModes[mode.Value] {
			t.Errorf("identifier input row %s has unsupported mode %q", name, mode.Value)
		}
	})
	if rowCount < 70 {
		t.Fatalf("identifier input row count=%d, want at least 70; registry coverage likely regressed", rowCount)
	}

	agentBoundary := mustMappingValue(t, cliSpecification(t), "agent_identity_boundary")
	agentRule := mustMappingValue(t, agentBoundary, "rule")
	assertScalarContains(t, agentRule, "unique")
	assertScalarContains(t, agentRule, "case-sensitive")
	assertScalarContains(t, agentRule, "mutating agent selectors remain full-slug-only")
	assertScalarContains(t, agentRule, "UUIDs")
	assertScalarContains(t, agentRule, "aliases")
}

func TestCLITopologyRevisionV22IsImplementedHistoricalRecord(t *testing.T) {
	revision := mustMappingValue(t, cliSpecification(t), "topology_revision_v2_2")
	assertScalarValue(t, mustMappingValue(t, revision, "status"), "implemented_historical_record")
	assertScalarValue(t, mustMappingValue(t, revision, "promoted_by"), "#1654")
	assertScalarValue(t, mustMappingValue(t, revision, "implemented_by"), "#1677")
	assertScalarContains(t, mustMappingValue(t, revision, "authority_rule"), "Historical decision record")

	policy := mustMappingValue(t, revision, "old_spelling_policy")
	assertScalarValue(t, mustMappingValue(t, policy, "default_disposition"), "fail_closed_retirement")

	groupField := mustMappingValue(t, revision, "group_field")
	assertScalarContains(t, mustMappingValue(t, groupField, "identifier_alignment"), "no translation table")
	assertScalarContains(t, mustMappingValue(t, groupField, "identifier_alignment"), "rename the cobra GroupID constants")

	binding := mustMappingValue(t, revision, "conformance_binding")
	assertScalarValue(t, mustMappingValue(t, binding, "decision"), "read_only_drift_test")
	assertScalarContains(t, mustMappingValue(t, binding, "rule"), "swarm describe")

	forkchat := mustMappingValue(t, revision, "forkchat_disposition")
	assertScalarValue(t, mustMappingValue(t, forkchat, "decision"), "keep_name_rename_rejected_for_now")
}

func TestCLITopologyTargetRowsInheritContractsAndNeverClaimImplemented(t *testing.T) {
	revision := mustMappingValue(t, cliSpecification(t), "topology_revision_v2_2")
	catalog := mustMappingValue(t, cliSpecification(t), "command_catalog")
	targets := mustMappingValue(t, revision, "target_rows")
	count := 0
	forEachMappingEntry(t, targets, func(name string, row *yaml.Node) {
		count++
		command := mustMappingValue(t, row, "command")
		if !strings.HasPrefix(command.Value, "swarm ") {
			t.Errorf("target_rows.%s: command %q must start with \"swarm \"", name, command.Value)
		}
		group := mustMappingValue(t, row, "group")
		if !cliGroupAllowedValues[group.Value] {
			t.Errorf("target_rows.%s: group %q not in allowed vocabulary", name, group.Value)
		}
		if status := mappingValue(row, "implementation_status"); status != nil {
			t.Errorf("target_rows.%s: must not carry implementation_status (revision is source_authority_only); found %q", name, status.Value)
		}
		inherits := mappingValue(row, "inherits_contract")
		if name == "run_group" {
			if inherits != nil {
				t.Errorf("target_rows.run_group must not inherit a contract; it defines new group-help behavior")
			}
			return
		}
		if inherits == nil {
			t.Errorf("target_rows.%s: missing inherits_contract", name)
			return
		}
		const prefix = "cli_specification.command_catalog."
		if !strings.HasPrefix(inherits.Value, prefix) {
			t.Errorf("target_rows.%s: inherits_contract %q must reference %s<row>", name, inherits.Value, prefix)
			return
		}
		source := strings.TrimPrefix(inherits.Value, prefix)
		if mappingValue(catalog, source) == nil {
			t.Errorf("target_rows.%s: inherits_contract references missing catalog row %q", name, source)
		}
	})
	if count != 11 {
		t.Fatalf("target rows = %d, want exactly 11 (run_group, run start/list/status/trace/fork, agent/event list, event follow, entity/conversation list)", count)
	}
	// The CLI-only supersession scope for run fork is load-bearing (#1654 gate
	// condition: runtime/API run.fork must not be disturbed).
	runFork := mustMappingValue(t, targets, "run_fork")
	assertScalarContains(t, mustMappingValue(t, runFork, "supersession_scope"), "CLI command spelling only")
}

func TestCLITopologySupersededSpellingsHaveCompleteDispositions(t *testing.T) {
	revision := mustMappingValue(t, cliSpecification(t), "topology_revision_v2_2")
	spellings := mustMappingValue(t, revision, "superseded_spellings")
	count := 0
	forEachMappingEntry(t, spellings, func(name string, row *yaml.Node) {
		count++
		assertScalarValue(t, mustMappingValue(t, row, "disposition"), "fail_closed_pointer")
		assertScalarValue(t, mustMappingValue(t, row, "exit_code"), "2")
		assertScalarValue(t, mustMappingValue(t, row, "current_status"), "retired")
		current := mustMappingValue(t, row, "current")
		replacement := mustMappingValue(t, row, "replacement")
		message := mustMappingValue(t, row, "message")
		// the pointer message must name the replacement's leading command words
		replacementHead := replacement.Value
		if idx := strings.Index(replacementHead, " ["); idx > 0 {
			replacementHead = replacementHead[:idx]
		}
		if !strings.Contains(message.Value, replacementHead) && !strings.Contains(message.Value, strings.Split(replacementHead, "|")[0]) {
			t.Errorf("superseded_spellings.%s: message %q does not name replacement %q", name, message.Value, replacementHead)
		}
		if current.Value == replacement.Value {
			t.Errorf("superseded_spellings.%s: current and replacement are identical", name)
		}
	})
	if count != 9 {
		t.Fatalf("superseded spellings = %d, want exactly 9 (run bare-start, runs, status, trace, fork, agents, events, entities, conversations)", count)
	}
}

func TestCLITopologyCatalogRowsImplementTargetSpellings(t *testing.T) {
	spec := cliSpecification(t)
	catalog := mustMappingValue(t, spec, "command_catalog")
	// After #1677 the catalog rows carry the v2.2 spellings as live behavior:
	// each row's command must match its historical target-row command, and the
	// Phase-2 supersession pointers must be gone.
	rowToTarget := map[string]string{
		"run":                "run_start",
		"run_group":          "run_group",
		"runs":               "run_list",
		"status":             "run_status",
		"run_fork":           "run_fork",
		"agents_list":        "agent_list",
		"events_list":        "event_list",
		"events_follow":      "event_follow",
		"entities_list":      "entity_list",
		"conversations_list": "conversation_list",
	}
	targets := mustMappingValue(t, mustMappingValue(t, spec, "topology_revision_v2_2"), "target_rows")
	for row, targetName := range rowToTarget {
		value := mustMappingValue(t, catalog, row)
		if pointer := mappingValue(value, "topology_v2_2"); pointer != nil {
			t.Errorf("command_catalog.%s: stale topology_v2_2 supersession pointer after implementation", row)
		}
		if status := mappingValue(value, "implementation_status"); status == nil || !strings.HasPrefix(status.Value, "implemented") {
			t.Errorf("command_catalog.%s: implemented row missing implemented status; got %v", row, status)
		}
		rowCommand := mustMappingValue(t, value, "command").Value
		targetCommand := mustMappingValue(t, mustMappingValue(t, targets, targetName), "command").Value
		targetHead := targetCommand
		if idx := strings.Index(targetHead, " ["); idx > 0 {
			targetHead = targetHead[:idx]
		}
		if !strings.HasPrefix(rowCommand, targetHead) {
			t.Errorf("command_catalog.%s: command %q does not implement target %q", row, rowCommand, targetCommand)
		}
	}
	// trace row: command carries the full filter shape; check the spelling head only.
	trace := mustMappingValue(t, catalog, "trace")
	if !strings.HasPrefix(mustMappingValue(t, trace, "command").Value, "swarm run trace") {
		t.Errorf("command_catalog.trace: command does not carry the v2.2 spelling")
	}
	retired := mustMappingValue(t, mustMappingValue(t, spec, "retired_namespaces"), "topology_v2_2_retired_spellings")
	assertScalarValue(t, mustMappingValue(t, retired, "implemented_by"), "#1677")
	assertScalarValue(t, mustMappingValue(t, retired, "exit_code"), "2")
	spellings := mustMappingValue(t, retired, "spellings")
	if len(spellings.Content)/2 != 9 {
		t.Errorf("retired spellings = %d, want 9", len(spellings.Content)/2)
	}
}

func TestCLIParentTailCarriesTopologyAccuracyNote(t *testing.T) {
	parentTail := mustMappingValue(t, cliSpecification(t), "parent_tail")
	note := mustMappingValue(t, parentTail, "topology_v2_2_note")
	assertScalarContains(t, note, "live CLI v2.2 topology")
	assertScalarContains(t, note, "#1677")
}

// Guard: the ten annotated rows and nine spellings must stay in sync — every
// superseded spelling maps to at least one annotated catalog row family.
func TestCLITopologySpellingsAndRowAnnotationsAgree(t *testing.T) {
	revision := mustMappingValue(t, cliSpecification(t), "topology_revision_v2_2")
	spellings := mustMappingValue(t, revision, "superseded_spellings")
	var currents []string
	forEachMappingEntry(t, spellings, func(name string, row *yaml.Node) {
		currents = append(currents, mustMappingValue(t, row, "current").Value)
	})
	for _, want := range []string{"swarm run ", "swarm runs", "swarm status", "swarm trace", "swarm fork", "swarm agents", "swarm events", "swarm entities", "swarm conversations"} {
		found := false
		for _, current := range currents {
			if strings.HasPrefix(current, strings.TrimSpace(want)) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("no superseded spelling covers %q; got %s", want, fmt.Sprintf("%v", currents))
		}
	}
}

func assertSequenceContainsSubstring(t *testing.T, node *yaml.Node, want string) {
	t.Helper()
	if node == nil || node.Kind != yaml.SequenceNode {
		t.Fatalf("node kind = %d, want sequence", nodeKind(node))
	}
	for _, item := range node.Content {
		if strings.Contains(scalarValue(item), want) {
			return
		}
	}
	t.Fatalf("sequence missing substring %q", want)
}
