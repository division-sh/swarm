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
	implemented := mustMappingValue(t, commandSupport, "implemented_output_mode_first_slice")
	consumers := mustMappingValue(t, implemented, "consumers")
	verify := mustMappingValue(t, consumers, "verify")
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
