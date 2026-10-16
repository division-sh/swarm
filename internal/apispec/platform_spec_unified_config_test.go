package apispec

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestPlatformSpecUnifiedSwarmConfigSourceAuthority(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	authority := mustMappingValue(t, mustMappingValue(t, root, "configuration_source_authority"), "unified_swarm_config")

	if got := scalarValue(mustMappingValue(t, authority, "promoted_by")); got != "#1804" {
		t.Fatalf("unified config promoted_by = %q, want #1804", got)
	}
	if got := scalarValue(mustMappingValue(t, authority, "implementation_status")); got != "single_deployment_document_implemented" {
		t.Fatalf("unified config implementation_status = %q, want single_deployment_document_implemented", got)
	}
	if got := scalarValue(mustMappingValue(t, authority, "canonical_owner")); got != "platform-spec.yaml#configuration_source_authority.unified_swarm_config" {
		t.Fatalf("unified config canonical owner = %q", got)
	}
	for _, want := range []string{"Env", "never a normal precedence layer", "schema-declared exact env delegation"} {
		if !strings.Contains(scalarValue(mustMappingValue(t, authority, "invariant")), want) {
			t.Fatalf("unified config invariant missing %q:\n%s", want, scalarValue(mustMappingValue(t, authority, "invariant")))
		}
	}

	sourceSelection := mustMappingValue(t, authority, "source_selection")
	wantOrder := []string{"explicit_config", "deployment_config", "built_in_defaults"}
	if got := scalarSequenceValues(mustMappingValue(t, sourceSelection, "order")); !sameStrings(got, wantOrder) {
		t.Fatalf("file selection = %#v, want %#v", got, wantOrder)
	}
	for _, retired := range []string{"source_precedence", "trust_tiers", "key_trust_classes", "merge_semantics", "doctor_provenance"} {
		if hasMappingKey(authority, retired) {
			t.Fatalf("retired config authority %s remains", retired)
		}
	}
	rules := mustMappingValue(t, sourceSelection, "rules")
	for key, phrases := range map[string][]string{
		"explicit_config":   {"whole document", "SWARM_CONFIG", "flag wins", "Relative selectors", "old grammar"},
		"deployment_config": {"captured invocation root", "Do not walk ancestors", "selected context"},
		"built_in_defaults": {"Only absence", "Omitted keys", "no additional file discovery"},
		"selected_failure":  {"broken symlink", "dangling default entry is present", "No fallback"},
		"ignored_locations": {"Project-root swarm.yaml", "user-global", "cannot contribute values", "delegate an env"},
		"command_overrides": {"after selected-source", "not file layers", "invalid declaration", "inline secrets"},
	} {
		for _, phrase := range phrases {
			assertContainsScalar(t, mustMappingValue(t, rules, key), phrase)
		}
	}
	document := mustMappingValue(t, authority, "document_semantics")
	for _, phrase := range []string{"aliases and merge keys only within", "duplicate-key checks", "No cross-file merge", "explicit false", "Null containers refuse"} {
		assertContainsScalar(t, mustMappingValue(t, document, "rule"), phrase)
	}
	for _, phrase := range []string{"selected file", "source coordinates", "cannot delegate env", "secret-reference-only"} {
		assertContainsScalar(t, mustMappingValue(t, document, "diagnostics"), phrase)
	}
	for _, phrase := range []string{"selected document's directory", "not a universal", "embedded platform spec is not a configurable path", "authored source root"} {
		assertContainsScalar(t, mustMappingValue(t, document, "path_bases"), phrase)
	}
	doctor := mustMappingValue(t, authority, "doctor_selected_source")
	requirements := strings.Join(scalarSequenceValues(mustMappingValue(t, doctor, "requirements")), "\n")
	for _, phrase := range []string{"tri-state presence", "not successful admission or readiness", "do not expose file layers", "never disclose secret values", "nonzero", "without parsing", "symlink or hardlink", "uninspected target/store"} {
		if !strings.Contains(requirements, phrase) {
			t.Fatalf("doctor selected-source requirements missing %q:\n%s", phrase, requirements)
		}
	}

	sections := mustMappingValue(t, mustMappingValue(t, authority, "schema"), "sections")
	for _, section := range []string{
		"connection",
		"serve",
		"runtime",
		"store",
		"database",
		"workspace",
		"llm",
		"provider_triggers",
		"budget",
		"sharding",
		"paths",
	} {
		if !hasMappingKey(sections, section) {
			t.Fatalf("unified config section %s missing", section)
		}
	}
	if hasMappingKey(sections, "human_tasks") {
		t.Fatal("human_tasks must remain nested under budget, not top-level")
	}
	for i := 1; i < len(sections.Content); i += 2 {
		if hasMappingKey(sections.Content[i], "trust") {
			t.Fatalf("section %s retains a source trust classification", sections.Content[i-1].Value)
		}
	}
	pathSection := mustMappingValue(t, sections, "paths")
	paths := mustMappingValue(t, pathSection, "keys")
	for _, key := range []string{"agent_config_map_file", "verification_gates_file", "tooling_lock_file", "platform_spec_path"} {
		if got := scalarValue(mustMappingValue(t, paths, key)); got != "retired_rejected" {
			t.Fatalf("paths.%s = %q, want retired_rejected", key, got)
		}
	}
	const teaching = "`paths.platform_spec_path` at <file>:<line>:<col> is not a config key; the platform spec is embedded in `swarm`."
	if got := scalarValue(mustMappingValue(t, pathSection, "platform_spec_path_remediation")); got != teaching {
		t.Fatalf("platform spec path teaching = %q, want %q", got, teaching)
	}
	for _, key := range []string{"swarm_dir", "monitor_dir"} {
		if got := scalarValue(mustMappingValue(t, paths, key)); got != "supported_path" {
			t.Fatalf("deployment path %s changed: %q", key, got)
		}
	}

	llm := mustMappingValue(t, sections, "llm")
	llmKeys := mustMappingValue(t, llm, "keys")
	for _, key := range []string{
		"claude_api.default_model",
		"claude_api.haiku_model",
		"openai_compatible.default_model",
		"openai_compatible.low_cost_model",
	} {
		if got := scalarValue(mustMappingValue(t, llmKeys, key)); got != "split_unsupported" {
			t.Fatalf("llm.%s = %q, want split_unsupported", key, got)
		}
	}
	for _, key := range []string{
		"claude_cli.retries",
		"claude_cli.no_session_persistence",
		"claude_cli.use_tmux",
	} {
		if got := scalarValue(mustMappingValue(t, llmKeys, key)); got != "split_unsupported" {
			t.Fatalf("llm.%s = %q, want split_unsupported", key, got)
		}
	}
	for _, want := range []string{"retired model-selection inputs", "keep rejecting", "llm.models"} {
		if !strings.Contains(scalarValue(mustMappingValue(t, llm, "retired_model_selection_keys")), want) {
			t.Fatalf("retired model-selection rule missing %q:\n%s", want, scalarValue(mustMappingValue(t, llm, "retired_model_selection_keys")))
		}
	}
	for _, want := range []string{"#1803", "remain split_unsupported", "no supported replacement"} {
		if !strings.Contains(scalarValue(mustMappingValue(t, llm, "claude_cli_control_cleanup")), want) {
			t.Fatalf("Claude CLI controls cleanup rule missing %q:\n%s", want, scalarValue(mustMappingValue(t, llm, "claude_cli_control_cleanup")))
		}
	}
	for _, want := range []string{"dynamic map keys", "max_concurrency", "Unknown policy fields", "MUST fail closed"} {
		if !strings.Contains(scalarValue(mustMappingValue(t, llm, "provider_limits_shape")), want) {
			t.Fatalf("provider limits shape rule missing %q:\n%s", want, scalarValue(mustMappingValue(t, llm, "provider_limits_shape")))
		}
	}

	budget := mustMappingValue(t, sections, "budget")
	budgetKeys := mustMappingValue(t, budget, "keys")
	for _, key := range []string{
		"human_tasks.max_tasks_per_week",
		"human_tasks.budget_reset",
		"human_tasks.auto_expire_hours",
		"human_tasks.categories_enabled",
	} {
		if got := scalarValue(mustMappingValue(t, budgetKeys, key)); got != "supported" {
			t.Fatalf("budget.%s = %q, want supported", key, got)
		}
	}
	for _, want := range []string{"budget.human_tasks", "top-level `human_tasks` section is not part"} {
		if !strings.Contains(scalarValue(mustMappingValue(t, budget, "nested_shape")), want) {
			t.Fatalf("budget nested shape rule missing %q:\n%s", want, scalarValue(mustMappingValue(t, budget, "nested_shape")))
		}
	}

	platform := mustMappingValue(t, sections, "platform")
	platformKeys := mustMappingValue(t, platform, "keys")
	if got := scalarValue(mustMappingValue(t, platformKeys, "packs.platform_dirs")); got != "supported_complete_development_replacement" {
		t.Fatalf("platform.packs.platform_dirs classification = %q", got)
	}
	for _, want := range []string{"optional", "complete development replacement", "no embedded fallback", "packs/manifest.yaml", "selected document's directory"} {
		if !strings.Contains(scalarValue(mustMappingValue(t, platform, "path_rule")), want) {
			t.Fatalf("platform pack path rule missing %q:\n%s", want, scalarValue(mustMappingValue(t, platform, "path_rule")))
		}
	}

	providerTriggers := mustMappingValue(t, sections, "provider_triggers")
	providerTriggerKeys := mustMappingValue(t, providerTriggers, "keys")
	if got := scalarValue(mustMappingValue(t, providerTriggerKeys, "packs.platform_dirs")); got != "split_unsupported_retired" {
		t.Fatalf("provider_triggers.packs.platform_dirs classification = %q", got)
	}
	if got := scalarValue(mustMappingValue(t, providerTriggerKeys, "packs.external_dirs")); got != "split_unsupported_retired" {
		t.Fatalf("provider_triggers.packs.external_dirs classification = %q", got)
	}
	for _, want := range []string{"retired", "swarm import <pack-id>", "platform.packs.platform_dirs", "no compatibility"} {
		if !strings.Contains(scalarValue(mustMappingValue(t, providerTriggers, "path_rule")), want) {
			t.Fatalf("provider trigger path rule missing %q:\n%s", want, scalarValue(mustMappingValue(t, providerTriggers, "path_rule")))
		}
	}

	sharding := mustMappingValue(t, sections, "sharding")
	if got := scalarValue(mustMappingValue(t, sharding, "admission")); got != "split_unsupported" {
		t.Fatalf("sharding admission = %q, want split_unsupported", got)
	}
	for _, want := range []string{"Current code rejects", "does not promote sharding runtime behavior"} {
		if !strings.Contains(scalarValue(mustMappingValue(t, sharding, "rule")), want) {
			t.Fatalf("sharding rule missing %q:\n%s", want, scalarValue(mustMappingValue(t, sharding, "rule")))
		}
	}

	consumerMatrix := mustMappingValue(t, authority, "consumer_matrix")
	wantConsumers := map[string]string{
		"flat_cli_config":                       "superseded_by_unified_owner_first_slice_implemented",
		"xdg_config_yaml":                       "ignored_implicit_location_current_format_allowed_only_by_explicit_selection",
		"root_swarm_yaml":                       "ignored_implicit_location_metadata_only_doctor_notice",
		"runtime_config_loader":                 "selected_document_projection_through_unified_owner",
		"executable_adjacent_config_yaml":       "ignored_implicit_location_current_format_allowed_only_by_explicit_selection",
		"env_guard_delegated_env_sources":       "consumed_by_unified_owner_first_slice_implemented",
		"platform_packs_platform_dirs":          "consumed_by_unified_owner_complete_development_replacement",
		"provider_triggers_packs_platform_dirs": "retired_fail_closed_use_platform_packs_platform_dirs",
		"provider_triggers_packs_external_dirs": "retired_fail_closed_use_project_import",
		"channels_packs_platform_dirs":          "retired_fail_closed_use_platform_packs_platform_dirs",
		"channels_packs_external_dirs":          "retired_fail_closed_use_project_import",
		"sharding_inline_extension":             "split_unsupported_retired",
		"llm_retired_model_selection_keys":      "split_unsupported_retired_use_llm.models",
		"llm_claude_cli_current_controls":       "split_unsupported_tracked_by_1803",
		"context_descriptors":                   "different_semantic_owner_local_target_context_registry",
		"token_and_secret_material":             "different_semantic_owner_secret_or_token_file_reference_only",
	}
	for key, want := range wantConsumers {
		if got := scalarValue(mustMappingValue(t, consumerMatrix, key)); got != want {
			t.Fatalf("consumer_matrix.%s = %q, want %q", key, got, want)
		}
	}

	oldShapePolicy := mustMappingValue(t, authority, "old_shape_and_unknown_key_policy")
	if got := scalarValue(mustMappingValue(t, oldShapePolicy, "unknown_keys")); got != "fail_closed_with_key_path_and_source" {
		t.Fatalf("unknown key policy = %q", got)
	}
	rejectionDiagnostics := mustMappingValue(t, oldShapePolicy, "rejection_diagnostics")
	for _, key := range []string{"unknown_key", "split_unsupported_key"} {
		if !hasMappingKey(rejectionDiagnostics, key) {
			t.Fatalf("rejection diagnostic %s missing", key)
		}
	}
	assertContainsScalar(t, mustMappingValue(t, rejectionDiagnostics, "unknown_key"), "unknown config key")
	if hasMappingKey(rejectionDiagnostics, "trust_tier_rejection") {
		t.Fatal("deleted file trust-tier refusal remains")
	}
	for _, want := range []string{"recognized but not wired", "not-yet-wired", "silently ignored or reported as typos", "no supported replacement"} {
		assertContainsScalar(t, mustMappingValue(t, rejectionDiagnostics, "split_unsupported_key"), want)
	}
	for _, key := range []string{"flat_cli_shape", "runtime_config_shape", "executable_adjacent_config_yaml"} {
		if !hasMappingKey(oldShapePolicy, key) {
			t.Fatalf("old-shape policy %s missing", key)
		}
	}

	envDrain := mustMappingValue(t, authority, "env_drain")
	for _, want := range []string{"six active sources under #1600", "no catalog or translation map", "unknown-env", "before effects"} {
		if !strings.Contains(scalarValue(mustMappingValue(t, envDrain, "rule")), want) {
			t.Fatalf("env_drain rule missing %q:\n%s", want, scalarValue(mustMappingValue(t, envDrain, "rule")))
		}
	}
}

func TestPlatformSpecUnifiedConfigSupersedesOldConfigCommitments(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)

	cliSpec := mustMappingValue(t, root, "cli_specification")
	foundations := mustMappingValue(t, cliSpec, "foundations")
	apiConfig := mustMappingValue(t, foundations, "api_connection_auth_config_precedence")
	cliConfig := mustMappingValue(t, apiConfig, "cli_config_file")
	assertContainsScalar(t, mustMappingValue(t, cliConfig, "unified_config_supersession"), "configuration_source_authority.unified_swarm_config")
	assertContainsScalar(t, mustMappingValue(t, cliConfig, "unified_config_supersession"), "global `--config`")

	pathConfig := mustMappingValue(t, foundations, "source_platform_spec_path_resolution")
	assertContainsScalar(t, mustMappingValue(t, pathConfig, "scope"), "authored source root is owned by filesystem_source_model.local_root_selection")
	assertContainsScalar(t, mustYAMLPath(t, pathConfig, "platform_spec_path", "rule"), "always embedded in the swarm binary")
	assertContainsScalar(t, mustYAMLPath(t, pathConfig, "platform_spec_path", "rule"), "No config key, environment variable, flag or file lookup")
	accepted := mustYAMLPath(t, pathConfig, "platform_spec_path", "accepted_sources")
	if hasMappingKey(accepted, "config_key") || scalarValue(mustMappingValue(t, accepted, "built_in_default")) != "embedded platform-spec.yaml" {
		t.Fatal("platform spec default/source contract restored a config override")
	}
	const teaching = "`paths.platform_spec_path` at <file>:<line>:<col> is not a config key; the platform spec is embedded in `swarm`."
	if got := scalarValue(mustYAMLPath(t, pathConfig, "platform_spec_path", "remediation")); got != teaching {
		t.Fatalf("path-resolution teaching = %q, want %q", got, teaching)
	}
	if got := scalarValue(mustYAMLPath(t, cliConfig, "rejected_keys", "paths.platform_spec_path")); got != teaching {
		t.Fatalf("shared config teaching = %q, want %q", got, teaching)
	}

	sourceRoot := mustYAMLPath(t, root, "filesystem_source_model", "local_root_selection")
	assertContainsScalar(t, mustMappingValue(t, sourceRoot, "rule"), "Omission and `.` select exactly the captured invocation directory")
	assertContainsScalar(t, mustMappingValue(t, sourceRoot, "rule"), "There is no ancestor")

	envAuthority := mustMappingValue(t, mustMappingValue(t, root, "environment_source_authority"), "repo_wide_swarm_env_accepted_set")
	typedDelegation := mustMappingValue(t, envAuthority, "typed_delegation")
	assertContainsScalar(t, mustMappingValue(t, typedDelegation, "rule"), "configuration_source_authority.unified_swarm_config")
	assertContainsScalar(t, mustMappingValue(t, typedDelegation, "rule"), "Only the selected, admitted whole document")
	assertContainsScalar(t, mustMappingValue(t, typedDelegation, "rule"), "ignored files cannot delegate or block")

	serve := mustMappingValue(t, mustMappingValue(t, cliSpec, "command_catalog"), "serve")
	runtimeConfig := mustMappingValue(t, serve, "runtime_config_backend_selection")
	assertContainsScalar(t, mustMappingValue(t, runtimeConfig, "unified_config_supersession"), "executable-adjacent")

	listenerTopology := mustMappingValue(t, serve, "listener_topology_v2_1")
	sourcePrecedence := mustMappingValue(t, listenerTopology, "source_precedence")
	assertContainsScalar(t, mustMappingValue(t, sourcePrecedence, "unified_config_supersession"), "`serve`")

	legacyCommitments := mustMappingValue(t, mustMappingValue(t, root, "configuration_source_authority"), "unified_swarm_config")
	legacyCommitments = mustMappingValue(t, legacyCommitments, "legacy_spec_commitments_superseded")
	if !sequenceContainsMappingValue(legacyCommitments, "owner", "cli_specification.foundations.api_connection_auth_config_precedence.cli_config_file") {
		t.Fatal("legacy supersession missing API CLI config owner")
	}
	if !sequenceContainsMappingValue(legacyCommitments, "owner", "cli_specification.command_catalog.serve.runtime_config_backend_selection") {
		t.Fatal("legacy supersession missing serve runtime config owner")
	}
}

func assertContainsScalar(t *testing.T, node *yaml.Node, want string) {
	t.Helper()
	if !strings.Contains(scalarValue(node), want) {
		t.Fatalf("scalar missing %q:\n%s", want, scalarValue(node))
	}
}

func scalarSequenceValues(node *yaml.Node) []string {
	if node == nil || node.Kind != yaml.SequenceNode {
		return nil
	}
	out := make([]string, 0, len(node.Content))
	for _, child := range node.Content {
		out = append(out, scalarValue(child))
	}
	return out
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sequenceContainsMappingValue(node *yaml.Node, key, want string) bool {
	if node == nil || node.Kind != yaml.SequenceNode {
		return false
	}
	for _, child := range node.Content {
		if scalarValue(mappingValue(child, key)) == want {
			return true
		}
	}
	return false
}
