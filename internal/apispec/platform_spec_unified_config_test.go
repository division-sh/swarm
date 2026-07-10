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
	if got := scalarValue(mustMappingValue(t, authority, "implementation_status")); got != "parser_discovery_first_slice_implemented" {
		t.Fatalf("unified config implementation_status = %q, want parser_discovery_first_slice_implemented", got)
	}
	if got := scalarValue(mustMappingValue(t, authority, "canonical_owner")); got != "platform-spec.yaml#configuration_source_authority.unified_swarm_config" {
		t.Fatalf("unified config canonical owner = %q", got)
	}
	for _, want := range []string{"Env", "never a normal precedence layer", "schema-declared exact env delegation"} {
		if !strings.Contains(scalarValue(mustMappingValue(t, authority, "invariant")), want) {
			t.Fatalf("unified config invariant missing %q:\n%s", want, scalarValue(mustMappingValue(t, authority, "invariant")))
		}
	}

	sourcePrecedence := mustMappingValue(t, authority, "source_precedence")
	wantOrder := []string{"flags", "explicit_config", "local_operator_config", "project_config", "user_global_config", "built_in_defaults"}
	if got := scalarSequenceValues(mustMappingValue(t, sourcePrecedence, "order")); !sameStrings(got, wantOrder) {
		t.Fatalf("source precedence = %#v, want %#v", got, wantOrder)
	}

	trustTiers := mustMappingValue(t, authority, "trust_tiers")
	for _, tier := range []string{"full", "medium", "low"} {
		if !hasMappingKey(trustTiers, tier) {
			t.Fatalf("trust tier %s missing", tier)
		}
	}
	keyTrustClasses := mustMappingValue(t, authority, "key_trust_classes")
	for _, class := range []string{"project_safe", "elevated", "secret_reference", "project_contained_path", "split_unsupported"} {
		if !hasMappingKey(keyTrustClasses, class) {
			t.Fatalf("key trust class %s missing", class)
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
		if got := scalarValue(mustMappingValue(t, budgetKeys, key)); got != "project_safe" {
			t.Fatalf("budget.%s = %q, want project_safe", key, got)
		}
	}
	for _, want := range []string{"budget.human_tasks", "top-level `human_tasks` section is not part"} {
		if !strings.Contains(scalarValue(mustMappingValue(t, budget, "nested_shape")), want) {
			t.Fatalf("budget nested shape rule missing %q:\n%s", want, scalarValue(mustMappingValue(t, budget, "nested_shape")))
		}
	}

	providerTriggers := mustMappingValue(t, sections, "provider_triggers")
	providerTriggerKeys := mustMappingValue(t, providerTriggers, "keys")
	if got := scalarValue(mustMappingValue(t, providerTriggerKeys, "packs.platform_dirs")); got != "elevated" {
		t.Fatalf("provider_triggers.packs.platform_dirs classification = %q", got)
	}
	if got := scalarValue(mustMappingValue(t, providerTriggerKeys, "packs.external_dirs")); got != "project_contained_path_or_elevated" {
		t.Fatalf("provider_triggers.packs.external_dirs classification = %q", got)
	}
	for _, want := range []string{"Project config cannot declare", "project-contained relative directories", "declared that key"} {
		if !strings.Contains(scalarValue(mustMappingValue(t, providerTriggers, "path_rule")), want) {
			t.Fatalf("provider trigger path rule missing %q:\n%s", want, scalarValue(mustMappingValue(t, providerTriggers, "path_rule")))
		}
	}

	sharding := mustMappingValue(t, sections, "sharding")
	if got := scalarValue(mustMappingValue(t, sharding, "trust")); got != "split_unsupported" {
		t.Fatalf("sharding trust = %q, want split_unsupported", got)
	}
	for _, want := range []string{"Current code rejects", "does not promote sharding runtime behavior"} {
		if !strings.Contains(scalarValue(mustMappingValue(t, sharding, "rule")), want) {
			t.Fatalf("sharding rule missing %q:\n%s", want, scalarValue(mustMappingValue(t, sharding, "rule")))
		}
	}

	consumerMatrix := mustMappingValue(t, authority, "consumer_matrix")
	wantConsumers := map[string]string{
		"flat_cli_config":                       "superseded_by_unified_owner_first_slice_implemented",
		"xdg_config_yaml":                       "invalid_legacy_discovery_replaced_by_user_global_swarm_yaml_by_1858",
		"runtime_config_loader":                 "consumed_by_unified_owner_first_slice_implemented",
		"executable_adjacent_config_yaml":       "invalid_ambient_reader_removed_by_1858",
		"env_guard_delegated_env_sources":       "consumed_by_unified_owner_first_slice_implemented",
		"provider_triggers_packs_platform_dirs": "consumed_by_unified_owner",
		"provider_triggers_packs_external_dirs": "consumed_by_unified_owner",
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
	for _, key := range []string{"unknown_key", "trust_tier_rejection", "split_unsupported_key"} {
		if !hasMappingKey(rejectionDiagnostics, key) {
			t.Fatalf("rejection diagnostic %s missing", key)
		}
	}
	assertContainsScalar(t, mustMappingValue(t, rejectionDiagnostics, "unknown_key"), "unknown config key")
	assertContainsScalar(t, mustMappingValue(t, rejectionDiagnostics, "trust_tier_rejection"), "not-allowed-here")
	for _, want := range []string{"recognized but not wired", "not-yet-wired", "MUST NOT be reported as typos or", "trust-tier violations"} {
		assertContainsScalar(t, mustMappingValue(t, rejectionDiagnostics, "split_unsupported_key"), want)
	}
	for _, key := range []string{"flat_cli_shape", "runtime_config_shape", "executable_adjacent_config_yaml"} {
		if !hasMappingKey(oldShapePolicy, key) {
			t.Fatalf("old-shape policy %s missing", key)
		}
	}

	envDrain := mustMappingValue(t, authority, "env_drain")
	for _, want := range []string{"seeded #1600 env row", "unified typed key", "terminal secret/token-file owner"} {
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

	pathConfig := mustMappingValue(t, mustMappingValue(t, foundations, "contract_platform_spec_path_resolution"), "cli_config_file")
	assertContainsScalar(t, mustMappingValue(t, pathConfig, "unified_config_supersession"), "`paths` section")

	envAuthority := mustMappingValue(t, mustMappingValue(t, root, "environment_source_authority"), "repo_wide_swarm_env_accepted_set")
	typedDelegation := mustMappingValue(t, envAuthority, "typed_delegation")
	assertContainsScalar(t, mustMappingValue(t, typedDelegation, "rule"), "configuration_source_authority.unified_swarm_config")
	assertContainsScalar(t, mustMappingValue(t, typedDelegation, "rule"), "executable-adjacent `config.yaml` is superseded")

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
