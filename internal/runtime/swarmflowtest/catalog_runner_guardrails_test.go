package swarmflowtest

import (
	"path/filepath"
	"testing"
)

func TestCatalogRunner_GuardOnFailDiscardPreventsStateAdvanceAndEmit(t *testing.T) {
	dir := t.TempDir()
	writeCatalogCaseFile(t, dir, "package.yaml", "name: guard-discard\n")
	writeCatalogCaseFixture(t, dir,
		`
initial_state: pending
terminal_states: [done]
states: [pending, done]
pins:
  inputs:
    events: [check.requested]
  outputs:
    events: [check.passed]
`,
		`
test-node:
  id: test-node
  execution_type: system_node
  subscribes_to: [check.requested]
  produces: [check.passed]
  event_handlers:
    check.requested:
      guard:
        check: "payload.score >= policy.minimum_score"
        on_fail: discard
      advances_to: done
      emit: check.passed
`,
		`
minimum_score: 80
`,
		`
trigger:
  event: check.requested
  payload:
    entity_id: 11111111-1111-4111-8111-111111111111
    score: 42

expected:
  handler_outcome: discard
  entity_state: pending
  emitted_events: []
`,
	)

	result, expected := runSimpleCatalogCase(t, dir)
	if got, want := result.handlerOutcome, expected.Expected.HandlerOutcome; got != want {
		t.Fatalf("handler outcome = %q, want %q", got, want)
	}
	if got, want := result.entityState, expected.Expected.EntityState; got != want {
		t.Fatalf("entity state = %q, want %q", got, want)
	}
	if diff := diffStringSet(normalizeSorted(result.emittedEvents), normalizeSorted(expected.Expected.EmittedEvents)); diff != "" {
		t.Fatalf("emitted events mismatch (%s)", diff)
	}
}

func TestCatalogGuardPasses_SupportsStrictComparisonOperators(t *testing.T) {
	entity := map[string]any{"low": "2", "high": 7}
	payload := map[string]any{"score": "5"}
	policy := map[string]any{"floor": 3, "ceiling": "6"}

	cases := []struct {
		name string
		spec any
		want bool
	}{
		{
			name: "greater than",
			spec: map[string]any{"check": "payload.score > policy.floor"},
			want: true,
		},
		{
			name: "less than",
			spec: map[string]any{"check": "payload.score < policy.ceiling"},
			want: true,
		},
		{
			name: "less than fails",
			spec: map[string]any{"check": "entity.high < payload.score"},
			want: false,
		},
		{
			name: "greater than fails with string numeric",
			spec: map[string]any{"check": "entity.low > policy.floor"},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := catalogGuardPasses(tc.spec, payload, entity, policy, ""); got != tc.want {
				t.Fatalf("catalogGuardPasses() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCatalogRunner_BootPermissionMismatchUsesToolRequiredPermission(t *testing.T) {
	dir := filepath.Join(repoRootForTest(t), "tests", "tier8-boot-verification", "test-boot-permission-tool-mismatch")
	result, expected := runSimpleCatalogCase(t, dir)
	if got, want := result.bootResult, expected.Expected.BootResult; got != want {
		t.Fatalf("boot result = %q, want %q", got, want)
	}
	if got, want := result.errorCategory, expected.Expected.ErrorCategory; got != want {
		t.Fatalf("error category = %q, want %q", got, want)
	}
}

func TestCatalogRunner_TerminalFollowUpPreservesPriorEmits(t *testing.T) {
	dir := filepath.Join(repoRootForTest(t), "tests", "tier5-flow-lifecycle", "test-terminal-state-preserves")
	result, expected := runSimpleCatalogCase(t, dir)
	if got, want := result.handlerOutcome, expected.Expected.HandlerOutcome; got != want {
		t.Fatalf("handler outcome = %q, want %q", got, want)
	}
	if diff := diffStringSet(normalizeSorted(result.emittedEvents), normalizeSorted(expected.Expected.EmittedEvents)); diff != "" {
		t.Fatalf("emitted events mismatch (%s)", diff)
	}
}

func TestCatalogRunner_AgentEmitsRecordedEvenWhenConsumed(t *testing.T) {
	dir := filepath.Join(repoRootForTest(t), "tests", "tier7-composition", "test-agent-emits-to-node")
	result, expected := runSimpleCatalogCase(t, dir)
	if got, want := result.handlerOutcome, expected.Expected.HandlerOutcome; got != want {
		t.Fatalf("handler outcome = %q, want %q", got, want)
	}
	if diff := diffStringSet(normalizeSorted(result.emittedEvents), normalizeSorted(expected.Expected.EmittedEvents)); diff != "" {
		t.Fatalf("emitted events mismatch (%s)", diff)
	}
}

func TestCatalogToolRequiredPermission_AcceptsPermissionAlias(t *testing.T) {
	spec := map[string]any{"permission": "external_api_access"}
	if got, want := catalogToolRequiredPermission("lookup_data", spec), "external_api_access"; got != want {
		t.Fatalf("required permission = %q, want %q", got, want)
	}
}

func TestCatalogRequiredAgentIssuesRequireMapKeyIdentity(t *testing.T) {
	issues := catalogRequiredAgentIssues(catalogBootScope{
		Name: "analysis",
		Agents: map[string]any{
			"worker-alias": map[string]any{
				"id":            "worker",
				"role":          "worker",
				"subscriptions": []any{"analysis.requested"},
				"emit_events":   []any{"analysis.done"},
			},
		},
		Schema: map[string]any{
			"required_agents": []any{map[string]any{
				"role":          "worker",
				"subscribes_to": []any{"analysis.requested"},
				"emits":         []any{"analysis.done"},
			}},
		},
	})

	if len(issues) != 1 || issues[0].Category != "REQUIRED-AGENT" {
		t.Fatalf("issues = %#v, want REQUIRED-AGENT", issues)
	}
}

func TestCatalogRequiredAgentIssuesInferOmittedFromAgents(t *testing.T) {
	issues := catalogRequiredAgentIssues(catalogBootScope{
		Name: "analysis",
		Agents: map[string]any{
			"worker": map[string]any{
				"subscriptions": []any{"analysis.requested"},
				"emit_events":   []any{"analysis.done"},
			},
		},
		Schema: map[string]any{},
	})

	if len(issues) != 0 {
		t.Fatalf("issues = %#v, want omitted required_agents inferred from agents.yaml", issues)
	}
}

func TestCatalogRequiredAgentRequirementsPreserveExplicitEmpty(t *testing.T) {
	agents := catalogRequiredAgentEntries(map[string]any{
		"worker": map[string]any{
			"subscriptions": []any{"analysis.requested"},
		},
	})
	required := catalogEffectiveRequiredAgentRequirements(map[string]any{
		"required_agents": []any{},
	}, agents)

	if len(required) != 0 {
		t.Fatalf("required = %#v, want explicit empty required_agents without inference", required)
	}
}
