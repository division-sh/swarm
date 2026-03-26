package swarmflowtest

import (
	"slices"
	"strings"
	"testing"
)

func TestCatalogRunner_GuardOnFailBlockedPreservesStateAndSuppressesEmit(t *testing.T) {
	dir := t.TempDir()
	writeCatalogCaseFile(t, dir, "package.yaml", "name: guard-blocked\n")
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
        on_fail: blocked
      advances_to: done
      emits: check.passed
`,
		`
minimum_score: 80
`,
		`
trigger:
  event: check.requested
  payload:
    entity_id: ent-001
    score: 12

expected:
  handler_outcome: blocked
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

func TestCatalogRunner_TimerEventRequiresActiveNodeTimer(t *testing.T) {
	nodes := map[string]catalogNodeContract{
		"task-node": {
			Timers: []catalogNodeTimerContract{{
				ID:      "task_timer",
				StartOn: "task.started",
				Emits:   "timer.task_timeout",
			}},
		},
	}
	err := catalogValidateTimerEventActivation(nodes, map[string]catalogNodeTimerContract{}, "timer.task_timeout")
	if err == nil {
		t.Fatal("expected inactive timer event to fail")
	}
	if !strings.Contains(err.Error(), "timer.task_timeout") {
		t.Fatalf("error = %q, want timer event name", err)
	}
}

func TestDiscoveredCatalogCaseDirs_ReturnsSortedMASCatalogPackages(t *testing.T) {
	dirs := discoveredCatalogCaseDirs(t)
	if len(dirs) == 0 {
		t.Fatal("expected MAS catalog packages")
	}
	if !slices.IsSorted(dirs) {
		t.Fatalf("expected discovered MAS catalog dirs to be sorted, got %v", dirs)
	}
	for _, dir := range dirs {
		if dir == "" {
			t.Fatal("expected non-empty catalog dir name")
		}
	}
}

func TestCatalogRunner_SequenceOnlyTriggerUsesTrimmedMASEventName(t *testing.T) {
	dir := t.TempDir()
	writeCatalogCaseFile(t, dir, "package.yaml", "name: sequence-only\n")
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
      advances_to: done
      emits: check.passed
`,
		"{}\n",
		`
trigger:
  sequence:
    - event: "  check.requested  "
      payload:
        entity_id: ent-001

expected:
  handler_outcome: success
  entity_state: done
  emitted_events: [check.passed]
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
