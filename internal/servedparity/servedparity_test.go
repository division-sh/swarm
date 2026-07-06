package servedparity

import (
	"strings"
	"testing"
)

func TestSettlementPostconditionFailuresRejectsUnsettledCounts(t *testing.T) {
	scenario := MustScenario(ScenarioEventPublishDynamicAutoEmitLifecycle)
	failures := SettlementPostconditionFailures(scenario, SettlementCounts{
		NonTerminalDeliveries: 1,
		PendingPipelineEvents: 2,
		UnfiredDueTimers:      3,
	})
	joined := strings.Join(failures, "\n")
	for _, want := range []string{
		"no_nonterminal_deliveries=1",
		"no_pending_pipeline_events=2",
		"no_unfired_due_timers=3",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("settlement failures missing %q:\n%s", want, joined)
		}
	}
}

func TestSettlementPostconditionFailuresAcceptsQuiescedCounts(t *testing.T) {
	scenario := MustScenario(ScenarioEventPublishDynamicAutoEmitLifecycle)
	if failures := SettlementPostconditionFailures(scenario, SettlementCounts{}); len(failures) != 0 {
		t.Fatalf("settlement failures = %v, want none", failures)
	}
}

func TestScenarioRegistryKeepsRequiredBackends(t *testing.T) {
	scenario := MustScenario(ScenarioEventPublishDynamicAutoEmitLifecycle)
	if len(scenario.Backends) != 2 {
		t.Fatalf("scenario backends = %v, want default SQLite and explicit Postgres", scenario.Backends)
	}
	if scenario.Backends[0] != BackendDefaultSQLite || scenario.Backends[1] != BackendExplicitPostgres {
		t.Fatalf("scenario backends = %v, want [%s %s]", scenario.Backends, BackendDefaultSQLite, BackendExplicitPostgres)
	}
}
