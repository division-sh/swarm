package servedparity

import (
	"fmt"
	"strings"
	"testing"
)

type Backend string

const (
	BackendDefaultSQLite    Backend = "default_sqlite"
	BackendExplicitPostgres Backend = "explicit_postgres"
)

var RequiredBackends = []Backend{BackendDefaultSQLite, BackendExplicitPostgres}

type Postcondition string

const (
	PostconditionNoNonTerminalDeliveries Postcondition = "no_nonterminal_deliveries"
	PostconditionNoPendingPipelineEvents Postcondition = "no_pending_pipeline_events"
	PostconditionNoUnfiredDueTimers      Postcondition = "no_unfired_due_timers"
)

const ScenarioEventPublishDynamicAutoEmitLifecycle = "event_publish_dynamic_auto_emit_lifecycle"

type Scenario struct {
	ID             string
	APIMethod      string
	TestName       string
	Backends       []Backend
	Postconditions []Postcondition
}

type SettlementCounts struct {
	NonTerminalDeliveries int
	PendingPipelineEvents int
	UnfiredDueTimers      int
}

func Scenarios() []Scenario {
	return []Scenario{
		{
			ID:        ScenarioEventPublishDynamicAutoEmitLifecycle,
			APIMethod: "event.publish",
			TestName:  "TestServedParityHarnessEventPublishDynamicAutoEmitLifecycle",
			Backends:  append([]Backend(nil), RequiredBackends...),
			Postconditions: []Postcondition{
				PostconditionNoNonTerminalDeliveries,
				PostconditionNoPendingPipelineEvents,
				PostconditionNoUnfiredDueTimers,
			},
		},
	}
}

func ScenarioByID(id string) (Scenario, bool) {
	id = strings.TrimSpace(id)
	for _, scenario := range Scenarios() {
		if scenario.ID == id {
			return scenario, true
		}
	}
	return Scenario{}, false
}

func MustScenario(id string) Scenario {
	scenario, ok := ScenarioByID(id)
	if !ok {
		panic(fmt.Sprintf("unknown served parity scenario %q", id))
	}
	return scenario
}

func Run(t *testing.T, scenario Scenario, run func(*testing.T, Backend)) {
	t.Helper()
	if strings.TrimSpace(scenario.ID) == "" {
		t.Fatal("served parity scenario missing id")
	}
	if len(scenario.Backends) == 0 {
		t.Fatalf("served parity scenario %s missing backends", scenario.ID)
	}
	for _, backend := range scenario.Backends {
		backend := backend
		t.Run(string(backend), func(t *testing.T) {
			run(t, backend)
		})
	}
}

func SettlementPostconditionFailures(scenario Scenario, counts SettlementCounts) []string {
	var failures []string
	for _, postcondition := range scenario.Postconditions {
		switch postcondition {
		case PostconditionNoNonTerminalDeliveries:
			if counts.NonTerminalDeliveries != 0 {
				failures = append(failures, fmt.Sprintf("%s=%d", postcondition, counts.NonTerminalDeliveries))
			}
		case PostconditionNoPendingPipelineEvents:
			if counts.PendingPipelineEvents != 0 {
				failures = append(failures, fmt.Sprintf("%s=%d", postcondition, counts.PendingPipelineEvents))
			}
		case PostconditionNoUnfiredDueTimers:
			if counts.UnfiredDueTimers != 0 {
				failures = append(failures, fmt.Sprintf("%s=%d", postcondition, counts.UnfiredDueTimers))
			}
		default:
			failures = append(failures, fmt.Sprintf("unknown_postcondition=%s", postcondition))
		}
	}
	return failures
}

func AssertSettlementPostconditions(t *testing.T, scenario Scenario, counts SettlementCounts) {
	t.Helper()
	if failures := SettlementPostconditionFailures(scenario, counts); len(failures) > 0 {
		t.Fatalf("served parity scenario %s settlement postconditions failed: %s", scenario.ID, strings.Join(failures, ", "))
	}
}
