package operatorread

import (
	"reflect"
	"testing"
	"time"
)

func TestRunOperationalStatusPreservesExistingWorkFacts(t *testing.T) {
	for _, tc := range []struct {
		name string
		work RunTestQuiescence
	}{
		{"delivery", RunTestQuiescence{ActiveDeliveries: 1}},
		{"pipeline", RunTestQuiescence{UnsettledPipelineEvents: 1}},
		{"fan_out_owed", RunTestQuiescence{FanOutOwed: 1}},
		{"fan_out_unsettled", RunTestQuiescence{FanOutUnsettled: 1}},
		{"barrier", RunTestQuiescence{FanOutBarriers: 1}},
		{"due_timer", RunTestQuiescence{DueTimers: 1}},
		{"session", RunTestQuiescence{ActiveSessionLeases: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ProjectRunOperationalStatus(RunDebugReport{
				RunTableStatus: "running", LastEventAt: time.Unix(10, 0).UTC(), TestQuiescence: tc.work,
			})
			if got.State != "running" || got.BlockingLayer != "" || got.BlockingReason != "" {
				t.Fatalf("existing %s work was classified as a stall: %+v", tc.name, got)
			}
		})
	}
}

func TestRunOperationalStatusProjectsEveryInstanceWithoutChangingRunClock(t *testing.T) {
	now := time.Unix(500, 0).UTC()
	first := RunTurnAttribution{
		FlowInstance: "workers/a", AgentID: "worker", DeliveryID: "delivery-a", EventID: "event-a",
		LogicalTurnID: "logical-a", ProviderTurns: []string{"provider-a"}, Emission: RunEmissionNone,
		GrantedEmitTools: []string{}, EmittedEvents: []RunAcceptedEmission{},
	}
	second := first
	second.FlowInstance, second.DeliveryID, second.EventID, second.LogicalTurnID = "workers/b", "delivery-b", "event-b", "logical-b"
	for _, tc := range []struct {
		name        string
		other       RunInstanceFacts
		want        string
		disposition string
	}{
		{"two_stalls", RunInstanceFacts{FlowInstance: second.FlowInstance, Complete: true, LastTurn: &second}, "stalled", "stalled"},
		{"active_sibling", RunInstanceFacts{FlowInstance: "workers/b", Complete: true, Work: RunInstanceWork{Deliveries: 1}}, "running", "active"},
		{"future_timer", RunInstanceFacts{FlowInstance: "workers/b", Complete: true, Work: RunInstanceWork{Timers: 1}}, "running", "waiting"},
		{"human_wait", RunInstanceFacts{FlowInstance: "workers/b", Complete: true, Work: RunInstanceWork{Await: 1}}, "running", "waiting"},
		{"final_sibling", RunInstanceFacts{FlowInstance: "workers/b", Complete: true, Final: true}, "stalled", "final"},
		{"unknown_sibling", RunInstanceFacts{FlowInstance: "workers/b"}, "running", "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := RunDebugReport{RunTableStatus: "running", LastEventAt: now, OperationalFacts: &RunOperationalFacts{
				Complete: true, LastProgressAt: now.Add(-400 * time.Second),
				Instances: []RunInstanceFacts{tc.other, {FlowInstance: first.FlowInstance, Complete: true, LastTurn: &first}},
			}}
			got := ProjectRunOperationalStatus(report)
			if got.State != tc.want || len(got.Attributions) < 1 || !reflect.DeepEqual(got.Attributions[0], first) ||
				len(got.Instances) != 2 || got.Instances[1].Disposition != tc.disposition {
				t.Fatalf("projection = %+v, want state=%s sibling=%s and exact first origin", got, tc.want, tc.disposition)
			}
			if tc.name == "two_stalls" && (len(got.Attributions) != 2 || !reflect.DeepEqual(got.Attributions[1], second)) {
				t.Fatalf("second same-slug origin lost: %+v", got.Attributions)
			}
			if report.LastEventAt != now || report.OperationalFacts.LastProgressAt != now.Add(-400*time.Second) {
				t.Fatal("projection changed the run-level progress clock")
			}
		})
	}
}

func TestRunOperationalStatusNeverInventsFinalCompletionOrOrigin(t *testing.T) {
	for _, status := range []string{"running", "paused", "completed", "failed", "cancelled", "forked"} {
		t.Run(status, func(t *testing.T) {
			got := ProjectRunOperationalStatus(RunDebugReport{RunTableStatus: status, OperationalFacts: &RunOperationalFacts{
				Complete: true, Instances: []RunInstanceFacts{{FlowInstance: "work/a", Complete: true, Final: true}},
			}})
			if got.State != status || len(got.Attributions) != 0 {
				t.Fatalf("final header invented run completion or origin: %+v", got)
			}
		})
	}
	foreign := RunTurnAttribution{FlowInstance: "work/old", AgentID: "old-worker", Emission: RunEmissionNone}
	got := ProjectRunOperationalStatus(RunDebugReport{RunTableStatus: "running", OperationalFacts: &RunOperationalFacts{
		Complete: true, Instances: []RunInstanceFacts{{FlowInstance: "work/new", Complete: true, LastTurn: &foreign}},
	}})
	if got.State != "stalled" || len(got.Attributions) != 1 || got.Attributions[0].AgentID != "" || got.Attributions[0].Emission != RunEmissionUnknown {
		t.Fatalf("foreign historical origin blamed current instance: %+v", got)
	}
}

func TestRunOperationalStatusDetachesAttributionAndClearsItAfterProgress(t *testing.T) {
	turn := RunTurnAttribution{FlowInstance: "work/a", ProviderTurns: []string{"turn"}, GrantedEmitTools: []string{"emit_done"}, EmittedEvents: []RunAcceptedEmission{{EventID: "output", EventType: "done"}}}
	report := RunDebugReport{RunTableStatus: "running", OperationalFacts: &RunOperationalFacts{
		Complete: true, Instances: []RunInstanceFacts{{FlowInstance: "work/a", Complete: true, LastTurn: &turn}},
	}}
	got := ProjectRunOperationalStatus(report)
	got.Attributions[0].ProviderTurns[0] = "corrupt"
	got.Attributions[0].GrantedEmitTools[0] = "corrupt"
	got.Attributions[0].EmittedEvents[0].EventID = "corrupt"
	if turn.ProviderTurns[0] != "turn" || turn.GrantedEmitTools[0] != "emit_done" || turn.EmittedEvents[0].EventID != "output" {
		t.Fatal("projection leaked mutable historical facts")
	}
	report.OperationalFacts.Instances[0].Work.Deliveries = 1
	if after := ProjectRunOperationalStatus(report); after.State != "running" || len(after.Attributions) != 0 {
		t.Fatalf("new instance work retained old stall attribution: %+v", after)
	}
}

func TestRunOperationalStatusDoesNotAssignUnscopedWorkOrHistoricalDrainToSuccessor(t *testing.T) {
	historical := RunTurnAttribution{FlowInstance: "work/a", AgentID: "worker", DeliveryID: "old-delivery", Historical: true,
		Emission: RunEmissionUnknown, EvidenceReason: "historical_drained_completion", ProviderTurns: []string{"old-turn"}}
	facts := &RunOperationalFacts{Complete: true, Instances: []RunInstanceFacts{{FlowInstance: "work/a", Complete: true, Revision: 2}},
		HistoricalTurns: []RunTurnAttribution{historical}}
	report := RunDebugReport{RunTableStatus: "running", OperationalFacts: facts}
	got := ProjectRunOperationalStatus(report)
	if got.State != "stalled" || len(got.Attributions) != 2 || !got.Attributions[0].Historical || got.Attributions[1].AgentID != "" || got.Attributions[1].Emission != RunEmissionUnknown {
		t.Fatalf("historical drain became successor blame or disappeared: %+v", got)
	}
	got.Attributions[0].ProviderTurns[0] = "changed"
	if facts.HistoricalTurns[0].ProviderTurns[0] != "old-turn" {
		t.Fatal("historical attribution is not detached")
	}
	facts.UnscopedWork.Pipeline = 1
	got = ProjectRunOperationalStatus(report)
	if got.State != "running" || got.Instances[0].Disposition != "unknown" || len(got.Attributions) != 1 || !got.Attributions[0].Historical {
		t.Fatalf("unscoped work fabricated current-instance quiescence: %+v", got)
	}
}
