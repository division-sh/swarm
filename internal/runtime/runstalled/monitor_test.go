package runstalled

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
)

func TestMonitorEmitsOnlyAfterThreshold(t *testing.T) {
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	reader := newFakeReader(RunSnapshot{
		RunID:          "run-1",
		RunTableStatus: "running",
		LastProgressAt: now.Add(-(Threshold - time.Second)),
		Diagnosis:      stalledDiagnosis("delivery_lifecycle", "no_active_deliveries"),
	})
	publisher := &fakePublisher{}
	monitor := Monitor{Reader: reader, Publisher: publisher, ExecutionPosture: executionposture.Live}
	if result, err := monitor.CheckOnce(context.Background(), now); err != nil {
		t.Fatalf("CheckOnce before threshold: %v", err)
	} else if result.Published != 0 || len(publisher.events) != 0 {
		t.Fatalf("before threshold published result=%+v events=%d, want none", result, len(publisher.events))
	}

	reader.snapshots["run-1"] = RunSnapshot{
		RunID:          "run-1",
		RunTableStatus: "running",
		LastProgressAt: now.Add(-Threshold),
		Diagnosis:      stalledDiagnosis("delivery_lifecycle", "no_active_deliveries"),
	}
	if result, err := monitor.CheckOnce(context.Background(), now); err != nil {
		t.Fatalf("CheckOnce at threshold: %v", err)
	} else if result.Published != 1 || len(publisher.events) != 1 {
		t.Fatalf("at threshold published result=%+v events=%d, want one", result, len(publisher.events))
	}
	payload := eventPayload(t, publisher.events[0])
	if got := payload["blocking_layer"]; got != "delivery_lifecycle" {
		t.Fatalf("blocking_layer = %#v, want delivery_lifecycle", got)
	}
	if got := payload["threshold_seconds"]; got != float64(DefaultThresholdSeconds) {
		t.Fatalf("threshold_seconds = %#v, want %d", got, DefaultThresholdSeconds)
	}
}

func TestMonitorDoesNotDuplicateSameStalledEvidence(t *testing.T) {
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	lastProgress := now.Add(-Threshold)
	reader := newFakeReader(RunSnapshot{
		RunID:          "run-1",
		RunTableStatus: "running",
		LastProgressAt: lastProgress,
		Diagnosis:      stalledDiagnosis("delivery_lifecycle", "no_active_deliveries"),
	})
	publisher := &fakePublisher{}
	monitor := Monitor{Reader: reader, Publisher: publisher, ExecutionPosture: executionposture.Live}
	_, key, ok, err := monitor.eventForSnapshot(reader.snapshots["run-1"], now)
	if err != nil || !ok {
		t.Fatalf("initial alert identity: %v %v", ok, err)
	}
	reader.existing[key] = true
	result, err := monitor.CheckOnce(context.Background(), now)
	if err != nil {
		t.Fatalf("CheckOnce: %v", err)
	}
	if result.Published != 0 || len(publisher.events) != 0 {
		t.Fatalf("published duplicate result=%+v events=%d, want none", result, len(publisher.events))
	}

	snapshot := reader.snapshots["run-1"]
	first := snapshot.Diagnosis.StalledInstances[0]
	first.FlowInstance, first.Attribution.FlowInstance = "review/one", "review/one"
	second := first
	second.FlowInstance, second.Attribution.FlowInstance = "review/two", "review/two"
	snapshot.Diagnosis.StalledInstances = []StalledInstance{first, second}
	evt, pairKey, ok, err := monitor.eventForSnapshot(snapshot, now)
	if err != nil || !ok {
		t.Fatalf("paired alert: %v %v", ok, err)
	}
	if pairKey == key {
		t.Fatal("changing the stalled-instance set did not change dedup identity")
	}
	reader.snapshots["run-1"] = snapshot
	if result, err := monitor.CheckOnce(context.Background(), now); err != nil || result.Published != 1 {
		t.Fatalf("new attribution set must publish despite unchanged progress: %+v %v", result, err)
	}
	retained := eventPayload(t, evt)
	snapshot.Diagnosis.StalledInstances = []StalledInstance{second, first, second}
	_, reorderedKey, ok, err := monitor.eventForSnapshot(snapshot, now)
	if err != nil || !ok || reorderedKey != pairKey {
		t.Fatalf("set order or duplicate changed identity: %+v %+v %v", pairKey, reorderedKey, err)
	}
	if matches, err := reorderedKey.MatchesPayload(retained); err != nil || !matches {
		t.Fatalf("retained payload lost exact set identity: %v %v", matches, err)
	}

	for _, field := range []string{"agent", "event", "delivery", "provider_turn", "reason", "progress", "run"} {
		t.Run(field, func(t *testing.T) {
			changed := snapshot
			changed.Diagnosis.StalledInstances = append([]StalledInstance{}, snapshot.Diagnosis.StalledInstances...)
			row := &changed.Diagnosis.StalledInstances[0]
			switch field {
			case "agent":
				row.Attribution.AgentID = "different-agent"
			case "event":
				row.Attribution.EventID = "different-event"
			case "delivery":
				row.Attribution.DeliveryID = "different-delivery"
			case "provider_turn":
				row.Attribution.ProviderTurns = []string{"different-turn"}
			case "reason":
				row.BlockingReason = "different-reason"
			case "progress":
				changed.LastProgressAt = changed.LastProgressAt.Add(-time.Second)
			case "run":
				changed.RunID = "different-run"
			}
			_, changedKey, ok, err := monitor.eventForSnapshot(changed, now)
			if err != nil || !ok {
				t.Fatalf("changed identity: %v %v", ok, err)
			}
			if matches, err := changedKey.MatchesPayload(retained); err != nil || matches {
				t.Fatalf("changed %s was suppressed by older evidence: %v %v", field, matches, err)
			}
		})
	}
	for _, mutation := range []string{"missing_list", "retired_scope", "historical", "foreign_instance", "unknown_field", "invalid_evidence"} {
		t.Run(mutation, func(t *testing.T) {
			payload := eventPayload(t, evt)
			rows := payload["stalled_instances"].([]any)
			row := rows[0].(map[string]any)
			origin := row["attribution"].(map[string]any)
			wantError := ""
			switch mutation {
			case "missing_list":
				delete(payload, "stalled_instances")
			case "retired_scope":
				payload["flow_instance"] = "review/one"
			case "historical":
				origin["historical"] = true
				wantError = "stalled-instance attribution contradicts current instance"
			case "foreign_instance":
				origin["flow_instance"] = "other/instance"
				wantError = "stalled-instance attribution contradicts current instance"
			case "unknown_field":
				row["unclassified"] = true
				wantError = `decode retained stalled-instance evidence: json: unknown field "unclassified"`
			case "invalid_evidence":
				origin["emission_evidence"] = "silence_assumed"
				wantError = "has invalid emission evidence"
			}
			matches, err := pairKey.MatchesPayload(payload)
			if matches || (wantError == "" && err != nil) || (wantError != "" && (err == nil || !strings.Contains(err.Error(), wantError))) {
				t.Fatalf("%s evidence matched or lost exact refusal: matches=%t err=%v want=%q", mutation, matches, err, wantError)
			}
		})
	}
}

func TestMonitorSuppressesNonStalledTerminalAndZeroProgressRuns(t *testing.T) {
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	reader := newFakeReader(
		RunSnapshot{
			RunID:          "active-delivery",
			RunTableStatus: "running",
			LastProgressAt: now.Add(-Threshold),
			Diagnosis: Diagnosis{
				OperationalState: "running",
			},
		},
		RunSnapshot{
			RunID:          "terminal",
			RunTableStatus: "completed",
			LastProgressAt: now.Add(-Threshold),
			Diagnosis: Diagnosis{
				OperationalState: "completed",
			},
		},
		RunSnapshot{
			RunID:          "zero-progress",
			RunTableStatus: "running",
			Diagnosis:      stalledDiagnosis("delivery_lifecycle", "no_active_deliveries"),
		},
	)
	publisher := &fakePublisher{}
	monitor := Monitor{Reader: reader, Publisher: publisher, ExecutionPosture: executionposture.Live}
	result, err := monitor.CheckOnce(context.Background(), now)
	if err != nil {
		t.Fatalf("CheckOnce: %v", err)
	}
	if result.Published != 0 || len(publisher.events) != 0 {
		t.Fatalf("published suppressed runs result=%+v events=%d, want none", result, len(publisher.events))
	}
}

func TestMonitorUsesFixedThresholdAcrossScopes(t *testing.T) {
	// Invariant: every run uses the same 300s threshold; former per-flow enable
	// and threshold choices no longer suppress any eligible scope.
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	reader := newFakeReader(
		RunSnapshot{
			RunID:          "disabled",
			RunTableStatus: "running",
			LastProgressAt: now.Add(-time.Hour),
			Diagnosis:      stalledDiagnosis("delivery_lifecycle", "no_active_deliveries"),
		},
		RunSnapshot{
			RunID:          "extended-too-young",
			RunTableStatus: "running",
			LastProgressAt: now.Add(-Threshold),
			Diagnosis:      stalledDiagnosis("delivery_lifecycle", "no_active_deliveries"),
		},
		RunSnapshot{
			RunID:          "extended-old-enough",
			RunTableStatus: "running",
			LastProgressAt: now.Add(-10 * time.Minute),
			Diagnosis:      stalledDiagnosis("delivery_lifecycle", "no_active_deliveries"),
		},
	)
	publisher := &fakePublisher{}
	monitor := Monitor{
		Reader:           reader,
		Publisher:        publisher,
		ExecutionPosture: executionposture.Live,
	}
	result, err := monitor.CheckOnce(context.Background(), now)
	if err != nil {
		t.Fatalf("CheckOnce: %v", err)
	}
	if result.Published != 3 || len(publisher.events) != 3 {
		t.Fatalf("fixed threshold result=%+v events=%d, want all three", result, len(publisher.events))
	}
}

func TestMonitorPublishesDomainNeutralDiagnosisWithExactAttribution(t *testing.T) {
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	reader := newFakeReader(
		RunSnapshot{
			RunID:          "delivery-run",
			RunTableStatus: "running",
			LastProgressAt: now.Add(-Threshold),
			Diagnosis:      stalledDiagnosis("delivery_lifecycle", "no_active_deliveries"),
		},
		RunSnapshot{
			RunID:          "unrelated-run",
			RunTableStatus: "running",
			LastProgressAt: now.Add(-Threshold),
			Diagnosis: Diagnosis{OperationalState: "stalled", BlockingLayer: "delivery_lifecycle", BlockingReason: "no_active_deliveries",
				StalledInstances: []StalledInstance{{FlowInstance: "review/one", BlockingLayer: "delivery_lifecycle", BlockingReason: "no_active_deliveries", Attribution: operatorread.RunTurnAttribution{FlowInstance: "review/one", AgentID: "reviewer", EventID: "request", DeliveryID: "delivery", Emission: operatorread.RunEmissionNone}}}},
		},
	)
	publisher := &fakePublisher{}
	monitor := Monitor{Reader: reader, Publisher: publisher, ExecutionPosture: executionposture.Live}
	result, err := monitor.CheckOnce(context.Background(), now)
	if err != nil {
		t.Fatalf("CheckOnce: %v", err)
	}
	if result.Published != 2 || len(publisher.events) != 2 {
		t.Fatalf("published result=%+v events=%d, want two", result, len(publisher.events))
	}
	got := map[string]string{}
	for _, evt := range publisher.events {
		payload := eventPayload(t, evt)
		if _, present := payload["flow_instance"]; present || evt.FlowInstance() != "" {
			t.Fatalf("run-level alert selected one instance: %#v %q", payload, evt.FlowInstance())
		}
		got[evt.RunID()] = payload["blocking_layer"].(string) + "/" + payload["blocking_reason"].(string)
		attributions, ok := payload["stalled_instances"].([]any)
		if !ok {
			t.Fatalf("stalled_instances must be an array: %#v", payload)
		}
		if evt.RunID() == "unrelated-run" {
			if len(attributions) != 1 {
				t.Fatalf("exact origin disappeared: %#v", payload)
			}
			attribution := attributions[0].(map[string]any)["attribution"].(map[string]any)
			if attribution["flow_instance"] != "review/one" || attribution["agent_id"] != "reviewer" || attribution["event_id"] != "request" || attribution["delivery_id"] != "delivery" || attribution["emission_evidence"] != "none" {
				t.Fatalf("exact origin changed: %#v", attribution)
			}
		}
	}
	if got["delivery-run"] != "delivery_lifecycle/no_active_deliveries" {
		t.Fatalf("delivery-run payload = %q", got["delivery-run"])
	}
	if got["unrelated-run"] != "delivery_lifecycle/no_active_deliveries" {
		t.Fatalf("unrelated-run payload = %q", got["unrelated-run"])
	}
}

func stalledDiagnosis(layer, reason string) Diagnosis {
	return Diagnosis{OperationalState: "stalled", BlockingLayer: layer, BlockingReason: reason, StalledInstances: []StalledInstance{{BlockingLayer: layer, BlockingReason: reason, Attribution: operatorread.RunTurnAttribution{Emission: operatorread.RunEmissionUnknown}}}}
}

type fakeReader struct {
	refs      []RunRef
	snapshots map[string]RunSnapshot
	existing  map[EscalationKey]bool
}

func newFakeReader(snapshots ...RunSnapshot) *fakeReader {
	reader := &fakeReader{
		refs:      []RunRef{},
		snapshots: map[string]RunSnapshot{},
		existing:  map[EscalationKey]bool{},
	}
	for _, snapshot := range snapshots {
		reader.refs = append(reader.refs, RunRef{RunID: snapshot.RunID})
		reader.snapshots[snapshot.RunID] = snapshot
	}
	return reader
}

func (f *fakeReader) ListRunningRuns(context.Context, int, string) ([]RunRef, string, error) {
	return append([]RunRef{}, f.refs...), "", nil
}

func (f *fakeReader) LoadRunSnapshot(_ context.Context, runID string) (RunSnapshot, error) {
	return f.snapshots[runID], nil
}

func (f *fakeReader) StalledRunEscalationExists(_ context.Context, key EscalationKey) (bool, error) {
	return f.existing[key], nil
}

type fakePublisher struct {
	events []events.Event
}

func (f *fakePublisher) Publish(_ context.Context, evt events.Event) error {
	f.events = append(f.events, evt)
	return nil
}

func eventPayload(t *testing.T, evt events.Event) map[string]any {
	t.Helper()
	payload := map[string]any{}
	if err := json.Unmarshal(evt.Payload(), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	return payload
}
