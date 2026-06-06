package runstalled

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
)

func TestMonitorEmitsOnlyAfterThreshold(t *testing.T) {
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	reader := newFakeReader(RunSnapshot{
		RunID:          "run-1",
		RunTableStatus: "running",
		LastProgressAt: now.Add(-(DefaultPolicy().Threshold - time.Second)),
		Diagnosis:      stalledDiagnosis("delivery_lifecycle", "no_active_deliveries"),
	})
	publisher := &fakePublisher{}
	monitor := Monitor{Reader: reader, Publisher: publisher}
	if result, err := monitor.CheckOnce(context.Background(), now); err != nil {
		t.Fatalf("CheckOnce before threshold: %v", err)
	} else if result.Published != 0 || len(publisher.events) != 0 {
		t.Fatalf("before threshold published result=%+v events=%d, want none", result, len(publisher.events))
	}

	reader.snapshots["run-1"] = RunSnapshot{
		RunID:          "run-1",
		RunTableStatus: "running",
		LastProgressAt: now.Add(-DefaultPolicy().Threshold),
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
	lastProgress := now.Add(-DefaultPolicy().Threshold)
	reader := newFakeReader(RunSnapshot{
		RunID:          "run-1",
		RunTableStatus: "running",
		LastProgressAt: lastProgress,
		Diagnosis:      stalledDiagnosis("delivery_lifecycle", "no_active_deliveries"),
	})
	reader.existing[EscalationKey{
		RunID:          "run-1",
		BlockingLayer:  "delivery_lifecycle",
		BlockingReason: "no_active_deliveries",
		LastProgressAt: lastProgress,
	}] = true
	publisher := &fakePublisher{}
	monitor := Monitor{Reader: reader, Publisher: publisher}
	result, err := monitor.CheckOnce(context.Background(), now)
	if err != nil {
		t.Fatalf("CheckOnce: %v", err)
	}
	if result.Published != 0 || len(publisher.events) != 0 {
		t.Fatalf("published duplicate result=%+v events=%d, want none", result, len(publisher.events))
	}
}

func TestMonitorSuppressesNonStalledTerminalAndZeroProgressRuns(t *testing.T) {
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	reader := newFakeReader(
		RunSnapshot{
			RunID:          "active-delivery",
			RunTableStatus: "running",
			LastProgressAt: now.Add(-DefaultPolicy().Threshold),
			Diagnosis: Diagnosis{
				OperationalState: "running",
			},
		},
		RunSnapshot{
			RunID:          "terminal",
			RunTableStatus: "completed",
			LastProgressAt: now.Add(-DefaultPolicy().Threshold),
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
	monitor := Monitor{Reader: reader, Publisher: publisher}
	result, err := monitor.CheckOnce(context.Background(), now)
	if err != nil {
		t.Fatalf("CheckOnce: %v", err)
	}
	if result.Published != 0 || len(publisher.events) != 0 {
		t.Fatalf("published suppressed runs result=%+v events=%d, want none", result, len(publisher.events))
	}
}

func TestMonitorHonorsDisabledAndExtendedPolicies(t *testing.T) {
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	reader := newFakeReader(
		RunSnapshot{
			RunID:          "disabled",
			RunTableStatus: "running",
			FlowInstance:   "disabled-flow/inst-1",
			LastProgressAt: now.Add(-time.Hour),
			Diagnosis:      stalledDiagnosis("delivery_lifecycle", "no_active_deliveries"),
		},
		RunSnapshot{
			RunID:          "extended-too-young",
			RunTableStatus: "running",
			FlowInstance:   "slow-flow/inst-1",
			LastProgressAt: now.Add(-DefaultPolicy().Threshold),
			Diagnosis:      stalledDiagnosis("delivery_lifecycle", "no_active_deliveries"),
		},
		RunSnapshot{
			RunID:          "extended-old-enough",
			RunTableStatus: "running",
			FlowInstance:   "slow-flow/inst-2",
			LastProgressAt: now.Add(-10 * time.Minute),
			Diagnosis:      stalledDiagnosis("delivery_lifecycle", "no_active_deliveries"),
		},
	)
	publisher := &fakePublisher{}
	monitor := Monitor{
		Reader:    reader,
		Publisher: publisher,
		PolicyResolver: func(flowInstance string) Policy {
			switch flowInstance {
			case "disabled-flow/inst-1":
				return Policy{Enabled: false, Threshold: DefaultPolicy().Threshold}
			case "slow-flow/inst-1", "slow-flow/inst-2":
				return Policy{Enabled: true, Threshold: 10 * time.Minute}
			default:
				return DefaultPolicy()
			}
		},
	}
	result, err := monitor.CheckOnce(context.Background(), now)
	if err != nil {
		t.Fatalf("CheckOnce: %v", err)
	}
	if result.Published != 1 || len(publisher.events) != 1 {
		t.Fatalf("published result=%+v events=%d, want exactly one", result, len(publisher.events))
	}
	if publisher.events[0].RunID() != "extended-old-enough" {
		t.Fatalf("published run = %q, want extended-old-enough", publisher.events[0].RunID())
	}
}

func TestMonitorPublishesBothSupportedStalledDiagnosisReasons(t *testing.T) {
	now := time.Date(2026, 6, 4, 12, 0, 0, 0, time.UTC)
	reader := newFakeReader(
		RunSnapshot{
			RunID:          "delivery-run",
			RunTableStatus: "running",
			LastProgressAt: now.Add(-DefaultPolicy().Threshold),
			Diagnosis:      stalledDiagnosis("delivery_lifecycle", "no_active_deliveries"),
		},
		RunSnapshot{
			RunID:          "scoring-run",
			RunTableStatus: "running",
			LastProgressAt: now.Add(-DefaultPolicy().Threshold),
			Diagnosis:      stalledDiagnosis("scoring_terminal_outcome", "terminal_scoring_outcome_missing"),
		},
	)
	publisher := &fakePublisher{}
	monitor := Monitor{Reader: reader, Publisher: publisher}
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
		got[evt.RunID()] = payload["blocking_layer"].(string) + "/" + payload["blocking_reason"].(string)
	}
	if got["delivery-run"] != "delivery_lifecycle/no_active_deliveries" {
		t.Fatalf("delivery-run payload = %q", got["delivery-run"])
	}
	if got["scoring-run"] != "scoring_terminal_outcome/terminal_scoring_outcome_missing" {
		t.Fatalf("scoring-run payload = %q", got["scoring-run"])
	}
}

func stalledDiagnosis(layer, reason string) Diagnosis {
	return Diagnosis{OperationalState: "stalled", BlockingLayer: layer, BlockingReason: reason}
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
