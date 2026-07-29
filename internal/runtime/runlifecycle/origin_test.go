package runlifecycle

import (
	"encoding/json"
	"testing"
)

func TestRunOriginRoundTripsEveryClosedVariant(t *testing.T) {
	event, err := EventRunOrigin("event-1", "scan.requested")
	if err != nil {
		t.Fatal(err)
	}
	standing, err := StandingGenerationRunOrigin("service-1", 3)
	if err != nil {
		t.Fatal(err)
	}
	fork, err := ForkMaterializationRunOrigin("run-parent", "event-parent")
	if err != nil {
		t.Fatal(err)
	}
	for _, origin := range []RunOrigin{
		event,
		ScenarioSetupRunOrigin(),
		standing,
		fork,
	} {
		raw, err := json.Marshal(origin)
		if err != nil {
			t.Fatalf("marshal %s origin: %v", origin.Kind(), err)
		}
		var decoded RunOrigin
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("unmarshal %s origin: %v", origin.Kind(), err)
		}
		if !decoded.Equal(origin) {
			t.Fatalf("round trip %s origin = %#v, want %#v", origin.Kind(), decoded, origin)
		}
	}
}

func TestRunOriginRejectsEveryPartialAndMixedShape(t *testing.T) {
	for _, tc := range []struct {
		name          string
		kind          string
		eventID       string
		eventType     string
		serviceID     string
		generation    int64
		sourceRunID   string
		sourceEventID string
	}{
		{name: "unknown_kind", kind: "unknown"},
		{name: "event_missing_id", kind: string(OriginEvent), eventType: "scan.requested"},
		{name: "event_missing_type", kind: string(OriginEvent), eventID: "event-1"},
		{name: "event_with_standing", kind: string(OriginEvent), eventID: "event-1", eventType: "scan.requested", serviceID: "service-1", generation: 1},
		{name: "scenario_with_event", kind: string(OriginScenarioSetup), eventID: "event-1", eventType: "scan.requested"},
		{name: "standing_missing_service", kind: string(OriginStandingGeneration), generation: 1},
		{name: "standing_nonpositive_generation", kind: string(OriginStandingGeneration), serviceID: "service-1"},
		{name: "standing_with_fork", kind: string(OriginStandingGeneration), serviceID: "service-1", generation: 1, sourceRunID: "run-1", sourceEventID: "event-1"},
		{name: "fork_missing_run", kind: string(OriginForkMaterialization), sourceEventID: "event-1"},
		{name: "fork_missing_event", kind: string(OriginForkMaterialization), sourceRunID: "run-1"},
		{name: "fork_with_event", kind: string(OriginForkMaterialization), eventID: "event-1", eventType: "scan.requested", sourceRunID: "run-1", sourceEventID: "event-2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeRunOrigin(
				tc.kind,
				tc.eventID,
				tc.eventType,
				tc.serviceID,
				tc.generation,
				tc.sourceRunID,
				tc.sourceEventID,
			); err == nil {
				t.Fatal("invalid run origin was accepted")
			}
		})
	}
}

func TestRunOriginJSONRejectsUnknownFields(t *testing.T) {
	var origin RunOrigin
	if err := json.Unmarshal(
		[]byte(`{"kind":"scenario_setup","unexpected":"value"}`),
		&origin,
	); err == nil {
		t.Fatal("run origin JSON accepted an unknown field")
	}
}

func TestRunOriginJSONRejectsTrailingValue(t *testing.T) {
	var origin RunOrigin
	if err := json.Unmarshal([]byte(`{"kind":"scenario_setup"} {}`), &origin); err == nil {
		t.Fatal("trailing run origin JSON was accepted")
	}
}
