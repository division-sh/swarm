package runforkpersistence

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func inheritedTimerSnapshot(t *testing.T) runforkrevision.TimerSnapshot {
	t.Helper()
	const runID = "11111111-1111-4111-8111-111111111111"
	const entityID = "22222222-2222-4222-8222-222222222222"
	source, err := events.NewRootRoutingSource(entityID)
	if err != nil {
		t.Fatal(err)
	}
	routing, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	ref := timeridentity.WorkflowTimerActivationRef{
		ActivationID: "33333333-3333-4333-8333-333333333333", DeclarationKey: "waiting.timeout",
		DeclarationRevision: "revision", Cause: timeridentity.WorkflowTimerActivationCauseInitial,
	}
	armedAt := time.Date(2026, 10, 9, 0, 0, 0, 123456000, time.UTC)
	return runforkrevision.TimerSnapshot{
		TimerID: ref.ActivationID, TimerName: ref.TaskID(), RunID: runID, EntityID: entityID,
		FlowScopeKey: ".", FlowInstanceID: runID, FlowInstance: runID, RoutingSource: routing,
		OwnerAgent: "timer-owner", OwnerKind: "system", FireEvent: "timer.elapsed", ExecutionMode: "live", FirePayload: json.RawMessage(`{}`), ClockSuspension: json.RawMessage(`null`),
		CreatedAt: armedAt.Add(3 * time.Hour), FireAt: armedAt.Add(time.Hour), TaskType: "workflow_timer", Status: "active",
		SourceTimerID: "44444444-4444-4444-8444-444444444444", ForkedFromRunID: "55555555-5555-4555-8555-555555555555",
		ForkedFromPointKind: forkpoint.RunStart, ForkedFromPointRevision: 7, SourceArmedAt: &armedAt, ReconstructionOwner: "selected-cut",
	}
}

func TestTimerHistoricalCodecRuleRemovalCancellation(t *testing.T) {
	want := inheritedTimerSnapshot(t)
	want.Status, want.CancelCause, want.CancelledAt = "cancelled", string(pipeline.WorkflowTimerCancelCauseRuleRemoved), &want.CreatedAt
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeRunForkTimerSnapshot(raw)
	if err != nil {
		t.Fatal(err)
	}
	activation, err := workflowTimerActivationFromSnapshot(decoded)
	if err != nil || activation.CancelCause != pipeline.WorkflowTimerCancelCauseRuleRemoved || !activation.CancelledAt.Equal(want.CreatedAt) {
		t.Fatalf("historical cancellation fields lost: %+v: %v", activation, err)
	}
	for _, test := range []struct {
		name   string
		change func(*runforkrevision.TimerSnapshot)
	}{
		{"missing_time", func(s *runforkrevision.TimerSnapshot) { s.CancelledAt = nil }},
		{"missing_cause", func(s *runforkrevision.TimerSnapshot) { s.CancelCause = "" }},
		{"unknown_cause", func(s *runforkrevision.TimerSnapshot) { s.CancelCause = "unknown" }},
		{"zero_time", func(s *runforkrevision.TimerSnapshot) { s.CancelledAt = new(time.Time) }},
		{"before_birth", func(s *runforkrevision.TimerSnapshot) { at := s.CreatedAt.Add(-time.Microsecond); s.CancelledAt = &at }},
		{"active_with_removal", func(s *runforkrevision.TimerSnapshot) { s.Status = "active" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid := want
			test.change(&invalid)
			raw, err := json.Marshal(invalid)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeRunForkTimerSnapshot(raw); err == nil {
				t.Fatal("historical cancellation admitted missing or contradictory disposition evidence")
			}
		})
	}
}

func TestTimerHistoricalCodecRetainsTypedPointAndOriginalArm(t *testing.T) {
	for _, kind := range []forkpoint.Kind{forkpoint.RunStart, forkpoint.Event, forkpoint.DeploymentRevision} {
		t.Run(string(kind), func(t *testing.T) {
			want := inheritedTimerSnapshot(t)
			want.ForkedFromPointKind = kind
			if kind == forkpoint.Event {
				want.ForkedFromEventID = "66666666-6666-4666-8666-666666666666"
			}
			raw, err := json.Marshal(want)
			if err != nil {
				t.Fatal(err)
			}
			got, err := decodeRunForkTimerSnapshot(raw)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("exact timer historical roundtrip: got=%+v: %v", got, err)
			}
			activation, err := workflowTimerActivationFromSnapshot(got)
			if err != nil {
				t.Fatal(err)
			}
			if !activation.FireAt.Before(activation.CreatedAt) || !activation.SourceArmedAt.Equal(*want.SourceArmedAt) ||
				activation.ForkedFromPointKind != kind || activation.ForkedFromPointRevision != want.ForkedFromPointRevision ||
				activation.ForkedFromEventID != want.ForkedFromEventID {
				t.Fatalf("historical activation reset due, arm, birth, or source point: %+v", activation)
			}
		})
	}
}

func TestTimerHistoricalCodecRejectsPartialLineageAndLooseJSON(t *testing.T) {
	snapshot := inheritedTimerSnapshot(t)
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		field string
		value any
	}{
		{"missing_kind", "forked_from_point_kind", nil},
		{"unknown_kind", "forked_from_point_kind", "latest"},
		{"missing_revision", "forked_from_point_revision", nil},
		{"negative_revision", "forked_from_point_revision", -1},
		{"fabricated_start_event", "forked_from_event_id", "66666666-6666-4666-8666-666666666666"},
		{"event_without_event_identity", "forked_from_point_kind", "event"},
		{"missing_arm", "source_armed_at", nil},
		{"invalid_arm", "source_armed_at", "yesterday"},
		{"missing_source_timer", "source_timer_id", nil},
		{"missing_source_run", "forked_from_run_id", nil},
		{"missing_owner", "reconstruction_owner", nil},
		{"arm_after_birth", "source_armed_at", snapshot.CreatedAt.Add(time.Hour)},
		{"unknown_field", "invented_authority", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var values map[string]any
			if err := json.Unmarshal(raw, &values); err != nil {
				t.Fatal(err)
			}
			values[test.field] = test.value
			changed, err := json.Marshal(values)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeRunForkTimerSnapshot(changed); err == nil {
				t.Fatal("partial or foreign historical evidence accepted")
			}
		})
	}
	for _, invalid := range [][]byte{append(append([]byte(nil), raw...), []byte(` {}`)...), []byte(`null`), append(append([]byte(nil), raw...), '!')} {
		if _, err := decodeRunForkTimerSnapshot(invalid); err == nil {
			t.Fatal("non-single-object historical timer accepted")
		}
	}
}
