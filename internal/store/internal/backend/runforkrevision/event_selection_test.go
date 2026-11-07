package runforkrevision

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
)

func TestEventHistoryExcludesOnlyRuntimeLog(t *testing.T) {
	for _, name := range []string{
		"business.created", "platform.run_started", "platform.inbound_recorded",
		"platform.agent_directive", "platform.pipeline", "platform.runtime_log.extra",
		"Platform.runtime_log", "application.runtime_log",
	} {
		if !CapturesEvent(name) {
			t.Errorf("non-runtime-log event excluded from fork history: %q", name)
		}
	}
	if CapturesEvent(string(events.EventTypePlatformRuntimeLog)) {
		t.Fatal("runtime log selected as fork evidence")
	}
	spec, ok := canonicalProjectionSpec(FamilyEvents)
	if !ok || spec.source != "events e "+eventrecord.SelectedForkLineageSQL || !strings.Contains(spec.query, "e.event_name <> '"+excludedEventName+"'") {
		t.Fatal("broad and exact event projections do not consume the canonical exclusion")
	}
	for _, life := range []int64{1, 7} {
		source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: "worker", FlowInstance: "worker/one"})
		if err != nil {
			t.Fatal(err)
		}
		source, err = source.WithLife(life)
		if err != nil {
			t.Fatal(err)
		}
		var route map[string]any
		if err := json.Unmarshal(eventrecord.EncodeSourceRoute(source), &route); err != nil {
			t.Fatal(err)
		}
		projected := spec.build(map[string]any{"routing_source_kind": source.Kind().StorageCode(), "source_route": route, "routing_source_authority": ""})
		raw, err := json.Marshal(projected["routing_source"])
		if err != nil {
			t.Fatal(err)
		}
		var restored events.RoutingSource
		if err := json.Unmarshal(raw, &restored); err != nil || restored != source {
			t.Fatalf("fixed-cut source changed captured life: got=%+v want=%+v err=%v", restored, source, err)
		}
	}
	for _, family := range []Family{FamilyCommittedReplayScopes, FamilyEventReceipts} {
		spec, ok := canonicalProjectionSpec(family)
		if !ok || !strings.Contains(spec.source, "JOIN events e ON e.event_id = ") || !strings.Contains(spec.query, "e.event_name <> '"+excludedEventName+"'") {
			t.Errorf("%s leaves diagnostic pipeline evidence without its source event", family)
		}
		ref, err := NewFactRef(family, "22222222-2222-4222-8222-222222222222")
		if err != nil {
			t.Fatal(err)
		}
		query, _, err := selectedProjectionQuery(spec, family, "11111111-1111-4111-8111-111111111111", []FactRef{ref})
		if err != nil || !strings.Contains(query, "e.event_name <> '"+excludedEventName+"'") {
			t.Errorf("exact %s projection lost diagnostic exclusion: %q, %v", family, query, err)
		}
	}
}
