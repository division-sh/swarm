package runfork

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func TestProjectEntityOwnershipMapsOnlyCanonicalRoot(t *testing.T) {
	for _, tc := range []struct {
		name, entity, flow   string
		wantSource, wantFork EntityIdentity
	}{
		{"root", "source-run", "source-run", EntityIdentity{"source-run", "source-run"}, EntityIdentity{"child-run", "child-run"}},
		{"static owner", "producer-entity", "producer", EntityIdentity{"producer-entity", "producer"}, EntityIdentity{"producer-entity", "producer"}},
		{"template owner", "producer-entity", "producer/one", EntityIdentity{"producer-entity", "producer/one"}, EntityIdentity{"producer-entity", "producer/one"}},
		{"canonical normalization", " source-run ", " /source-run/ ", EntityIdentity{"source-run", "source-run"}, EntityIdentity{"child-run", "child-run"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ProjectEntityOwnership("source-run", "child-run", tc.entity, tc.flow)
			if err != nil || got.Source != tc.wantSource || got.Fork != tc.wantFork {
				t.Fatalf("ownership = %#v, %v; want source=%#v child=%#v", got, err, tc.wantSource, tc.wantFork)
			}
		})
	}
}

func TestProjectEntityOwnershipRejectsContradictoryCoordinates(t *testing.T) {
	for _, tc := range []struct{ name, source, child, entity, flow string }{
		{"missing source", "", "child-run", "entity", "producer"},
		{"missing child", "source-run", "", "entity", "producer"},
		{"same run", "source-run", "source-run", "entity", "producer"},
		{"missing entity", "source-run", "child-run", "", "producer"},
		{"missing flow", "source-run", "child-run", "entity", ""},
		{"foreign entity in root flow", "source-run", "child-run", "entity-one", "source-run"},
		{"root entity in nonroot flow", "source-run", "child-run", "source-run", "producer/one"},
		{"root declaration is not execution coordinate", "source-run", "child-run", "source-run", "."},
		{"source entity in child flow", "source-run", "child-run", "source-run", "child-run"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := ProjectEntityOwnership(tc.source, tc.child, tc.entity, tc.flow); err == nil {
				t.Fatalf("contradictory ownership accepted: %#v", got)
			}
		})
	}
}

func TestProjectSelectedContractSourceEventPreservesProducerAndIsIdempotent(t *testing.T) {
	external, err := events.NewExternalIngressRoutingSource("ingress", "source-run", events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		t.Fatal(err)
	}
	childExternal, err := events.NewExternalIngressRoutingSource("ingress", "child-run", events.RoutingSourceAuthorityProviderAdmissionPlan)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name               string
		source, wantSource events.RoutingSource
	}{
		{"root", eventtest.RootRoutingSource("source-run"), eventtest.RootRoutingSource("child-run")},
		{"already child root", eventtest.RootRoutingSource("child-run"), eventtest.RootRoutingSource("child-run")},
		{"external root preserves authority", external, childExternal},
		{"static producer", eventtest.StaticFlowRoutingSource("producer", "producer", "entity-one"), eventtest.StaticFlowRoutingSource("producer", "producer", "entity-one")},
		{"template producer", eventtest.ConcreteTemplateRoutingSource("producer", "producer/one", "entity-one"), eventtest.ConcreteTemplateRoutingSource("producer", "producer/one", "entity-one")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := RunForkSelectedContractSourceEvent{
				SourceEventID: "source-event", EventName: "producer/work.ready", ExecutionMode: executionmode.Live,
				RoutingSource: tc.source, Scope: "independent-business-scope",
				Payload: json.RawMessage(`{ "target": {"entity_id":"receiver-entity","flow_instance":"receiver/other"}, "n":9007199254740993 }`),
			}
			before := sourceOwnershipJSON(t, input)
			want := input
			want.RoutingSource = tc.wantSource
			first, err := ProjectSelectedContractSourceEvent("source-run", "child-run", input)
			if err != nil || !reflect.DeepEqual(first, want) {
				t.Fatalf("producer projection = %#v, %v; want %#v", first, err, want)
			}
			second, err := ProjectSelectedContractSourceEvent("source-run", "child-run", first)
			if err != nil || !reflect.DeepEqual(second, first) || sourceOwnershipJSON(t, input) != before {
				t.Fatalf("double projection changed ownership or input: first=%#v second=%#v err=%v", first, second, err)
			}
		})
	}
}

func TestProjectSelectedContractSourceEventExcludesIndependentHistoricalTarget(t *testing.T) {
	nonroot := eventtest.ConcreteTemplateRoutingSource("producer", "producer/one", "producer-entity")
	for _, tc := range []struct {
		name               string
		source, wantSource events.RoutingSource
		target             events.RouteIdentity
	}{
		{"nonroot producer and independent receiver", nonroot, nonroot,
			events.RouteIdentity{FlowID: "receiver", FlowInstance: "receiver/one", EntityID: "receiver-entity"}},
		{"root producer and independent receiver", eventtest.RootRoutingSource("source-run"), eventtest.RootRoutingSource("child-run"),
			events.RouteIdentity{FlowID: "receiver", FlowInstance: "receiver/one", EntityID: "receiver-entity"}},
		{"nonroot producer and canonical root receiver", nonroot, nonroot,
			events.RouteIdentity{FlowID: ".", FlowInstance: "source-run", EntityID: "source-run"}},
		{"producer without receiver scalars", nonroot, nonroot, events.RouteIdentity{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Receiver compatibility scalars are not copied into source authority.
			historical := eventtest.PersistedProjectionWithRoutingSource("source-event", "producer/work.ready", "producer", "", nil, 0, "source-run", "", (events.EventEnvelope{Target: tc.target}).Normalized(), tc.source, time.Time{})
			if historical.EntityID() != tc.target.EntityID || historical.FlowInstance() != tc.target.FlowInstance {
				t.Fatalf("fixture lost independent target: %#v", historical.NormalizedEnvelope())
			}
			input := RunForkSelectedContractSourceEvent{
				SourceEventID: historical.ID(), EventName: string(historical.Type()),
				Scope: string(historical.Scope()), RoutingSource: historical.RoutingSource(),
			}
			before := sourceOwnershipJSON(t, input)
			want := input
			want.RoutingSource = tc.wantSource
			got, err := ProjectSelectedContractSourceEvent("source-run", "child-run", input)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("independent producer/target projection = %#v, %v; want %#v", got, err, want)
			}
			again, err := ProjectSelectedContractSourceEvent("source-run", "child-run", got)
			if err != nil || !reflect.DeepEqual(again, got) || sourceOwnershipJSON(t, input) != before {
				t.Fatalf("independent target was mutated or reprojected: %#v, %v", again, err)
			}
		})
	}
}

func TestProjectSelectedContractSourceEventRejectsInvalidIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, sourceRun, childRun string
		event                     RunForkSelectedContractSourceEvent
	}{
		{"missing source run", "", "child-run", sourceOwnershipRootEvent()},
		{"missing child run", "source-run", "", sourceOwnershipRootEvent()},
		{"same run", "source-run", "source-run", sourceOwnershipRootEvent()},
		{"missing event identity", "source-run", "child-run", RunForkSelectedContractSourceEvent{RoutingSource: eventtest.RootRoutingSource("source-run")}},
		{"missing producer", "source-run", "child-run", RunForkSelectedContractSourceEvent{SourceEventID: "event"}},
		{"foreign root", "source-run", "child-run", RunForkSelectedContractSourceEvent{SourceEventID: "event", RoutingSource: eventtest.RootRoutingSource("entity-one")}},
		{"nonroot claiming source root entity", "source-run", "child-run", RunForkSelectedContractSourceEvent{SourceEventID: "event", RoutingSource: eventtest.StaticFlowRoutingSource("producer", "producer", "source-run")}},
		{"nonroot claiming child root entity", "source-run", "child-run", RunForkSelectedContractSourceEvent{SourceEventID: "event", RoutingSource: eventtest.StaticFlowRoutingSource("producer", "producer", "child-run")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := sourceOwnershipJSON(t, tc.event)
			if got, err := ProjectSelectedContractSourceEvent(tc.sourceRun, tc.childRun, tc.event); err == nil {
				t.Fatalf("invalid source identity accepted: %#v", got)
			}
			if sourceOwnershipJSON(t, tc.event) != before {
				t.Fatal("rejected projection mutated source evidence")
			}
		})
	}
}

func TestProjectSelectedContractSourceEventActivityPreservesExactNumericBytes(t *testing.T) {
	for _, root := range []bool{false, true} {
		name, flow := "nonroot", "producer/one"
		input := RunForkSelectedContractSourceEvent{
			SourceEventID: "activity", EventName: RunForkSelectedContractPlatformActivityEvent,
			RoutingSource: eventtest.ConcreteTemplateRoutingSource("producer", flow, "entity-one"),
		}
		if root {
			name, flow = "root", "source-run"
			input = sourceOwnershipRootEvent()
			input.EventName = RunForkSelectedContractPlatformActivityEvent
		}
		t.Run(name, func(t *testing.T) {
			input.Payload = json.RawMessage(`{"entity_id":"` + input.RoutingSource.Route().EntityID + `","flow_instance":"` + flow + `","large":9007199254740993,"decimal":1.2300e+09,"negative_zero":-0,"nested":{"precise":123456789012345678901234567890},"business_root":"source-run"}`)
			before := sourceOwnershipJSON(t, input)
			got, err := ProjectSelectedContractSourceEvent("source-run", "child-run", input)
			if err != nil {
				t.Fatal(err)
			}
			var original, projected map[string]json.RawMessage
			if err := json.Unmarshal(input.Payload, &original); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(got.Payload, &projected); err != nil {
				t.Fatal(err)
			}
			if root {
				original["flow_instance"] = json.RawMessage(`"child-run"`)
				original["entity_id"] = json.RawMessage(`"child-run"`)
			}
			if !reflect.DeepEqual(projected, original) {
				t.Fatalf("activity payload lost exact field bytes: got=%s want=%v", got.Payload, original)
			}
			want := input
			want.Payload = got.Payload
			if root {
				want.RoutingSource = eventtest.RootRoutingSource("child-run")
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("activity producer fields changed: got=%#v want=%#v", got, want)
			}
			again, err := ProjectSelectedContractSourceEvent("source-run", "child-run", got)
			if err != nil || !reflect.DeepEqual(again, got) || sourceOwnershipJSON(t, input) != before {
				t.Fatalf("activity projection is not immutable/idempotent: %v", err)
			}
		})
	}
}

func TestProjectSelectedContractSourceEventActivityRequiresExactSourceAgreement(t *testing.T) {
	for _, tc := range []struct{ name, payload, producerFlow string }{
		{"non-object", `[]`, "producer/one"},
		{"null", `null`, "producer/one"},
		{"invalid JSON", `{`, "producer/one"},
		{"missing coordinate", `{}`, "producer/one"},
		{"numeric coordinate", `{"flow_instance":123}`, "producer/one"},
		{"null coordinate", `{"flow_instance":null}`, "producer/one"},
		{"different payload flow", `{"flow_instance":"receiver/one"}`, "producer/one"},
		{"root payload for nonroot producer", `{"flow_instance":"source-run"}`, "producer/one"},
		{"different typed producer flow", `{"flow_instance":"producer/one"}`, "producer/other"},
		{"missing entity", `{"flow_instance":"producer/one"}`, "producer/one"},
		{"numeric entity", `{"flow_instance":"producer/one","entity_id":123}`, "producer/one"},
		{"null entity", `{"flow_instance":"producer/one","entity_id":null}`, "producer/one"},
		{"foreign entity", `{"flow_instance":"producer/one","entity_id":"receiver-entity"}`, "producer/one"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := RunForkSelectedContractSourceEvent{
				SourceEventID: "activity", EventName: RunForkSelectedContractPlatformActivityEvent,
				RoutingSource: eventtest.ConcreteTemplateRoutingSource("producer", tc.producerFlow, "entity-one"), Payload: json.RawMessage(tc.payload),
			}
			before := string(input.Payload)
			if got, err := ProjectSelectedContractSourceEvent("source-run", "child-run", input); err == nil {
				t.Fatalf("activity source disagreement accepted: %#v", got)
			}
			if string(input.Payload) != before {
				t.Fatal("rejected activity changed payload bytes")
			}
		})
	}
	root := sourceOwnershipRootEvent()
	root.EventName = RunForkSelectedContractPlatformActivityEvent
	root.Payload = json.RawMessage(`{"flow_instance":"sibling-run"}`)
	if _, err := ProjectSelectedContractSourceEvent("source-run", "child-run", root); err == nil {
		t.Fatal("root activity accepted a sibling execution coordinate")
	}
}

func sourceOwnershipRootEvent() RunForkSelectedContractSourceEvent {
	return RunForkSelectedContractSourceEvent{
		SourceEventID: "source-event", EventName: "work.ready", RoutingSource: eventtest.RootRoutingSource("source-run"),
	}
}

func sourceOwnershipJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
