package semanticview

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestClockScheduleProjectionAndProducerCensus(t *testing.T) {
	bundle := endpointCensusBundle(nil)
	bundle.RootSchema = &contracts.FlowSchemaDocument{Schedules: map[string]contracts.FlowSchedule{
		"poll": {Every: "5m", Emit: "work.requested"},
	}}
	schema := bundle.FlowSchemas["worker"]
	schema.Schedules = map[string]contracts.FlowSchedule{"poll": {Cron: "0 9 * * *", Emit: "work.requested"}}
	bundle.FlowSchemas["worker"] = schema
	bundle.FlowTree.ByID["worker"].Schema = schema
	if err := contracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	source := Wrap(bundle)
	projection := ClockSchedules(source)
	if len(projection) != 2 || projection[0].FlowID != "." || projection[1].FlowID != "worker" {
		t.Fatalf("projection lost deterministic declaration identity: %#v", projection)
	}
	projection[0].Declaration.Every = "changed"
	if ClockSchedules(source)[0].Declaration.Every != "5m" {
		t.Fatal("projection exposed mutable declaration storage")
	}
	census := BuildAuthoredEventEndpointCensus(source)
	var clocks []AuthoredEventEndpoint
	for _, endpoint := range census.Producers() {
		if endpoint.Kind == EventEndpointClockSchedule {
			clocks = append(clocks, endpoint)
		}
	}
	if len(clocks) != 2 || clocks[0].ID == clocks[1].ID {
		t.Fatalf("same-name sibling clocks collapsed: %#v", clocks)
	}
	for _, endpoints := range [][]AuthoredEventEndpoint{census.Consumers(), census.InputPins(), census.OutputPins()} {
		for _, endpoint := range endpoints {
			if endpoint.Kind == EventEndpointClockSchedule {
				t.Fatalf("clock fabricated interface or consumer: %#v", endpoint)
			}
		}
	}
}
