package runforkpersistence

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func TestRunForkHistoricalFactRevisionContext(t *testing.T) {
	const run = "10000000-0000-0000-0000-000000000001"
	const event = "20000000-0000-0000-0000-000000000001"
	raw := []byte(`{"event_id":"` + event + `","event_name":"proof","payload_base64":"eyJ2YWx1ZSI6MS4wfQ=="}`)
	valid := runForkHistoricalFactContext{RunID: run, Family: runforkrevision.FamilyEvents, Key: event, FirstRevision: 2, Revision: 3}
	for _, tc := range []struct {
		name   string
		change func(*runForkRevisionSnapshot, *runForkHistoricalFactContext)
	}{
		{"missing_selected_run", func(s *runForkRevisionSnapshot, _ *runForkHistoricalFactContext) { s.RunID = "" }},
		{"malformed_selected_run", func(s *runForkRevisionSnapshot, _ *runForkHistoricalFactContext) { s.RunID = "invalid" }},
		{"zero_selected_revision", func(s *runForkRevisionSnapshot, _ *runForkHistoricalFactContext) { s.Revision = 0 }},
		{"negative_selected_revision", func(s *runForkRevisionSnapshot, _ *runForkHistoricalFactContext) { s.Revision = -1 }},
		{"missing_wrapper_run", func(_ *runForkRevisionSnapshot, c *runForkHistoricalFactContext) { c.RunID = "" }},
		{"foreign_wrapper_run", func(_ *runForkRevisionSnapshot, c *runForkHistoricalFactContext) { c.RunID = event }},
		{"zero_first", func(_ *runForkRevisionSnapshot, c *runForkHistoricalFactContext) { c.FirstRevision = 0 }},
		{"negative_first", func(_ *runForkRevisionSnapshot, c *runForkHistoricalFactContext) { c.FirstRevision = -1 }},
		{"zero_fact", func(_ *runForkRevisionSnapshot, c *runForkHistoricalFactContext) { c.Revision = 0 }},
		{"reversed", func(_ *runForkRevisionSnapshot, c *runForkHistoricalFactContext) { c.FirstRevision = 4 }},
		{"after_cut", func(_ *runForkRevisionSnapshot, c *runForkHistoricalFactContext) { c.Revision = 5 }},
		{"unknown_family", func(_ *runForkRevisionSnapshot, c *runForkHistoricalFactContext) { c.Family = "future_family" }},
		{"wrong_family", func(_ *runForkRevisionSnapshot, c *runForkHistoricalFactContext) {
			c.Family = runforkrevision.FamilyEntityMetadata
		}},
		{"missing_key", func(_ *runForkRevisionSnapshot, c *runForkHistoricalFactContext) { c.Key = "" }},
		{"aliased_key", func(_ *runForkRevisionSnapshot, c *runForkHistoricalFactContext) { c.Key = run }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := &runForkRevisionSnapshot{RunID: run, Revision: 4}
			context := valid
			tc.change(snapshot, &context)
			before := *snapshot
			input := bytes.Clone(raw)
			if err := appendRunForkHistoricalFact(snapshot, context, raw); err == nil {
				t.Fatal("invalid historical context accepted")
			}
			if !reflect.DeepEqual(before, *snapshot) || !bytes.Equal(input, raw) {
				t.Fatal("rejected fact mutated its destination or input")
			}
		})
	}
	if err := appendRunForkHistoricalFact(nil, valid, raw); err == nil {
		t.Fatal("nil snapshot accepted")
	}
	snapshot := &runForkRevisionSnapshot{RunID: run, Revision: 4}
	if err := appendRunForkHistoricalFact(snapshot, valid, raw); err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Events) != 1 || snapshot.Events[0].FirstRevision != 2 || snapshot.Events[0].Revision != 3 || string(snapshot.Events[0].Payload) != `{"value":1.0}` {
		t.Fatalf("admitted event lost exact stamps or payload: %#v", snapshot.Events)
	}
	if err := appendRunForkHistoricalFact(snapshot, valid, raw); err == nil || len(snapshot.Events) != 1 {
		t.Fatal("duplicate canonical fact accepted or mutated snapshot")
	}
}

func TestRunForkHistoricalFactFailureDoesNotReserveIdentity(t *testing.T) {
	const run = "10000000-0000-0000-0000-000000000001"
	const event = "20000000-0000-0000-0000-000000000001"
	context := runForkHistoricalFactContext{RunID: run, Family: runforkrevision.FamilyEvents, Key: event, FirstRevision: 1, Revision: 1}
	snapshot := &runForkRevisionSnapshot{RunID: run, Revision: 1}
	bad := []byte(`{"event_id":"` + event + `","payload_base64":"broken"}`)
	if err := appendRunForkHistoricalFact(snapshot, context, bad); err == nil || len(snapshot.Events) != 0 || len(snapshot.admittedFacts) != 0 {
		t.Fatal("bad body accepted or partially reserved its identity")
	}
	good := []byte(`{"event_id":"` + event + `","payload_base64":"e30="}`)
	if err := appendRunForkHistoricalFact(snapshot, context, good); err != nil {
		t.Fatalf("corrected input was poisoned by prior failed admission: %v", err)
	}
}
