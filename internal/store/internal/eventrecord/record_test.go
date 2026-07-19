package eventrecord

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/google/uuid"
)

func validRecord(t *testing.T) Record {
	t.Helper()
	event := eventtest.RootIngress(uuid.NewString(), "record.valid", "gateway", "task-1", []byte(`{"nested":{"a":null}}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Date(2026, 7, 19, 2, 0, 0, 123456000, time.UTC))
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	record, err := FromAdmitted(admitted)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func TestRecordEqualDistinguishesNestedNullMapKeys(t *testing.T) {
	left := validRecord(t)
	right := left.Clone()
	right.Payload = []byte(`{"nested":{"b":null}}`)
	if left.Equal(right) {
		t.Fatal("records with different nested null keys compared equal")
	}
}

func TestRecordValidateRejectsEveryNormalizedScalarFamily(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Record)
	}{
		{"class", func(r *Record) { r.Class = events.EventAdmissionClass(" root_ingress") }},
		{"event_id", func(r *Record) { r.EventID += " " }},
		{"run_id", func(r *Record) { r.RunID += " " }},
		{"event_name", func(r *Record) { r.EventName = " record.valid " }},
		{"task_id", func(r *Record) { r.TaskID += " " }},
		{"entity_id", func(r *Record) { r.EntityID = " " }},
		{"flow_instance", func(r *Record) { r.FlowInstance = "/flow/a/" }},
		{"producer_id", func(r *Record) { r.ProducedBy += " " }},
		{"producer_type", func(r *Record) { r.ProducedByType = events.EventProducerType(" external ") }},
		{"source_event_id", func(r *Record) { r.SourceEventID = " " }},
		{"routing_source_kind", func(r *Record) { r.RoutingSourceKind = events.RoutingSourceKind(" ") }},
		{"routing_authority", func(r *Record) { r.RoutingSourceAuthority = " " }},
		{"operator_reference", func(r *Record) { r.OperatorReferencedEventID = " " }},
		{"selected_source_run", func(r *Record) { r.SelectedForkSourceRunID = " " }},
		{"selected_source_event", func(r *Record) { r.SelectedForkSourceEventID = " " }},
		{"selected_authority", func(r *Record) { r.SelectedForkAuthorityStamp = " " }},
		{"scope", func(r *Record) { r.Scope = events.EventScope(" global ") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := validRecord(t)
			test.mutate(&record)
			if err := record.Validate(); err == nil || !strings.Contains(err.Error(), "not canonical") {
				t.Fatalf("Validate error = %v", err)
			}
		})
	}
}

func TestRecordValidateRejectsNoncanonicalTimestamp(t *testing.T) {
	record := validRecord(t)
	record.CreatedAt = record.CreatedAt.Add(time.Nanosecond)
	if err := record.Validate(); err == nil || !strings.Contains(err.Error(), "microsecond") {
		t.Fatalf("Validate error = %v", err)
	}
}
