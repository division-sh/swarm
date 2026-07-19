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
	event := eventtest.RunCreatingRootIngress(uuid.NewString(), "record.valid", "gateway", "task-1", []byte(`{"nested":{"a":null}}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Date(2026, 7, 19, 2, 0, 0, 123456000, time.UTC))
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

func TestDiagnosticDirectSubtypeRecordValidationAndReadbackMatrix(t *testing.T) {
	runID := uuid.NewString()
	entityEnvelope := events.EnvelopeForEntityID(events.EventEnvelope{}, uuid.NewString())
	valid := []struct {
		name      string
		eventType events.EventType
		runID     string
		envelope  events.EventEnvelope
	}{
		{"runtime_log_without_run", events.EventTypePlatformRuntimeLog, "", events.EventEnvelope{}},
		{"runtime_log_with_run", events.EventTypePlatformRuntimeLog, runID, events.EventEnvelope{}},
		{"inbound_recorded_global", events.EventTypePlatformInboundRecord, runID, events.EventEnvelope{}},
		{"inbound_recorded_entity", events.EventTypePlatformInboundRecord, runID, entityEnvelope},
		{"agent_directive_global", events.EventTypePlatformAgentDirective, runID, events.EventEnvelope{}},
	}
	for _, test := range valid {
		t.Run("valid/"+test.name, func(t *testing.T) {
			record := validDiagnosticDirectRecord(t, test.eventType, test.runID, test.envelope)
			if err := record.Validate(); err != nil {
				t.Fatalf("validate valid diagnostic record: %v", err)
			}
			decoded, err := record.Decode()
			if err != nil {
				t.Fatalf("decode valid diagnostic record: %v", err)
			}
			if decoded.Event().Type() != test.eventType || decoded.Event().RunID() != test.runID || decoded.Event().Scope() != test.envelope.Normalized().Scope {
				t.Fatalf("decoded subtype facts = %s/%s/%s", decoded.Event().Type(), decoded.Event().RunID(), decoded.Event().Scope())
			}
		})
	}

	type hostileRecord struct {
		name      string
		eventType events.EventType
		mutate    func(*Record)
	}
	hostile := make([]hostileRecord, 0, 16)
	for _, eventType := range events.DiagnosticDirectEventTypes() {
		for _, producerType := range []events.EventProducerType{events.EventProducerExternal, events.EventProducerAgent, events.EventProducerNode} {
			typeCopy := producerType
			hostile = append(hostile, hostileRecord{
				name: string(eventType) + "_producer_" + string(typeCopy), eventType: eventType,
				mutate: func(record *Record) { record.ProducedByType = typeCopy },
			})
		}
	}
	hostile = append(hostile,
		hostileRecord{"runtime_log_wrong_platform_id", events.EventTypePlatformRuntimeLog, func(record *Record) { record.ProducedBy = "not-runtime" }},
		hostileRecord{"runtime_log_entity_scope", events.EventTypePlatformRuntimeLog, func(record *Record) { record.Scope = events.EventScopeEntity }},
		hostileRecord{"runtime_log_flow_scope", events.EventTypePlatformRuntimeLog, func(record *Record) { record.Scope = events.EventScopeFlow }},
		hostileRecord{"inbound_recorded_missing_run", events.EventTypePlatformInboundRecord, func(record *Record) { record.RunID = "" }},
		hostileRecord{"inbound_recorded_flow_scope", events.EventTypePlatformInboundRecord, func(record *Record) { record.Scope = events.EventScopeFlow }},
		hostileRecord{"agent_directive_missing_run", events.EventTypePlatformAgentDirective, func(record *Record) { record.RunID = "" }},
		hostileRecord{"agent_directive_entity_scope", events.EventTypePlatformAgentDirective, func(record *Record) { record.Scope = events.EventScopeEntity }},
		hostileRecord{"agent_directive_flow_scope", events.EventTypePlatformAgentDirective, func(record *Record) { record.Scope = events.EventScopeFlow }},
	)
	for _, test := range hostile {
		t.Run("hostile/"+test.name, func(t *testing.T) {
			baseRunID := runID
			if test.eventType == events.EventTypePlatformRuntimeLog {
				baseRunID = ""
			}
			record := validDiagnosticDirectRecord(t, test.eventType, baseRunID, events.EventEnvelope{})
			test.mutate(&record)
			if err := record.Validate(); err == nil {
				t.Fatal("record validation accepted invalid subtype tuple")
			}
			if _, err := record.Decode(); err == nil {
				t.Fatal("canonical readback accepted invalid subtype tuple")
			}
		})
	}
}

func validDiagnosticDirectRecord(t *testing.T, eventType events.EventType, runID string, envelope events.EventEnvelope) Record {
	t.Helper()
	event := eventtest.DiagnosticDirect(
		uuid.NewString(), eventType, "runtime", "", []byte(`{}`), 0, runID, "", envelope,
		time.Date(2026, 7, 19, 4, 0, 0, 0, time.UTC),
	)
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
