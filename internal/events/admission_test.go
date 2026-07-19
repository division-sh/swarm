package events

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func TestProducerIdentityConstructorsRejectEveryInvalidOrigin(t *testing.T) {
	for _, test := range []ProducerClaim{
		{},
		{Type: EventProducerNode},
		{Type: "unknown", ID: "producer"},
		{Type: EventProducerAgent, ID: "   "},
	} {
		if _, err := NewProducerIdentity(test.Type, test.ID); err == nil {
			t.Fatalf("NewProducerIdentity(%q, %q) succeeded", test.Type, test.ID)
		}
	}
}

func TestEventConstructionClassInvariantMatrix(t *testing.T) {
	selectedLineage, err := NewSelectedForkLineage(
		testRunID,
		"22222222-2222-4222-8222-222222222222",
		"33333333-3333-4333-8333-333333333333",
		"selection:contract-v1",
		"task-1",
		executionmode.Live,
	)
	if err != nil {
		t.Fatalf("NewSelectedForkLineage: %v", err)
	}
	tests := []struct {
		name      string
		baseFacts EventFacts
		build     func(EventFacts) error
	}{
		{name: "root", baseFacts: validFacts(), build: func(f EventFacts) error { _, err := NewRootIngressEvent(RootIngressEventInput{Facts: f}); return err }},
		{name: "operator", baseFacts: validFacts(), build: func(f EventFacts) error {
			_, err := NewOperatorInjectedEvent(OperatorInjectedEventInput{Facts: f, RunID: testRunID})
			return err
		}},
		{name: "child", baseFacts: validFacts(), build: func(f EventFacts) error {
			_, err := NewChildEvent(ChildEventInput{Facts: f, Lineage: validLineage()})
			return err
		}},
		{name: "replay", baseFacts: validFacts(), build: func(f EventFacts) error {
			_, err := NewReplayEvent(ReplayEventInput{Facts: f, Lineage: validLineage()})
			return err
		}},
		{name: "selected fork replay", baseFacts: validFacts(), build: func(f EventFacts) error {
			_, err := NewSelectedForkReplayEvent(SelectedForkReplayEventInput{Facts: f, Lineage: selectedLineage})
			return err
		}},
		{name: "runtime control", baseFacts: validFacts(), build: func(f EventFacts) error { _, err := NewRuntimeControlEvent(RuntimeEventInput{Facts: f}); return err }},
		{name: "runtime diagnostic", baseFacts: validFacts(), build: func(f EventFacts) error { _, err := NewRuntimeDiagnosticEvent(RuntimeEventInput{Facts: f}); return err }},
		{name: "diagnostic direct", baseFacts: diagnosticDirectFacts(), build: func(f EventFacts) error {
			_, err := NewDiagnosticDirectEvent(DiagnosticDirectEventInput{Facts: f})
			return err
		}},
	}
	for _, test := range tests {
		for _, mutation := range []struct {
			name   string
			want   string
			mutate func(*EventFacts)
		}{
			{name: "invalid producer", want: "producer", mutate: func(f *EventFacts) { f.Producer.ID = "" }},
			{name: "empty event type", want: "event type", mutate: func(f *EventFacts) { f.Type = "" }},
			{name: "invalid payload", want: "valid JSON", mutate: func(f *EventFacts) { f.Payload = []byte(`{"broken":`) }},
			{name: "negative chain depth", want: "chain_depth", mutate: func(f *EventFacts) { f.ChainDepth = -1 }},
			{name: "invalid envelope", want: "target route", mutate: func(f *EventFacts) {
				f.Envelope = EventEnvelope{
					EntityID: "entity-two", FlowInstance: "flow/two", Scope: EventScopeEntity,
					Target: RouteIdentity{FlowID: "flow", FlowInstance: "flow/one", EntityID: "entity-one"},
				}
			}},
			{name: "untyped routing source", want: "typed routing source", mutate: func(f *EventFacts) {
				f.Envelope.Source = RouteIdentity{FlowID: "flow", FlowInstance: "flow/one", EntityID: "entity-one"}
			}},
		} {
			t.Run(test.name+"/"+mutation.name, func(t *testing.T) {
				facts := test.baseFacts
				mutation.mutate(&facts)
				want := mutation.want
				if test.name == "diagnostic direct" && (mutation.name == "invalid envelope" || mutation.name == "untyped routing source") {
					want = "non-routed"
				}
				if err := test.build(facts); err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("error = %v, want %q", err, want)
				}
			})
		}
		if test.name != "child" && test.name != "replay" && test.name != "selected fork replay" {
			t.Run(test.name+"/invalid mode", func(t *testing.T) {
				facts := test.baseFacts
				facts.ExecutionMode = ""
				if err := test.build(facts); err == nil || !strings.Contains(err.Error(), "execution_mode") {
					t.Fatalf("error = %v, want execution_mode failure", err)
				}
			})
		}
	}
}

func TestAdmissionAllocatesOnlyAuthorizedFacts(t *testing.T) {
	now := time.Date(2026, 7, 18, 10, 11, 12, 123456789, time.UTC)
	rootCandidate, constructErr := NewRootIngressEvent(RootIngressEventInput{Facts: validFactsWithoutIdentity()})
	root := mustConstruct(t, rootCandidate, constructErr)
	admitted, err := AdmitForPublish(root, AdmissionOptions{Now: now})
	if err != nil {
		t.Fatalf("AdmitForPublish: %v", err)
	}
	event := admitted.Event()
	if event.ID() == "" || event.RunID() == "" {
		t.Fatalf("admitted identity = event %q run %q", event.ID(), event.RunID())
	}
	if want := now.Truncate(time.Microsecond); !event.CreatedAt().Equal(want) {
		t.Fatalf("created_at = %s, want %s", event.CreatedAt(), want)
	}
}

func TestReplayClassesRequireExactTypedLineage(t *testing.T) {
	if _, err := NewChildEvent(ChildEventInput{Facts: validFacts(), Lineage: EventLineage{ExecutionMode: executionmode.Live}}); err == nil {
		t.Fatal("child construction accepted missing lineage")
	}
	if _, err := NewReplayEvent(ReplayEventInput{Facts: validFacts(), Lineage: EventLineage{RunID: testRunID, ExecutionMode: executionmode.Live}}); err == nil {
		t.Fatal("replay construction accepted missing parent")
	}
	if _, err := NewChildEvent(ChildEventInput{Facts: validFacts(), Lineage: EventLineage{RunID: testRunID, ParentEventID: "22222222-2222-4222-8222-222222222222"}}); err == nil {
		t.Fatal("child construction accepted missing execution mode")
	}
	if _, err := NewSelectedForkLineage(testRunID, "22222222-2222-4222-8222-222222222222", "33333333-3333-4333-8333-333333333333", "selection:contract-v1", "", ""); err == nil {
		t.Fatal("selected-fork lineage accepted missing execution mode")
	}
	lineage, err := NewSelectedForkLineage(testRunID, "22222222-2222-4222-8222-222222222222", "33333333-3333-4333-8333-333333333333", "selection:contract-v1", "", executionmode.Live)
	if err != nil {
		t.Fatalf("NewSelectedForkLineage: %v", err)
	}
	event, err := NewSelectedForkReplayEvent(SelectedForkReplayEventInput{Facts: validFacts(), Lineage: lineage})
	if err != nil {
		t.Fatalf("NewSelectedForkReplayEvent: %v", err)
	}
	if event.ParentEventID() != "" {
		t.Fatalf("selected-fork generic parent = %q, want absent", event.ParentEventID())
	}
}

func TestClassSpecificConstructorsRejectFalseLineage(t *testing.T) {
	if _, err := NewOperatorInjectedEvent(OperatorInjectedEventInput{Facts: validFacts()}); err == nil {
		t.Fatal("operator-injected construction accepted missing target run")
	}
	for _, test := range []struct {
		name  string
		build func() error
	}{
		{name: "runtime control", build: func() error {
			_, err := NewRuntimeControlEvent(RuntimeEventInput{Facts: validFacts(), ParentEventID: "22222222-2222-4222-8222-222222222222"})
			return err
		}},
		{name: "runtime diagnostic", build: func() error {
			_, err := NewRuntimeDiagnosticEvent(RuntimeEventInput{Facts: validFacts(), ParentEventID: "22222222-2222-4222-8222-222222222222"})
			return err
		}},
		{name: "diagnostic direct", build: func() error {
			_, err := NewDiagnosticDirectEvent(DiagnosticDirectEventInput{Facts: diagnosticDirectFacts(), ParentEventID: "22222222-2222-4222-8222-222222222222"})
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.build(); err == nil || !strings.Contains(err.Error(), "requires run_id") {
				t.Fatalf("error = %v, want run lineage failure", err)
			}
		})
	}
}

func TestOperatorInjectedReferenceIsProvenanceNotCausalParent(t *testing.T) {
	ref, err := NewOperatorReferenceProvenance("33333333-3333-4333-8333-333333333333")
	if err != nil {
		t.Fatalf("NewOperatorReferenceProvenance: %v", err)
	}
	event, err := NewOperatorInjectedEvent(OperatorInjectedEventInput{Facts: validFacts(), RunID: testRunID, Provenance: &ref})
	if err != nil {
		t.Fatalf("NewOperatorInjectedEvent: %v", err)
	}
	if event.ParentEventID() != "" {
		t.Fatalf("operator causal parent = %q, want absent", event.ParentEventID())
	}
	got, ok := event.OperatorReference()
	if !ok || got.ReferencedEventID() != ref.ReferencedEventID() {
		t.Fatalf("operator provenance = %#v, %v", got, ok)
	}
}

func TestRoutingSourceConstructorsRejectIncompleteRuntimeIdentity(t *testing.T) {
	for _, route := range []RouteIdentity{
		{FlowID: "flow", EntityID: "entity"},
		{FlowID: "flow", FlowInstance: "flow/one"},
		{FlowInstance: "flow/one", EntityID: "entity"},
	} {
		if _, err := NewRuntimeRoutingSource(route.FlowID, route.FlowInstance, route.EntityID); err == nil {
			t.Fatalf("runtime source %#v succeeded", route)
		}
	}
}

func TestAdmissionRuntimePlatformEventAllocatesStandaloneRun(t *testing.T) {
	facts := validFactsWithoutIdentity()
	facts.Type = "platform.boot"
	facts.Producer = ProducerClaim{Type: EventProducerPlatform, ID: "runtime"}
	candidate, constructErr := NewRuntimeControlEvent(RuntimeEventInput{Facts: facts})
	event := mustConstruct(t, candidate, constructErr)
	admitted, err := AdmitForPublish(event, AdmissionOptions{})
	if err != nil {
		t.Fatalf("AdmitForPublish: %v", err)
	}
	if admitted.Event().RunID() == "" {
		t.Fatal("standalone runtime event run_id is empty")
	}
}

func TestDiagnosticDirectRequiresClosedCatalogAndNamedAdmission(t *testing.T) {
	facts := validFactsWithoutIdentity()
	facts.Type = EventTypePlatformRuntimeLog
	facts.Producer = ProducerClaim{Type: EventProducerPlatform, ID: "runtime"}
	candidate, constructErr := NewDiagnosticDirectEvent(DiagnosticDirectEventInput{Facts: facts})
	event := mustConstruct(t, candidate, constructErr)
	if _, err := AdmitForPublish(event, AdmissionOptions{}); err == nil {
		t.Fatal("generic publish admitted diagnostic-direct event")
	}
	if _, err := AdmitForPersistence(event, AdmissionOptions{}); err != nil {
		t.Fatalf("named persistence admission: %v", err)
	}
	facts.Type = "platform.unregistered"
	if _, err := NewDiagnosticDirectEvent(DiagnosticDirectEventInput{Facts: facts}); err == nil {
		t.Fatal("diagnostic-direct constructor accepted unregistered type")
	}
}

func TestRestoreAdmittedEventRejectsNonPersistentIdentity(t *testing.T) {
	facts := validFacts()
	facts.ID = "not-a-uuid"
	if _, err := RestoreAdmittedEvent(RestoredEventInput{
		Class: EventAdmissionRootIngress,
		Facts: facts,
		RunID: testRunID,
	}); err == nil || !strings.Contains(err.Error(), "must be a UUID") {
		t.Fatalf("error = %v, want persistent identity failure", err)
	}
}

const testRunID = "11111111-1111-4111-8111-111111111111"

func validFacts() EventFacts {
	facts := validFactsWithoutIdentity()
	facts.ID = "44444444-4444-4444-8444-444444444444"
	facts.CreatedAt = time.Date(2026, 7, 18, 10, 11, 12, 0, time.UTC)
	return facts
}

func validFactsWithoutIdentity() EventFacts {
	return EventFacts{
		Type: "task.completed", Producer: ProducerClaim{Type: EventProducerAgent, ID: "agent-1"},
		Payload: []byte(`{}`), ExecutionMode: executionmode.Live,
	}
}

func diagnosticDirectFacts() EventFacts {
	facts := validFacts()
	facts.Type = EventTypePlatformRuntimeLog
	facts.Producer = ProducerClaim{Type: EventProducerPlatform, ID: "runtime"}
	return facts
}

func validLineage() EventLineage {
	return EventLineage{RunID: testRunID, ParentEventID: "22222222-2222-4222-8222-222222222222", ExecutionMode: executionmode.Live}
}

func mustConstruct(t *testing.T, event Event, err error) Event {
	t.Helper()
	if err != nil {
		t.Fatalf("construct event: %v", err)
	}
	return event
}
