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
		{name: "runtime control", baseFacts: validFacts(), build: func(f EventFacts) error {
			_, err := NewStandaloneRuntimeControlEvent(StandaloneRuntimeEventInput{Facts: f})
			return err
		}},
		{name: "runtime diagnostic", baseFacts: validFacts(), build: func(f EventFacts) error {
			_, err := NewStandaloneRuntimeDiagnosticEvent(StandaloneRuntimeEventInput{Facts: f})
			return err
		}},
		{name: "diagnostic direct", baseFacts: diagnosticDirectFacts(), build: func(f EventFacts) error {
			_, err := NewStandaloneDiagnosticDirectEvent(StandaloneRuntimeEventInput{Facts: f})
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

func TestAdmissionCarriesExactRunDispositionAndReadbackNeverRegainsCreation(t *testing.T) {
	runtimeFacts := func(eventType EventType) EventFacts {
		facts := validFacts()
		facts.Type = eventType
		facts.Producer = ProducerClaim{Type: EventProducerPlatform, ID: "runtime"}
		facts.TaskID = ""
		return facts
	}
	childFacts := func() EventFacts {
		facts := validFacts()
		facts.Producer = ProducerClaim{Type: EventProducerAgent, ID: "worker"}
		return facts
	}
	directiveFacts := func() EventFacts {
		facts := diagnosticDirectFacts()
		facts.Type = EventTypePlatformAgentDirective
		return facts
	}
	tests := []struct {
		name  string
		build func() (Event, error)
		want  AdmittedRunDisposition
	}{
		{name: "root create", build: func() (Event, error) {
			return NewRootIngressEvent(RootIngressEventInput{Facts: validFacts(), RunID: testRunID})
		}, want: AdmittedRunCreateAuthorized},
		{name: "operator requires active", build: func() (Event, error) {
			return NewOperatorInjectedEvent(OperatorInjectedEventInput{Facts: validFacts(), RunID: testRunID})
		}, want: AdmittedRunRequireActive},
		{name: "child requires active", build: func() (Event, error) {
			return NewChildEvent(ChildEventInput{Facts: childFacts(), Lineage: validLineage()})
		}, want: AdmittedRunRequireActive},
		{name: "standalone runtime creates", build: func() (Event, error) {
			return NewStandaloneRuntimeControlEvent(StandaloneRuntimeEventInput{Facts: runtimeFacts("platform.boot")})
		}, want: AdmittedRunCreateAuthorized},
		{name: "run scoped runtime requires active", build: func() (Event, error) {
			return NewRunScopedRuntimeControlEvent(RunScopedRuntimeEventInput{Facts: runtimeFacts("platform.paused"), RunID: testRunID})
		}, want: AdmittedRunRequireActive},
		{name: "new-run directive creates", build: func() (Event, error) {
			return NewRunCreatingDiagnosticDirectEvent(RunCreatingRuntimeEventInput{Facts: directiveFacts(), RunID: testRunID})
		}, want: AdmittedRunCreateAuthorized},
		{name: "global runtime log is runless", build: func() (Event, error) {
			return NewStandaloneDiagnosticDirectEvent(StandaloneRuntimeEventInput{Facts: diagnosticDirectFacts()})
		}, want: AdmittedRunless},
		{name: "run runtime log requires present", build: func() (Event, error) {
			return NewRunScopedDiagnosticDirectEvent(RunScopedRuntimeEventInput{Facts: diagnosticDirectFacts(), RunID: testRunID})
		}, want: AdmittedRunRequirePresent},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event, err := test.build()
			if err != nil {
				t.Fatalf("construct event: %v", err)
			}
			admitted, err := AdmitForPersistence(event, AdmissionOptions{RequirePersistentUUIDIdentity: true})
			if err != nil {
				t.Fatalf("admit event: %v", err)
			}
			if admitted.RunDisposition() != test.want {
				t.Fatalf("run disposition = %q, want %q", admitted.RunDisposition(), test.want)
			}
			restored, err := RevalidatePersistedEvent(admitted.Event())
			if err != nil {
				t.Fatalf("revalidate event: %v", err)
			}
			wantRestored := test.want
			if wantRestored == AdmittedRunCreateAuthorized {
				wantRestored = AdmittedRunRequireActive
			}
			if restored.RunDisposition() != wantRestored {
				t.Fatalf("restored run disposition = %q, want %q", restored.RunDisposition(), wantRestored)
			}
		})
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
			_, err := NewCausalRuntimeControlEvent(CausalRuntimeEventInput{Facts: validFacts(), Lineage: EventLineage{ParentEventID: "22222222-2222-4222-8222-222222222222", ExecutionMode: executionmode.Live}})
			return err
		}},
		{name: "runtime diagnostic", build: func() error {
			_, err := NewCausalRuntimeDiagnosticEvent(CausalRuntimeEventInput{Facts: validFacts(), Lineage: EventLineage{ParentEventID: "22222222-2222-4222-8222-222222222222", ExecutionMode: executionmode.Live}})
			return err
		}},
		{name: "diagnostic direct", build: func() error {
			_, err := NewCausalDiagnosticDirectEvent(CausalRuntimeEventInput{Facts: diagnosticDirectFacts(), Lineage: EventLineage{ParentEventID: "22222222-2222-4222-8222-222222222222", ExecutionMode: executionmode.Live}})
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

func TestRuntimeConstructorsEncodeExplicitLineageIntent(t *testing.T) {
	const parentID = "22222222-2222-4222-8222-222222222222"
	lineage := EventLineage{RunID: testRunID, ParentEventID: parentID, TaskID: "task-lineage", ExecutionMode: executionmode.Mock}
	runtimeFacts := func(eventType EventType) EventFacts {
		facts := validFacts()
		facts.Type = eventType
		facts.Producer = ProducerClaim{Type: EventProducerPlatform, ID: "runtime"}
		facts.TaskID = ""
		return facts
	}
	directFacts := func() EventFacts {
		facts := diagnosticDirectFacts()
		facts.TaskID = ""
		return facts
	}
	tests := []struct {
		name       string
		build      func() (Event, error)
		wantRun    string
		wantParent string
		wantTask   string
		wantMode   executionmode.Mode
	}{
		{name: "causal control", build: func() (Event, error) {
			return NewCausalRuntimeControlEvent(CausalRuntimeEventInput{Facts: runtimeFacts("platform.auth_required"), Lineage: lineage})
		}, wantRun: testRunID, wantParent: parentID, wantTask: "task-lineage", wantMode: executionmode.Mock},
		{name: "causal diagnostic", build: func() (Event, error) {
			return NewCausalRuntimeDiagnosticEvent(CausalRuntimeEventInput{Facts: runtimeFacts("platform.dead_letter"), Lineage: lineage})
		}, wantRun: testRunID, wantParent: parentID, wantTask: "task-lineage", wantMode: executionmode.Mock},
		{name: "causal direct", build: func() (Event, error) {
			return NewCausalDiagnosticDirectEvent(CausalRuntimeEventInput{Facts: directFacts(), Lineage: lineage})
		}, wantRun: testRunID, wantParent: parentID, wantTask: "task-lineage", wantMode: executionmode.Mock},
		{name: "run-scoped control", build: func() (Event, error) {
			return NewRunScopedRuntimeControlEvent(RunScopedRuntimeEventInput{Facts: runtimeFacts("platform.scheduled"), RunID: testRunID})
		}, wantRun: testRunID, wantMode: executionmode.Live},
		{name: "run-scoped diagnostic", build: func() (Event, error) {
			return NewRunScopedRuntimeDiagnosticEvent(RunScopedRuntimeEventInput{Facts: runtimeFacts("platform.run_stalled"), RunID: testRunID})
		}, wantRun: testRunID, wantMode: executionmode.Live},
		{name: "run-scoped direct", build: func() (Event, error) {
			return NewRunScopedDiagnosticDirectEvent(RunScopedRuntimeEventInput{Facts: directFacts(), RunID: testRunID})
		}, wantRun: testRunID, wantMode: executionmode.Live},
		{name: "run-creating direct", build: func() (Event, error) {
			facts := directFacts()
			facts.Type = EventTypePlatformAgentDirective
			return NewRunCreatingDiagnosticDirectEvent(RunCreatingRuntimeEventInput{Facts: facts, RunID: testRunID})
		}, wantRun: testRunID, wantMode: executionmode.Live},
		{name: "standalone control", build: func() (Event, error) {
			return NewStandaloneRuntimeControlEvent(StandaloneRuntimeEventInput{Facts: runtimeFacts("platform.boot")})
		}, wantMode: executionmode.Live},
		{name: "standalone diagnostic", build: func() (Event, error) {
			return NewStandaloneRuntimeDiagnosticEvent(StandaloneRuntimeEventInput{Facts: runtimeFacts("platform.recovery_failed")})
		}, wantMode: executionmode.Live},
		{name: "standalone direct", build: func() (Event, error) {
			return NewStandaloneDiagnosticDirectEvent(StandaloneRuntimeEventInput{Facts: directFacts()})
		}, wantMode: executionmode.Live},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event, err := test.build()
			if err != nil {
				t.Fatalf("construct event: %v", err)
			}
			if event.RunID() != test.wantRun || event.ParentEventID() != test.wantParent || event.TaskID() != test.wantTask || event.ExecutionMode() != test.wantMode {
				t.Fatalf("lineage = run:%q parent:%q task:%q mode:%q, want run:%q parent:%q task:%q mode:%q", event.RunID(), event.ParentEventID(), event.TaskID(), event.ExecutionMode(), test.wantRun, test.wantParent, test.wantTask, test.wantMode)
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
	candidate, constructErr := NewStandaloneRuntimeControlEvent(StandaloneRuntimeEventInput{Facts: facts})
	event := mustConstruct(t, candidate, constructErr)
	admitted, err := AdmitForPublish(event, AdmissionOptions{})
	if err != nil {
		t.Fatalf("AdmitForPublish: %v", err)
	}
	if admitted.Event().RunID() == "" {
		t.Fatal("standalone runtime event run_id is empty")
	}
}

func TestRuntimeAdmissionRejectsOmittedConstructorIntent(t *testing.T) {
	facts := validFacts()
	facts.Type = "platform.boot"
	facts.Producer = ProducerClaim{Type: EventProducerPlatform, ID: "runtime"}
	event, err := NewStandaloneRuntimeControlEvent(StandaloneRuntimeEventInput{Facts: facts})
	if err != nil {
		t.Fatalf("NewStandaloneRuntimeControlEvent: %v", err)
	}
	event.runtimeIntent = runtimeLineageIntent("")
	if _, err := AdmitForPersistence(event, AdmissionOptions{}); err == nil || !strings.Contains(err.Error(), "requires explicit causal, run-scoped, or standalone lineage intent") {
		t.Fatalf("AdmitForPersistence error = %v, want explicit lineage-intent failure", err)
	}
}

func TestDiagnosticDirectRequiresClosedCatalogAndNamedAdmission(t *testing.T) {
	facts := validFactsWithoutIdentity()
	facts.Type = EventTypePlatformRuntimeLog
	facts.Producer = ProducerClaim{Type: EventProducerPlatform, ID: "runtime"}
	candidate, constructErr := NewStandaloneDiagnosticDirectEvent(StandaloneRuntimeEventInput{Facts: facts})
	event := mustConstruct(t, candidate, constructErr)
	if _, err := AdmitForPublish(event, AdmissionOptions{}); err == nil {
		t.Fatal("generic publish admitted diagnostic-direct event")
	}
	if _, err := AdmitForPersistence(event, AdmissionOptions{}); err != nil {
		t.Fatalf("named persistence admission: %v", err)
	}
	facts.Type = "platform.unregistered"
	if _, err := NewStandaloneDiagnosticDirectEvent(StandaloneRuntimeEventInput{Facts: facts}); err == nil {
		t.Fatal("diagnostic-direct constructor accepted unregistered type")
	}
}

func TestRevalidatePersistedEventPreservesSelectedForkFactsWithoutAllocation(t *testing.T) {
	lineage, err := NewSelectedForkLineage(
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
	persisted, err := NewSelectedForkReplayEvent(SelectedForkReplayEventInput{Facts: validFacts(), Lineage: lineage})
	if err != nil {
		t.Fatalf("NewSelectedForkReplayEvent: %v", err)
	}

	restored, err := RevalidatePersistedEvent(persisted)
	if err != nil {
		t.Fatalf("RevalidatePersistedEvent: %v", err)
	}
	got := restored.Event()
	if got.ID() != persisted.ID() || got.RunID() != persisted.RunID() || !got.CreatedAt().Equal(persisted.CreatedAt()) {
		t.Fatalf("revalidated identity changed: got id=%q run=%q created=%s; want id=%q run=%q created=%s", got.ID(), got.RunID(), got.CreatedAt(), persisted.ID(), persisted.RunID(), persisted.CreatedAt())
	}
	if got.AdmissionClass() != EventAdmissionSelectedForkReplay {
		t.Fatalf("revalidated class = %q, want %q", got.AdmissionClass(), EventAdmissionSelectedForkReplay)
	}
}

func TestRevalidatePersistedEventRejectsMissingDurableTimestamp(t *testing.T) {
	candidate, err := NewRootIngressEvent(RootIngressEventInput{Facts: validFactsWithoutIdentity()})
	if err != nil {
		t.Fatalf("NewRootIngressEvent: %v", err)
	}
	if _, err := RevalidatePersistedEvent(candidate); err == nil || !strings.Contains(err.Error(), "created_at") {
		t.Fatalf("RevalidatePersistedEvent error = %v, want missing created_at", err)
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
		Type: "task.completed", Producer: ProducerClaim{Type: EventProducerExternal, ID: "external-1"},
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
