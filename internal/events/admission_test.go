package events

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func TestAdmissionRootIngressAllocatesPersistedFacts(t *testing.T) {
	now := time.Date(2026, 6, 6, 10, 11, 12, 0, time.UTC)
	admitted, err := AdmitForPublish(NewRootIngressEvent(
		"",
		EventType("operator.started"),
		ExternalProducer("operator"),
		"",
		nil,
		0,
		"",
		"",
		EventEnvelope{},
		time.Time{},
	), AdmissionOptions{Now: now})
	if err != nil {
		t.Fatalf("AdmitForPublish: %v", err)
	}
	if admitted.ID() == "" {
		t.Fatal("admitted event_id is empty")
	}
	if admitted.RunID() == "" {
		t.Fatal("admitted root run_id is empty")
	}
	if !admitted.CreatedAt().Equal(now) {
		t.Fatalf("created_at = %s, want %s", admitted.CreatedAt(), now)
	}
}

func TestAdmissionCanonicalizesCreatedAtToDurableMicrosecondPrecision(t *testing.T) {
	createdAt := time.Date(2026, 7, 16, 12, 13, 14, 123456789, time.FixedZone("source", -3*60*60))
	evt := NewProjectionEvent(
		"11111111-1111-4111-8111-111111111111",
		EventType("provider.replied"),
		NodeProducer("provider-node"),
		"task-logical",
		nil,
		0,
		"22222222-2222-4222-8222-222222222222",
		"",
		EventEnvelope{},
		createdAt,
	)
	want := createdAt.UTC().Truncate(time.Microsecond)

	for name, admit := range map[string]func(Event) (Event, error){
		"publish": func(candidate Event) (Event, error) {
			return AdmitForPublish(candidate, AdmissionOptions{})
		},
		"persistence": func(candidate Event) (Event, error) {
			return AdmitForPersistence(candidate, AdmissionOptions{RequirePersistentUUIDIdentity: true})
		},
	} {
		t.Run(name, func(t *testing.T) {
			admitted, err := admit(evt)
			if err != nil {
				t.Fatalf("admit: %v", err)
			}
			if !admitted.CreatedAt().Equal(want) || admitted.CreatedAt().Location() != time.UTC {
				t.Fatalf("created_at = %s, want UTC microsecond %s", admitted.CreatedAt(), want)
			}
		})
	}
}

func TestAdmissionPreservesPlatformDeliveryContext(t *testing.T) {
	want := DeliveryContext{Reply: &ReplyContextRef{ID: "reply-v1:admission"}}
	admitted, err := AdmitForPublish(NewProjectionEvent(
		"11111111-1111-4111-8111-111111111111",
		EventType("provider.replied"),
		NodeProducer("provider-node"),
		"",
		nil,
		0,
		"22222222-2222-4222-8222-222222222222",
		"33333333-3333-4333-8333-333333333333",
		EventEnvelope{},
		time.Now().UTC(),
	).WithDeliveryContext(want), AdmissionOptions{})
	if err != nil {
		t.Fatalf("AdmitForPublish: %v", err)
	}
	if got := admitted.DeliveryContext().ReplyContextID(); got != want.ReplyContextID() {
		t.Fatalf("admitted reply context = %q, want %q", got, want.ReplyContextID())
	}
	if got := admitted.ProducerType(); got != EventProducerNode {
		t.Fatalf("admitted producer type = %q, want node", got)
	}
}

func TestEnvelopeClaimIsDeepClonedThroughConstructionProjectionAndClone(t *testing.T) {
	constructedRoutes := []RouteIdentity{{FlowID: "constructed", FlowInstance: "constructed/one"}}
	evt := NewProjectionEvent(
		"11111111-1111-4111-8111-111111111111",
		EventType("provider.replied"),
		NodeProducer("provider-node"),
		"task-logical",
		nil,
		0,
		"22222222-2222-4222-8222-222222222222",
		"",
		EventEnvelope{Scope: EventScopeGlobal, TargetSet: constructedRoutes},
		time.Now().UTC(),
	)
	constructedRoutes[0].FlowID = "mutated-after-construction"

	projectedRoutes := []RouteIdentity{{FlowID: "projected", FlowInstance: "projected/one"}}
	projected := Project(evt, ProjectEnvelope(EventEnvelope{Scope: EventScopeGlobal, TargetSet: projectedRoutes}))
	cloned := projected.Clone()
	projectedRoutes[0].FlowID = "mutated-after-projection"

	admitted, err := AdmitForPersistence(cloned, AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatalf("AdmitForPersistence: %v", err)
	}
	if got := admitted.TargetRoutes(); len(got) != 1 || got[0].FlowID != "projected" {
		t.Fatalf("admitted target_set = %#v, want deep-cloned projected claim", got)
	}
}

func TestAdmissionRequiresUUIDIdentityOnlyAtSelectedStoreBoundary(t *testing.T) {
	evt := NewProjectionEvent(
		"event-logical",
		EventType("provider.replied"),
		NodeProducer("provider-node"),
		"task-logical",
		nil,
		0,
		"run-logical",
		"parent-logical",
		EventEnvelope{EntityID: "entity-logical"},
		time.Now().UTC(),
	)

	if _, err := AdmitForPublish(evt, AdmissionOptions{}); err != nil {
		t.Fatalf("semantic publish admission rejected logical identity: %v", err)
	}
	_, err := AdmitForPersistence(evt, AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err == nil || !strings.Contains(err.Error(), `event_id "event-logical" must be a UUID`) {
		t.Fatalf("selected-store admission error = %v, want event_id UUID failure", err)
	}

	evt = NewProjectionEvent(
		"11111111-1111-4111-8111-111111111111",
		EventType("provider.replied"),
		NodeProducer("provider-node"),
		"task-logical",
		nil,
		0,
		"22222222-2222-4222-8222-222222222222",
		"33333333-3333-4333-8333-333333333333",
		EventEnvelope{EntityID: "entity-logical"},
		time.Now().UTC(),
	)
	_, err = AdmitForPersistence(evt, AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err == nil || !strings.Contains(err.Error(), `entity_id "entity-logical" must be a UUID`) {
		t.Fatalf("selected-store envelope error = %v, want entity_id UUID failure", err)
	}
}

func TestAdmissionRuntimePlatformEventAllocatesStandaloneRun(t *testing.T) {
	admitted, err := AdmitForPublish(NewRuntimeControlEvent(
		"",
		EventType("platform.boot"),
		PlatformProducer("runtime"),
		"",
		nil,
		0,
		"",
		"",
		EventEnvelope{},
		time.Time{},
	), AdmissionOptions{Now: time.Date(2026, 6, 6, 10, 11, 12, 0, time.UTC)})
	if err != nil {
		t.Fatalf("AdmitForPublish: %v", err)
	}
	if admitted.ID() == "" {
		t.Fatal("admitted event_id is empty")
	}
	if admitted.RunID() == "" {
		t.Fatal("admitted standalone runtime platform run_id is empty")
	}
}

func TestAdmissionDiagnosticDirectAllowsGlobalNoRun(t *testing.T) {
	admitted, err := AdmitForPersistence(NewDiagnosticDirectEvent(
		"",
		EventType("platform.runtime_log"),
		PlatformProducer("runtime"),
		"",
		nil,
		0,
		"",
		"",
		EventEnvelope{},
		time.Time{},
	), AdmissionOptions{Now: time.Date(2026, 6, 6, 10, 11, 12, 0, time.UTC)})
	if err != nil {
		t.Fatalf("AdmitForPersistence: %v", err)
	}
	if admitted.ID() == "" {
		t.Fatal("admitted diagnostic event_id is empty")
	}
	if admitted.RunID() != "" {
		t.Fatalf("diagnostic run_id = %q, want empty global/no-run", admitted.RunID())
	}
	if admitted.CreatedAt().IsZero() {
		t.Fatal("diagnostic created_at is zero")
	}
}

func TestAdmissionRejectsProjectionPersistenceWithoutAuthoritativeFacts(t *testing.T) {
	_, err := AdmitForPersistence(NewProjectionEvent(
		"",
		EventType("task.completed"),
		AgentProducer("agent-1"),
		"",
		nil,
		0,
		"",
		"",
		EventEnvelope{},
		time.Time{},
	), AdmissionOptions{Now: time.Date(2026, 6, 6, 10, 11, 12, 0, time.UTC)})
	if err == nil {
		t.Fatal("expected projection persistence admission error")
	}
	if !strings.Contains(err.Error(), "authoritative event_id") {
		t.Fatalf("error = %v, want missing authoritative event_id", err)
	}

	_, err = AdmitForPersistence(NewProjectionEvent(
		"evt-projection",
		EventType("task.completed"),
		AgentProducer("agent-1"),
		"",
		nil,
		0,
		"",
		"",
		EventEnvelope{},
		time.Date(2026, 6, 6, 10, 11, 12, 0, time.UTC),
	), AdmissionOptions{RunIDCandidate: "run-from-context"})
	if err == nil {
		t.Fatal("expected projection persistence admission error for missing run_id")
	}
	if !strings.Contains(err.Error(), "authoritative run_id") {
		t.Fatalf("error = %v, want missing authoritative run_id", err)
	}
}

func TestAdmissionPublishStillDefaultsProjectionShell(t *testing.T) {
	now := time.Date(2026, 6, 6, 10, 11, 12, 0, time.UTC)
	admitted, err := AdmitForPublish(NewProjectionEvent(
		"",
		EventType("task.completed"),
		AgentProducer("agent-1"),
		"",
		nil,
		0,
		"",
		"",
		EventEnvelope{},
		time.Time{},
	), AdmissionOptions{
		Now:            now,
		RunIDCandidate: "44444444-4444-4444-8444-444444444444",
	})
	if err != nil {
		t.Fatalf("AdmitForPublish projection shell: %v", err)
	}
	if admitted.ID() == "" {
		t.Fatal("admitted projection shell event_id is empty")
	}
	if got := admitted.RunID(); got != "44444444-4444-4444-8444-444444444444" {
		t.Fatalf("admitted projection shell run_id = %q, want publish candidate", got)
	}
	if !admitted.CreatedAt().Equal(now) {
		t.Fatalf("created_at = %s, want %s", admitted.CreatedAt(), now)
	}
}

func TestAdmissionRejectsMissingChildLineage(t *testing.T) {
	_, err := AdmitForPersistence(NewChildEventWithLineage(
		"",
		EventType("task.completed"),
		AgentProducer("agent-1"),
		"",
		nil,
		0,
		EventLineage{ExecutionMode: executionmode.Live},
		EventEnvelope{},
		time.Time{},
	), AdmissionOptions{})
	if err == nil {
		t.Fatal("expected missing child lineage admission error")
	}
	if !strings.Contains(err.Error(), "requires admitted run_id") {
		t.Fatalf("error = %v, want missing run_id", err)
	}
}

func TestAdmissionRejectsMissingLineageExecutionModeInsteadOfDefaultingLive(t *testing.T) {
	_, err := AdmitForPersistence(NewChildEventWithLineage(
		"child-event",
		EventType("task.completed"),
		AgentProducer("agent-1"),
		"",
		nil,
		0,
		EventLineage{RunID: "run-1", ParentEventID: "parent-1"},
		EventEnvelope{},
		time.Now().UTC(),
	), AdmissionOptions{})
	if err == nil || !strings.Contains(err.Error(), "execution_mode") {
		t.Fatalf("missing child mode error = %v, want execution_mode failure", err)
	}

	_, err = AdmitForPublish(NewReplayEvent(
		"replay-event",
		EventType("task.completed"),
		AgentProducer("agent-1"),
		"",
		nil,
		0,
		EventLineage{RunID: "run-1", ParentEventID: "parent-1"},
		EventEnvelope{},
		time.Now().UTC(),
	), AdmissionOptions{})
	if err == nil || !strings.Contains(err.Error(), "execution_mode") {
		t.Fatalf("missing replay mode error = %v, want execution_mode failure", err)
	}
}

func TestAdmissionReplayAllowsSelectedForkTypedLineageOwner(t *testing.T) {
	admitted, err := AdmitForPublish(NewReplayEvent(
		"55555555-5555-4555-8555-555555555555",
		EventType("task.completed"),
		PlatformProducer("runtime.run_fork.selected_contract_execution"),
		"",
		nil,
		0,
		EventLineage{RunID: "66666666-6666-4666-8666-666666666666", ExecutionMode: executionmode.Live},
		EventEnvelope{},
		time.Time{},
	), AdmissionOptions{
		SelectedForkLineageOwner: "runtime.run_fork.selected_contract_execution.fork_local_runtime_container",
		Now:                      time.Date(2026, 6, 6, 10, 11, 12, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("AdmitForPublish: %v", err)
	}
	if got := admitted.ParentEventID(); got != "" {
		t.Fatalf("selected-fork replay parent_event_id = %q, want empty generic parent", got)
	}
}

func TestAdmissionRejectsReplayWithoutParentOrSelectedForkLineage(t *testing.T) {
	_, err := AdmitForPublish(NewReplayEvent(
		"evt-fork",
		EventType("task.completed"),
		PlatformProducer("runtime.run_fork.selected_contract_execution"),
		"",
		nil,
		0,
		EventLineage{RunID: "run-fork", ExecutionMode: executionmode.Live},
		EventEnvelope{},
		time.Time{},
	), AdmissionOptions{})
	if err == nil {
		t.Fatal("expected missing replay lineage admission error")
	}
	if !strings.Contains(err.Error(), "selected_fork_lineage_owner") {
		t.Fatalf("error = %v, want selected_fork_lineage_owner", err)
	}
}

func TestAdmissionRejectsPersistedRouteProbe(t *testing.T) {
	_, err := AdmitForPersistence(NewRouteProbeEvent(EventType("task.started")), AdmissionOptions{})
	if err == nil {
		t.Fatal("expected route probe persistence admission error")
	}
	if !strings.Contains(err.Error(), "not persistable") {
		t.Fatalf("error = %v, want not persistable", err)
	}
}
