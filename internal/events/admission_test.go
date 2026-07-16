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
		"operator",
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

func TestAdmissionPreservesPlatformDeliveryContext(t *testing.T) {
	want := DeliveryContext{Reply: &ReplyContextRef{ID: "reply-v1:admission"}}
	admitted, err := AdmitForPublish(NewProjectionEvent(
		"event-1",
		EventType("provider.replied"),
		"provider-node",
		"",
		nil,
		0,
		"run-1",
		"request-1",
		EventEnvelope{},
		time.Now().UTC(),
	).WithProducerType(EventProducerNode).WithDeliveryContext(want), AdmissionOptions{})
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

func TestAdmissionRuntimePlatformEventAllocatesStandaloneRun(t *testing.T) {
	admitted, err := AdmitForPublish(NewRuntimeControlEvent(
		"",
		EventType("platform.boot"),
		"runtime",
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
		"runtime",
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
		"agent-1",
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
		"agent-1",
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
		"agent-1",
		"",
		nil,
		0,
		"",
		"",
		EventEnvelope{},
		time.Time{},
	), AdmissionOptions{
		Now:            now,
		RunIDCandidate: "run-from-publish",
	})
	if err != nil {
		t.Fatalf("AdmitForPublish projection shell: %v", err)
	}
	if admitted.ID() == "" {
		t.Fatal("admitted projection shell event_id is empty")
	}
	if got := admitted.RunID(); got != "run-from-publish" {
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
		"agent-1",
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
		"agent-1",
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
		"agent-1",
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
		"evt-fork",
		EventType("task.completed"),
		"runtime.run_fork.selected_contract_execution",
		"",
		nil,
		0,
		EventLineage{RunID: "run-fork", ExecutionMode: executionmode.Live},
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
		"runtime.run_fork.selected_contract_execution",
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
