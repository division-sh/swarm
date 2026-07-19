package events

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

func TestEventContractRejectsHostileClassCatalogProducerCombinations(t *testing.T) {
	runID := uuid.NewString()
	base := EventFacts{ID: uuid.NewString(), Type: "work.started", Producer: ProducerClaim{Type: EventProducerExternal, ID: "gateway"}, Payload: []byte(`{}`), CreatedAt: time.Now().UTC(), ExecutionMode: executionmode.Live}
	tests := []struct {
		name  string
		build func() (Event, error)
		want  string
	}{
		{"root platform producer", func() (Event, error) {
			f := base
			f.Producer.Type = EventProducerPlatform
			return NewRootIngressEvent(RootIngressEventInput{Facts: f, RunID: runID})
		}, "requires external producer"},
		{"runtime external producer", func() (Event, error) { return NewRuntimeControlEvent(RuntimeEventInput{Facts: base, RunID: runID}) }, "requires platform producer"},
		{"closed label under root", func() (Event, error) {
			f := base
			f.Type = EventTypePlatformRuntimeLog
			return NewRootIngressEvent(RootIngressEventInput{Facts: f, RunID: runID})
		}, "requires diagnostic_direct class"},
		{"unregistered diagnostic direct", func() (Event, error) {
			f := base
			f.Producer.Type = EventProducerPlatform
			return NewDiagnosticDirectEvent(DiagnosticDirectEventInput{Facts: f, RunID: runID})
		}, "not in the closed catalog"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.build(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestGenericPublishRejectsEveryClosedSubtypeAndSelectedFork(t *testing.T) {
	runID := uuid.NewString()
	for _, eventType := range DiagnosticDirectEventTypes() {
		event, err := NewDiagnosticDirectEvent(DiagnosticDirectEventInput{Facts: EventFacts{
			ID: uuid.NewString(), Type: eventType, Producer: ProducerClaim{Type: EventProducerPlatform, ID: "runtime"},
			Payload: []byte(`{}`), CreatedAt: time.Now().UTC(), ExecutionMode: executionmode.Live,
		}, RunID: runID})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := AdmitForPublish(event, AdmissionOptions{RequirePersistentUUIDIdentity: true}); err == nil || !strings.Contains(err.Error(), "named persistence operation") {
			t.Fatalf("AdmitForPublish(%s) error = %v", eventType, err)
		}
	}
	lineage, err := NewSelectedForkLineage(uuid.NewString(), runID, uuid.NewString(), "selection:test", "", executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := NewSelectedForkReplayEvent(SelectedForkReplayEventInput{Facts: EventFacts{
		ID: uuid.NewString(), Type: "work.replayed", Producer: ProducerClaim{Type: EventProducerPlatform, ID: "fork-owner"},
		Payload: []byte(`{}`), CreatedAt: time.Now().UTC(), ExecutionMode: executionmode.Live,
	}, Lineage: lineage})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AdmitForPublish(selected, AdmissionOptions{RequirePersistentUUIDIdentity: true}); err == nil || !strings.Contains(err.Error(), "selected-fork") {
		t.Fatalf("AdmitForPublish(selected fork) error = %v", err)
	}
}

func TestPersistentContractValidatesNestedDurableUUIDFacts(t *testing.T) {
	runID := uuid.NewString()
	source, err := NewDeclaredIngressRoutingSource("flow-a", "", "not-a-uuid", "resolution:test")
	if err != nil {
		t.Fatal(err)
	}
	event, err := NewRootIngressEvent(RootIngressEventInput{Facts: EventFacts{
		ID: uuid.NewString(), Type: "work.received", Producer: ProducerClaim{Type: EventProducerExternal, ID: "gateway"},
		Payload: []byte(`{}`), RoutingSource: source, CreatedAt: time.Now().UTC(), ExecutionMode: executionmode.Live,
	}, RunID: runID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AdmitForPersistence(event, AdmissionOptions{RequirePersistentUUIDIdentity: true}); err == nil || !strings.Contains(err.Error(), "routing_source.entity_id") {
		t.Fatalf("declared source identity error = %v", err)
	}

	lineage, err := NewSelectedForkLineage(uuid.NewString(), "not-a-uuid", uuid.NewString(), "selection:test", "", executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := NewSelectedForkReplayEvent(SelectedForkReplayEventInput{Facts: EventFacts{
		ID: uuid.NewString(), Type: "work.replayed", Producer: ProducerClaim{Type: EventProducerPlatform, ID: "fork-owner"},
		Payload: []byte(`{}`), CreatedAt: time.Now().UTC(), ExecutionMode: executionmode.Live,
	}, Lineage: lineage})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AdmitForPersistence(selected, AdmissionOptions{RequirePersistentUUIDIdentity: true}); err == nil || !strings.Contains(err.Error(), "selected_fork.source_run_id") {
		t.Fatalf("selected source identity error = %v", err)
	}
}
