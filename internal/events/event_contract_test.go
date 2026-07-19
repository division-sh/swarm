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
		{"runtime external producer", func() (Event, error) {
			return NewRunScopedRuntimeControlEvent(RunScopedRuntimeEventInput{Facts: base, RunID: runID})
		}, "requires platform producer"},
		{"closed label under root", func() (Event, error) {
			f := base
			f.Type = EventTypePlatformRuntimeLog
			return NewRootIngressEvent(RootIngressEventInput{Facts: f, RunID: runID})
		}, "requires diagnostic_direct class"},
		{"unregistered diagnostic direct", func() (Event, error) {
			f := base
			f.Producer.Type = EventProducerPlatform
			return NewRunScopedDiagnosticDirectEvent(RunScopedRuntimeEventInput{Facts: f, RunID: runID})
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
		event, err := NewRunScopedDiagnosticDirectEvent(RunScopedRuntimeEventInput{Facts: EventFacts{
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

func TestDiagnosticDirectSubtypePolicyIsCompleteAtEveryRuntimeBoundary(t *testing.T) {
	runID := uuid.NewString()
	entityEnvelope := EnvelopeForEntityID(EventEnvelope{}, uuid.NewString())
	flowEnvelope := EnvelopeForFlowInstance(EventEnvelope{}, "flow/instance")
	globalEnvelope := EventEnvelope{}

	valid := []struct {
		name      string
		eventType EventType
		runID     string
		envelope  EventEnvelope
	}{
		{"runtime_log_global_without_run", EventTypePlatformRuntimeLog, "", globalEnvelope},
		{"runtime_log_global_with_run", EventTypePlatformRuntimeLog, runID, globalEnvelope},
		{"inbound_recorded_global", EventTypePlatformInboundRecord, runID, globalEnvelope},
		{"inbound_recorded_entity", EventTypePlatformInboundRecord, runID, entityEnvelope},
		{"agent_directive_global", EventTypePlatformAgentDirective, runID, globalEnvelope},
	}
	for _, test := range valid {
		t.Run("valid/"+test.name, func(t *testing.T) {
			event, err := diagnosticDirectContractEvent(test.eventType, EventProducerPlatform, "runtime", test.runID, test.envelope)
			if err != nil {
				t.Fatalf("construct valid subtype: %v", err)
			}
			admitted, err := AdmitForPersistence(event, AdmissionOptions{RequirePersistentUUIDIdentity: true})
			if err != nil {
				t.Fatalf("admit valid subtype: %v", err)
			}
			if err := ValidateNamedEvent(admitted, EventAdmissionDiagnosticDirect, test.eventType); err != nil {
				t.Fatalf("validate valid named subtype: %v", err)
			}
		})
	}

	type hostileTuple struct {
		name         string
		eventType    EventType
		producerType EventProducerType
		producerID   string
		runID        string
		envelope     EventEnvelope
	}
	hostile := make([]hostileTuple, 0, 16)
	for _, eventType := range DiagnosticDirectEventTypes() {
		for _, producerType := range []EventProducerType{EventProducerExternal, EventProducerAgent, EventProducerNode} {
			hostile = append(hostile, hostileTuple{
				name: string(eventType) + "_producer_" + string(producerType), eventType: eventType,
				producerType: producerType, producerID: "hostile", runID: runID, envelope: globalEnvelope,
			})
		}
	}
	hostile = append(hostile,
		hostileTuple{"runtime_log_wrong_platform_id", EventTypePlatformRuntimeLog, EventProducerPlatform, "not-runtime", runID, globalEnvelope},
		hostileTuple{"runtime_log_entity_scope", EventTypePlatformRuntimeLog, EventProducerPlatform, "runtime", runID, entityEnvelope},
		hostileTuple{"runtime_log_flow_scope", EventTypePlatformRuntimeLog, EventProducerPlatform, "runtime", runID, flowEnvelope},
		hostileTuple{"inbound_recorded_missing_run", EventTypePlatformInboundRecord, EventProducerPlatform, "runtime", "", globalEnvelope},
		hostileTuple{"inbound_recorded_flow_scope", EventTypePlatformInboundRecord, EventProducerPlatform, "runtime", runID, flowEnvelope},
		hostileTuple{"agent_directive_missing_run", EventTypePlatformAgentDirective, EventProducerPlatform, "runtime", "", globalEnvelope},
		hostileTuple{"agent_directive_entity_scope", EventTypePlatformAgentDirective, EventProducerPlatform, "runtime", runID, entityEnvelope},
		hostileTuple{"agent_directive_flow_scope", EventTypePlatformAgentDirective, EventProducerPlatform, "runtime", runID, flowEnvelope},
	)
	for _, test := range hostile {
		t.Run("hostile/"+test.name, func(t *testing.T) {
			if _, err := diagnosticDirectContractEvent(test.eventType, test.producerType, test.producerID, test.runID, test.envelope); err == nil {
				t.Fatal("constructor accepted invalid subtype tuple")
			}

			baseRunID := runID
			if test.eventType == EventTypePlatformRuntimeLog {
				baseRunID = ""
			}
			base, err := diagnosticDirectContractEvent(test.eventType, EventProducerPlatform, "runtime", baseRunID, globalEnvelope)
			if err != nil {
				t.Fatalf("construct hostile base: %v", err)
			}
			producer, err := NewProducerIdentity(test.producerType, test.producerID)
			if err != nil {
				t.Fatalf("construct hostile producer: %v", err)
			}
			base.producer = producer
			base.runID = test.runID
			base.envelope = test.envelope.Normalized()
			if err := ValidateEventContract(base); err == nil {
				t.Fatal("canonical contract accepted invalid subtype tuple")
			}
			if _, err := AdmitForPersistence(base, AdmissionOptions{RequirePersistentUUIDIdentity: true}); err == nil {
				t.Fatal("persistence admission accepted invalid subtype tuple")
			}
			if err := ValidateNamedEvent(newAdmittedEvent(base), EventAdmissionDiagnosticDirect, test.eventType); err == nil {
				t.Fatal("named operation gate accepted invalid subtype tuple")
			}
		})
	}
}

func diagnosticDirectContractEvent(eventType EventType, producerType EventProducerType, producerID, runID string, envelope EventEnvelope) (Event, error) {
	facts := EventFacts{
		ID: uuid.NewString(), Type: eventType, Producer: ProducerClaim{Type: producerType, ID: producerID},
		Payload: []byte(`{}`), Envelope: envelope, CreatedAt: time.Now().UTC().Truncate(time.Microsecond), ExecutionMode: executionmode.Live,
	}
	if strings.TrimSpace(runID) == "" {
		return NewStandaloneDiagnosticDirectEvent(StandaloneRuntimeEventInput{Facts: facts})
	}
	return NewRunScopedDiagnosticDirectEvent(RunScopedRuntimeEventInput{Facts: facts, RunID: runID})
}
