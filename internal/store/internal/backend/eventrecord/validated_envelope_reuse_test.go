package eventrecord

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
)

// Frozen pre-reuse validation and decoding. Do not delegate these three bodies
// to the new envelope helpers. Unchanged admission, reconstruction and equality
// (including the JSON equality shortcut) are intentionally shared by both paths.
func oldEnvelopeReuseValidate(r Record, settlement events.RouteSettlement) error {
	for field, value := range map[string]string{
		"event_class": string(r.Class), "event_id": r.EventID, "run_id": r.RunID, "event_name": r.EventName,
		"task_id": r.TaskID, "entity_id": r.EntityID, "produced_by": r.ProducedBy,
		"produced_by_type": string(r.ProducedByType), "source_event_id": r.SourceEventID,
		"routing_source_kind": r.RoutingSourceKind, "routing_source_authority": r.RoutingSourceAuthority,
		"operator_reference_event_id":   r.OperatorReferencedEventID,
		"selected_fork_source_run_id":   r.SelectedForkSourceRunID,
		"selected_fork_source_event_id": r.SelectedForkSourceEventID,
		"selected_fork_authority_stamp": r.SelectedForkAuthorityStamp,
		"scope":                         string(r.Scope), "execution_mode": string(r.ExecutionMode),
		"payload_schema_bundle_hash": r.PayloadSchemaBundleHash,
		"payload_schema_flow_id":     r.PayloadSchemaFlowID, "payload_schema_event_key": r.PayloadSchemaEventKey,
		"payload_schema_digest": r.PayloadSchemaDigest, "payload_schema_class": string(r.PayloadSchemaClass),
	} {
		if value != strings.TrimSpace(value) {
			return fmt.Errorf("event record %s is not canonical", field)
		}
	}
	if r.FlowInstance != strings.Trim(strings.TrimSpace(r.FlowInstance), "/") {
		return fmt.Errorf("event record flow_instance is not canonical")
	}
	switch r.Class {
	case events.EventAdmissionRootIngress,
		events.EventAdmissionOperatorInjected,
		events.EventAdmissionChild,
		events.EventAdmissionReplay,
		events.EventAdmissionSelectedForkReplay,
		events.EventAdmissionInheritedFanOut,
		events.EventAdmissionRuntimeControl,
		events.EventAdmissionRuntimeDiagnostic,
		events.EventAdmissionDiagnosticDirect:
	default:
		return fmt.Errorf("event record class %q is invalid", r.Class)
	}
	if strings.TrimSpace(r.EventID) == "" {
		return fmt.Errorf("event record event_id is required")
	}
	if strings.TrimSpace(r.EventName) == "" {
		return fmt.Errorf("event record event_name is required")
	}
	if r.CreatedAt.IsZero() {
		return fmt.Errorf("event record created_at is required")
	}
	_, offset := r.CreatedAt.Zone()
	if offset != 0 || r.CreatedAt.Nanosecond()%1000 != 0 {
		return fmt.Errorf("event record created_at must be canonical UTC microsecond precision")
	}
	if !json.Valid(r.Payload) {
		return fmt.Errorf("event record payload must be valid JSON")
	}
	payloadBinding, err := events.RestorePayloadSchemaBinding(events.PayloadSchemaBindingInput{
		BundleHash: r.PayloadSchemaBundleHash, FlowID: r.PayloadSchemaFlowID,
		EventKey: r.PayloadSchemaEventKey, SchemaDigest: r.PayloadSchemaDigest, SchemaClass: r.PayloadSchemaClass,
	})
	if err != nil {
		return fmt.Errorf("event record payload schema binding: %w", err)
	}
	if _, err := events.NewPayloadAdmission(r.Payload, payloadBinding); err != nil {
		return fmt.Errorf("event record payload admission: %w", err)
	}
	if err := validateSettlementEventClass(r.Class, events.EventType(r.EventName), settlement.WriteClass()); err != nil {
		return fmt.Errorf("event record route settlement: %w", err)
	}
	if r.ChainDepth < 0 {
		return fmt.Errorf("event record chain_depth must be nonnegative")
	}
	if !r.ExecutionMode.Valid() {
		return fmt.Errorf("event record execution_mode must be live or mock")
	}
	producer, err := events.NewProducerIdentity(r.ProducedByType, r.ProducedBy)
	if err != nil {
		return fmt.Errorf("event record producer identity: %w", err)
	}
	if err := events.ValidateEventStructuralContract(r.Class, events.EventType(r.EventName), producer, r.RunID, r.Scope); err != nil {
		return fmt.Errorf("event record identity contract: %w", err)
	}
	if _, err := oldEnvelopeReuseEnvelope(r); err != nil {
		return fmt.Errorf("event record envelope: %w", err)
	}
	if err := r.validateClassFacts(); err != nil {
		return err
	}
	return nil
}

func oldEnvelopeReuseEnvelope(r Record) (events.EventEnvelope, error) {
	source, err := unmarshalRoute("source_route", r.SourceRoute)
	if err != nil {
		return events.EventEnvelope{}, err
	}
	if r.RoutingSourceKind == events.RoutingSourceExternalIngress.StorageCode() {
		source = events.RouteIdentity{}
	}
	target, err := unmarshalRoute("target_route", r.TargetRoute)
	if err != nil {
		return events.EventEnvelope{}, err
	}
	var targets []events.RouteIdentity
	if len(bytes.TrimSpace(r.TargetSet)) == 0 {
		return events.EventEnvelope{}, fmt.Errorf("target_set is required")
	}
	if err := json.Unmarshal(r.TargetSet, &targets); err != nil {
		return events.EventEnvelope{}, fmt.Errorf("decode target_set: %w", err)
	}
	envelope := events.EventEnvelope{
		EntityID: strings.TrimSpace(r.EntityID), FlowInstance: strings.Trim(strings.TrimSpace(r.FlowInstance), "/"),
		Scope: r.Scope, Source: source, Target: target, TargetSet: targets,
	}
	if err := events.ValidateEnvelope(envelope); err != nil {
		return events.EventEnvelope{}, err
	}
	return envelope.Normalized(), nil
}

func oldEnvelopeReuseDecode(r Record, settlement events.RouteSettlement) (events.AdmittedEvent, error) {
	if err := oldEnvelopeReuseValidate(r, settlement); err != nil {
		return events.AdmittedEvent{}, err
	}
	envelope, err := oldEnvelopeReuseEnvelope(r)
	if err != nil {
		return events.AdmittedEvent{}, err
	}
	routingSourceRoute, err := unmarshalRoute("source_route", r.SourceRoute)
	if err != nil {
		return events.AdmittedEvent{}, err
	}
	routingSource, err := events.RestoreRoutingSource(r.RoutingSourceKind, routingSourceRoute, r.RoutingSourceAuthority)
	if err != nil {
		return events.AdmittedEvent{}, fmt.Errorf("event record routing source: %w", err)
	}
	facts := events.EventFacts{
		ID:            r.EventID,
		Type:          events.EventType(r.EventName),
		Producer:      events.ProducerClaim{Type: r.ProducedByType, ID: r.ProducedBy},
		TaskID:        r.TaskID,
		Payload:       bytes.Clone(r.Payload),
		ChainDepth:    r.ChainDepth,
		Envelope:      envelope,
		RoutingSource: routingSource,
		CreatedAt:     r.CreatedAt,
		ExecutionMode: r.ExecutionMode,
	}
	var operatorRef *events.OperatorReferenceProvenance
	if referenceID := strings.TrimSpace(r.OperatorReferencedEventID); referenceID != "" {
		value, err := events.NewOperatorReferenceProvenance(referenceID)
		if err != nil {
			return events.AdmittedEvent{}, fmt.Errorf("event record operator provenance: %w", err)
		}
		operatorRef = &value
	}
	var selectedFork *events.SelectedForkLineage
	if r.Class == events.EventAdmissionSelectedForkReplay {
		value, err := events.NewSelectedForkLineage(
			r.RunID,
			r.SelectedForkSourceRunID,
			r.SelectedForkSourceEventID,
			r.SelectedForkAuthorityStamp,
			r.TaskID,
			r.ExecutionMode,
		)
		if err != nil {
			return events.AdmittedEvent{}, fmt.Errorf("event record selected-fork lineage: %w", err)
		}
		selectedFork = &value
	}
	payloadBinding, err := events.RestorePayloadSchemaBinding(events.PayloadSchemaBindingInput{
		BundleHash: r.PayloadSchemaBundleHash, FlowID: r.PayloadSchemaFlowID,
		EventKey: r.PayloadSchemaEventKey, SchemaDigest: r.PayloadSchemaDigest, SchemaClass: r.PayloadSchemaClass,
	})
	if err != nil {
		return events.AdmittedEvent{}, fmt.Errorf("event record payload schema binding: %w", err)
	}
	payloadAdmission, err := events.NewPayloadAdmission(r.Payload, payloadBinding)
	if err != nil {
		return events.AdmittedEvent{}, fmt.Errorf("event record payload admission: %w", err)
	}
	origin, err := r.decodeInheritedFanOutOrigin()
	if err != nil {
		return events.AdmittedEvent{}, err
	}
	restored, err := events.RestoreAdmittedEvent(events.RestoredEventInput{
		Class:           r.Class,
		Facts:           facts,
		RunID:           r.RunID,
		ParentEventID:   r.SourceEventID,
		OperatorRef:     operatorRef,
		SelectedFork:    selectedFork,
		InheritedFanOut: origin,
		Payload:         payloadAdmission,
	})
	if err != nil {
		return events.AdmittedEvent{}, fmt.Errorf("decode event record %s: %w", strings.TrimSpace(r.EventID), err)
	}
	decoded, err := FromAdmitted(restored, settlement)
	if err != nil {
		return events.AdmittedEvent{}, fmt.Errorf("decode event record %s: reconstruct durable record: %w", strings.TrimSpace(r.EventID), err)
	}
	if !r.Equal(decoded) {
		return events.AdmittedEvent{}, fmt.Errorf("decode event record %s changed durable facts", strings.TrimSpace(r.EventID))
	}
	return restored, nil
}

func oldEnvelopeReuseDecodeWithSettlement(r Record) (events.AdmittedEvent, events.RouteSettlement, error) {
	settlement, err := r.DecodeSettlement()
	if err != nil {
		return events.AdmittedEvent{}, events.RouteSettlement{}, Corrupt(r.EventID, fmt.Errorf("event record route settlement: %w", err))
	}
	admitted, err := oldEnvelopeReuseDecode(r, settlement)
	if err != nil {
		return events.AdmittedEvent{}, events.RouteSettlement{}, Corrupt(r.EventID, err)
	}
	return admitted, settlement, nil
}

func envelopeReuseFixture(tb testing.TB, plans int, shape string) Record {
	tb.Helper()
	admitted, settlement, payload := admittedSerializationFixture(tb, max(plans, 0), "consumer/<tag>&\"quote\"\\slash")
	if plans < 0 {
		ledger, err := events.NewConnectEvaluationLedger(nil)
		if err != nil {
			tb.Fatal(err)
		}
		settlement, err = events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryDeclaredConsumerNoPlan, ledger)
		if err != nil {
			tb.Fatal(err)
		}
	}
	if shape != "absent" {
		const entityID = "33333333-3333-4333-8333-333333333333"
		source, err := events.NewExternalIngressRoutingSource("ingress", entityID, events.RoutingSourceAuthorityProviderAdmissionPlan)
		if err != nil {
			tb.Fatal(err)
		}
		envelope := events.EventEnvelope{}
		switch shape {
		case "external":
		case "root_source":
			source, err = events.NewRootRoutingSource(entityID)
			if err != nil {
				tb.Fatal(err)
			}
			envelope = events.EnvelopeForSourceRoute(envelope, source.Route())
		case "target":
			envelope = events.EnvelopeForTargetRoute(envelope, events.RouteIdentity{FlowID: "consumer", FlowInstance: "consumer/one", EntityID: entityID})
		case "target_set":
			for i := 0; i < 8; i++ {
				envelope.TargetSet = append(envelope.TargetSet, events.RouteIdentity{FlowID: "consumer", FlowInstance: fmt.Sprintf("consumer/%d", i), EntityID: entityID})
			}
		default:
			tb.Fatalf("unknown envelope shape %q", shape)
		}
		event := eventtest.RunCreatingRootIngressWithRoutingSource(admitted.ID(), admitted.Event().Type(), "gateway", "task", payload, 0,
			admitted.Event().RunID(), "", envelope, source, admitted.Event().CreatedAt())
		event, err = eventtest.AdmitPayload(event, "", "record.serialization")
		if err != nil {
			tb.Fatal(err)
		}
		admitted, err = events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
		if err != nil {
			tb.Fatal(err)
		}
	}
	record, err := FromAdmitted(admitted, settlement)
	if err != nil {
		tb.Fatal(err)
	}
	return record
}

func requireEnvelopeReuseErrorParity(t *testing.T, oldErr, newErr error) {
	t.Helper()
	// Deep comparison includes the complete wrapping chain, CorruptError.EventID,
	// and JSON syntax/type error metadata, not just matching error strings.
	if !reflect.DeepEqual(oldErr, newErr) || fmt.Sprint(oldErr) != fmt.Sprint(newErr) || errors.Is(oldErr, ErrCorrupt) != errors.Is(newErr, ErrCorrupt) {
		t.Fatalf("error parity: old=%T %v; new=%T %v", oldErr, oldErr, newErr, newErr)
	}
}

func requireEnvelopeReuseDecodeParity(t *testing.T, record Record, wantError string) {
	t.Helper()
	before := record.Clone()
	oldEvent, oldSettlement, oldErr := oldEnvelopeReuseDecodeWithSettlement(record)
	newEvent, newSettlement, newErr := record.DecodeWithSettlement()
	requireEnvelopeReuseErrorParity(t, oldErr, newErr)
	if wantError == "" && oldErr != nil {
		t.Fatalf("valid fixture failed: %v", oldErr)
	}
	if wantError != "" && (oldErr == nil || !strings.Contains(oldErr.Error(), wantError)) {
		t.Fatalf("old error = %v, want %q", oldErr, wantError)
	}
	if !reflect.DeepEqual(oldEvent, newEvent) || !reflect.DeepEqual(oldSettlement, newSettlement) {
		t.Fatal("decoded event or settlement changed")
	}
	if !reflect.DeepEqual(record, before) {
		t.Fatal("decode mutated the input record")
	}
	settlement, err := record.DecodeSettlement()
	if err == nil {
		requireEnvelopeReuseErrorParity(t, oldEnvelopeReuseValidate(record, settlement), record.validateWithSettlement(settlement))
	}
	if oldErr != nil {
		if !errors.Is(newErr, ErrCorrupt) || !reflect.DeepEqual(newEvent, events.AdmittedEvent{}) || !reflect.DeepEqual(newSettlement, events.RouteSettlement{}) {
			t.Fatal("failed decode exposed partial projections or lost corruption identity")
		}
		return
	}
	oldRecord, oldErr := FromAdmitted(oldEvent, oldSettlement)
	newRecord, newErr := FromAdmitted(newEvent, newSettlement)
	requireEnvelopeReuseErrorParity(t, oldErr, newErr)
	if oldErr != nil || !reflect.DeepEqual(oldRecord, newRecord) || !record.Equal(newRecord) {
		t.Fatalf("reconstruction changed durable facts: %v", oldErr)
	}
	if record.RoutingSourceKind == events.RoutingSourceExternalIngress.StorageCode() {
		stored, err := unmarshalRoute("source_route", record.SourceRoute)
		if err != nil || stored.Empty() || !newEvent.Event().SourceRoute().Empty() || newEvent.Event().RoutingSource().Route() != stored {
			t.Fatal("external ingress lost its stored source or exposed it as envelope source")
		}
	}
}

func TestValidatedEnvelopeReuseDifferential(t *testing.T) {
	for _, shape := range []string{"absent", "external", "root_source", "target", "target_set"} {
		for _, plans := range []int{-1, 0, 1, 32} {
			t.Run(fmt.Sprintf("%s/plans_%d", shape, plans), func(t *testing.T) {
				requireEnvelopeReuseDecodeParity(t, envelopeReuseFixture(t, plans, shape), "")
			})
		}
	}
	t.Run("inherited", func(t *testing.T) {
		requireEnvelopeReuseDecodeParity(t, inheritedFanOutRecord(t), "")
	})
	t.Run("diagnostic_direct", func(t *testing.T) {
		requireEnvelopeReuseDecodeParity(t, validDiagnosticDirectRecord(t, events.EventTypePlatformRuntimeLog, "", events.EventEnvelope{}), "")
	})
	for _, shape := range []string{"external", "target_set"} {
		t.Run("allocations/"+shape, func(t *testing.T) {
			record := envelopeReuseFixture(t, 1, shape)
			allocations := func(decode func(Record) (events.AdmittedEvent, events.RouteSettlement, error)) float64 {
				return testing.AllocsPerRun(20, func() {
					event, settlement, err := decode(record)
					if err != nil {
						t.Fatal(err)
					}
					envelopeReuseEventSink, envelopeReuseSettlementSink = event, settlement
				})
			}
			oldAllocs := allocations(oldEnvelopeReuseDecodeWithSettlement)
			newAllocs := allocations(Record.DecodeWithSettlement)
			// Relative counts avoid pinning Go's allocator implementation while
			// detecting a regression back to the old redundant decoding path.
			if newAllocs >= oldAllocs {
				t.Fatalf("envelope reuse did not reduce allocations: old=%.0f current=%.0f", oldAllocs, newAllocs)
			}
			t.Logf("decode allocations: old=%.0f current=%.0f", oldAllocs, newAllocs)
		})
	}
}

func TestValidatedEnvelopeReuseHostileParity(t *testing.T) {
	// Only one noncanonical scalar per case: the existing scalar map deliberately
	// does not define precedence between multiple noncanonical scalar fields.
	for _, test := range []struct {
		name   string
		mutate func(*Record)
		want   string
	}{
		{"settlement_before_scalar", func(r *Record) { r.RouteSettlement = nil; r.EventID += " " }, "event record route settlement"},
		{"scalar_before_envelope", func(r *Record) { r.EventID += " "; r.SourceRoute = nil }, "event record event_id is not canonical"},
		{"flow_before_envelope", func(r *Record) { r.FlowInstance = "/bad/"; r.SourceRoute = nil }, "flow_instance is not canonical"},
		{"timestamp_before_envelope", func(r *Record) { r.CreatedAt = r.CreatedAt.Add(time.Nanosecond); r.SourceRoute = nil }, "canonical UTC microsecond"},
		{"payload_before_envelope", func(r *Record) { r.Payload = []byte(`{`); r.SourceRoute = nil }, "payload must be valid JSON"},
		{"payload_null", func(r *Record) { r.Payload = []byte(`{"value":null}`) }, "payload admission"},
		{"source_absent", func(r *Record) { r.SourceRoute = nil }, "event record envelope: source_route is required"},
		{"source_type", func(r *Record) { r.SourceRoute = []byte(`[]`) }, "event record envelope: decode source_route"},
		{"source_before_target", func(r *Record) { r.SourceRoute = []byte(`{`); r.TargetRoute = nil }, "event record envelope: decode source_route"},
		{"target_before_set", func(r *Record) { r.TargetRoute = []byte(`{}` + `{}`); r.TargetSet = nil }, "event record envelope: decode target_route"},
		{"target_absent", func(r *Record) { r.TargetRoute = nil }, "event record envelope: target_route is required"},
		{"set_absent", func(r *Record) { r.TargetSet = nil }, "event record envelope: target_set is required"},
		{"set_type", func(r *Record) { r.TargetSet = []byte(`{}`) }, "event record envelope: decode target_set"},
		{"set_trailing", func(r *Record) { r.TargetSet = []byte(`[] []`) }, "event record envelope: decode target_set"},
		{"envelope_before_class", func(r *Record) { r.TargetSet = nil; r.SourceEventID = r.EventID }, "event record envelope: target_set is required"},
		{"class_before_routing_source", func(r *Record) { r.SourceEventID = r.EventID; r.RoutingSourceKind = "invalid" }, "cannot carry source_event_id"},
		{"origin_before_routing_source", func(r *Record) { r.InheritedFanOutOrigin = []byte(`{}`); r.RoutingSourceKind = "invalid" }, "cannot carry inherited fan-out origin"},
		{"routing_kind", func(r *Record) { r.RoutingSourceKind = "invalid" }, "event record routing source"},
		{"external_authority", func(r *Record) { r.RoutingSourceAuthority = "invalid" }, "event record routing source"},
		{"external_instance", func(r *Record) {
			r.SourceRoute = []byte(`{"flow_id":"ingress","flow_instance":"ingress/one","entity_id":"33333333-3333-4333-8333-333333333333"}`)
		}, "external ingress routing source forbids flow_instance"},
		{"source_null", func(r *Record) { r.SourceRoute = []byte(`null`) }, "external ingress routing source requires"},
		{"persistent_identity", func(r *Record) { r.EventID = "not-a-uuid" }, "decode event record not-a-uuid"},
		{"source_normalization_equality", func(r *Record) {
			r.SourceRoute = bytes.Replace(r.SourceRoute, []byte(`"ingress"`), []byte(`" ingress "`), 1)
		}, "changed durable facts"},
		{"source_unknown_field_equality", func(r *Record) { r.SourceRoute = append([]byte(`{"extra":true,`), r.SourceRoute[1:]...) }, "changed durable facts"},
		{"set_null_equality", func(r *Record) { r.TargetSet = []byte(`null`) }, "changed durable facts"},
	} {
		t.Run(test.name, func(t *testing.T) {
			record := envelopeReuseFixture(t, 1, "external")
			test.mutate(&record)
			requireEnvelopeReuseDecodeParity(t, record, test.want)
		})
	}
	t.Run("target_and_set", func(t *testing.T) {
		record := envelopeReuseFixture(t, 1, "target")
		record.TargetSet = []byte(`[{"flow_id":"consumer","flow_instance":"consumer/two"}]`)
		requireEnvelopeReuseDecodeParity(t, record, "cannot declare both target and target_set")
	})
	t.Run("target_projection", func(t *testing.T) {
		record := envelopeReuseFixture(t, 1, "target")
		record.FlowInstance = "consumer/other"
		requireEnvelopeReuseDecodeParity(t, record, "target route must exactly match")
	})
	t.Run("inherited_origin", func(t *testing.T) {
		record := inheritedFanOutRecord(t)
		record.InheritedFanOutOrigin = append(record.InheritedFanOutOrigin, []byte(` {}`)...)
		requireEnvelopeReuseDecodeParity(t, record, "exactly one JSON object")
	})
}

func TestValidatedEnvelopeReuseDoesNotRetainFacts(t *testing.T) {
	record := envelopeReuseFixture(t, 1, "target_set")
	first, settlement, err := record.DecodeWithSettlement()
	if err != nil {
		t.Fatal(err)
	}
	before, err := FromAdmitted(first, settlement)
	if err != nil {
		t.Fatal(err)
	}
	// Mutate existing backing storage, not just the Record's slice headers.
	record.TargetSet[0] = '!'
	requireEnvelopeReuseDecodeParity(t, record, "event record envelope: decode target_set")
	after, err := FromAdmitted(first, settlement)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("decoded result aliases source record: %v", err)
	}
}

var envelopeReuseEventSink events.AdmittedEvent
var envelopeReuseSettlementSink events.RouteSettlement

func BenchmarkValidatedEnvelopeReuse(b *testing.B) {
	for _, shape := range []string{"absent", "external", "root_source", "target", "target_set"} {
		for _, plans := range []int{-1, 0, 1, 32} {
			record := envelopeReuseFixture(b, plans, shape)
			b.Run(fmt.Sprintf("%s/plans_%d", shape, plans), func(b *testing.B) {
				for _, variant := range []struct {
					name   string
					decode func(Record) (events.AdmittedEvent, events.RouteSettlement, error)
				}{{"old", oldEnvelopeReuseDecodeWithSettlement}, {"current", Record.DecodeWithSettlement}} {
					b.Run(variant.name, func(b *testing.B) {
						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							event, settlement, err := variant.decode(record)
							if err != nil {
								b.Fatal(err)
							}
							envelopeReuseEventSink, envelopeReuseSettlementSink = event, settlement
						}
					})
				}
			})
		}
	}
}
