package eventrecord

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

// Frozen pre-optimization constructor, for same-binary differential proof and
// measurement. Keep its complete validation and old encoding/Clone costs.
func fromAdmittedOuterMarshalClone(admitted events.AdmittedEvent, settlement events.RouteSettlement) (Record, error) {
	event := admitted.Event()
	if err := events.ValidatePersistentEvent(event); err != nil {
		return Record{}, fmt.Errorf("admitted event: %w", err)
	}
	envelope := event.NormalizedEnvelope()
	payloadAdmission, ok := event.PayloadAdmission()
	if !ok {
		return Record{}, fmt.Errorf("admitted event payload schema binding is required")
	}
	payloadBinding := payloadAdmission.Binding()
	rawSettlement, err := json.Marshal(settlement)
	if err != nil {
		return Record{}, fmt.Errorf("event record route settlement: %w", err)
	}
	record := Record{
		Class: event.AdmissionClass(), EventID: event.ID(), RunID: event.RunID(),
		EventName: string(event.Type()), TaskID: event.TaskID(), EntityID: envelope.EntityID,
		FlowInstance: envelope.FlowInstance, Scope: envelope.Scope, Payload: payloadAdmission.Payload(),
		PayloadSchemaBundleHash: payloadBinding.BundleHash(), PayloadSchemaFlowID: payloadBinding.FlowID(),
		PayloadSchemaEventKey: payloadBinding.EventKey(), PayloadSchemaDigest: payloadBinding.SchemaDigest(),
		PayloadSchemaClass: payloadBinding.SchemaClass(), ExecutionMode: event.ExecutionMode(),
		ChainDepth: event.ChainDepth(), ProducedBy: event.Producer().ID(), ProducedByType: event.Producer().Type(),
		SourceEventID: event.ParentEventID(), CreatedAt: event.CreatedAt().UTC().Truncate(time.Microsecond),
		RoutingSourceKind:      event.RoutingSource().Kind().StorageCode(),
		RoutingSourceAuthority: event.RoutingSource().Authority().StorageCode(),
		SourceRoute:            marshalRoute(event.RoutingSource().Route()), TargetRoute: marshalRoute(envelope.Target),
		TargetSet: marshalRouteSet(envelope.TargetSet), RouteSettlement: rawSettlement,
	}
	if provenance, ok := event.OperatorReference(); ok {
		record.OperatorReferencedEventID = provenance.ReferencedEventID()
	}
	if lineage, ok := event.SelectedForkLineage(); ok {
		record.SelectedForkSourceRunID = lineage.SourceRunID()
		record.SelectedForkSourceEventID = lineage.SourceEventID()
		record.SelectedForkAuthorityStamp = lineage.AuthorityStamp()
		record.SelectedForkLineageOwners = 1
	}
	if origin, ok := event.InheritedFanOutOrigin(); ok {
		var err error
		record.InheritedFanOutOrigin, err = json.Marshal(origin)
		if err != nil {
			return Record{}, err
		}
	}
	if err := record.validateWithSettlement(settlement); err != nil {
		return Record{}, err
	}
	return record.Clone(), nil
}

func admittedSerializationFixture(tb testing.TB, plans int, path string) (events.AdmittedEvent, events.RouteSettlement, []byte) {
	tb.Helper()
	payload, err := json.Marshal(map[string]string{"value": path})
	if err != nil {
		tb.Fatal(err)
	}
	event := eventtest.RunCreatingRootIngress("11111111-1111-4111-8111-111111111111", "record.serialization", "gateway", "task", payload, 0,
		"22222222-2222-4222-8222-222222222222", "", events.EventEnvelope{}, time.Date(2026, 9, 19, 1, 0, 0, 123456000, time.UTC))
	event, err = eventtest.AdmitPayload(event, "", "record.serialization")
	if err != nil {
		tb.Fatal(err)
	}
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		tb.Fatal(err)
	}
	node, err := identity.AdmitExecutableNodeDeclaration(".", "consumer")
	if err != nil {
		tb.Fatal(err)
	}
	var evaluations []events.ConnectPlanEvaluation
	for i := 0; i < plans; i++ {
		candidate, err := events.NewConnectCandidateEvidence(events.AdmitConnectReceiverIdentity(sha256.Sum256([]byte(fmt.Sprintf("receiver-%d", i)))),
			events.MustNodeDeliveryRecipient(node), path, agentidentity.Plan{}, events.ConnectCandidateAccepted)
		if err != nil {
			tb.Fatal(err)
		}
		plan, err := events.NewConnectPlanEvaluation(events.AdmitConnectPlanIdentity(sha256.Sum256([]byte(fmt.Sprintf("plan-%d", i)))),
			events.ConnectPlanResolved, []events.RouteIdentity{{FlowID: "consumer", FlowInstance: fmt.Sprintf("consumer/%d", i)}}, []events.ConnectCandidateEvidence{candidate})
		if err != nil {
			tb.Fatal(err)
		}
		evaluations = append(evaluations, plan)
	}
	ledger, err := events.NewConnectEvaluationLedger(evaluations)
	if err != nil {
		tb.Fatal(err)
	}
	settlement, err := events.NewDeliverySettlement(events.EventWriteNormalPublication, ledger)
	if err != nil {
		tb.Fatal(err)
	}
	return admitted, settlement, payload
}

func TestFromAdmittedSerializationBytesAndErrors(t *testing.T) {
	for _, value := range []string{"consumer", "consumer/<tag>&\"quote\"\\slash\n\t\x00tail", "consumer/\u2028\u2029\u00e9", "consumer/\xff\xfe-invalid"} {
		for _, plans := range []int{0, 1, 32} {
			admitted, settlement, _ := admittedSerializationFixture(t, plans, value)
			outer, err := json.Marshal(settlement)
			if err != nil {
				t.Fatal(err)
			}
			direct, err := settlement.MarshalJSON()
			if err != nil || !bytes.Equal(outer, direct) {
				t.Fatalf("settlement encoding differs: plans=%d value=%q err=%v", plans, value, err)
			}
			before, err := fromAdmittedOuterMarshalClone(admitted, settlement)
			if err != nil {
				t.Fatal(err)
			}
			after, err := FromAdmitted(admitted, settlement)
			if err != nil || !reflect.DeepEqual(before, after) || !before.Equal(after) {
				t.Fatalf("constructor bytes changed: plans=%d value=%q err=%v", plans, value, err)
			}
			if err := after.Validate(); err != nil {
				t.Fatal(err)
			}
		}
	}
	admitted, settlement, _ := admittedSerializationFixture(t, 1, "consumer")
	wrongClass, err := events.NewNoDeliverySettlement(events.EventWriteRuntimeLogDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range []struct {
		event      events.AdmittedEvent
		settlement events.RouteSettlement
	}{
		{admitted, events.RouteSettlement{}}, {events.AdmittedEvent{}, settlement}, {admitted, wrongClass},
	} {
		oldRecord, oldErr := fromAdmittedOuterMarshalClone(pair.event, pair.settlement)
		newRecord, newErr := FromAdmitted(pair.event, pair.settlement)
		if oldErr == nil || newErr == nil || oldErr.Error() != newErr.Error() || !reflect.DeepEqual(oldRecord, newRecord) {
			t.Fatalf("error contract changed: old=%v new=%v", oldErr, newErr)
		}
		var oldMarshal, newMarshal *json.MarshalerError
		oldAs, newAs := errors.As(oldErr, &oldMarshal), errors.As(newErr, &newMarshal)
		if oldAs != newAs {
			t.Fatalf("MarshalerError wrapping changed: old=%v new=%v", oldErr, newErr)
		}
		if oldAs && (oldMarshal.Type != newMarshal.Type || oldMarshal.Err.Error() != newMarshal.Err.Error() || errors.Unwrap(newMarshal) != newMarshal.Err) {
			t.Fatal("MarshalerError type/cause contract changed")
		}
	}
}

func TestFromAdmittedSerializationOwnsEveryByteSlice(t *testing.T) {
	for _, input := range []Record{validRecord(t), inheritedFanOutRecord(t)} {
		admitted, settlement, err := input.DecodeWithSettlement()
		if err != nil {
			t.Fatal(err)
		}
		first, err := FromAdmitted(admitted, settlement)
		if err != nil {
			t.Fatal(err)
		}
		second, err := FromAdmitted(admitted, settlement)
		if err != nil {
			t.Fatal(err)
		}
		baseline := second.Clone()
		old, err := fromAdmittedOuterMarshalClone(admitted, settlement)
		if err != nil || !reflect.DeepEqual(first, old) {
			t.Fatalf("old/new input encoding mismatch: %v", err)
		}
		seen := 0
		// Reflect over all fields so a future mutable slice cannot silently
		// escape the constructor's allocation proof.
		for i := 0; i < reflect.TypeOf(first).NumField(); i++ {
			field := reflect.ValueOf(&first).Elem().Field(i)
			if field.Kind() != reflect.Slice {
				continue
			}
			if field.Type() != reflect.TypeOf([]byte(nil)) {
				t.Fatalf("unaccounted mutable field %s", reflect.TypeOf(first).Field(i).Name)
			}
			seen++
			if field.Len() > 0 {
				field.Index(0).SetUint(field.Index(0).Uint() ^ 1)
			}
		}
		if seen != 6 {
			t.Fatalf("update byte-slice ownership census: %d", seen)
		}
		if !reflect.DeepEqual(second, baseline) {
			t.Fatal("independent constructor outputs share storage")
		}
		fresh, err := FromAdmitted(admitted, settlement)
		if err != nil || !reflect.DeepEqual(fresh, baseline) {
			t.Fatalf("output mutation reached retained admitted/settlement inputs: %v", err)
		}
		for i := 0; i < reflect.TypeOf(input).NumField(); i++ {
			field := reflect.ValueOf(&input).Elem().Field(i)
			if field.Kind() == reflect.Slice && field.Len() > 0 {
				field.Index(0).SetUint(field.Index(0).Uint() ^ 1)
			}
		}
		if !reflect.DeepEqual(second, baseline) {
			t.Fatal("record retained mutable input storage")
		}
		fresh, err = FromAdmitted(admitted, settlement)
		if err != nil || !reflect.DeepEqual(fresh, baseline) {
			t.Fatalf("mutating source record changed admitted input: %v", err)
		}
	}
	admitted, settlement, payload := admittedSerializationFixture(t, 1, "original")
	record, err := FromAdmitted(admitted, settlement)
	if err != nil {
		t.Fatal(err)
	}
	baseline := record.Clone()
	payload[0] ^= 1
	if !reflect.DeepEqual(record, baseline) {
		t.Fatal("record aliases caller payload")
	}
}

var serializationRecordSink Record
var serializationBytesSink []byte

func BenchmarkFromAdmittedSerialization(b *testing.B) {
	for _, plans := range []int{0, 1, 32} {
		admitted, settlement, _ := admittedSerializationFixture(b, plans, "consumer/"+strings.Repeat("value<&>", 16))
		b.Run(fmt.Sprintf("plans_%d", plans), func(b *testing.B) {
			for _, variant := range []struct {
				name      string
				construct func(events.AdmittedEvent, events.RouteSettlement) (Record, error)
			}{{"old_outer_clone", fromAdmittedOuterMarshalClone}, {"direct_owned", FromAdmitted}} {
				b.Run(variant.name, func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						record, err := variant.construct(admitted, settlement)
						if err != nil {
							b.Fatal(err)
						}
						serializationRecordSink = record
					}
				})
			}
			b.Run("encoding_outer", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					raw, err := json.Marshal(settlement)
					if err != nil {
						b.Fatal(err)
					}
					serializationBytesSink = raw
				}
			})
			b.Run("encoding_direct", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					raw, err := settlement.MarshalJSON()
					if err != nil {
						b.Fatal(err)
					}
					serializationBytesSink = raw
				}
			})
		})
	}
}
