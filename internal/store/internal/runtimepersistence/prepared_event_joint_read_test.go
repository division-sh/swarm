package runtimepersistence

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/google/uuid"
)

func TestPreparedEventJointReadRejectsCorruptionAndReadsFreshFactsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "prepared.joint", "gateway", "", []byte(`{"version":1}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticEventFixture(ctx, fixture.store.(eventRecordContractStore), event); err != nil {
				t.Fatal(err)
			}
			original, found, err := loadEventProducerIdentityRecord(ctx, fixture, event.ID())
			if err != nil || !found {
				t.Fatalf("original record: found=%v err=%v", found, err)
			}
			reader := fixture.store.(preparedPublishEventReadbackStore)
			publicReader := fixture.store.(interface {
				LoadOperatorEvent(context.Context, string) (operatorread.OperatorEventFull, error)
			})
			assertRead := func(want eventrecord.Record) {
				t.Helper()
				prepared, found, err := reader.LoadPreparedPublishEvent(ctx, event.ID())
				if err != nil || !found {
					t.Fatalf("prepared read: found=%v err=%v", found, err)
				}
				got, err := eventrecord.FromAdmitted(prepared.Event, prepared.Settlement)
				if err != nil || !want.Equal(got) {
					t.Fatalf("prepared read changed or reused stale facts: err=%v", err)
				}
				if err := prepared.Validate(); err != nil {
					t.Fatalf("prepared aggregate: %v", err)
				}
				public, err := publicReader.LoadOperatorEvent(ctx, event.ID())
				if err != nil {
					t.Fatal(err)
				}
				wantPublic, err := operatorread.NewOperatorEventFull(prepared.Event.Event())
				if err != nil || public.EventID != want.EventID || !reflect.DeepEqual(public.Payload, wantPublic.Payload) {
					t.Fatalf("public read changed or reused stale facts: err=%v", err)
				}
			}
			assertRead(original)
			missing, found, err := reader.LoadPreparedPublishEvent(ctx, uuid.NewString())
			if err != nil || found || missing.Event.ID() != "" {
				t.Fatalf("missing record leaked admission: found=%v err=%v", found, err)
			}
			for _, corruption := range []string{"unknown_field", "invalid_arm", "wrong_event_class", "invalid_payload"} {
				t.Run(corruption, func(t *testing.T) {
					hostile := original.Clone()
					var wire map[string]json.RawMessage
					if err := json.Unmarshal(hostile.RouteSettlement, &wire); err != nil {
						t.Fatal(err)
					}
					switch corruption {
					case "unknown_field":
						wire["unowned"] = json.RawMessage(`true`)
					case "invalid_arm":
						wire["arm"] = json.RawMessage(`"invented"`)
					case "wrong_event_class":
						wire["write_class"], err = json.Marshal(events.EventWriteDirectiveDirect.Code())
						if err != nil {
							t.Fatal(err)
						}
					case "invalid_payload":
						hostile.Payload = []byte(`{"unowned":null}`)
					}
					hostile.RouteSettlement, err = json.Marshal(wire)
					if err != nil {
						t.Fatal(err)
					}
					writeJointSourceReadRecord(t, ctx, fixture.db, backend.name == "postgres", hostile)
					defer writeJointSourceReadRecord(t, ctx, fixture.db, backend.name == "postgres", original)
					prepared, found, err := reader.LoadPreparedPublishEvent(ctx, event.ID())
					assertJointSourceReadCorrupt(t, event.ID(), err)
					if found || prepared.Event.ID() != "" || prepared.Settlement.WriteClass().Code() != "" || len(prepared.DeliveryRoutes) != 0 {
						t.Fatal("corrupt record leaked partial prepared authority")
					}
					public, err := publicReader.LoadOperatorEvent(ctx, event.ID())
					assertJointSourceReadCorrupt(t, event.ID(), err)
					if public.EventID != "" || len(public.Deliveries) != 0 {
						t.Fatal("corrupt record leaked partial public projection")
					}
				})
				assertRead(original)
			}
			changed := original.Clone()
			changed.Payload = []byte(`{"version":2}`)
			writeJointSourceReadRecord(t, ctx, fixture.db, backend.name == "postgres", changed)
			assertRead(changed)
			writeJointSourceReadRecord(t, ctx, fixture.db, backend.name == "postgres", original)
			assertRead(original)
		})
	}
}
