package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/google/uuid"
)

func TestOperatorEventSnapshotInheritedOwnerRefusalBothStores(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		t.Run(fmt.Sprintf("postgres_%v", postgres), func(t *testing.T) {
			f := newOperatorSnapshotFixture(t, postgres)
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 15*time.Second)
			defer cancel()
			record, found, err := loadEventProducerIdentityRecord(ctx, f.authorActivityReceiptFixture, f.ids[1])
			if err != nil || !found {
				t.Fatalf("source record: found=%v err=%v", found, err)
			}
			declaration, err := identity.AdmitDeclarationIdentity(".", "fan_out", `handlers["snapshot.event"].rules[0]`)
			if err != nil {
				t.Fatal(err)
			}
			origin, err := events.NewInheritedFanOutOrigin(record.RunID, uuid.NewString(), f.ids[0], uuid.NewString(), declaration, record.PayloadSchemaBundleHash, "sha256:"+strings.Repeat("3", 64), 0)
			if err != nil {
				t.Fatal(err)
			}
			record.Class, record.SourceEventID = events.EventAdmissionInheritedFanOut, ""
			producer := eventtest.Producer(events.EventProducerNode, "snapshot-node")
			record.ProducedBy, record.ProducedByType = producer.ID(), producer.Type()
			record.InheritedFanOutOrigin, err = json.Marshal(origin)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := record.DecodeWithSettlement(); err != nil {
				t.Fatalf("owner-refusal fixture must pass full codec admission first: %v", err)
			}
			// Class, origin and producer form one checked physical row. Install
			// them atomically, without disabling any schema constraint.
			if _, err := f.writer.ExecContext(ctx, `UPDATE events SET event_class=$1,source_event_id=NULL,produced_by=$2,produced_by_type=$3,inherited_fan_out_origin=$4 WHERE event_id=$5`, record.Class, record.ProducedBy, record.ProducedByType, string(record.InheritedFanOutOrigin), record.EventID); err != nil {
				t.Fatal(err)
			}
			store := f.store.(routeSettlementOperatorStore)
			for _, operation := range []string{"list", "get"} {
				t.Run(operation, func(t *testing.T) {
					_, release := f.probe.arm("none", 0, nil, nil)
					defer release()
					var err error
					if operation == "list" {
						var page operatorread.OperatorEventListResult
						page, err = store.ListOperatorEvents(ctx, f.opts)
						if !reflect.DeepEqual(page, operatorread.OperatorEventListResult{}) {
							t.Fatalf("orphan inherited owner leaked partial list: %+v", page)
						}
					} else {
						var event operatorread.OperatorEventFull
						event, err = store.LoadOperatorEvent(ctx, record.EventID)
						if !reflect.DeepEqual(event, operatorread.OperatorEventFull{}) {
							t.Fatalf("orphan inherited owner leaked partial event: %+v", event)
						}
					}
					if !errors.Is(err, eventrecord.ErrCorrupt) || !strings.Contains(err.Error(), "committed ordinal owner") {
						t.Fatalf("expected actual inherited-owner SQL refusal, got %v", err)
					}
					assertOperatorSnapshotTransaction(t, f.probe, postgres)
					if f.probe.seen["inherited-owner"] != 1 {
						t.Fatalf("inherited owner query count=%d", f.probe.seen["inherited-owner"])
					}
					f.probe.disarm()
				})
			}
		})
	}
}
