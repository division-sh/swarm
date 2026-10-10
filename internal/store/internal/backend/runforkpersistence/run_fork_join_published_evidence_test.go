package runforkpersistence

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/google/uuid"
)

func genericPublicationRevisionEvent(record eventrecord.Record, event events.Event) runForkRevisionEvent {
	return runForkRevisionEvent{
		runForkRevisionedFact: runForkRevisionedFact{FirstRevision: 3, Revision: 3},
		RunID:                 record.RunID, EventClass: string(record.Class), ExecutionMode: string(record.ExecutionMode),
		EventID: record.EventID, EventName: record.EventName, TaskID: record.TaskID,
		EntityID: record.EntityID, FlowInstance: record.FlowInstance, Scope: string(record.Scope),
		RoutingSource: event.RoutingSource(), TargetRoute: record.TargetRoute, TargetSet: record.TargetSet,
		RouteSettlement: record.RouteSettlement, Payload: record.Payload, ChainDepth: record.ChainDepth,
		ProducedBy: record.ProducedBy, ProducedByType: string(record.ProducedByType), SourceEventID: record.SourceEventID,
		SelectedForkSourceRunID: record.SelectedForkSourceRunID, SelectedForkSourceEventID: record.SelectedForkSourceEventID,
		SelectedForkAuthorityStamp: record.SelectedForkAuthorityStamp, SelectedForkLineageOwners: record.SelectedForkLineageOwners,
		CreatedAt: record.CreatedAt, PayloadSchemaBundleHash: record.PayloadSchemaBundleHash,
		PayloadSchemaFlowID: record.PayloadSchemaFlowID, PayloadSchemaEventKey: record.PayloadSchemaEventKey,
		PayloadSchemaDigest: record.PayloadSchemaDigest, PayloadSchemaClass: record.PayloadSchemaClass,
	}
}

func TestPublishedArrivalEvidenceRequiresExactFixedCutWithoutRearming(t *testing.T) {
	for _, root := range []bool{false, true} {
		for _, completion := range []bool{false, true} {
			for _, fault := range []string{"exact", "missing", "duplicate", "foreign_cut", "future_event", "unversioned_event", "foreign_event", "wrong_task", "wrong_payload"} {
				t.Run(map[bool]string{false: "flow/", true: "root/"}[root]+map[bool]string{false: "timeout/", true: "complete/"}[completion]+fault, func(t *testing.T) {
					activation, record := selectedGenericPublicationFixture(t, root, completion)
					admitted, err := record.Decode()
					if err != nil {
						t.Fatal(err)
					}
					snapshot := &runForkRevisionSnapshot{RunID: activation.Command.RunID, Revision: 7,
						Events: []runForkRevisionEvent{genericPublicationRevisionEvent(record, admitted.Event())}}
					switch fault {
					case "missing":
						snapshot.Events = nil
					case "duplicate":
						snapshot.Events = append(snapshot.Events, snapshot.Events[0])
					case "foreign_cut":
						snapshot.RunID = uuid.NewString()
					case "future_event":
						snapshot.Events[0].Revision = snapshot.Revision + 1
					case "unversioned_event":
						snapshot.Events[0].FirstRevision = 0
					case "foreign_event":
						snapshot.Events[0].RunID = uuid.NewString()
					case "wrong_task":
						snapshot.Events[0].TaskID = "unrelated-task"
					case "wrong_payload":
						snapshot.Events[0].Payload = []byte(`{"hostile":true}`)
					}
					before := activation.Canonical()
					got, err := runForkPublishedArrivalEvidence(snapshot, activation)
					if fault == "exact" {
						if err != nil || got.Event().ID() != activation.CurrentEventID || got.Event().RunID() != activation.Command.RunID {
							t.Fatalf("exact historical publication: event=%v err=%v", got, err)
						}
					} else if err == nil {
						t.Fatal("corrupt or out-of-cut publication became historical evidence")
					}
					if err := activation.ValidateForkJoinRestorationSource(); err == nil {
						t.Fatal("publication proof rearmed a historical occurrence")
					}
					if !reflect.DeepEqual(before, activation) {
						t.Fatal("historical publication validation changed source evidence")
					}
				})
			}
		}
	}
}
