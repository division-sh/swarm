package runforkpersistence

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/google/uuid"
)

func activityPublicationCutFixture(t *testing.T) (runForkSourceStateAdmission, string, runForkActivityAttemptEvidence, runForkActivityRequestPayload) {
	t.Helper()
	f, source, fact := continuedActivityIdentityFixture(t)
	request, err := f.record.Decode()
	if err != nil {
		t.Fatal(err)
	}
	resultID := activityidentity.ResultEventID(fact, "compute.succeeded")
	raw := []byte(`{"activity_id":"compute","attempt":1,"effect_class":"non_idempotent_write","result":{"value":"recorded"},"tool":"compute"}`)
	producer, err := events.NewProducerIdentity(events.EventProducerAgent, "provider")
	if err != nil {
		t.Fatal(err)
	}
	result := eventtest.ChildForProducerWithRoutingSource(resultID, "compute.succeeded", producer, "", raw, 1,
		events.EventLineage{RunID: f.record.RunID, ParentEventID: source.SourceEventID, ExecutionMode: source.ExecutionMode}, request.Event().Envelope(), source.RoutingSource, f.record.CreatedAt)
	schema, err := events.NewPayloadSchemaBinding(events.PayloadSchemaBindingInput{BundleHash: f.record.PayloadSchemaBundleHash, FlowID: ".", EventKey: "compute.succeeded",
		SchemaDigest: f.record.PayloadSchemaDigest, SchemaClass: events.PayloadSchemaAuthored})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := events.NewPayloadAdmission(raw, schema)
	if err != nil {
		t.Fatal(err)
	}
	result, err = events.ApplyPayloadAdmission(result, payload)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := events.AdmitForPersistence(result, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		t.Fatal(err)
	}
	settlement, err := events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryDeclaredConsumerNoPlan, ledger)
	if err != nil {
		t.Fatal(err)
	}
	record, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &runForkRevisionSnapshot{RunID: f.record.RunID, Revision: 3,
		Events: []runForkRevisionEvent{genericPublicationRevisionEvent(f.record, request.Event()), genericPublicationRevisionEvent(record, result)}}
	child := runForkActivityRequestPayload{SourceRunID: uuid.NewString(), SourceEventID: uuid.NewString(), EntityID: uuid.NewString(),
		NodeID: fact.Owner.Key(), FlowID: ".", HandlerEventKey: fact.HandlerEventKey, ActivityID: fact.ActivityID, Tool: fact.Tool}
	evidence := runForkActivityAttemptEvidence{Status: "succeeded", ExecutionMode: source.ExecutionMode,
		ResultEventID: resultID, ResultEventType: "compute.succeeded", ResultPayload: raw, InputHash: "exact-input", CompletedAt: result.CreatedAt()}
	return runForkSourceStateAdmission{snapshot: snapshot, forkRunID: child.SourceRunID}, source.SourceEventID, evidence, child
}

func TestActivityPublicationContinuationUsesCutNotCurrentProgress(t *testing.T) {
	for _, phase := range []string{"journal_unpublished", "published_unfinished", "published_terminal", "later_source_result"} {
		t.Run(phase, func(t *testing.T) {
			a, requestID, evidence, child := activityPublicationCutFixture(t)
			if phase == "journal_unpublished" || phase == "later_source_result" {
				// The completed mutable journal remains, but this publication is
				// absent from the admitted earlier cut.
				a.snapshot.Events = a.snapshot.Events[:1]
			} else {
				status := deliverylifecycle.StatusPending
				if phase == "published_terminal" {
					status = deliverylifecycle.StatusDelivered
				}
				a.snapshot.Deliveries = []runForkRevisionDelivery{{runForkRevisionedFact: runForkRevisionedFact{FirstRevision: 3, Revision: 3},
					Snapshot: deliverylifecycle.Snapshot{DeliveryID: uuid.NewString(), RunID: a.snapshot.RunID, EventID: evidence.ResultEventID, Status: status,
						SubscriberClass: deliverylifecycle.SubscriberNode, SubscriberID: "provider",
						Route: events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(identitytest.RootNode(t, "provider")),
							Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: a.snapshot.RunID, EntityID: a.snapshot.RunID})}}}}
			}
			publication, err := a.publishedActivityResult(context.Background(), nil, requestID, evidence, child, nil, "")
			if err != nil || publication.Present() != (phase == "published_unfinished" || phase == "published_terminal") {
				t.Fatalf("phase=%s continuation=%+v error=%v", phase, publication, err)
			}
			if publication.Present() && publication.SourceResultID() != evidence.ResultEventID {
				t.Fatal("publication continuation changed its exact physical source result")
			}
			work, err := loadRunForkPendingWorkFromRevision(a.snapshot)
			if err != nil {
				t.Fatal(err)
			}
			inputs := 0
			for _, item := range work {
				if item.EventID == requestID {
					inputs++
					if item.Classification != runfork.RunForkPendingClassificationPending || item.ReceiptAt != nil || item.DeliveredAt != nil {
						t.Fatal("published result forged an input receipt or terminal disposition")
					}
				}
			}
			if inputs != 1 {
				t.Fatalf("input accounting disappeared or duplicated: %+v", work)
			}
		})
	}
}

func TestActivityResultOnlyCutDoesNotInferRequestOwnership(t *testing.T) {
	a, _, evidence, _ := activityPublicationCutFixture(t)
	a.snapshot.Events = a.snapshot.Events[1:]
	routes, found, err := a.publishedActivityResultRoutes(context.Background(), nil, evidence.ResultEventID)
	if err != nil || found || len(routes) != 0 {
		t.Fatalf("result payload/type inferred a missing ordinary request: routes=%+v found=%t error=%v", routes, found, err)
	}
	cut, err := newRunForkReplayCut(a.snapshot)
	if err != nil {
		t.Fatal(err)
	}
	result, err := cut.event(evidence.ResultEventID)
	if err != nil {
		t.Fatal(err)
	}
	visiting := map[string]bool{cut.runID + "/" + result.ID(): true}
	if found, err := a.admitPublishedActivityResultAtCut(context.Background(), nil, cut, result, visiting); err == nil || found {
		t.Fatalf("result ownership admitted cyclic evidence: found=%t error=%v", found, err)
	}
	if len(visiting) != 1 || !visiting[cut.runID+"/"+result.ID()] {
		t.Fatal("cycle refusal changed its enclosing traversal evidence")
	}
}

func TestActivityPublicationContinuationRejectsContradictoryCutEvidence(t *testing.T) {
	for _, fault := range []string{"duplicate_result", "foreign_run", "future_result", "producer", "causal_parent", "payload", "completion", "journal_payload"} {
		t.Run(fault, func(t *testing.T) {
			a, requestID, evidence, child := activityPublicationCutFixture(t)
			switch fault {
			case "duplicate_result":
				a.snapshot.Events = append(a.snapshot.Events, a.snapshot.Events[1])
			case "foreign_run":
				a.snapshot.Events[1].RunID = uuid.NewString()
			case "future_result":
				a.snapshot.Events[1].Revision++
			case "producer":
				a.snapshot.Events[1].ProducedBy = "foreign"
			case "causal_parent":
				a.snapshot.Events[1].SourceEventID = uuid.NewString()
			case "payload":
				a.snapshot.Events[1].Payload = json.RawMessage(`{"result":"foreign"}`)
			case "completion":
				evidence.CompletedAt = evidence.CompletedAt.Add(1)
			case "journal_payload":
				evidence.ResultPayload = json.RawMessage(`{"result":"foreign"}`)
			}
			if _, err := a.publishedActivityResult(context.Background(), nil, requestID, evidence, child, nil, ""); err == nil {
				t.Fatal("contradictory fixed-cut result became publication discharge")
			}
		})
	}
}
