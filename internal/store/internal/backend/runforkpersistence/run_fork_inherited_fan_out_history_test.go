package runforkpersistence

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

func inheritedFanOutHistoryFixture(t *testing.T) *runForkRevisionSnapshot {
	t.Helper()
	run, ancestor, trigger, delivery := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	node, err := identity.AdmitExecutableNodeDeclaration(".", "scatter")
	if err != nil {
		t.Fatal(err)
	}
	declaration, err := identity.AdmitDeclarationIdentity(".", "fan_out", "scatter/handlers/items.ready/fan_out")
	if err != nil {
		t.Fatal(err)
	}
	origin, err := events.NewInheritedFanOutOrigin(run, ancestor, trigger, delivery, declaration, "exact-bundle", "exact-digest", 0)
	if err != nil {
		t.Fatal(err)
	}
	capsule := fanoutobligation.Capsule{NodeKey: node.Key(), ExecutionFlowID: ".", Route: flowidentity.StoredRoute(".", run, run), EntityID: run,
		HandlerEventKey: "items.ready", ProducerSource: eventtest.RootRoutingSource(run), Lineage: events.EventLineage{RunID: ancestor, ParentEventID: trigger, ExecutionMode: executionmode.Live}}
	raw, err := json.Marshal(capsule)
	if err != nil {
		t.Fatal(err)
	}
	originRaw, err := json.Marshal(origin)
	if err != nil {
		t.Fatal(err)
	}
	eventID := uuid.NewString()
	ordinal := 0
	ledger, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		t.Fatal(err)
	}
	settlement, err := events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryMatchedNoRecipient, ledger)
	if err != nil {
		t.Fatal(err)
	}
	settlementRaw, err := json.Marshal(settlement)
	if err != nil {
		t.Fatal(err)
	}
	return &runForkRevisionSnapshot{RunID: run, Revision: 5,
		Events: []runForkRevisionEvent{{RunID: run, EventID: eventID, EventClass: string(events.EventAdmissionInheritedFanOut), EventName: "item.created",
			ExecutionMode: "live", InheritedFanOutOrigin: originRaw, RoutingSource: capsule.ProducerSource, ProducedBy: node.Key(), ProducedByType: "node", ChainDepth: 1,
			TargetRoute: json.RawMessage(`{}`), TargetSet: json.RawMessage(`[]`),
			Payload: json.RawMessage(`{"value":"one"}`), Scope: "global", CreatedAt: time.Now().UTC().Truncate(time.Microsecond), RouteSettlement: settlementRaw}},
		FanOutFacts: []runForkRevisionFanOutFact{
			{FactKind: "intent", TriggeringDeliveryID: delivery, FlowPath: ".", DeclarationFamily: "fan_out", SemanticPath: declaration.SemanticPath(), BundleHash: "exact-bundle", SemanticDigest: "exact-digest", Capsule: raw, Cursor: 1, Cardinality: 2},
			{FactKind: "outcome", TriggeringDeliveryID: delivery, FlowPath: ".", DeclarationFamily: "fan_out", SemanticPath: declaration.SemanticPath(), Ordinal: &ordinal, OutcomeKind: "committed", EventID: eventID},
		},
	}
}

func TestInheritedFanOutHistoryRequiresExactFixedOrdinal(t *testing.T) {
	snapshot := inheritedFanOutHistoryFixture(t)
	if err := admitRunForkInheritedFanOutHistory(snapshot); err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{"missing_origin", "unknown_origin", "run", "causal_parent", "class", "missing_outcome", "duplicate_outcome", "failed_outcome", "ordinal", "missing_intent", "duplicate_intent", "foreign_declaration", "foreign_delivery", "bundle", "digest", "cursor", "cardinality", "capsule_source", "capsule_trigger", "capsule_producer"} {
		t.Run(variant, func(t *testing.T) {
			snapshot := inheritedFanOutHistoryFixture(t)
			switch variant {
			case "missing_origin":
				snapshot.Events[0].InheritedFanOutOrigin = nil
			case "unknown_origin":
				snapshot.Events[0].InheritedFanOutOrigin = append([]byte(`{"unchecked":true,`), snapshot.Events[0].InheritedFanOutOrigin[1:]...)
			case "run":
				snapshot.Events[0].RunID = uuid.NewString()
			case "causal_parent":
				snapshot.Events[0].SourceEventID = uuid.NewString()
			case "class":
				snapshot.Events[0].EventClass = "child"
			case "missing_outcome":
				snapshot.FanOutFacts = snapshot.FanOutFacts[:1]
			case "duplicate_outcome":
				snapshot.FanOutFacts = append(snapshot.FanOutFacts, snapshot.FanOutFacts[1])
			case "failed_outcome":
				snapshot.FanOutFacts[1].OutcomeKind = "failed"
			case "ordinal":
				*snapshot.FanOutFacts[1].Ordinal = 1
			case "missing_intent":
				snapshot.FanOutFacts = snapshot.FanOutFacts[1:]
			case "duplicate_intent":
				snapshot.FanOutFacts = append(snapshot.FanOutFacts, snapshot.FanOutFacts[0])
			case "foreign_declaration":
				snapshot.FanOutFacts[0].SemanticPath += ".wrong"
			case "foreign_delivery":
				snapshot.FanOutFacts[1].TriggeringDeliveryID = uuid.NewString()
			case "bundle":
				snapshot.FanOutFacts[0].BundleHash += "-wrong"
			case "digest":
				snapshot.FanOutFacts[0].SemanticDigest += "-wrong"
			case "cursor":
				snapshot.FanOutFacts[0].Cursor = 0
			case "cardinality":
				snapshot.FanOutFacts[0].Cardinality = 0
			default:
				var capsule fanoutobligation.Capsule
				if err := json.Unmarshal(snapshot.FanOutFacts[0].Capsule, &capsule); err != nil {
					t.Fatal(err)
				}
				switch variant {
				case "capsule_source":
					capsule.ProducerSource = eventtest.RootRoutingSource(uuid.NewString())
				case "capsule_trigger":
					capsule.Lineage.ParentEventID = uuid.NewString()
				case "capsule_producer":
					node, err := identity.AdmitExecutableNodeDeclaration(".", "other")
					if err != nil {
						t.Fatal(err)
					}
					capsule.NodeKey = node.Key()
				}
				raw, err := json.Marshal(capsule)
				if err != nil {
					t.Fatal(err)
				}
				snapshot.FanOutFacts[0].Capsule = raw
			}
			before, _ := json.Marshal(snapshot)
			if err := admitRunForkInheritedFanOutHistory(snapshot); err == nil {
				t.Fatal("contradictory fixed-R origin accepted")
			}
			after, _ := json.Marshal(snapshot)
			if !bytes.Equal(before, after) {
				t.Fatal("failed admission changed fixed-R evidence")
			}
		})
	}
}
