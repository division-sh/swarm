package runforkpersistence

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func originalStartRouteFixture(t *testing.T) (runfork.RunForkPlan, runfork.RunForkContractFrontierAdmission) {
	t.Helper()
	runID := uuid.NewString()
	event := eventtest.RunCreatingRootIngress(uuid.NewString(), "work.start", "operator", "", []byte(`{"value":7}`), 0,
		runID, "", events.EventEnvelope{}, time.Unix(100, 0).UTC())
	event, err := eventtest.AdmitPayload(event, ".", "work.start")
	if err != nil {
		t.Fatal(err)
	}
	input, err := runfork.InputPublicationFromEvent(event)
	if err != nil {
		t.Fatal(err)
	}
	plan := (runfork.RunForkPlan{SourceRunID: runID, ForkPoint: runfork.RunForkPoint{Kind: runfork.RunForkPointRunStart, Revision: 1}, StartFirstTurn: &input}).WithHistoricalEvents(1, nil)
	return plan, runfork.RunForkContractFrontierAdmission{
		Owner: runfork.RunForkContractFrontierAdmissionOwner, NonMutating: true,
		ContractSelection:  runfork.RunForkContractSelection{Mode: "selected_contracts"},
		FrontierEventCount: 1,
		FrontierEvents: []runfork.RunForkContractFrontierEvent{{
			SourceEventID: event.ID(), EventName: string(event.Type()), SourceClassifications: []string{"start_first_turn"}, SourceFlowInstances: []string{"."},
		}},
	}
}

func TestOriginalStartRouteResolutionKeepsExclusiveCutAndExactIntention(t *testing.T) {
	for _, variant := range []string{
		"original", "eventless", "event-cut", "deployment-cut", "missing-certificate", "missing-origin", "foreign-run", "foreign-event", "changed-name", "changed-classification", "missing-classification", "extra-classification", "duplicate", "extra-first-turn", "omitted-first-turn", "included-in-history", "event-borrows-first-turn", "deployment-borrows-first-turn", "ordinary-outside-cut", "event-crossed-origin", "deployment-crossed-origin",
	} {
		t.Run(variant, func(t *testing.T) {
			plan, frontier := originalStartRouteFixture(t)
			first := frontier.FrontierEvents[0]
			valid := false
			switch variant {
			case "original":
				valid = true
			case "eventless":
				plan.StartFirstTurn, frontier.FrontierEvents = nil, nil
				valid = true
			case "event-cut", "ordinary-outside-cut", "event-borrows-first-turn", "event-crossed-origin":
				plan.ForkPoint = runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, Revision: 1, EventID: first.SourceEventID}
				if variant != "event-crossed-origin" {
					plan.StartFirstTurn = nil
				}
				if variant != "ordinary-outside-cut" {
					plan = plan.WithHistoricalEvents(1, []string{first.SourceEventID})
				}
				if variant != "event-borrows-first-turn" {
					frontier.FrontierEvents[0].SourceClassifications = []string{"pending"}
				}
				valid = variant == "event-cut"
			case "deployment-cut", "deployment-borrows-first-turn", "deployment-crossed-origin":
				plan.ForkPoint.Kind = runfork.RunForkPointDeploymentRevision
				if variant != "deployment-crossed-origin" {
					plan.StartFirstTurn = nil
				}
				plan = plan.WithHistoricalEvents(1, []string{first.SourceEventID})
				if variant != "deployment-borrows-first-turn" {
					frontier.FrontierEvents[0].SourceClassifications = []string{"pending"}
				}
				valid = variant == "deployment-cut"
			case "missing-certificate":
				plan = runfork.RunForkPlan{SourceRunID: plan.SourceRunID, ForkPoint: plan.ForkPoint, StartFirstTurn: plan.StartFirstTurn}
			case "missing-origin":
				plan.StartFirstTurn = nil
			case "foreign-run":
				plan.SourceRunID = uuid.NewString()
			case "foreign-event":
				frontier.FrontierEvents[0].SourceEventID = uuid.NewString()
			case "changed-name":
				frontier.FrontierEvents[0].EventName = "work.other"
			case "changed-classification":
				frontier.FrontierEvents[0].SourceClassifications = []string{"pending"}
			case "missing-classification":
				frontier.FrontierEvents[0].SourceClassifications = nil
			case "extra-classification":
				frontier.FrontierEvents[0].SourceClassifications = []string{"start_first_turn", "pending"}
			case "duplicate":
				frontier.FrontierEvents = append(frontier.FrontierEvents, first)
			case "extra-first-turn":
				first.SourceEventID = uuid.NewString()
				frontier.FrontierEvents = append(frontier.FrontierEvents, first)
			case "omitted-first-turn":
				frontier.FrontierEvents = nil
			case "included-in-history":
				plan = plan.WithHistoricalEvents(1, []string{first.SourceEventID})
			}
			// Hostile inputs receive self-consistent hashes; those alone must not
			// confer original-start authority on a changed event or classification.
			count, ids, fingerprint, err := runfork.RunForkContractFrontierEvidenceBinding(frontier)
			if err != nil {
				t.Fatal(err)
			}
			frontier.FrontierEventCount = count
			topology := runfork.RunForkSelectedContractRouteTopology{
				Owner: runfork.RunForkSelectedContractRouteTopologyOwner, NonMutating: true,
				ContractSelection: frontier.ContractSelection, StaticTopologySupported: true, DynamicTopologySupported: true,
				FrontierEventCount: count, FrontierSourceEventIDs: ids, FrontierEvidenceFingerprint: fingerprint,
			}
			planning := runfork.RunForkSelectedContractRecipientPlanning{
				Owner: runfork.RunForkSelectedContractRecipientPlanningOwner, NonMutating: true, RecipientPlanningSupported: true,
				ContractSelection: frontier.ContractSelection, RouteTopologyOwner: topology.Owner, FrontierEvidenceFingerprint: fingerprint,
			}
			originalIDs, known := plan.HistoricalEventIDs(1)
			recovery, err := prepareRunForkSelectedContractRouteResolution(plan, uuid.NewString(), frontier.ContractSelection, frontier, topology, planning)
			if (err == nil) != valid {
				t.Fatalf("valid=%t recovery=%+v err=%v", valid, recovery, err)
			}
			currentIDs, stillKnown := plan.HistoricalEventIDs(1)
			if known != stillKnown || !reflect.DeepEqual(originalIDs, currentIDs) || len(plan.PendingWork) != 0 {
				t.Fatal("route resolution fabricated historical membership or deliveries")
			}
		})
	}
}
