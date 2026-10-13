package runforkadmission

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

type fanOutRecipientPartitionSlot struct {
	route    events.DeliveryRoute
	selected forkrecipient.Evidence
}

func TestIssue642RequestFrontierUsesExactNativePartition(t *testing.T) {
	run := uuid.NewString()
	slots := []fanOutRecipientPartitionSlot{
		fanOutRecipientPartitionNode(t, run, ".", ".", "pending-observer"),
		fanOutRecipientPartitionNode(t, run, "provider", "provider", "settled-provider"),
	}
	plan, partition := fanOutRecipientPartitionPlan(t, run, slots, 1)
	plan.FanOutObligations = nil
	plan.ForkPoint.Revision = 5
	plan = plan.WithHistoricalEvents(5, []string{partition.SourceEventID})
	plan, err := plan.WithHistoricalRequestPublications(5, map[string][]string{partition.SourceEventID: partition.SourceDeliveryIDs})
	if err != nil {
		t.Fatal(err)
	}
	ids, present, err := plan.RequestPendingRecipientPartition(partition.SourceEventID)
	if err != nil || !present {
		t.Fatalf("native request partition missing: %v", err)
	}
	for _, fault := range []string{"exact", "missing", "duplicate", "wrong_handler"} {
		t.Run(fault, func(t *testing.T) {
			selected := []forkrecipient.Evidence{slots[0].selected, slots[1].selected}
			switch fault {
			case "missing":
				selected = selected[1:]
			case "duplicate":
				selected = append(selected, selected[0])
			case "wrong_handler":
				selected[0] = fanOutRecipientPartitionNode(t, run, ".", ".", "different-observer").selected
			}
			before := append([]forkrecipient.Evidence(nil), selected...)
			got, err := continuingPublicationRecipients(plan, partition.SourceEventID, ids, selected)
			if fault == "exact" {
				if err != nil || !reflect.DeepEqual(got, []forkrecipient.Evidence{slots[0].selected}) {
					t.Fatalf("settled provider returned to planning: %+v error=%v", got, err)
				}
			} else if err == nil {
				t.Fatal("inexact request recipient partition admitted")
			}
			if !reflect.DeepEqual(before, selected) {
				t.Fatal("recipient narrowing mutated its input")
			}
		})
	}
}

func fanOutRecipientPartitionNode(t *testing.T, runID, flow, path, name string) fanOutRecipientPartitionSlot {
	t.Helper()
	node := identitytest.FlowNode(t, flow, name)
	selected, err := forkrecipient.NewLocal(forkrecipient.Input{Recipient: events.MustNodeDeliveryRecipient(node),
		Path: path, HandlerNode: node, HandlerEvent: "work.ready"})
	if err != nil {
		t.Fatal(err)
	}
	instance := path
	if flow == "." && path == "." {
		instance = runID
	}
	return fanOutRecipientPartitionSlot{selected: selected, route: events.DeliveryRoute{Recipient: selected.Recipient,
		Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: flow, FlowInstance: instance})}}
}

func fanOutRecipientPartitionAgentPlan(t *testing.T, owner, scope, instance, path string) agentidentity.Plan {
	t.Helper()
	name, err := agentidentity.DeclaredName("worker", owner)
	if err != nil {
		t.Fatal(err)
	}
	route := agentidentity.RootRoute()
	if path != "." {
		route, err = agentidentity.PresentRoute(scope, instance, path)
		if err != nil {
			t.Fatal(err)
		}
	}
	plan, err := agentidentity.NewPlan(name, route)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func fanOutRecipientPartitionAgent(t *testing.T, runID string, plan agentidentity.Plan) fanOutRecipientPartitionSlot {
	t.Helper()
	path := plan.FlowInstance()
	if plan.Route.Presence == agentidentity.RouteRoot {
		path = "."
	}
	selected, err := forkrecipient.NewLocal(forkrecipient.Input{Recipient: events.MustAgentDeliveryRecipient(plan.AgentID()),
		Path: path, AgentPlan: plan, HandlerEvent: "work.ready"})
	if err != nil {
		t.Fatal(err)
	}
	live, err := plan.Live(runID)
	if err != nil {
		t.Fatal(err)
	}
	return fanOutRecipientPartitionSlot{selected: selected,
		route: events.DeliveryRoute{Recipient: selected.Recipient, AgentIdentity: live}}
}

// Every source ID is derived from the event and complete typed route. The
// carrier is admitted before it reaches the selected-recipient narrowing seam.
func fanOutRecipientPartitionPlan(t *testing.T, runID string, slots []fanOutRecipientPartitionSlot, unfinished int) (runfork.RunForkPlan, runfork.RunForkFanOutPendingReplay) {
	t.Helper()
	eventID := uuid.NewString()
	now := time.Unix(100, 0).UTC()
	replay := runfork.RunForkFanOutPendingReplay{Ordinal: 0, SourceEventID: eventID}
	if unfinished < len(slots) {
		replay.InheritedDisposition = fanoutobligation.InheritedDeadLettered
	}
	plan := runfork.RunForkPlan{SourceRunID: runID}
	states := []struct{ classification, status string }{
		{runfork.RunForkPendingClassificationPending, "pending"},
		{runfork.RunForkPendingClassificationInProgress, "in_progress"},
		{runfork.RunForkPendingClassificationFailedRetryable, "failed"},
	}
	terminal := []struct{ classification, status string }{
		{runfork.RunForkPendingClassificationDeliveredCompleted, "delivered"},
		{runfork.RunForkPendingClassificationDeadLetter, "dead_letter"},
		{runfork.RunForkPendingClassificationCanceled, "canceled"},
	}
	var routes []events.DeliveryRoute
	for index, slot := range slots {
		if err := slot.selected.Validate(); err != nil {
			t.Fatal(err)
		}
		id, err := deliverylifecycle.DeliveryID(eventID, slot.route)
		if err != nil {
			t.Fatal(err)
		}
		routes = append(routes, slot.route)
		pending := runfork.RunForkPendingWork{EventID: eventID, EventName: "work.ready", DeliveryID: id, DeliveryRoute: slot.route,
			SubscriberType: slot.route.Recipient.Code(), SubscriberID: slot.route.Recipient.ID(), CreatedAt: now}
		if index < unfinished {
			state := states[index%len(states)]
			pending.Classification, pending.Status = state.classification, state.status
			pending.ClaimVersion, pending.RetryCount, pending.ActiveSessionID, pending.StartedAt = 7, 2, uuid.NewString(), &now
			replay.SourceDeliveryIDs = append(replay.SourceDeliveryIDs, id)
		} else {
			state := terminal[(index-unfinished)%len(terminal)]
			pending.Classification, pending.Status, pending.DeliveredAt = state.classification, state.status, &now
		}
		plan.PendingWork = append(plan.PendingWork, pending)
	}
	if err := events.ValidateDeliveryRoutes(routes); err != nil {
		t.Fatalf("invalid historical route fixture: %v", err)
	}
	plan.PendingWork = append(plan.PendingWork, runfork.RunForkPendingWork{EventID: eventID, SubscriberType: "platform",
		SubscriberID: "pipeline", ReceiptOutcome: "success", ReceiptAt: &now})
	plan.FanOutObligations = []runfork.RunForkFanOutObligation{{
		Intent: fanoutobligation.Intent{Request: fanoutobligation.IntentRequest{
			Key: fanoutobligation.IntentKey{RunID: runID, TriggeringDeliveryID: uuid.NewString(),
				ElementRef: contracts.FanOutElementRef{FlowPath: ".", Family: "handler_rule", SemanticPath: `handlers["work.ready"].rules[0]`}},
		}, Cursor: 1}, PendingReplays: []runfork.RunForkFanOutPendingReplay{replay},
	}}
	admitted, present, err := plan.FanOutPendingReplay(eventID)
	if err != nil || !present || !reflect.DeepEqual(admitted, replay) {
		t.Fatalf("exact typed source partition not admitted: present=%t err=%v", present, err)
	}
	return plan, admitted
}

func fanOutRecipientPartitionFixture(t *testing.T) (runfork.RunForkPlan, runfork.RunForkFanOutPendingReplay, []forkrecipient.Evidence, []forkrecipient.Evidence) {
	t.Helper()
	runID := uuid.NewString()
	agent := fanOutRecipientPartitionAgentPlan(t, "fixture/alpha/worker", "alpha", uuid.NewString(), "alpha/one")
	slots := []fanOutRecipientPartitionSlot{
		fanOutRecipientPartitionNode(t, runID, "alpha", "alpha/one", "worker"),
		fanOutRecipientPartitionAgent(t, runID, agent),
		fanOutRecipientPartitionNode(t, runID, "alpha", "alpha/two", "worker"),
		fanOutRecipientPartitionNode(t, runID, "beta", "beta/one", "worker"),
		fanOutRecipientPartitionAgent(t, runID, fanOutRecipientPartitionAgentPlan(t, "fixture/alpha/worker", "alpha", uuid.NewString(), "alpha/two")),
	}
	plan, replay := fanOutRecipientPartitionPlan(t, runID, slots, 2)
	want := []forkrecipient.Evidence{slots[0].selected, slots[1].selected}
	selected := []forkrecipient.Evidence{slots[4].selected, slots[1].selected, slots[2].selected,
		fanOutRecipientPartitionNode(t, runID, "gamma", "gamma/one", "worker").selected,
		slots[3].selected, slots[0].selected,
		fanOutRecipientPartitionAgent(t, runID, fanOutRecipientPartitionAgentPlan(t, "fixture/gamma/worker", "gamma", uuid.NewString(), "gamma/one")).selected,
		slots[2].selected}
	return plan, replay, selected, want
}

func TestFanOutRecipientPartitionSelectsOnlyExactUnfinishedSlots(t *testing.T) {
	for _, order := range []string{"mixed", "reversed", "rotated"} {
		t.Run(order, func(t *testing.T) {
			plan, replay, selected, want := fanOutRecipientPartitionFixture(t)
			if order == "reversed" {
				for i, j := 0, len(selected)-1; i < j; i, j = i+1, j-1 {
					selected[i], selected[j] = selected[j], selected[i]
				}
			} else if order == "rotated" {
				selected = append(selected[3:], selected[:3]...)
			}
			beforePlan, beforeSelected := recipientAuthorityJSON(t, plan), recipientAuthorityJSON(t, selected)
			got, err := continuingFanOutRecipients(plan, replay, selected)
			if err != nil {
				t.Fatal(err)
			}
			want, err = forkrecipient.CanonicalSet(want)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("terminal/extra recipients entered the unfinished partition: got=%+v want=%+v", got, want)
			}
			if recipientAuthorityJSON(t, plan) != beforePlan || recipientAuthorityJSON(t, selected) != beforeSelected {
				t.Fatal("recipient narrowing mutated historical or selected evidence")
			}
		})
	}
}

func TestFanOutRecipientPartitionRejectsMissingMismatchedOrDuplicateSlots(t *testing.T) {
	for _, name := range []string{"empty", "missing_node", "missing_agent", "wrong_node_flow", "wrong_node_instance", "wrong_node_name",
		"wrong_agent_owner", "wrong_agent_name_source", "wrong_agent_scope", "wrong_agent_instance_id", "wrong_agent_instance_path",
		"duplicate_node", "duplicate_agent", "duplicate_node_handler", "terminal_only", "missing_source_row", "wrong_source_event"} {
		t.Run(name, func(t *testing.T) {
			plan, replay, all, want := fanOutRecipientPartitionFixture(t)
			selected := append([]forkrecipient.Evidence(nil), want...)
			switch name {
			case "empty":
				selected = nil
			case "missing_node":
				selected = selected[1:]
			case "missing_agent":
				selected = selected[:1]
			case "wrong_node_flow":
				selected[0] = fanOutRecipientPartitionNode(t, plan.SourceRunID, "beta", "alpha/one", "worker").selected
			case "wrong_node_instance":
				selected[0] = fanOutRecipientPartitionNode(t, plan.SourceRunID, "alpha", "alpha/two", "worker").selected
			case "wrong_node_name":
				selected[0] = fanOutRecipientPartitionNode(t, plan.SourceRunID, "alpha", "alpha/one", "other").selected
			case "wrong_agent_owner", "wrong_agent_name_source", "wrong_agent_scope", "wrong_agent_instance_id", "wrong_agent_instance_path":
				agent := want[1].AgentPlan
				switch name {
				case "wrong_agent_owner":
					agent.Name.Owner = "fixture/other/worker"
				case "wrong_agent_name_source":
					agent.Name.Source = agentidentity.NameSourceRuntimeCreated
				case "wrong_agent_scope":
					agent.Route.ScopeKey = "beta"
				case "wrong_agent_instance_id":
					agent.Route.InstanceID = uuid.NewString()
				case "wrong_agent_instance_path":
					agent.Route.InstancePath = "alpha/two"
				}
				selected[1] = fanOutRecipientPartitionAgent(t, plan.SourceRunID, agent).selected
			case "duplicate_node":
				selected = append(selected, want[0])
			case "duplicate_agent":
				selected = append(selected, want[1])
			case "duplicate_node_handler":
				other, err := forkrecipient.NewLocal(forkrecipient.Input{Recipient: want[0].Recipient, Path: want[0].Path,
					HandlerNode: want[0].HandlerNode(), HandlerEvent: "work.other"})
				if err != nil {
					t.Fatal(err)
				}
				selected = append(selected, other)
			case "terminal_only":
				selected = []forkrecipient.Evidence{all[0], all[2], all[4]}
			case "missing_source_row":
				plan.PendingWork = plan.PendingWork[1:]
			case "wrong_source_event":
				plan.PendingWork[0].EventID = uuid.NewString()
			}
			for _, recipient := range selected {
				if err := recipient.Validate(); err != nil {
					t.Fatalf("invalid selected negative control: %v", err)
				}
			}
			if got, err := continuingFanOutRecipients(plan, replay, selected); err == nil || len(got) != 0 {
				t.Fatalf("inexact chosen slots admitted: got=%+v err=%v", got, err)
			}
		})
	}
}

func TestFanOutRecipientPartitionSameNamedSlotsRemainDistinct(t *testing.T) {
	runID := uuid.NewString()
	agentPlan := fanOutRecipientPartitionAgentPlan(t, "fixture/alpha/worker", "alpha", uuid.NewString(), "alpha/one")
	otherOwner, otherInstance := agentPlan, agentPlan
	otherOwner.Name.Owner, otherInstance.Route.InstanceID = "fixture/other/worker", uuid.NewString()
	slots := []fanOutRecipientPartitionSlot{
		fanOutRecipientPartitionNode(t, runID, "alpha", "alpha/one", "worker"),
		fanOutRecipientPartitionNode(t, runID, "alpha", "alpha/two", "worker"),
		fanOutRecipientPartitionNode(t, runID, "beta", "beta/one", "worker"),
		fanOutRecipientPartitionAgent(t, runID, agentPlan),
		fanOutRecipientPartitionAgent(t, runID, otherOwner),
		fanOutRecipientPartitionAgent(t, runID, otherInstance),
		fanOutRecipientPartitionAgent(t, runID, fanOutRecipientPartitionAgentPlan(t, "fixture/alpha/worker", "alpha", uuid.NewString(), "alpha/two")),
		fanOutRecipientPartitionAgent(t, runID, fanOutRecipientPartitionAgentPlan(t, "fixture/beta/worker", "beta", uuid.NewString(), "beta/one")),
		fanOutRecipientPartitionNode(t, runID, ".", ".", "worker"),
		fanOutRecipientPartitionAgent(t, runID, fanOutRecipientPartitionAgentPlan(t, "fixture/root/worker", "", "", ".")),
	}
	plan, replay := fanOutRecipientPartitionPlan(t, runID, slots, len(slots))
	var selected []forkrecipient.Evidence
	for i, slot := range slots {
		selected = append(selected, slot.selected)
		for j, other := range slots {
			if got := matchesHistoricalRecipient(runID, slot.route, other.selected); got != (i == j) {
				t.Fatalf("same-named historical slot %d matched candidate %d: %t", i, j, got)
			}
		}
	}
	got, err := continuingFanOutRecipients(plan, replay, selected)
	if err != nil {
		t.Fatal(err)
	}
	want, err := forkrecipient.CanonicalSet(selected)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(slots) || !reflect.DeepEqual(got, want) {
		t.Fatalf("flow/instance/full-agent-plan slots collapsed: got=%+v want=%+v", got, want)
	}
}

func TestFanOutRecipientPartitionRequiresExplicitSourceDeliveryIDs(t *testing.T) {
	for _, name := range []string{"absent", "empty", "omitted", "extra", "duplicate"} {
		t.Run(name, func(t *testing.T) {
			plan, replay, _, _ := fanOutRecipientPartitionFixture(t)
			carrier := &plan.FanOutObligations[0].PendingReplays[0]
			switch name {
			case "absent":
				carrier.SourceDeliveryIDs = nil
			case "empty":
				carrier.SourceDeliveryIDs = []string{}
			case "omitted":
				carrier.SourceDeliveryIDs = carrier.SourceDeliveryIDs[:1]
			case "extra":
				carrier.SourceDeliveryIDs = append(carrier.SourceDeliveryIDs, uuid.NewString())
			case "duplicate":
				carrier.SourceDeliveryIDs = append(carrier.SourceDeliveryIDs, carrier.SourceDeliveryIDs[0])
			}
			if _, present, err := plan.FanOutPendingReplay(replay.SourceEventID); err == nil || present {
				t.Fatal("PendingWork substituted for an explicit exact source-ID carrier")
			}
		})
	}
}

func TestFanOutRecipientPartitionConnectHandlerIdentity(t *testing.T) {
	source := recipientAuthoritySource(t, []recipientAuthorityReceiver{{path: "consumer", node: "worker", pins: []string{"work.ready", "work.other"}}})
	runID := uuid.NewString()
	node := identitytest.FlowNode(t, "consumer", "worker")
	connected := func(event string) fanOutRecipientPartitionSlot {
		compiled := recipientAuthorityConnectPlan(t, source, "consumer", event)
		planID, err := pinrouting.ConnectPlanIdentity(compiled)
		if err != nil {
			t.Fatal(err)
		}
		selected, err := forkrecipient.NewConnect(forkrecipient.Input{Recipient: events.MustNodeDeliveryRecipient(node),
			Path: "consumer", HandlerNode: node, HandlerEvent: events.EventType(event)}, planID, compiled.ReceiverPinIdentity().EvidenceIdentity())
		if err != nil {
			t.Fatal(err)
		}
		blueprint := pinrouting.ConnectDeliveryRoute{Recipient: selected.Recipient, Target: compiled.ReceiverRoute("consumer", uuid.NewString()),
			Handler: pinrouting.MustConnectReceiverHandler(node)}
		claim, err := pinrouting.ConnectExecutionClaim(compiled, blueprint)
		if err != nil {
			t.Fatal(err)
		}
		return fanOutRecipientPartitionSlot{selected: selected, route: events.DeliveryRoute{Recipient: selected.Recipient,
			Target: events.MustExistingEntityTarget(blueprint.Target), ConnectClaim: claim}}
	}
	owed, other := connected("work.ready"), connected("work.other")
	plan, replay := fanOutRecipientPartitionPlan(t, runID, []fanOutRecipientPartitionSlot{owed}, 1)
	got, err := continuingFanOutRecipients(plan, replay, []forkrecipient.Evidence{other.selected, owed.selected})
	if err != nil || !reflect.DeepEqual(got, []forkrecipient.Evidence{owed.selected}) {
		t.Fatalf("exact compiled node handler not selected: got=%+v err=%v", got, err)
	}
	if got, err := continuingFanOutRecipients(plan, replay, []forkrecipient.Evidence{other.selected}); err == nil || len(got) != 0 {
		t.Fatalf("another compiled handler satisfied the unfinished slot: got=%+v err=%v", got, err)
	}
	local := fanOutRecipientPartitionNode(t, runID, "consumer", "consumer", "worker")
	plan, replay = fanOutRecipientPartitionPlan(t, runID, []fanOutRecipientPartitionSlot{local}, 1)
	if got, err := continuingFanOutRecipients(plan, replay, []forkrecipient.Evidence{owed.selected}); err == nil || len(got) != 0 {
		t.Fatalf("local historical slot borrowed connected selected authority: got=%+v err=%v", got, err)
	}
}

func TestFanOutRecipientPartitionRejectsOneCandidateForMultipleDeliveryIDs(t *testing.T) {
	runID := uuid.NewString()
	first := fanOutRecipientPartitionNode(t, runID, "alpha", "alpha/one", "worker")
	second := first
	first.route.Context.Reply = &events.ReplyContextRef{ID: uuid.NewString()}
	second.route.Context.Reply = &events.ReplyContextRef{ID: uuid.NewString()}
	plan, replay := fanOutRecipientPartitionPlan(t, runID, []fanOutRecipientPartitionSlot{first, second}, 2)
	if replay.SourceDeliveryIDs[0] == replay.SourceDeliveryIDs[1] {
		t.Fatal("distinct typed reply-context slots acquired the same delivery ID")
	}
	if got, err := continuingFanOutRecipients(plan, replay, []forkrecipient.Evidence{first.selected}); err == nil || len(got) != 0 {
		t.Fatalf("one selected candidate collapsed two owed delivery IDs: got=%+v err=%v", got, err)
	}
}
