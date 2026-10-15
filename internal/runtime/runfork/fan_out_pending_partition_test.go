package runfork

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/google/uuid"
)

// This fixture isolates fixed-cut admission from selected execution authority.
func fanOutPendingPartitionFixture(t testing.TB) RunForkPlan {
	t.Helper()
	runID, eventID := uuid.NewString(), "abcdef01-2345-4678-9abc-def012345678"
	now := time.Unix(100, 0).UTC()
	name, err := agentidentity.DeclaredName("worker", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	agent, err := agentidentity.New(runID, name, agentidentity.RootRoute())
	if err != nil {
		t.Fatal(err)
	}
	nodeRoute := func(name string) events.DeliveryRoute {
		node, err := identity.AdmitExecutableNodeDeclaration(".", name)
		if err != nil {
			t.Fatal(err)
		}
		return events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node),
			Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID})}
	}
	plan := RunForkPlan{SourceRunID: runID, FanOutObligations: []RunForkFanOutObligation{{
		Intent: fanoutobligation.Intent{Request: fanoutobligation.IntentRequest{
			Key: fanoutobligation.IntentKey{RunID: runID, TriggeringDeliveryID: uuid.NewString()},
		}, Cursor: 1},
		PendingReplays: []RunForkFanOutPendingReplay{{Ordinal: 0, SourceEventID: eventID,
			InheritedDisposition: fanoutobligation.InheritedDeadLettered}},
	}}}
	routes := []events.DeliveryRoute{
		nodeRoute("pending"),
		{Recipient: events.MustAgentDeliveryRecipient(agent.AgentID()), AgentIdentity: agent},
		nodeRoute("succeeded"), nodeRoute("dead-lettered"), nodeRoute("canceled"),
	}
	statuses := []string{"pending", "pending", "delivered", "dead_letter", "canceled"}
	classifications := []string{RunForkPendingClassificationPending, RunForkPendingClassificationPending,
		RunForkPendingClassificationDeliveredCompleted, RunForkPendingClassificationDeadLetter, RunForkPendingClassificationCanceled}
	for index, route := range routes {
		deliveryID, err := deliverylifecycle.DeliveryID(eventID, route)
		if err != nil {
			t.Fatal(err)
		}
		pending := RunForkPendingWork{EventID: eventID, DeliveryID: deliveryID, DeliveryRoute: route,
			SubscriberType: route.Recipient.Code(), SubscriberID: route.Recipient.ID(),
			Status: statuses[index], Classification: classifications[index], CreatedAt: now}
		if index < 2 {
			// Historical attempt coordinates are deliberately non-fresh lineage.
			pending.ClaimVersion, pending.RetryCount = 7, 2
			pending.ActiveSessionID, pending.StartedAt = uuid.NewString(), &now
			plan.FanOutObligations[0].PendingReplays[0].SourceDeliveryIDs = append(
				plan.FanOutObligations[0].PendingReplays[0].SourceDeliveryIDs, deliveryID)
		} else {
			pending.DeliveredAt = &now
		}
		plan.PendingWork = append(plan.PendingWork, pending)
	}
	for _, outcome := range []string{"success", "dead_letter"} {
		plan.PendingWork = append(plan.PendingWork, RunForkPendingWork{EventID: eventID, SubscriberType: "platform",
			SubscriberID: "pipeline", ReceiptOutcome: outcome, ReceiptAt: &now})
	}
	return plan
}

func TestFanOutPendingPartitionAdmission(t *testing.T) {
	states := []struct{ classification, status string }{
		{RunForkPendingClassificationPending, "pending"},
		{RunForkPendingClassificationInProgress, "in_progress"},
		{RunForkPendingClassificationFailedRetryable, "failed"},
	}
	for _, nodeState := range states {
		for _, agentState := range states {
			for _, retained := range []fanoutobligation.InheritedTerminalDisposition{"", fanoutobligation.InheritedSucceeded,
				fanoutobligation.InheritedDeadLettered, fanoutobligation.InheritedCanceled} {
				t.Run(nodeState.classification+"/"+agentState.classification+"/"+string(retained), func(t *testing.T) {
					plan := fanOutPendingPartitionFixture(t)
					plan.PendingWork[0].Classification, plan.PendingWork[0].Status = nodeState.classification, nodeState.status
					plan.PendingWork[1].Classification, plan.PendingWork[1].Status = agentState.classification, agentState.status
					plan.FanOutObligations[0].PendingReplays[0].InheritedDisposition = retained
					if err := ValidateFanOutPendingReplayAdmission(plan); err != nil {
						t.Fatalf("exact unfinished partition with terminal rows/receipts rejected: %v", err)
					}
				})
			}
		}
	}
}

func TestFanOutPendingPartitionAdmissionRejectsInexactEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*RunForkPlan)
	}{
		{"absent_source_ids", func(p *RunForkPlan) { p.FanOutObligations[0].PendingReplays[0].SourceDeliveryIDs = nil }},
		{"empty_source_ids", func(p *RunForkPlan) { p.FanOutObligations[0].PendingReplays[0].SourceDeliveryIDs = []string{} }},
		{"omitted_delivery", func(p *RunForkPlan) {
			p.FanOutObligations[0].PendingReplays[0].SourceDeliveryIDs = p.FanOutObligations[0].PendingReplays[0].SourceDeliveryIDs[:1]
		}},
		{"extra_delivery", func(p *RunForkPlan) {
			r := &p.FanOutObligations[0].PendingReplays[0]
			r.SourceDeliveryIDs = append(r.SourceDeliveryIDs, uuid.NewString())
		}},
		{"missing_fixed_row", func(p *RunForkPlan) { p.PendingWork = p.PendingWork[1:] }},
		{"wrong_event", func(p *RunForkPlan) { p.PendingWork[0].EventID = uuid.NewString() }},
		{"wrong_carrier_event", func(p *RunForkPlan) { p.FanOutObligations[0].PendingReplays[0].SourceEventID = uuid.NewString() }},
		{"terminal_success", func(p *RunForkPlan) {
			r := &p.FanOutObligations[0].PendingReplays[0]
			r.SourceDeliveryIDs = append(r.SourceDeliveryIDs, p.PendingWork[2].DeliveryID)
		}},
		{"terminal_dead_letter", func(p *RunForkPlan) {
			r := &p.FanOutObligations[0].PendingReplays[0]
			r.SourceDeliveryIDs = append(r.SourceDeliveryIDs, p.PendingWork[3].DeliveryID)
		}},
		{"terminal_canceled", func(p *RunForkPlan) {
			r := &p.FanOutObligations[0].PendingReplays[0]
			r.SourceDeliveryIDs = append(r.SourceDeliveryIDs, p.PendingWork[4].DeliveryID)
		}},
		{"duplicate_source_id", func(p *RunForkPlan) {
			r := &p.FanOutObligations[0].PendingReplays[0]
			r.SourceDeliveryIDs = append(r.SourceDeliveryIDs, r.SourceDeliveryIDs[0])
		}},
		{"duplicate_fixed_row", func(p *RunForkPlan) { p.PendingWork = append(p.PendingWork, p.PendingWork[0]) }},
		{"duplicate_id_wrong_event", func(p *RunForkPlan) {
			other := p.PendingWork[0]
			other.EventID = uuid.NewString()
			p.PendingWork = append(p.PendingWork, other)
		}},
		{"multiple_ordinals", func(p *RunForkPlan) {
			o := &p.FanOutObligations[0]
			o.Intent.Cursor = 2
			other := o.PendingReplays[0]
			other.Ordinal = 1
			o.PendingReplays = append(o.PendingReplays, other)
		}},
		{"multiple_handler_intents", func(p *RunForkPlan) {
			other := p.FanOutObligations[0]
			other.Intent.Request.Key.TriggeringDeliveryID = uuid.NewString()
			p.FanOutObligations = append(p.FanOutObligations, other)
		}},
		{"missing_prefix", func(p *RunForkPlan) { p.FanOutObligations[0].Intent.Cursor = 2 }},
		{"outside_cursor", func(p *RunForkPlan) { p.FanOutObligations[0].PendingReplays[0].Ordinal = 1 }},
		{"foreign_intent_run", func(p *RunForkPlan) { p.FanOutObligations[0].Intent.Request.Key.RunID = uuid.NewString() }},
		{"missing_source_run", func(p *RunForkPlan) { p.SourceRunID = "" }},
		{"event_whitespace", func(p *RunForkPlan) { p.FanOutObligations[0].PendingReplays[0].SourceEventID += " " }},
		{"event_urn", func(p *RunForkPlan) {
			p.FanOutObligations[0].PendingReplays[0].SourceEventID = "urn:uuid:" + p.PendingWork[0].EventID
		}},
		{"event_uppercase", func(p *RunForkPlan) {
			p.FanOutObligations[0].PendingReplays[0].SourceEventID = strings.ToUpper(p.PendingWork[0].EventID)
		}},
		{"event_nil", func(p *RunForkPlan) { p.FanOutObligations[0].PendingReplays[0].SourceEventID = uuid.Nil.String() }},
		{"delivery_whitespace", func(p *RunForkPlan) { p.FanOutObligations[0].PendingReplays[0].SourceDeliveryIDs[0] += " " }},
		{"delivery_urn", func(p *RunForkPlan) {
			p.FanOutObligations[0].PendingReplays[0].SourceDeliveryIDs[0] = "urn:uuid:" + p.PendingWork[0].DeliveryID
		}},
		{"delivery_uppercase", func(p *RunForkPlan) {
			p.FanOutObligations[0].PendingReplays[0].SourceDeliveryIDs[0] = "ABCDEF01-2345-4678-9ABC-DEF012345678"
		}},
		{"delivery_nil", func(p *RunForkPlan) {
			p.FanOutObligations[0].PendingReplays[0].SourceDeliveryIDs[0] = uuid.Nil.String()
		}},
		{"fixed_event_whitespace", func(p *RunForkPlan) { p.PendingWork[0].EventID += " " }},
		{"fixed_delivery_whitespace", func(p *RunForkPlan) { p.PendingWork[0].DeliveryID += " " }},
		{"retained_no_route", func(p *RunForkPlan) {
			p.FanOutObligations[0].PendingReplays[0].InheritedDisposition = fanoutobligation.InheritedNoRoute
		}},
		{"retained_unknown", func(p *RunForkPlan) { p.FanOutObligations[0].PendingReplays[0].InheritedDisposition = "failure" }},
		{"retained_whitespace", func(p *RunForkPlan) { p.FanOutObligations[0].PendingReplays[0].InheritedDisposition = "succeeded " }},
		{"classification_whitespace", func(p *RunForkPlan) { p.PendingWork[0].Classification += " " }},
		{"status_whitespace", func(p *RunForkPlan) { p.PendingWork[0].Status += " " }},
		{"pending_status_mismatch", func(p *RunForkPlan) { p.PendingWork[0].Status = "in_progress" }},
		{"in_progress_status_mismatch", func(p *RunForkPlan) { p.PendingWork[0].Classification = RunForkPendingClassificationInProgress }},
		{"failed_status_mismatch", func(p *RunForkPlan) { p.PendingWork[0].Classification = RunForkPendingClassificationFailedRetryable }},
		{"missing_typed_route", func(p *RunForkPlan) { p.PendingWork[0].DeliveryRoute = events.DeliveryRoute{} }},
		{"missing_node_target", func(p *RunForkPlan) { p.PendingWork[0].DeliveryRoute.Target = events.DeliveryTargetOwnership{} }},
		{"missing_agent_identity", func(p *RunForkPlan) { p.PendingWork[1].DeliveryRoute.AgentIdentity = agentidentity.Identity{} }},
		{"subscriber_type_mismatch", func(p *RunForkPlan) { p.PendingWork[0].SubscriberType = "agent" }},
		{"subscriber_type_whitespace", func(p *RunForkPlan) { p.PendingWork[0].SubscriberType += " " }},
		{"subscriber_id_mismatch", func(p *RunForkPlan) { p.PendingWork[0].SubscriberID = "other" }},
		{"subscriber_id_whitespace", func(p *RunForkPlan) { p.PendingWork[0].SubscriberID += " " }},
		{"route_id_mismatch", func(p *RunForkPlan) {
			p.PendingWork[0].DeliveryRoute.Target = events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: ".", FlowInstance: uuid.NewString()})
		}},
		{"foreign_agent_run", func(p *RunForkPlan) {
			p.PendingWork[1].DeliveryRoute.AgentIdentity.RunID = uuid.NewString()
			id, err := deliverylifecycle.DeliveryID(p.PendingWork[1].EventID, p.PendingWork[1].DeliveryRoute)
			if err != nil {
				panic(err)
			}
			p.PendingWork[1].DeliveryID = id
			p.FanOutObligations[0].PendingReplays[0].SourceDeliveryIDs[1] = id
		}},
		{"foreign_route_event_id", func(p *RunForkPlan) {
			id, err := deliverylifecycle.DeliveryID(uuid.NewString(), p.PendingWork[0].DeliveryRoute)
			if err != nil {
				panic(err)
			}
			p.PendingWork[0].DeliveryID = id
			p.FanOutObligations[0].PendingReplays[0].SourceDeliveryIDs[0] = id
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := fanOutPendingPartitionFixture(t)
			tc.mutate(&plan)
			if err := ValidateFanOutPendingReplayAdmission(plan); err == nil {
				t.Fatal("inexact unfinished partition admitted")
			}
		})
	}
}

func TestFanOutPendingPartitionDoesNotBroadenGenericReplay(t *testing.T) {
	plan := fanOutPendingPartitionFixture(t)
	for index := 0; index < 2; index++ {
		pending := plan.PendingWork[index]
		pending.ClaimVersion, pending.RetryCount, pending.ActiveSessionID, pending.StartedAt = 0, 0, "", nil
		if got := RunForkPendingWorkReplayableForHistoricalReplay(pending); got != (index == 1) {
			t.Fatalf("generic replay taxonomy changed for %s: %t", pending.SubscriberType, got)
		}
		if RunForkPendingWorkReplayableForHistoricalReplay(plan.PendingWork[index]) {
			t.Fatal("historical attempts acquired generic replay permission")
		}
	}
	if err := ValidateFanOutPendingReplayAdmission(plan); err != nil {
		t.Fatalf("bounded handler relation refused: %v", err)
	}
}

func TestFanOutPendingPartitionDistinctHandlerEvents(t *testing.T) {
	plan := fanOutPendingPartitionFixture(t)
	other := plan.FanOutObligations[0]
	other.Intent.Request.Key.TriggeringDeliveryID = uuid.NewString()
	eventID := uuid.NewString()
	other.PendingReplays = []RunForkFanOutPendingReplay{{Ordinal: 0, SourceEventID: eventID}}
	for index := 0; index < 2; index++ {
		pending := plan.PendingWork[index]
		pending.EventID = eventID
		id, err := deliverylifecycle.DeliveryID(eventID, pending.DeliveryRoute)
		if err != nil {
			t.Fatal(err)
		}
		pending.DeliveryID = id
		plan.PendingWork = append(plan.PendingWork, pending)
		other.PendingReplays[0].SourceDeliveryIDs = append(other.PendingReplays[0].SourceDeliveryIDs, id)
	}
	plan.FanOutObligations = append(plan.FanOutObligations, other)
	if err := ValidateFanOutPendingReplayAdmission(plan); err != nil {
		t.Fatalf("distinct exact handler publications rejected: %v", err)
	}
	if got, present, err := plan.FanOutPendingReplay(eventID); err != nil || !present || !reflect.DeepEqual(got, other.PendingReplays[0]) {
		t.Fatalf("lookup confused distinct handler publications: %+v %t %v", got, present, err)
	}
	work := fanOutPendingPartitionExecution(plan)
	if err := ValidateFanOutPendingReplayExecution(plan, work); err != nil {
		t.Fatalf("distinct exact execution pairs rejected: %v", err)
	}
	work[0].SourceEventID, work[2].SourceEventID = work[2].SourceEventID, work[0].SourceEventID
	if err := ValidateFanOutPendingReplayExecution(plan, work); err == nil {
		t.Fatal("crossed handler source event/delivery pairs admitted")
	}
}

func fanOutPendingPartitionExecution(plan RunForkPlan) []RunForkHistoricalReplayExecutableWork {
	var work []RunForkHistoricalReplayExecutableWork
	for _, obligation := range plan.FanOutObligations {
		for _, replay := range obligation.PendingReplays {
			for _, id := range replay.SourceDeliveryIDs {
				for _, fixed := range plan.PendingWork {
					if fixed.DeliveryID == id {
						work = append(work, RunForkHistoricalReplayExecutableWork{Fact: RunForkHistoricalReplayFactEventDeliveries,
							SourceEventID: replay.SourceEventID, SourceDeliveryID: id, SubscriberType: fixed.SubscriberType,
							SubscriberID: fixed.SubscriberID, Classification: fixed.Classification, ReasonCode: fixed.ReasonCode})
					}
				}
			}
		}
	}
	return work
}

func TestFanOutPendingPartitionExecution(t *testing.T) {
	for _, tc := range []struct {
		name   string
		valid  bool
		mutate func(RunForkPlan, []RunForkHistoricalReplayExecutableWork) []RunForkHistoricalReplayExecutableWork
	}{
		{"exact", true, func(_ RunForkPlan, w []RunForkHistoricalReplayExecutableWork) []RunForkHistoricalReplayExecutableWork {
			return w
		}},
		{"unrelated_work", true, func(_ RunForkPlan, w []RunForkHistoricalReplayExecutableWork) []RunForkHistoricalReplayExecutableWork {
			return append(w, RunForkHistoricalReplayExecutableWork{SourceEventID: uuid.NewString(), SourceDeliveryID: uuid.NewString()})
		}},
		{"omitted", false, func(_ RunForkPlan, w []RunForkHistoricalReplayExecutableWork) []RunForkHistoricalReplayExecutableWork {
			return w[:1]
		}},
		{"duplicate", false, func(_ RunForkPlan, w []RunForkHistoricalReplayExecutableWork) []RunForkHistoricalReplayExecutableWork {
			return append(w, w[0])
		}},
		{"extra", false, func(p RunForkPlan, w []RunForkHistoricalReplayExecutableWork) []RunForkHistoricalReplayExecutableWork {
			return append(w, RunForkHistoricalReplayExecutableWork{SourceEventID: p.PendingWork[0].EventID, SourceDeliveryID: uuid.NewString()})
		}},
		{"terminal", false, func(p RunForkPlan, w []RunForkHistoricalReplayExecutableWork) []RunForkHistoricalReplayExecutableWork {
			return append(w, RunForkHistoricalReplayExecutableWork{SourceEventID: p.PendingWork[2].EventID, SourceDeliveryID: p.PendingWork[2].DeliveryID})
		}},
		{"wrong_event", false, func(_ RunForkPlan, w []RunForkHistoricalReplayExecutableWork) []RunForkHistoricalReplayExecutableWork {
			w[0].SourceEventID = uuid.NewString()
			return w
		}},
		{"event_whitespace", false, func(_ RunForkPlan, w []RunForkHistoricalReplayExecutableWork) []RunForkHistoricalReplayExecutableWork {
			w[0].SourceEventID += " "
			return w
		}},
		{"delivery_whitespace", false, func(_ RunForkPlan, w []RunForkHistoricalReplayExecutableWork) []RunForkHistoricalReplayExecutableWork {
			w[0].SourceDeliveryID += " "
			return w
		}},
		{"duplicate_unrelated", false, func(_ RunForkPlan, w []RunForkHistoricalReplayExecutableWork) []RunForkHistoricalReplayExecutableWork {
			other := RunForkHistoricalReplayExecutableWork{SourceEventID: uuid.NewString(), SourceDeliveryID: uuid.NewString()}
			return append(w, other, other)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := fanOutPendingPartitionFixture(t)
			work := tc.mutate(plan, fanOutPendingPartitionExecution(plan))
			if err := ValidateFanOutPendingReplayExecution(plan, work); (err == nil) != tc.valid {
				t.Fatalf("execution validity=%t: %v", tc.valid, err)
			}
		})
	}
	plan := fanOutPendingPartitionFixture(t)
	plan.FanOutObligations[0].PendingReplays[0].SourceDeliveryIDs = nil
	if err := ValidateFanOutPendingReplayExecution(plan, nil); err == nil {
		t.Fatal("execution bypassed full admission")
	}
}

func TestFanOutPendingPartitionLookup(t *testing.T) {
	plan := fanOutPendingPartitionFixture(t)
	want := plan.FanOutObligations[0].PendingReplays[0]
	firstDeliveryID := want.SourceDeliveryIDs[0]
	got, present, err := plan.FanOutPendingReplay(want.SourceEventID)
	if err != nil || !present || !reflect.DeepEqual(got, want) {
		t.Fatalf("exact lookup: got=%+v present=%t err=%v", got, present, err)
	}
	got.SourceDeliveryIDs[0] = uuid.NewString()
	if plan.FanOutObligations[0].PendingReplays[0].SourceDeliveryIDs[0] != firstDeliveryID {
		t.Fatal("lookup exposed mutable plan-owned delivery IDs")
	}
	if got, present, err := plan.FanOutPendingReplay(uuid.NewString()); err != nil || present || !reflect.DeepEqual(got, RunForkFanOutPendingReplay{}) {
		t.Fatalf("absent event invented a relation: %+v %t %v", got, present, err)
	}
	for _, eventID := range []string{"", want.SourceEventID + " ", "urn:uuid:" + want.SourceEventID, uuid.Nil.String()} {
		if _, present, err := plan.FanOutPendingReplay(eventID); err == nil || present {
			t.Fatalf("noncanonical lookup silently accepted: %q", eventID)
		}
	}
	other := plan.FanOutObligations[0]
	other.Intent.Request.Key.TriggeringDeliveryID = uuid.NewString()
	plan.FanOutObligations = append(plan.FanOutObligations, other)
	for _, eventID := range []string{want.SourceEventID, uuid.NewString()} {
		if _, present, err := plan.FanOutPendingReplay(eventID); err == nil || present {
			t.Fatal("lookup swallowed invalid full-plan admission")
		}
	}
}

func TestFanOutPendingPartitionRejectsMalformedOutcomeShape(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*fanoutobligation.Outcome)
	}{
		{"unknown_kind", func(o *fanoutobligation.Outcome) { o.Kind = "unknown" }},
		{"committed_both_identities", func(o *fanoutobligation.Outcome) {
			o.SourceEventID = uuid.NewString()
			o.InheritedDisposition = fanoutobligation.InheritedSucceeded
		}},
		{"committed_no_event", func(o *fanoutobligation.Outcome) { o.EventID = "" }},
		{"owned_with_retained", func(o *fanoutobligation.Outcome) { o.InheritedDisposition = fanoutobligation.InheritedSucceeded }},
		{"missing_created_at", func(o *fanoutobligation.Outcome) { o.CreatedAt = time.Time{} }},
		{"continued_same_event", func(o *fanoutobligation.Outcome) {
			o.Kind = fanoutobligation.OutcomeContinued
			o.SourceEventID = o.EventID
		}},
		{"continued_no_route", func(o *fanoutobligation.Outcome) {
			o.Kind, o.SourceEventID, o.InheritedDisposition = fanoutobligation.OutcomeContinued, uuid.NewString(), fanoutobligation.InheritedNoRoute
		}},
		{"committed_failure", func(o *fanoutobligation.Outcome) { o.Failure = json.RawMessage(`{}`) }},
		{"rejected_untyped_failure", func(o *fanoutobligation.Outcome) {
			o.Kind, o.EventID, o.Failure = fanoutobligation.OutcomeSemanticRejected, "", json.RawMessage(`{}`)
		}},
		{"duplicate_pending_ordinal", func(o *fanoutobligation.Outcome) { o.Ordinal = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := fanOutPendingPartitionFixture(t)
			o := fanoutobligation.Outcome{Ordinal: 1, Kind: fanoutobligation.OutcomeCommitted, EventID: uuid.NewString(), CreatedAt: time.Unix(100, 0).UTC()}
			plan.FanOutObligations[0].Intent.Cursor = 2
			plan.FanOutObligations[0].Outcomes = []fanoutobligation.Outcome{o}
			if err := ValidateFanOutPendingReplayAdmission(plan); err != nil {
				t.Fatalf("valid sibling outcome rejected: %v", err)
			}
			tc.mutate(&plan.FanOutObligations[0].Outcomes[0])
			if err := ValidateFanOutPendingReplayAdmission(plan); err == nil {
				t.Fatal("malformed outcome shape admitted")
			}
			if _, present, err := plan.FanOutPendingReplay(plan.PendingWork[0].EventID); err == nil || present {
				t.Fatal("lookup swallowed malformed sibling outcome")
			}
		})
	}
}

func TestFanOutPendingPartitionDeploymentRetainedAdmission(t *testing.T) {
	for _, phase := range []RunForkDeploymentPendingPhase{RunForkDeploymentPendingPublication, RunForkDeploymentPendingReceiver} {
		for _, disposition := range []fanoutobligation.InheritedTerminalDisposition{"", fanoutobligation.InheritedSucceeded,
			fanoutobligation.InheritedDeadLettered, fanoutobligation.InheritedCanceled, fanoutobligation.InheritedNoRoute, "failure", "succeeded "} {
			t.Run(string(phase)+"/"+string(disposition), func(t *testing.T) {
				eventID, deliveryID := uuid.NewString(), uuid.NewString()
				source, err := events.NewDeploymentFeedRoutingSource(".")
				if err != nil {
					t.Fatal(err)
				}
				now := time.Unix(100, 0).UTC()
				pending := RunForkPendingWork{EventID: eventID, DeliveryID: deliveryID, SubscriberType: "node", SubscriberID: "receiver",
					RoutingSource: source, Classification: RunForkPendingClassificationPending, Status: "pending"}
				plan := RunForkPlan{PendingWork: []RunForkPendingWork{pending}, FanOutObligations: []RunForkFanOutObligation{{
					Intent: fanoutobligation.Intent{Request: fanoutobligation.IntentRequest{Deployment: &fanoutobligation.DeploymentOrigin{}}, Cursor: 1},
					PendingDeployment: []RunForkDeploymentPendingEvent{{Ordinal: 0, SourceEventID: eventID,
						SourceDeliveryIDs: []string{deliveryID}, Phase: phase, InheritedDisposition: disposition}},
				}}}
				if phase == RunForkDeploymentPendingReceiver {
					plan.PendingWork = append(plan.PendingWork, RunForkPendingWork{EventID: eventID, SubscriberType: "platform",
						SubscriberID: "pipeline", ReceiptOutcome: "success", ReceiptAt: &now})
				}
				valid := disposition == "" || disposition == fanoutobligation.InheritedSucceeded || disposition == fanoutobligation.InheritedDeadLettered || disposition == fanoutobligation.InheritedCanceled
				if err := ValidateFanOutPendingReplayAdmission(plan); (err == nil) != valid {
					t.Fatalf("deployment retained validity=%t: %v", valid, err)
				}
				if !valid {
					return
				}
				if _, present, err := plan.FanOutPendingReplay(eventID); err != nil || present {
					t.Fatalf("deployment borrowed handler-only lookup: %t %v", present, err)
				}
				plan.PendingWork[0].Classification, plan.PendingWork[0].Status = RunForkPendingClassificationInProgress, "in_progress"
				if err := ValidateFanOutPendingReplayAdmission(plan); err == nil {
					t.Fatal("retained contribution weakened deployment phase/status admission")
				}
			})
		}
	}
}
