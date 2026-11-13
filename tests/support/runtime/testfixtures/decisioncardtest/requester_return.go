package decisioncardtest

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/google/uuid"
)

func RequesterReturn(t testing.TB, original events.Event, requester string, delivery events.DeliveryContext) runtimeengine.EmitIntent {
	t.Helper()
	run := original.RunID()
	if run == "" {
		t.Fatal("requester-return fixture requires its selected run")
	}
	route := original.TargetRoute()
	if route.Empty() {
		route.EntityID = original.EntityID()
	}
	var source events.RoutingSource
	var err error
	if route.FlowID == "" && route.FlowInstance == "" {
		source, err = events.NewRootRoutingSource(route.EntityID)
	} else {
		source, err = events.NewStaticFlowRoutingSource(route)
	}
	if err != nil {
		t.Fatal(err)
	}
	parent := original.ID()
	if parent == "" {
		parent = uuid.NewString()
	}
	at := decisioncard.CanonicalTimestamp(original.CreatedAt())
	if at.IsZero() {
		at = decisioncard.CanonicalTimestamp(time.Now().UTC())
	}
	anchor, err := decisioncard.NewHumanTaskAnchor(decisioncard.HumanTaskAnchor{
		RequesterAgentID: requester, OperationID: HumanOperation(t, run, parent), Category: "review",
		Scope: decisioncard.Scope{Kind: decisioncard.ScopeGlobal}, Source: source,
	})
	if err != nil {
		t.Fatal(err)
	}
	card := decisioncard.Card{CardID: uuid.NewString(), RunID: run, Anchor: anchor,
		Status: decisioncard.StatusDecided, Verdict: "approve", DecisionEventID: parent}
	continuation := decisioncard.HumanTaskContinuation{CardID: card.CardID, RunID: run,
		RequesterRoute: route, ReplyContextID: delivery.ReplyContextID(), SourceEventID: parent,
		DeadlineAt: at.Add(time.Hour), CreatedAt: at, UpdatedAt: at, BudgetBundleHash: "fixture-bundle",
		BudgetWindowStart: at, BudgetWindowEnd: at.Add(time.Hour),
		State: decisioncard.HumanTaskContinuationDecisionCommitted, OutcomeEventID: parent}
	payload, err := canonicaljson.Bytes(map[string]any{"card_id": card.CardID, "requester_agent_id": requester,
		"status": "approved", "fields": map[string]any{}, "decided_by": "fixture", "decided_at": at.Format(time.RFC3339Nano)})
	if err != nil {
		t.Fatal(err)
	}
	product := eventtest.ChildForProducerWithRoutingSource(decisioncard.HumanTaskOutcomeEventID(card.CardID, parent), "human_task.approved",
		eventtest.Producer(events.EventProducerPlatform, "workflow-runtime"), original.TaskID(), payload, original.ChainDepth()+1,
		events.EventLineage{RunID: run, ParentEventID: parent, ExecutionMode: original.ExecutionMode()},
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, route), source, at)
	binding, err := decisioncard.BindHumanTaskReturn(card, continuation, product)
	if err != nil {
		t.Fatal(err)
	}
	return runtimeengine.EmitIntent{Event: product, Context: binding.Context(), RequesterReturn: binding}
}
