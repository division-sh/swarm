package serveapp

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type lifecycleGateRestartReadback interface {
	ListWorkflowInstances(context.Context, string) ([]pipeline.WorkflowInstance, error)
	GetDecisionCard(context.Context, string) (decisioncard.Card, error)
	ListDecisionCards(context.Context, decisioncard.ListOptions) ([]decisioncard.ListItem, string, error)
	ListWorkflowTimerActivations(context.Context, string, string, bool) ([]pipeline.WorkflowTimerActivation, error)
}

type lifecycleGateRestartEvidence struct {
	Instance pipeline.WorkflowInstance
	Cards    map[string]decisioncard.Card
	Timers   []pipeline.WorkflowTimerActivation
}

func readLifecycleGateRestartEvidence(t *testing.T, reader lifecycleGateRestartReadback, run string) lifecycleGateRestartEvidence {
	t.Helper()
	if reader == nil {
		t.Fatal("restart evidence requires the original selected card/timer readers")
	}
	instances, err := reader.ListWorkflowInstances(t.Context(), run)
	if err != nil || len(instances) != 1 {
		t.Fatalf("restart instance inventory: got=%+v err=%v", instances, err)
	}
	rows, next, err := reader.ListDecisionCards(t.Context(), decisioncard.ListOptions{RunID: run, Limit: 100})
	if err != nil || next != "" {
		t.Fatalf("restart complete bounded card inventory: got=%+v next=%q err=%v", rows, next, err)
	}
	evidence := lifecycleGateRestartEvidence{Instance: instances[0], Cards: map[string]decisioncard.Card{}}
	for _, row := range rows {
		card, err := reader.GetDecisionCard(t.Context(), row.CardID)
		if err != nil || card.RunID != run || evidence.Cards[row.CardID].CardID != "" {
			t.Fatalf("restart exact native card: row=%+v card=%+v err=%v", row, card, err)
		}
		evidence.Cards[card.CardID] = card
	}
	evidence.Timers, err = reader.ListWorkflowTimerActivations(t.Context(), run, instances[0].EntityID, false)
	if err != nil {
		t.Fatal(err)
	}
	return evidence
}

func requireLifecycleGateRestartReplacement(t *testing.T, reader lifecycleGateRestartReadback, source semanticview.Source, bundleHash, run string, before lifecycleGateRestartEvidence) lifecycleGateRestartEvidence {
	t.Helper()
	after := readLifecycleGateRestartEvidence(t, reader, run)
	priorHistory, err := json.Marshal(before.Instance.TransitionHistory)
	if err != nil {
		t.Fatal(err)
	}
	currentHistory, err := json.Marshal(after.Instance.TransitionHistory)
	if err != nil || string(priorHistory) != string(currentHistory) {
		t.Fatalf("completed transition evidence bytes changed: before=%s after=%s err=%v", priorHistory, currentHistory, err)
	}
	var old, successor decisioncard.Card
	for id, card := range before.Cards {
		if card.Status == decisioncard.StatusPending {
			if old.CardID != "" {
				t.Fatalf("multiple predecessor waits: first=%+v next=%+v", old, card)
			}
			old = card
		}
		if after.Cards[id].CardID == "" {
			t.Fatalf("replacement lost immutable predecessor %s: got=%+v", id, after.Cards)
		}
	}
	for id, card := range after.Cards {
		if before.Cards[id].CardID == "" {
			successor = card
		}
	}
	if len(after.Cards) != len(before.Cards)+1 || old.CardID == "" || successor.CardID == "" || successor.Status != decisioncard.StatusPending || successor.BundleHash != bundleHash || successor.Fields.Len() != 0 || successor.Verdict != "" || successor.DecisionEventID != "" || successor.DecidedBy != "" || !successor.DecidedAt.IsZero() || !successor.DeferredUntil.IsZero() || !successor.CreatedAt.After(old.CreatedAt) {
		t.Fatalf("replacement must have one new empty current card: before=%+v after=%+v successor=%+v", before.Cards, after.Cards, successor)
	}
	for id, card := range before.Cards {
		actual := after.Cards[id]
		issuedBefore, err := decisioncard.SnapshotJSON(card)
		if err != nil {
			t.Fatal(err)
		}
		issuedAfter, err := decisioncard.SnapshotJSON(actual)
		if err != nil || string(issuedBefore) != string(issuedAfter) {
			t.Fatalf("%s.issued_offer: before=%s after=%s err=%v", id, issuedBefore, issuedAfter, err)
		}
		if id == old.CardID {
			card.Status, card.SupersededReason, card.UpdatedAt = decisioncard.StatusSuperseded, decisioncard.SupersededByUpdate, successor.CreatedAt
		}
		requireLifecycleGateRestartMembers(t, "card "+id, card, actual)
	}
	anchor, err := old.Anchor.StageGate()
	if err != nil {
		t.Fatal(err)
	}
	offer, err := decisioncard.CompileGateOffer(source, anchor.FlowID, anchor.Stage)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := offer.RoutesJSON()
	if err != nil {
		t.Fatal(err)
	}
	plan, found := source.WorkflowGateForStage(anchor.FlowID, anchor.Stage)
	if !found {
		t.Fatal("current admitted source lost its exact gate")
	}
	carrier, err := engine.StateCarrierFromPersisted(before.Instance.Fields, before.Instance.Bookkeeping, before.Instance.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	if persisted := carrier.PersistedStateBuckets(); !reflect.DeepEqual(before.Instance.StateBuckets, persisted) {
		t.Fatalf("pre-mutation state-bucket roundtrip changed evidence: before=%#v after=%#v", before.Instance.StateBuckets, persisted)
	}
	activations, err := gateruntime.List(carrier.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	var predecessor gateruntime.Activation
	for _, activation := range activations {
		if activation.CardID == old.CardID {
			predecessor = activation
		}
	}
	replacement, err := gateruntime.Replacement(predecessor, plan.Decision, bundleHash, routes, successor.CreatedAt)
	if err != nil || replacement.CardID != successor.CardID {
		t.Fatalf("independent successor identity: expected=%+v actual=%+v err=%v", replacement, successor, err)
	}
	issued, err := decisioncard.IssuedGateOffer(successor, replacement)
	if err != nil || !issued.Equal(offer) {
		t.Fatalf("successor differs from current admitted full offer: card=%+v err=%v", successor, err)
	}
	expected := before.Instance
	if !predecessor.Supersede(decisioncard.SupersededByUpdate, successor.CreatedAt) {
		t.Fatal("expected predecessor was not an open wait")
	}
	for _, activation := range []gateruntime.Activation{predecessor, replacement} {
		if err := gateruntime.Store(carrier.StateBuckets, activation); err != nil {
			t.Fatal(err)
		}
	}
	expected.StateBuckets = carrier.PersistedStateBuckets()
	expected.Revision++
	expected.UpdatedAt = successor.CreatedAt
	requireLifecycleGateRestartMembers(t, "instance", expected, after.Instance)
	if !reflect.DeepEqual(before.Timers, after.Timers) {
		t.Fatalf("stage timers changed: before=%+v after=%+v", before.Timers, after.Timers)
	}
	return after
}

func requireLifecycleGateRestartMembers(t *testing.T, label string, expected, actual any) {
	t.Helper()
	want, got := reflect.ValueOf(expected), reflect.ValueOf(actual)
	different := false
	for i := 0; i < want.NumField(); i++ {
		if !reflect.DeepEqual(want.Field(i).Interface(), got.Field(i).Interface()) {
			t.Errorf("%s.%s: expected=%#v actual=%#v", label, want.Type().Field(i).Name, want.Field(i).Interface(), got.Field(i).Interface())
			different = true
		}
	}
	if different {
		t.FailNow()
	}
}
