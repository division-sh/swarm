package channeldelivery

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/google/uuid"
)

func TestChannelRenderFreezesFullOrderedCardWithoutPrivateAnswerEcho(t *testing.T) {
	principalID := uuid.NewString()
	audience := Audience{
		PrincipalID: principalID, InterfaceKey: "mock-channel", DeliveryEpoch: 1,
		ExternalAccountRef: "account", ConversationRef: "group", ConversationScope: operatorchannel.ConversationScopeShared,
	}
	entityID := uuid.NewString()
	source, err := events.NewRootRoutingSource(entityID)
	if err != nil {
		t.Fatal(err)
	}
	anchor, err := decisioncard.NewStageGateAnchor(decisioncard.StageGateAnchor{
		Route: runtimeflowidentity.RouteForInstancePath("root"), FlowID: ".", EntityID: entityID,
		Stage: "awaiting_review", StageActivationID: uuid.NewString(), Source: source,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := decisioncard.FreezeSnapshot("launch_review", "Launch review", map[string]any{"summary": "ready"}, map[string]runtimecontracts.WorkflowGateOutcomePlan{
		"accept": {Verdict: "accept"},
		"revise": {Verdict: "revise", Input: map[string]runtimecontracts.WorkflowGateInputField{
			"zeta": {Type: "text", Required: true}, "alpha": {Type: "text", Required: true},
		}, InputOrder: []string{"zeta", "alpha"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	card, err := decisioncard.New(decisioncard.Card{
		CardID: uuid.NewString(), RunID: uuid.NewString(), Anchor: anchor, Snapshot: snapshot,
		ExecutionMode: "live", BundleHash: "sha256:example", WorkflowVersion: "1", CreatedAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := FreezeCard(card, 17, "", audience)
	if err != nil {
		t.Fatal(err)
	}
	if frozen.Hash != canonicaljson.HashBytes(frozen.Input) || frozen.Revision != 17 ||
		!strings.Contains(frozen.FullText, "Gate: . / awaiting_review") ||
		strings.Index(frozen.FullText, "zeta: text") > strings.Index(frozen.FullText, "alpha: text") ||
		len(frozen.Choices) != 2 || frozen.Choices[1].Verdict != "revise" ||
		frozen.Choices[1].Fields[0].Name != "zeta" || frozen.Choices[1].Fields[1].Name != "alpha" {
		t.Fatalf("frozen card = %#v", frozen)
	}
	repeated, err := FreezeCard(card, 17, "", audience)
	if err != nil || !bytes.Equal(frozen.Input, repeated.Input) || frozen.Hash != repeated.Hash {
		t.Fatalf("repeated freeze changed: %#v, %v", repeated, err)
	}
	fields, err := canonicaljson.FromGo(map[string]any{"zeta": "private-answer-one", "alpha": "private-answer-two"})
	if err != nil {
		t.Fatal(err)
	}
	card.Status, card.Verdict, card.Fields = decisioncard.StatusDecided, "revise", fields
	card.DecidedBy, card.DecidedAt, card.DecisionEventID = principalID, now.Add(time.Minute), uuid.NewString()
	decided, err := FreezeCard(card, 18, "", audience)
	if err != nil {
		t.Fatal(err)
	}
	if len(decided.Choices) != 0 || !strings.Contains(decided.FullText, "Decision: revise") ||
		!strings.Contains(decided.FullText, "Actor: "+principalID) ||
		strings.Contains(decided.FullText, "private-answer") || decided.Hash == frozen.Hash {
		t.Fatalf("decided render = %#v", decided)
	}
	if _, err := FreezeCard(card, 18, "sent", audience); err == nil {
		t.Fatal("non-effect card accepted dispatch state")
	}
}

func TestChannelRenderNoticeIsRunlessAndHasNoDecisionActions(t *testing.T) {
	audience := Audience{PrincipalID: uuid.NewString(), InterfaceKey: "mock-channel", DeliveryEpoch: 1,
		ExternalAccountRef: "account", ConversationRef: "direct", ConversationScope: operatorchannel.ConversationScopeDirect}
	notice := Notice{ID: uuid.NewString(), Type: "notify_human", Summary: "Budget is low", Priority: "critical", Context: []byte(`{"remaining":2}`)}
	frozen, err := FreezeNotice(notice, audience)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(frozen.FullText, "Scope: global") || strings.Contains(frozen.FullText, "Run:") ||
		len(frozen.Choices) != 0 || frozen.Hash != canonicaljson.HashBytes(frozen.Input) {
		t.Fatalf("global notice render = %#v", frozen)
	}
	summary, err := FreezeSummary(uuid.NewString(), 2, audience)
	if err != nil || !strings.Contains(summary.FullText, "2 earlier notices") ||
		!strings.Contains(summary.FullText, "Open inbox from the chat menu") ||
		strings.Contains(summary.FullText, "swarm mailbox") || len(summary.Choices) != 0 {
		t.Fatalf("phone backlog summary = %#v, %v", summary, err)
	}
}
