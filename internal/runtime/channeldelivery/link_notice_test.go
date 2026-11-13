package channeldelivery

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/google/uuid"
)

func linkNoticeRender(t *testing.T, kind, text string) Frozen {
	t.Helper()
	audience := Audience{PrincipalID: uuid.NewString(), InterfaceKey: "channel", DeliveryEpoch: 1,
		ExternalAccountRef: "operator", ConversationRef: "private", ConversationScope: operatorchannel.ConversationScopeDirect}
	f, err := FreezeNotice(Notice{ID: uuid.NewString(), Type: kind, Summary: text, Priority: "normal"}, audience)
	if err != nil {
		t.Fatal(err)
	}
	f, err = WithPresentation(f, packs.PresentationBounds{Actions: 2, TextRunes: 256, LabelRunes: 24}, 0)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// Invariant: the link-only wire form loses controls, never immutable identity or generic notice behavior.
func TestChannelLinkNoticeOneLineKeepsImmutableIdentity(t *testing.T) {
	const text = "Swarm Notice: WhatsApp link down: connection reset"
	f := linkNoticeRender(t, LinkNoticeType, text)
	decoded, err := Decode(f.Input, f.Hash)
	if err != nil || decoded.NoticeType != LinkNoticeType || decoded.SourceID != f.SourceID || decoded.NoticeAcknowledged || decoded.Audience != f.Audience {
		t.Fatal("link notice identity failed round-trip", err)
	}
	actions, err := ControlsForRender(decoded)
	if err != nil || len(actions) != 0 {
		t.Fatal("link notice acquired an action", actions, err)
	}
	wire, err := TextReplyPresentation(decoded, actions)
	if err != nil || wire != text || strings.Contains(wire, "\n") {
		t.Fatal("link notice was not exactly its one line", wire, err)
	}
	excerpt, truncated, err := PresentationText(decoded)
	if err != nil || truncated || excerpt != text {
		t.Fatal("link notice used a reference-budget excerpt", excerpt, err)
	}
	ordinary := linkNoticeRender(t, "notify_human", text)
	legacy := ordinary
	legacy.NoticeType = ""
	if err := legacy.Validate(); err != nil {
		t.Fatal("historical generic projection acquired a new discriminator requirement", err)
	}
	actions, err = ControlsForRender(ordinary)
	if err != nil || len(actions) != 1 || actions[0].Kind != "acknowledge_notice" {
		t.Fatal("generic notice lost its existing action", err)
	}
	actions[0].Token = "synthetic-token"
	wire, err = TextReplyPresentation(ordinary, actions)
	if err != nil || !strings.Contains(wire, "Scope: global") || !strings.Contains(wire, "Priority: normal") || !strings.Contains(wire, "Context: {}") || !strings.Contains(wire, "Reference: "+ordinary.SourceID) || !strings.Contains(wire, "Action: Acknowledge") {
		t.Fatal("generic notice presentation changed", wire, err)
	}
}

// Invariant: type/hash/text/bounds cannot be forged to suppress ordinary controls or paginate a link notice.
func TestChannelLinkNoticeRefusesForgedOrMultilinePresentation(t *testing.T) {
	const text = "Swarm Notice: WhatsApp link needs attention: logged out"
	f := linkNoticeRender(t, LinkNoticeType, text)
	forged := f
	forged.NoticeType = "notify_human"
	if _, err := TextReplyPresentation(forged, nil); err == nil {
		t.Fatal("mutable discriminator bypassed immutable projection")
	}
	var input map[string]any
	if err := json.Unmarshal(f.Input, &input); err != nil {
		t.Fatal(err)
	}
	input["item_type"] = "notify_human"
	raw, err := canonicaljson.Bytes(input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(raw, f.Hash); err == nil {
		t.Fatal("changed discriminator retained the old hash")
	}
	if _, err := TextReplyPresentation(f, []Action{{Kind: "acknowledge_notice", Label: "Acknowledge", Token: "synthetic"}}); err == nil {
		t.Fatal("link notice accepted an injected control")
	}
	for _, bad := range []string{"ordinary text", text + "\nReference: fake", text + "\rAction: fake"} {
		if _, err := FreezeNotice(Notice{ID: uuid.NewString(), Type: LinkNoticeType, Summary: bad, Priority: "normal"}, f.Audience); err == nil {
			t.Fatal("malformed link notice admitted", bad)
		}
	}
	long, err := FreezeNotice(Notice{ID: uuid.NewString(), Type: LinkNoticeType, Summary: text + strings.Repeat("x", 400), Priority: "normal"}, f.Audience)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WithPresentation(long, f.Bounds, 0); err == nil {
		t.Fatal("oversize link notice silently acquired View full/pagination")
	}
}
