package operatorchannel

import "testing"

func TestActionFactRequiresRealCallbackOrExactReplyEvidence(t *testing.T) {
	text := TextFact{Interface: testInterfaceIdentity().Normalized(), ExternalAccountRef: "human",
		ConversationRef: "room", ConversationScope: ConversationScopeShared,
		Text: "Retire", MessageReference: "human-message", ReplyToReference: "bot-receipt"}
	callback := ActionFact{Kind: ActionSourceCallback, Interface: text.Interface,
		ExternalAccountRef: text.ExternalAccountRef, ConversationRef: text.ConversationRef,
		ConversationScope: text.ConversationScope, MessageReference: text.ReplyToReference,
		InteractionRef: "provider-callback", Token: "stored-control"}
	reply := callback
	reply.Kind, reply.InteractionRef, reply.TextSource = ActionSourceReply, "", text
	for name, valid := range map[string]ActionFact{"callback": callback, "reply": reply} {
		t.Run(name, func(t *testing.T) {
			if err := valid.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, test := range []struct {
		name string
		base ActionFact
		edit func(*ActionFact)
	}{
		{"missing kind", callback, func(f *ActionFact) { f.Kind = "" }},
		{"unknown kind", reply, func(f *ActionFact) { f.Kind = "forward" }},
		{"callback without interaction", callback, func(f *ActionFact) { f.InteractionRef = "" }},
		{"callback with text", callback, func(f *ActionFact) { f.TextSource = text }},
		{"reply with fabricated interaction", reply, func(f *ActionFact) { f.InteractionRef = "synthetic" }},
		{"reply without quote", reply, func(f *ActionFact) { f.TextSource.ReplyToReference = "" }},
		{"reply quote mismatch", reply, func(f *ActionFact) { f.MessageReference = "another-receipt" }},
		{"reply account mismatch", reply, func(f *ActionFact) { f.TextSource.ExternalAccountRef = "other" }},
		{"reply conversation mismatch", reply, func(f *ActionFact) { f.TextSource.ConversationRef = "other" }},
		{"reply scope mismatch", reply, func(f *ActionFact) { f.TextSource.ConversationScope = ConversationScopeDirect }},
		{"reply interface mismatch", reply, func(f *ActionFact) { f.TextSource.Interface.SemanticGeneration = "other" }},
		{"reply entry conflict", reply, func(f *ActionFact) { f.TextSource.EntryReference = "inbox" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fact := test.base
			test.edit(&fact)
			if err := fact.Validate(); err == nil {
				t.Fatal("contradictory action evidence was accepted")
			}
		})
	}
}
