package operatorchannel

import (
	"strings"
	"testing"
)

func TestInterfaceIdentitySelectorOwnsCompleteSemanticIdentity(t *testing.T) {
	base := testInterfaceIdentity().Normalized()
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(base.Selector, base.ChannelPackID+"@") || strings.ContainsRune(base.Selector, '\x00') {
		t.Fatalf("selector = %q, want public exact selector", base.Selector)
	}
	mutations := []struct {
		name string
		edit func(*InterfaceIdentity)
	}{
		{name: "interface", edit: func(i *InterfaceIdentity) { i.InterfaceRef = "example.channel/v2" }},
		{name: "pack", edit: func(i *InterfaceIdentity) { i.ChannelPackID = "channel.other" }},
		{name: "version", edit: func(i *InterfaceIdentity) { i.ChannelPackVersion = "2.0.0" }},
		{name: "manifest", edit: func(i *InterfaceIdentity) { i.ChannelManifestHash = "sha256:" + strings.Repeat("b", 64) }},
		{name: "generation", edit: func(i *InterfaceIdentity) { i.SemanticGeneration = "generation-b" }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			changed := base
			changed.Selector = ""
			mutation.edit(&changed)
			changed = changed.Normalized()
			if changed.Selector == base.Selector || changed.Key() == base.Key() {
				t.Fatalf("identity mutation did not change exact identity: %#v", changed)
			}
		})
	}

	forged := base
	forged.Selector = "channel.telegram@forged"
	if err := forged.Validate(); err == nil || !strings.Contains(err.Error(), "selector") {
		t.Fatalf("forged selector validation error = %v", err)
	}
}

func TestChallengeGrammarIsClosed(t *testing.T) {
	challenge, err := NewChallenge()
	if err != nil {
		t.Fatal(err)
	}
	if !ValidChallenge(challenge) {
		t.Fatalf("generated challenge %q is invalid", challenge)
	}
	for _, invalid := range []string{
		strings.ToLower(challenge),
		challenge + "A",
		strings.TrimPrefix(challenge, ChallengePrefix),
		"SWARM-AAAAAAAAAAAAAAA0",
		"prefix " + challenge,
	} {
		if ValidChallenge(invalid) {
			t.Fatalf("invalid challenge %q was accepted", invalid)
		}
	}
	if got, ok := ChallengeFromText("  " + challenge + "\n"); !ok || got != challenge {
		t.Fatalf("ChallengeFromText = %q,%v", got, ok)
	}
}

func testInterfaceIdentity() InterfaceIdentity {
	return InterfaceIdentity{
		InterfaceRef: InterfaceHITLChannelV2, ChannelPackID: "provider.telegram.hitl_channel",
		ChannelPackVersion: "1.0.0", ChannelManifestHash: "sha256:" + strings.Repeat("a", 64),
		SemanticGeneration: "generation-a",
	}
}
