package channelonboarding

import (
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

func TestA9ChannelCandidateUsesExactAlias(t *testing.T) {
	candidate := testCandidate(strings.Repeat("a", 64), "support")
	for _, alias := range []string{" support", "support ", "/support", "support/", "support/child", ".support"} {
		changed := candidate
		changed.Target.Alias = alias
		if _, err := NewCandidateCatalog([]Candidate{changed}); err == nil {
			t.Fatalf("unadmitted alias %q entered channel discovery", alias)
		}
	}
}

func TestChannelOnboardingCandidateCatalogRequiresOneExactCandidate(t *testing.T) {
	left := testCandidate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "support")
	right := testCandidate("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "alerts")
	catalog, err := NewCandidateCatalog([]Candidate{right, left})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Resolve(CandidateSelection{Provider: "telegram"}); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "--source") || !strings.Contains(err.Error(), "--target "+right.Target.Selector) {
		t.Fatalf("ambiguous shorthand error = %v", err)
	}
	resolved, err := catalog.Resolve(CandidateSelection{Provider: "telegram", BundleHash: right.Coordinate.BundleHash, InterfaceSelector: right.Interface.Selector, TargetSelector: right.Target.Selector})
	if err != nil {
		t.Fatal(err)
	}
	if !resolved.Coordinate.Matches(right.Coordinate) || resolved.Target.Selector != right.Target.Selector {
		t.Fatalf("resolved candidate = %#v, want exact right context", resolved)
	}
	if _, err := catalog.Resolve(CandidateSelection{Provider: "discord"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing provider error = %v", err)
	}
	if _, err := catalog.Resolve(CandidateSelection{Provider: "telegram", InterfaceSelector: left.Interface.Selector}); !errors.Is(err, ErrConflict) {
		t.Fatalf("partial exact selection error = %v", err)
	}
}

func TestChannelOnboardingCandidateCatalogRetainsExactBundleRuntimeContexts(t *testing.T) {
	left := testCandidate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "support")
	right := testCandidate("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "support")
	for _, candidates := range [][]Candidate{{left, right}, {right, left}} {
		catalog, err := NewCandidateCatalog(candidates)
		if err != nil {
			t.Fatal(err)
		}
		if got := catalog.Candidates(); len(got) != 2 || got[0].Coordinate.BundleHash == got[1].Coordinate.BundleHash {
			t.Fatalf("exact runtime contexts were deduplicated: %#v", got)
		}
		resolved, err := catalog.Resolve(CandidateSelection{
			Provider: "telegram", BundleHash: right.Coordinate.BundleHash,
			InterfaceSelector: right.Interface.Selector, TargetSelector: right.Target.Selector,
		})
		if err != nil || !resolved.Coordinate.Matches(right.Coordinate) {
			t.Fatalf("exact context selection = %#v, %v", resolved, err)
		}
	}
}

func TestCandidateCatalogDiscoversDeclarationWithoutExecutableGeneration(t *testing.T) {
	candidate := testCandidate(strings.Repeat("a", 64), "support")
	candidate.Coordinate.TargetGeneration = 0
	candidate.Target.Generation = 0
	candidate.Target.PublicationSequence = 0
	catalog, err := NewCandidateCatalog([]Candidate{candidate})
	if err != nil {
		t.Fatalf("credential-absent declaration is undiscoverable: %v", err)
	}
	selected, err := catalog.Resolve(CandidateSelection{Provider: candidate.Provider})
	if err != nil || selected.Target.Generation != 0 || selected.Target.PublicationSequence != 0 || selected.Coordinate.TargetGeneration != 0 {
		t.Fatalf("declaration selection fabricated execution: %#v, %v", selected, err)
	}
	if err := selected.Validate(); err == nil {
		t.Fatal("declaration candidate granted live execution authority")
	}
}

func TestCandidateCatalogExactDeclarationLookupKeepsExecutionFence(t *testing.T) {
	for _, executable := range []bool{false, true} {
		candidate := testCandidate(strings.Repeat("a", 64), "support")
		if !executable {
			candidate.Coordinate.TargetGeneration, candidate.Target.Generation, candidate.Target.PublicationSequence = 0, 0, 0
		}
		catalog, err := NewCandidateCatalog([]Candidate{candidate})
		if err != nil {
			t.Fatal(err)
		}
		if _, found := catalog.FindExactDeclaration(candidate.Provider, candidate.Interface, candidate.Coordinate, candidate.Target.Selector); !found {
			t.Fatal("exact declaration lookup refused its own candidate")
		}
		if _, found := catalog.FindExact(candidate.Provider, candidate.Interface, candidate.Coordinate, candidate.Target.Selector); found != executable {
			t.Fatal("executable lookup changed its generation requirement", executable, found)
		}
		for _, mutate := range []func(*ChannelRuntimeContextCoordinate){
			func(c *ChannelRuntimeContextCoordinate) { c.BundleHash = "bundle-v2:sha256:" + strings.Repeat("b", 64) },
			func(c *ChannelRuntimeContextCoordinate) { c.BundleIdentity = "foreign" },
			func(c *ChannelRuntimeContextCoordinate) { c.PackInventoryGeneration = "foreign" },
			func(c *ChannelRuntimeContextCoordinate) { c.RuntimeInstanceID = "foreign" },
			func(c *ChannelRuntimeContextCoordinate) { c.ContextPublicationGeneration++ },
			func(c *ChannelRuntimeContextCoordinate) { c.TargetGeneration++ },
		} {
			changed := candidate.Coordinate
			mutate(&changed)
			if _, found := catalog.FindExactDeclaration(candidate.Provider, candidate.Interface, changed, candidate.Target.Selector); found {
				t.Fatal("declaration lookup adopted a foreign occurrence", changed)
			}
		}
		if _, found := catalog.FindExactDeclaration(candidate.Provider, candidate.Interface, candidate.Coordinate, "ingress:other:telegram"); found {
			t.Fatal("declaration lookup guessed an alternate target")
		}
	}
}

func TestSessionCandidateRequiresDeclarationNotExecutableTarget(t *testing.T) {
	candidate := testCandidate(strings.Repeat("a", 64), "support")
	candidate.Provider, candidate.Target.Provider = "whatsapp", "whatsapp"
	candidate.Target.Selector = "ingress:support:whatsapp"
	candidate.Posture = ActivationSessionConnection
	candidate.ProviderCredentialRole, candidate.SigningCredentialRole, candidate.Target.SigningCredentialKey = "", "", ""
	candidate.ConnectionHealth = "provider_connection"
	candidate.Target.Generation, candidate.Target.PublicationSequence, candidate.Coordinate.TargetGeneration = 0, 0, 0
	if _, err := NewCandidateCatalog([]Candidate{candidate}); err != nil {
		t.Fatal("unpaired declaration is undiscoverable", err)
	}
	if candidate.Validate() == nil {
		t.Fatal("unpaired declaration granted executable target authority")
	}
	for _, cell := range []string{"missing", "alias", "signing", "generation"} {
		t.Run(cell, func(t *testing.T) {
			changed := candidate
			switch cell {
			case "missing":
				changed.Target = CandidateTarget{}
			case "alias":
				changed.Target.Alias = ""
			case "signing":
				changed.Target.SigningCredentialKey = "not-a-session-credential"
			case "generation":
				changed.Target.Generation = 1
			}
			if _, err := NewCandidateCatalog([]Candidate{changed}); err == nil {
				t.Fatal("session discovery accepted contradictory target evidence")
			}
		})
	}
}

func TestCredentialReservationsFollowDeclaredRoles(t *testing.T) {
	candidate := testCandidate(strings.Repeat("a", 64), "support")
	for _, roles := range [][2]string{{"bot_token", "webhook_signing"}, {"session_secret", ""}, {"", ""}} {
		candidate.ProviderCredentialRole, candidate.SigningCredentialRole = roles[0], roles[1]
		reservations := credentialReservations(candidate)
		var wanted []string
		for _, role := range roles {
			if role != "" {
				wanted = append(wanted, role)
			}
		}
		if len(reservations) != len(wanted) {
			t.Fatal("reservation count contradicts declared credential roles", roles, reservations)
		}
		for i, reservation := range reservations {
			if reservation.Validate() != nil || reservation.Role != wanted[i] {
				t.Fatal("reservation invented or reordered a credential role", reservations)
			}
		}
	}
}

func TestCandidateCatalogRejectsDuplicateExactCoordinate(t *testing.T) {
	candidate := testCandidate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "support")
	if _, err := NewCandidateCatalog([]Candidate{candidate, candidate}); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate candidate error = %v", err)
	}
}

func TestCandidateCatalogResolvesOnlyExactDurableSuccessor(t *testing.T) {
	predecessor := testCandidate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "support")
	successor := predecessor
	successor.Coordinate.RuntimeInstanceID = "22222222-2222-4222-8222-222222222222"
	successor.Coordinate.ContextPublicationGeneration++
	successor.Coordinate.TargetGeneration++
	successor.Target.Generation = successor.Coordinate.TargetGeneration
	catalog, err := NewCandidateCatalog([]Candidate{successor})
	if err != nil {
		t.Fatal(err)
	}
	resolved, found := catalog.FindDurableSuccessor(
		predecessor.Provider, predecessor.Interface, predecessor.Coordinate, predecessor.Target.Selector,
		predecessor.Posture, predecessor.Ceremony,
	)
	if !found || !resolved.Coordinate.Matches(successor.Coordinate) {
		t.Fatalf("durable successor = %#v found=%v", resolved, found)
	}
	if _, found := catalog.FindExact(predecessor.Provider, predecessor.Interface, predecessor.Coordinate, predecessor.Target.Selector); found {
		t.Fatal("predecessor live occurrence remained exact-current")
	}

	tests := []struct {
		name   string
		mutate func(*Candidate)
	}{
		{name: "bundle identity", mutate: func(c *Candidate) { c.Coordinate.BundleIdentity = "bundle:changed@sha256:identity" }},
		{name: "pack inventory", mutate: func(c *Candidate) { c.Coordinate.PackInventoryGeneration = "sha256:changed" }},
		{name: "plan", mutate: func(c *Candidate) { c.Coordinate.PlanGeneration = testPlanGeneration("changed") }},
		{name: "target", mutate: func(c *Candidate) { c.Target.Selector = "ingress:other:telegram" }},
		{name: "posture", mutate: func(c *Candidate) { c.Posture = ActivationSessionConnection }},
		{name: "ceremony", mutate: func(c *Candidate) { c.Ceremony = IdentityCeremony("provider_pairing") }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			changed := successor
			tc.mutate(&changed)
			changedCatalog := &CandidateCatalog{candidates: []Candidate{changed}}
			if _, found := changedCatalog.FindDurableSuccessor(
				predecessor.Provider, predecessor.Interface, predecessor.Coordinate, predecessor.Target.Selector,
				predecessor.Posture, predecessor.Ceremony,
			); found {
				t.Fatal("changed candidate was accepted as durable successor")
			}
		})
	}
}

func TestCandidateCatalogRejectsAmbiguousDurableSuccessorOccurrences(t *testing.T) {
	predecessor := testCandidate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "support")
	successor := predecessor
	successor.Coordinate.RuntimeInstanceID = "22222222-2222-4222-8222-222222222222"
	successor.Coordinate.ContextPublicationGeneration++
	successor.Coordinate.TargetGeneration++
	successor.Target.Generation = successor.Coordinate.TargetGeneration
	if _, err := NewCandidateCatalog([]Candidate{predecessor, successor}); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("ambiguous durable successor error = %v", err)
	}
}

func testCandidate(hashSuffix, flow string) Candidate {
	identity := operatorchannel.InterfaceIdentity{
		InterfaceRef: operatorchannel.InterfaceHITLChannelV2, ChannelPackID: "provider.telegram.hitl_channel",
		ChannelPackVersion: "0.1.0", ChannelManifestHash: "sha256:manifest", SemanticGeneration: "sha256:plan",
	}.Normalized()
	coordinate := testCoordinate()
	coordinate.BundleHash = "bundle-v2:sha256:" + hashSuffix
	return Candidate{
		Provider: "telegram", Interface: identity, Coordinate: coordinate,
		Target: CandidateTarget{Selector: "ingress:" + flow + ":telegram", ServiceID: "service-" + flow, FlowPath: flow, Alias: flow, Provider: "telegram", Generation: coordinate.TargetGeneration,
			PublicationSequence: 1, AdmissionGeneration: triggergeneration.FromCanonicalBytes([]byte("catalog")), SigningCredentialKey: "telegram_signing"},
		Posture: ActivationWebhookRegistration, Ceremony: CeremonyAuthenticatedTextChallenge,
		ProviderCredentialRole: "bot_token", SigningCredentialRole: "webhook_signing", ConfirmationOperation: "deliver",
	}
}
