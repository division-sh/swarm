package channelonboarding

import (
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

func TestChannelOnboardingCandidateCatalogRequiresOneExactCandidate(t *testing.T) {
	left := testCandidate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "support")
	right := testCandidate("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "alerts")
	catalog, err := NewCandidateCatalog([]Candidate{right, left})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Resolve(CandidateSelection{Provider: "telegram"}); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "--bundle "+left.Coordinate.BundleHash) || !strings.Contains(err.Error(), "--target "+right.Target.Selector) {
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

func TestCandidateCatalogRejectsDuplicateExactCoordinate(t *testing.T) {
	candidate := testCandidate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "support")
	if _, err := NewCandidateCatalog([]Candidate{candidate, candidate}); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate candidate error = %v", err)
	}
}

func testCandidate(hashSuffix, flow string) Candidate {
	identity := operatorchannel.InterfaceIdentity{
		InterfaceRef: operatorchannel.InterfaceHITLChannelV2, ChannelPackID: "provider.telegram.hitl_channel",
		ChannelPackVersion: "0.1.0", ChannelManifestHash: "sha256:manifest", SemanticGeneration: "sha256:plan",
	}.Normalized()
	coordinate := testCoordinate()
	coordinate.BundleHash = "bundle-v1:sha256:" + hashSuffix
	return Candidate{
		Provider: "telegram", Interface: identity, Coordinate: coordinate,
		Target: CandidateTarget{Selector: "ingress:workflow:" + flow + ":telegram", ServiceID: "service-" + flow, PackageKey: "workflow", FlowID: flow, Alias: flow, Provider: "telegram", Generation: coordinate.TargetGeneration,
			PublicationSequence: 1, AdmissionGeneration: triggergeneration.FromCanonicalBytes([]byte("catalog")), SigningCredentialKey: "telegram_signing"},
		Posture: ActivationWebhookRegistration, Ceremony: CeremonyAuthenticatedTextChallenge,
		ProviderCredentialRole: "bot_token", SigningCredentialRole: "webhook_signing", ConfirmationOperation: "deliver",
	}
}
