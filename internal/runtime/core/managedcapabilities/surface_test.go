package managedcapabilities

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
)

func managedCapabilityTestIdentity(agentID string) agentidentity.Identity {
	return agentidentity.Identity{
		Name:  agentidentity.Name{AgentID: agentID, Owner: "managed-capability-test", Source: agentidentity.NameSourceDeclared},
		Route: agentidentity.RootRoute(),
	}
}

func managedCapabilityTestRoutedIdentity(agentID, instanceID string) agentidentity.Identity {
	return agentidentity.Identity{
		Name: agentidentity.Name{AgentID: agentID, Owner: "managed-capability-test", Source: agentidentity.NameSourceDeclared},
		Route: agentidentity.Route{
			Presence: agentidentity.RoutePresent, ScopeKey: "review", InstanceID: instanceID,
			InstancePath: "review/" + instanceID,
		},
	}
}

func TestSurfaceIdentitySeparatesSameSlugConcreteActors(t *testing.T) {
	plan := Plan{
		ActorIdentity: managedCapabilityTestRoutedIdentity("worker", "inst-a"),
		RuntimeMode:   "task", Provider: "test", Transport: "api", ProviderContract: "test.v1",
		Authority: Authority{
			Kind: AuthorityProviderTurn, ID: "00000000-0000-0000-0000-000000000011",
			ExecutionKind: ExecutionNormalAgent, ExecutionAuthorityID: "runtime-owner",
			SessionID: "00000000-0000-0000-0000-000000000012", TurnOrdinal: 1,
		},
		CreatedAt: time.Unix(1, 0).UTC(),
	}
	first, err := New(plan)
	if err != nil {
		t.Fatalf("build first sibling surface: %v", err)
	}
	if first.ActorID != "worker" || !first.MatchesActor(plan.ActorIdentity) {
		t.Fatalf("first surface actor = %q %#v", first.ActorID, first.ActorIdentity)
	}

	secondPlan := plan
	secondPlan.ActorIdentity = managedCapabilityTestRoutedIdentity("worker", "inst-b")
	second, err := New(secondPlan)
	if err != nil {
		t.Fatalf("build second sibling surface: %v", err)
	}
	if first.ID == second.ID || first.MatchesActor(second.ActorIdentity) {
		t.Fatalf("same-slug sibling surfaces were not separated: first=%s second=%s", first.ID, second.ID)
	}
	firstPlan, err := first.PlanFingerprint()
	if err != nil {
		t.Fatalf("fingerprint first sibling surface: %v", err)
	}
	secondPlanFingerprint, err := second.PlanFingerprint()
	if err != nil {
		t.Fatalf("fingerprint second sibling surface: %v", err)
	}
	if firstPlan == secondPlanFingerprint {
		t.Fatal("same-slug sibling surfaces shared one capability plan fingerprint")
	}

	forged := first.Clone()
	forged.ActorID = "other-worker"
	if err := forged.refreshIntegrityHash(); err != nil {
		t.Fatalf("refresh forged surface integrity: %v", err)
	}
	if err := forged.Validate(); err == nil {
		t.Fatal("surface accepted actor_id that was not the typed identity projection")
	}
}

func TestSurfaceAcceptsCanonicalProviderTransports(t *testing.T) {
	for _, transport := range []string{"api", "cli", "in_process"} {
		plan := Plan{
			ActorIdentity: managedCapabilityTestIdentity("worker"), RuntimeMode: "task", Provider: "test", Transport: transport,
			ProviderContract: "test.v1", CreatedAt: time.Unix(1, 0).UTC(),
			Authority: Authority{
				Kind: AuthorityProviderTurn, ID: "00000000-0000-0000-0000-000000000001",
				ExecutionKind: ExecutionNormalAgent, ExecutionAuthorityID: "runtime-owner",
				SessionID: "00000000-0000-0000-0000-000000000002", TurnOrdinal: 1,
			},
		}
		if _, err := New(plan); err != nil {
			t.Fatalf("transport %q: %v", transport, err)
		}
	}
}

func TestSurfaceRequiresConfirmedDeliveryEvidenceAndNarrowsMonotonically(t *testing.T) {
	plan := Plan{
		ActorIdentity: managedCapabilityTestIdentity("worker"), RuntimeMode: "task", Provider: "anthropic", Transport: "api",
		ProviderContract: "messages.v1", CreatedAt: time.Unix(1, 0).UTC(),
		Authority: Authority{
			Kind: AuthorityProviderTurn, ID: "00000000-0000-0000-0000-000000000101",
			ExecutionKind: ExecutionNormalAgent, ExecutionAuthorityID: "runtime-owner",
			SessionID: "00000000-0000-0000-0000-000000000102", TurnOrdinal: 1,
		},
		Tools: []PlannedTool{{
			Name: "event.publish", DefinitionHash: "definition-hash",
			Capability: toolcapabilities.Capability{Name: "event.publish", Visible: true, Callable: true},
			Bindings: []DeliveryBinding{{
				Kind: BindingAPIDefinition, ExactName: "event.publish", RequiredEvidenceKind: "definition_attached",
			}},
		}},
	}

	planned, err := New(plan)
	if err != nil {
		t.Fatalf("build planned surface: %v", err)
	}
	if got := planned.EffectiveNames(); len(got) != 0 {
		t.Fatalf("effective names before delivery evidence = %v, want none", got)
	}
	duplicate, err := New(plan)
	if err != nil || duplicate.ID != planned.ID {
		t.Fatalf("deterministic plan identity = %q err=%v, want %q", duplicate.ID, err, planned.ID)
	}

	confirmed, err := planned.Observe(DeliveryEvidence{
		BindingKind: BindingAPIDefinition, ExactName: "event.publish", Kind: "definition_attached", Status: EvidenceConfirmed,
	})
	if err != nil {
		t.Fatalf("confirm delivery evidence: %v", err)
	}
	if got := confirmed.EffectiveNames(); len(got) != 1 || got[0] != "event.publish" {
		t.Fatalf("effective names after confirmation = %v", got)
	}
	if err := confirmed.CanAdvanceFrom(planned); err != nil {
		t.Fatalf("confirmed surface did not advance planned surface: %v", err)
	}

	unavailable, err := confirmed.Observe(DeliveryEvidence{
		BindingKind: BindingAPIDefinition, ExactName: "event.publish", Kind: "definition_attached", Status: EvidenceUnavailable,
	})
	if err != nil {
		t.Fatalf("narrow confirmed evidence: %v", err)
	}
	if got := unavailable.EffectiveNames(); len(got) != 0 {
		t.Fatalf("effective names after unavailable evidence = %v, want none", got)
	}
	if err := unavailable.CanAdvanceFrom(confirmed); err != nil {
		t.Fatalf("unavailable surface did not narrow confirmed surface: %v", err)
	}
	if _, err := unavailable.Observe(DeliveryEvidence{
		BindingKind: BindingAPIDefinition, ExactName: "event.publish", Kind: "definition_attached", Status: EvidenceConfirmed,
	}); err == nil {
		t.Fatal("unavailable evidence widened back to confirmed")
	}
	if _, err := unavailable.Observe(DeliveryEvidence{
		BindingKind: BindingAPIDefinition, ExactName: "event.publish", Kind: "definition_attached", Status: EvidenceUnavailable, Detail: "rewritten",
	}); err == nil {
		t.Fatal("unavailable evidence detail was rewritten")
	}

	mismatched, err := confirmed.ObserveMismatch(DeliveryMismatch{
		BindingKind: BindingAPIDefinition, ExactName: "unexpected-tool", Kind: "unexpected_delivery",
	})
	if err != nil {
		t.Fatalf("record delivery mismatch: %v", err)
	}
	if !mismatched.HasMismatch() || len(mismatched.EffectiveNames()) != 0 {
		t.Fatalf("mismatched surface remained effective: %#v", mismatched)
	}
	if err := mismatched.CanAdvanceFrom(confirmed); err != nil {
		t.Fatalf("mismatch did not narrow confirmed surface: %v", err)
	}
}

func TestSurfaceRejectsMalformedTypedDeliveryFacts(t *testing.T) {
	plan := Plan{
		ActorIdentity: managedCapabilityTestIdentity("worker"), RuntimeMode: "task", Provider: "anthropic", Transport: "api",
		ProviderContract: "messages.v1", CreatedAt: time.Unix(1, 0).UTC(),
		Authority: Authority{
			Kind: AuthorityProviderTurn, ID: "00000000-0000-0000-0000-000000000201",
			ExecutionKind: ExecutionNormalAgent, ExecutionAuthorityID: "runtime-owner",
			SessionID: "00000000-0000-0000-0000-000000000202", TurnOrdinal: 1,
		},
		Tools: []PlannedTool{{
			Name: "event.publish", DefinitionHash: "definition-hash",
			Capability: toolcapabilities.Capability{Name: "event.publish", Visible: true, Callable: true},
			Bindings: []DeliveryBinding{{
				Kind: BindingAPIDefinition, ExactName: "event.publish", RequiredEvidenceKind: "definition_attached",
			}},
		}},
	}

	if _, err := New(Plan{
		ActorIdentity: plan.ActorIdentity, RuntimeMode: plan.RuntimeMode, Provider: plan.Provider, Transport: plan.Transport,
		ProviderContract: plan.ProviderContract, CreatedAt: plan.CreatedAt, Authority: plan.Authority,
		Tools: []PlannedTool{{
			Name: "event.publish", DefinitionHash: "definition-hash",
			Capability: toolcapabilities.Capability{Name: "event.publish", Visible: true, Callable: true},
			Bindings:   []DeliveryBinding{{ExactName: "event.publish", RequiredEvidenceKind: "definition_attached"}},
		}},
	}); err == nil {
		t.Fatal("planned surface accepted an untyped delivery binding")
	}

	surface, err := New(plan)
	if err != nil {
		t.Fatalf("build planned surface: %v", err)
	}
	surface.Tools[0].Evidence = []DeliveryEvidence{{
		BindingKind: BindingAPIDefinition, ExactName: "different-tool", Kind: "definition_attached", Status: EvidenceConfirmed,
	}}
	resolveTool(&surface.Tools[0])
	if err := surface.refreshIntegrityHash(); err != nil {
		t.Fatalf("refresh malformed surface hash: %v", err)
	}
	if err := surface.Validate(); err == nil {
		t.Fatal("surface accepted evidence for an unplanned exact binding")
	}
}

func TestAuthorityRejectsMalformedSelectedForkCoordinates(t *testing.T) {
	authority := Authority{
		Kind: AuthorityProviderTurn, ID: "00000000-0000-0000-0000-000000000301",
		ExecutionKind: ExecutionSelectedContractFork, ExecutionAuthorityID: "not-a-uuid", RunID: "also-not-a-uuid",
		SessionID: "00000000-0000-0000-0000-000000000302", TurnOrdinal: 1,
	}
	if err := authority.Validate(); err == nil {
		t.Fatal("selected-fork authority accepted malformed execution coordinates")
	}
}

func TestPlanFingerprintSeparatesAttemptAuthorityFromCallablePlan(t *testing.T) {
	plan := Plan{
		ActorIdentity: managedCapabilityTestIdentity("worker"), RuntimeMode: "session", Provider: "anthropic", Transport: "api",
		ProviderContract: "messages.v1", CreatedAt: time.Unix(1, 0).UTC(),
		Authority: Authority{
			Kind: AuthorityProviderTurn, ID: "00000000-0000-0000-0000-000000000401",
			ExecutionKind: ExecutionNormalAgent, ExecutionAuthorityID: "runtime-owner",
			SessionID: "00000000-0000-0000-0000-000000000402", TurnOrdinal: 1,
		},
		Tools: []PlannedTool{{
			Name: "event.publish", DefinitionHash: "definition-hash",
			Capability: toolcapabilities.Capability{Name: "event.publish", Visible: true, Callable: true},
			Bindings: []DeliveryBinding{{
				Kind: BindingAPIDefinition, ExactName: "event.publish", RequiredEvidenceKind: "definition_attached",
			}},
		}},
	}
	first, err := New(plan)
	if err != nil {
		t.Fatalf("build first attempt surface: %v", err)
	}
	plan.Authority.ID = "00000000-0000-0000-0000-000000000403"
	plan.CreatedAt = time.Unix(2, 0).UTC()
	second, err := New(plan)
	if err != nil {
		t.Fatalf("build retry attempt surface: %v", err)
	}
	if first.ID == second.ID {
		t.Fatal("retry attempt reused provider-turn surface identity")
	}
	firstPlan, err := first.PlanFingerprint()
	if err != nil {
		t.Fatalf("fingerprint first plan: %v", err)
	}
	secondPlan, err := second.PlanFingerprint()
	if err != nil {
		t.Fatalf("fingerprint retry plan: %v", err)
	}
	if firstPlan != secondPlan {
		t.Fatalf("retry plan fingerprint = %q, want %q", secondPlan, firstPlan)
	}

	plan.Tools[0].Capability.Callable = false
	plan.Tools[0].Capability.DenialReason = "policy_denied"
	narrowed, err := New(plan)
	if err != nil {
		t.Fatalf("build narrowed retry plan: %v", err)
	}
	narrowedPlan, err := narrowed.PlanFingerprint()
	if err != nil {
		t.Fatalf("fingerprint narrowed plan: %v", err)
	}
	if narrowedPlan == firstPlan {
		t.Fatal("callability change preserved operation plan fingerprint")
	}
}

func TestContinuationFingerprintIgnoresOnlyNormalRuntimeEphemera(t *testing.T) {
	plan := Plan{
		ActorIdentity: managedCapabilityTestIdentity("worker"), RuntimeMode: "session", Provider: "anthropic", Transport: "api",
		ProviderContract: "messages.v1", CreatedAt: time.Unix(1, 0).UTC(),
		Authority: Authority{
			Kind: AuthorityProviderTurn, ID: "00000000-0000-0000-0000-000000000501",
			ExecutionKind: ExecutionNormalAgent, ExecutionAuthorityID: "runtime-generation-one",
			RunID: "00000000-0000-0000-0000-000000000502", SessionID: "00000000-0000-0000-0000-000000000503", TurnOrdinal: 1,
		},
		Tools: []PlannedTool{{
			Name: "event.publish", DefinitionHash: "definition-hash",
			Capability: toolcapabilities.Capability{Name: "event.publish", Visible: true, Callable: true},
			Bindings: []DeliveryBinding{{
				Kind: BindingAPIDefinition, ExactName: "event.publish", RequiredEvidenceKind: "definition_attached",
			}},
		}},
	}
	first, err := New(plan)
	if err != nil {
		t.Fatalf("build first continuation surface: %v", err)
	}
	firstFingerprint, err := first.ContinuationFingerprint()
	if err != nil {
		t.Fatalf("fingerprint first continuation: %v", err)
	}
	plan.Authority.ID = "00000000-0000-0000-0000-000000000504"
	plan.Authority.ExecutionAuthorityID = "runtime-generation-two"
	plan.Authority.SessionID = "00000000-0000-0000-0000-000000000505"
	second, err := New(plan)
	if err != nil {
		t.Fatalf("build successor continuation surface: %v", err)
	}
	secondFingerprint, err := second.ContinuationFingerprint()
	if err != nil {
		t.Fatalf("fingerprint successor continuation: %v", err)
	}
	if secondFingerprint != firstFingerprint {
		t.Fatalf("successor continuation fingerprint = %q, want %q", secondFingerprint, firstFingerprint)
	}

	plan.Tools[0].Capability.Callable = false
	plan.Tools[0].Capability.DenialReason = "policy_denied"
	narrowed, err := New(plan)
	if err != nil {
		t.Fatalf("build narrowed continuation surface: %v", err)
	}
	narrowedFingerprint, err := narrowed.ContinuationFingerprint()
	if err != nil {
		t.Fatalf("fingerprint narrowed continuation: %v", err)
	}
	if narrowedFingerprint == firstFingerprint {
		t.Fatal("callability change preserved continuation fingerprint")
	}
}
