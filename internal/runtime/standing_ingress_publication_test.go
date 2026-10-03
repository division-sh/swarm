package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type refusingStandingPromotionStore struct {
	channelonboarding.Store
	reads int
}

func (s *refusingStandingPromotionStore) GetChannelOnboarding(context.Context, string) (channelonboarding.Operation, error) {
	s.reads++
	return channelonboarding.Operation{}, errors.New("standing mutation must not be reached")
}

func TestStandingChannelAdmissionRefusesBeforeDurableMutation(t *testing.T) {
	for _, refusal := range []string{"occupied_alias", "undeclared_alias", "stale_runtime", "stale_publication"} {
		t.Run(refusal, func(t *testing.T) {
			source, catalog := standingTelegramDeclarationSource(t, "inbound.telegram")
			bundle, _ := semanticview.Bundle(source)
			projection, err := packadmission.FromBundle(bundle)
			if err != nil {
				t.Fatal(err)
			}
			plan := projection.ChannelPlans[0]
			identity, err := plan.InterfaceIdentity()
			if err != nil {
				t.Fatal(err)
			}
			generation, err := plan.Generation()
			if err != nil {
				t.Fatal(err)
			}
			profile, ok := plan.OnboardingProfile()
			if !ok {
				t.Fatal("fixture has no admitted onboarding profile")
			}
			file, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := file.Set(context.Background(), "webhook_signing.telegram", "admitted-signing"); err != nil {
				t.Fatal(err)
			}
			active := testBundleContext(t, runtimeContextTestHashA, "inbound.telegram")
			active.Source, active.ProviderTriggerCatalog = source, catalog
			active.RuntimeInstanceID = active.WorkOwner.Identity().RuntimeInstanceID
			active.Runtime.Options = RuntimeOptions{WorkflowModule: semanticOnlyWorkflowRuntime{source: source},
				ProviderTriggerCatalog: catalog, ProviderCredentials: file, SourceArtifactFact: active.SourceArtifactFact, RuntimeInstanceID: active.RuntimeInstanceID}
			active.StandingTargets, err = active.Runtime.PlanStandingTargets()
			if err != nil {
				t.Fatal(err)
			}
			applyRuntimeAdmissionCatalog(t, &active, catalog)
			incoming := testBundleContext(t, runtimeContextTestHashB, "inbound.telegram")
			incoming.Source, incoming.ProviderTriggerCatalog = source, catalog
			incoming.RuntimeInstanceID = incoming.WorkOwner.Identity().RuntimeInstanceID
			store := &refusingStandingPromotionStore{}
			incoming.Runtime.Options = RuntimeOptions{WorkflowModule: semanticOnlyWorkflowRuntime{source: source},
				ChannelOnboardingStore: store, SourceArtifactFact: incoming.SourceArtifactFact, RuntimeInstanceID: incoming.RuntimeInstanceID}
			applyRuntimeAdmissionCatalog(t, &incoming, catalog)
			manager, err := newTestRuntimeContextManager(t, nil, active, incoming)
			if err != nil {
				t.Fatal(err)
			}
			loaded, found := manager.LookupBundleHash(runtimeContextTestHashB)
			if !found {
				t.Fatal("incoming declaration is not discoverable")
			}
			target := active.StandingTargets[0]
			candidate := channelonboarding.Candidate{
				Provider: "telegram", Interface: identity, Plan: plan,
				Coordinate: channelonboarding.ChannelRuntimeContextCoordinate{
					BundleHash: loaded.BundleHash(), BundleIdentity: "telegram@1.0.0", PackInventoryGeneration: loaded.PackInventoryDigest,
					RuntimeInstanceID: loaded.RuntimeInstanceID, ContextPublicationGeneration: loaded.PublicationGeneration, PlanGeneration: generation,
				},
				Target: channelonboarding.CandidateTarget{Selector: "ingress:coordinator:telegram", ServiceID: target.ServiceID,
					FlowPath: target.FlowPath, Alias: target.Alias, Provider: target.Provider, AdmissionGeneration: target.AdmissionPlan.Generation()},
				Posture: channelonboarding.ActivationPosture(profile.ActivationPosture()), Ceremony: channelonboarding.IdentityCeremony(profile.IdentityCeremony()),
				ProviderCredentialRole: profile.ProviderCredential(), SigningCredentialRole: profile.SigningCredential(), ConfirmationOperation: profile.ConfirmationOperation(),
			}
			want := "duplicate standing ingress alias"
			switch refusal {
			case "undeclared_alias":
				candidate.Target.Alias, want = "unowned", "no exact declaration owner"
			case "stale_runtime":
				candidate.Coordinate.RuntimeInstanceID, want = "retired-runtime", "no longer current"
			case "stale_publication":
				candidate.Coordinate.ContextPublicationGeneration++
				want = "no longer current"
			}
			err = manager.AdmitChannelStandingTarget(context.Background(), channelonboarding.Operation{}, candidate, nil)
			if err == nil || !strings.Contains(err.Error(), want) || store.reads != 0 {
				t.Fatalf("pre-mutation refusal = %v, owner reads=%d", err, store.reads)
			}
			if lookup := manager.LookupIngress(target.Alias, target.Provider); !lookup.Loaded() || lookup.Target.BundleHash != runtimeContextTestHashA {
				t.Fatalf("refused admission changed incumbent route: %#v", lookup)
			}
		})
	}
}
