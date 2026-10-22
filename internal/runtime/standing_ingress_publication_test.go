package runtime

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/google/uuid"
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
			file, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
			if err != nil {
				t.Fatal(err)
			}
			var contexts []BundleContext
			var plan packs.SatisfactionPlan
			var incomingDeclaration StandingTargetDeclaration
			var incomingBinding StandingIngressBinding
			for i, version := range []string{"1.0.0", "1.0.1"} {
				members := map[string][]byte{
					"manifest.yaml":             []byte("version: " + version + "\nplatform_version: '*'\n"),
					"schema.yaml":               []byte("stages: []\n"),
					"coordinator/manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
					"coordinator/schema.yaml":   []byte("stages: []\npins: {inputs: [inbound.telegram.raw]}\n"),
				}
				for _, entry := range packfixture.EmbeddedInventory(t).Entries() {
					if entry.ID() != "telegram" {
						continue
					}
					members["coordinator/telegram/manifest.yaml"] = entry.ManifestBody()
					for _, role := range []packmodel.BodyRole{packmodel.BodyTrigger, packmodel.BodyConnector, packmodel.BodyChannel} {
						if body, present := entry.Provider().Body(role); present {
							members["coordinator/telegram/"+role.FileName()] = body.Bytes()
						}
					}
					break
				}
				if members["coordinator/telegram/manifest.yaml"] == nil {
					t.Fatal("embedded Telegram provider is unavailable")
				}
				captured, rooted, _ := loadS1EffectiveProviderScope(t, members)
				source := semanticview.Wrap(captured)
				providers, err := packadmission.AdmitProviderSource(rooted, captured.Platform)
				if err != nil {
					t.Fatal(err)
				}
				scope, err := providers.Scope("coordinator")
				if err != nil {
					t.Fatal(err)
				}
				catalog, err := scope.TriggerCatalog()
				if err != nil {
					t.Fatal(err)
				}
				provider, present := scope.Provider("telegram")
				if !present {
					t.Fatal("captured Telegram provider is unavailable")
				}
				capturedPlan, present := provider.ChannelPlan()
				if !present {
					t.Fatal("captured Telegram channel plan is unavailable")
				}
				declarations, err := ResolveStandingTargetDeclarations(source, catalog)
				if err != nil || len(declarations) != 1 {
					t.Fatalf("captured standing declarations: %+v, %v", declarations, err)
				}
				declaration := declarations[0]
				if declaration.FlowPath != "coordinator" || len(declaration.Ingress) != 1 || declaration.Ingress[0].Provider != "telegram" || declaration.Ingress[0].SigningSecret == "" {
					t.Fatalf("captured coordinator/Telegram declaration: %+v", declaration)
				}
				binding := declaration.Ingress[0]
				contextDef := rootedContextForSource(t, source, rooted)
				contextDef.ProviderTriggerCatalog = catalog
				contextDef.ChannelPlans = []packs.SatisfactionPlan{capturedPlan}
				contextDef.BundleIdentity, err = runtimecontracts.BootBundleIdentity(captured)
				if err != nil {
					t.Fatal(err)
				}
				contextDef.RuntimeInstanceID = contextDef.WorkOwner.Identity().RuntimeInstanceID
				contextDef.Runtime.Options = RuntimeOptions{WorkflowModule: semanticOnlyWorkflowRuntime{source: source},
					ProviderTriggerCatalog: catalog, ChannelPlans: contextDef.ChannelPlans,
					SourceArtifactFact: contextDef.SourceArtifactFact, RuntimeInstanceID: contextDef.RuntimeInstanceID}
				if i == 0 {
					if err := file.Set(context.Background(), binding.SigningSecret, "admitted-signing"); err != nil {
						t.Fatal(err)
					}
					contextDef.Runtime.Options.ProviderCredentials = file
					contextDef.StandingTargets, err = contextDef.Runtime.PlanStandingTargets()
					if err != nil {
						t.Fatal(err)
					}
					contextDef.StandingTargets = bindStandingContextFixtureTargets(t, source, contextDef.StandingTargets, uuid.NewString())
				} else {
					plan, incomingDeclaration, incomingBinding = capturedPlan, declaration, binding
				}
				applyRuntimeAdmissionCatalog(t, &contextDef, catalog)
				contexts = append(contexts, contextDef)
			}
			active, incoming := contexts[0], contexts[1]
			if active.BundleHash() == incoming.BundleHash() || len(active.StandingTargets) != 1 || active.StandingTargets[0].Alias != incomingDeclaration.Alias {
				t.Fatal("fixture requires unequal captured content with the same compiled ingress alias")
			}
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
			store := &refusingStandingPromotionStore{}
			incoming.Runtime.Options.ChannelOnboardingStore = store
			manager, err := newTestRuntimeContextManager(t, nil, active, incoming)
			if err != nil {
				t.Fatal(err)
			}
			loaded, found := manager.LookupBundleHash(incoming.BundleHash())
			if !found {
				t.Fatal("incoming declaration is not discoverable")
			}
			target := active.StandingTargets[0]
			candidate := channelonboarding.Candidate{
				Provider: "telegram", Interface: identity, Plan: plan,
				Coordinate: channelonboarding.ChannelRuntimeContextCoordinate{
					BundleHash: loaded.BundleHash(), BundleIdentity: fmt.Sprintf("%s@%s#%s", loaded.BundleIdentity.WorkflowName, loaded.BundleIdentity.WorkflowVersion, loaded.BundleIdentity.BundleHash), PackInventoryGeneration: loaded.PackInventoryDigest,
					RuntimeInstanceID: loaded.RuntimeInstanceID, ContextPublicationGeneration: loaded.PublicationGeneration, PlanGeneration: generation,
				},
				Target: channelonboarding.CandidateTarget{Selector: "ingress:" + incomingDeclaration.FlowPath + ":telegram", ServiceID: runtimeflowidentity.StandingServiceID(incomingDeclaration.FlowPath),
					FlowPath: incomingDeclaration.FlowPath, Alias: incomingDeclaration.Alias, Provider: incomingBinding.Provider, AdmissionGeneration: incomingBinding.AdmissionPlan.Generation()},
				Posture: channelonboarding.ActivationPosture(profile.ActivationPosture()), Ceremony: channelonboarding.IdentityCeremony(profile.IdentityCeremony()),
				ProviderCredentialRole: profile.ProviderCredential(), SigningCredentialRole: profile.SigningCredential(), ConfirmationOperation: profile.ConfirmationOperation(),
			}
			want := "duplicate enabled ingress alias"
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
			if lookup := manager.LookupIngress(target.Alias, target.Provider); !lookup.Loaded() || lookup.Target.BundleHash != active.BundleHash() {
				t.Fatalf("refused admission changed incumbent route: %#v", lookup)
			}
		})
	}
}
