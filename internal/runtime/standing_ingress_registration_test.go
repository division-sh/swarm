package runtime

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/google/uuid"
)

func TestPublicIngressClassificationUsesCompleteDeclaredAndLearnedOwners(t *testing.T) {
	for _, mode := range []string{"pack_unmanaged", "declared", "learned", "prebinding"} {
		t.Run(mode, func(t *testing.T) {
			use, target, _, _ := publicIngressRegistrationFixture(t, mode)
			admitted, managed, err := use.AdmitPublicIngressTarget(target)
			wantManaged := mode != "pack_unmanaged"
			if err != nil || managed != wantManaged || admitted.registrationAdmission == nil {
				t.Fatalf("mode=%s managed=%t guard=%t err=%v", mode, managed, admitted.registrationAdmission != nil, err)
			}
			if err := admitted.registrationAdmission(t.Context()); err != nil {
				t.Fatalf("exact admitted binding refused: %v", err)
			}
			for _, owned := range use.Context.StandingTargets {
				if owned.Provider == target.Provider && declaredAuthorIngress(use.runtime, &use.Context, owned) == wantManaged {
					t.Fatalf("mode=%s pre-availability classification lost the frozen declaration/learned split", mode)
				}
			}
			wantSource := providertriggers.PolicySourceVerifiedPack
			if got := admitted.AdmissionPlan.PolicySource(); got != wantSource {
				t.Fatalf("policy source=%s want=%s", got, wantSource)
			}
		})
	}
}

func TestPublicIngressClassificationRejectsMissingAndForeignEvidence(t *testing.T) {
	for _, mismatch := range []string{"missing_publication", "executable_publication", "foreign_publication", "foreign_bundle", "foreign_alias", "foreign_provider", "foreign_service", "foreign_run", "foreign_generation", "foreign_publication_sequence", "missing_credentials"} {
		t.Run(mismatch, func(t *testing.T) {
			use, target, _, _ := publicIngressRegistrationFixture(t, "pack_unmanaged")
			switch mismatch {
			case "missing_publication":
				use.Context.DeclaredChannelPublication = channelonboarding.ChannelActivationPublication{}
			case "executable_publication":
				publication, err := channelonboarding.NewChannelActivationPublication(nil)
				if err != nil {
					t.Fatal(err)
				}
				use.Context.DeclaredChannelPublication = publication
			case "foreign_publication":
				other, _, _, _ := publicIngressRegistrationFixture(t, "declared")
				use.Context.DeclaredChannelPublication = other.Context.DeclaredChannelPublication
			case "foreign_bundle":
				target.BundleHash = runtimeContextTestHashB
			case "foreign_alias":
				target.Alias = "foreign"
			case "foreign_provider":
				target.Provider = "foreign"
			case "foreign_service":
				target.ServiceID = uuid.NewString()
			case "foreign_run":
				target.RunID = uuid.NewString()
			case "foreign_generation":
				target.Generation++
			case "foreign_publication_sequence":
				target.PublicationSequence++
			case "missing_credentials":
				use.runtime.standingCredentialAdmission = nil
			}
			admitted, _, err := use.AdmitPublicIngressTarget(target)
			if err == nil || admitted.registrationAdmission != nil {
				t.Fatalf("%s admitted incomplete ownership: %+v err=%v", mismatch, admitted, err)
			}
		})
	}
}

func TestPublicIngressCapturedClassificationFencesLearnedReplacementBeforePublication(t *testing.T) {
	use, target, _, store := publicIngressRegistrationFixture(t, "pack_unmanaged")
	admitted, managed, err := use.AdmitPublicIngressTarget(target)
	if err != nil || managed {
		t.Fatalf("author classification=%t err=%v", managed, err)
	}
	// Promote the same keys and ingress plan through the existing admission
	// owner. Only the learned operation changes, not the request's coordinates.
	store.operations[0].Phase = channelonboarding.PhaseAwaitingOperatorConfirmation
	if err := use.runtime.refreshStandingCredentialAdmission(t.Context(), "ingress:coordinator:telegram"); err != nil {
		t.Fatal(err)
	}
	_, nowManaged, err := use.AdmitPublicIngressTarget(target)
	if err != nil || !nowManaged {
		t.Fatalf("replacement classification=%t err=%v", nowManaged, err)
	}
	if err := admitted.registrationAdmission(t.Context()); err == nil {
		t.Fatal("captured author classification survived learned replacement")
	}
	gateway := NewInboundGateway(nil, nil, nil, executionposture.Live)
	gateway.SetCredentialAdmission(use.runtime.AdmitInboundCredentials)
	response := httptest.NewRecorder()
	gateway.HandleResolvedWebhook(response, httptest.NewRequest(http.MethodPost, "/webhooks/coordinator/telegram", strings.NewReader(`{}`)), admitted, use.Context.Source)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "ingress registration admission became stale") {
		t.Fatalf("replacement reached publication instead of captured fence: %d %q", response.Code, response.Body.String())
	}
}

func TestPublicIngressMixedBindingsKeepAuthorAndManagedTargetsDistinct(t *testing.T) {
	use, target, _, _ := publicIngressRegistrationFixture(t, "mixed")
	_, managed, err := use.AdmitPublicIngressTarget(target)
	if err != nil || !managed {
		t.Fatalf("explicit managed sibling classification=%t err=%v", managed, err)
	}
	for _, author := range use.Context.StandingTargets {
		if author.Provider != "partner" {
			continue
		}
		_, managed, err := use.AdmitPublicIngressTarget(InboundTarget{
			BundleHash: author.BundleHash, ServiceID: author.ServiceID, FlowPath: author.FlowPath,
			RunID: author.RunID, Generation: author.Generation, PublicationSequence: author.PublicationSequence,
			Alias: author.Alias, Provider: author.Provider, SigningSecret: author.SigningSecret, AdmissionPlan: author.AdmissionPlan,
		})
		if err != nil || managed {
			t.Fatalf("independent author sibling classification=%t err=%v", managed, err)
		}
		return
	}
	t.Fatal("fixture has no independently declared author sibling")
}

// This is a component proof of declaration/credential classification, not a
// substitute for the served both-store runtime-use and publication proof.
func publicIngressRegistrationFixture(t *testing.T, mode string) (*RuntimeContextUse, InboundTarget, *runtimecredentials.FileStore, *standingLearnedCredentialStore) {
	t.Helper()
	rt, file, store := publicIngressRegistrationRuntime(t, mode)
	targets, err := rt.PlanStandingTargets()
	wantTargets := 1
	if mode == "mixed" {
		wantTargets = 2
	}
	if err != nil || len(targets) != wantTargets {
		t.Fatalf("standing target=%+v err=%v", targets, err)
	}
	target := targets[0]
	for _, candidate := range targets {
		if candidate.Provider == "telegram" {
			target = candidate
		}
	}
	use := &RuntimeContextUse{runtime: rt, Context: BundleContext{
		SourceArtifactFact: rt.Options.SourceArtifactFact, Source: rt.Options.WorkflowModule.SemanticSource(), DeclaredChannelPublication: rt.Options.DeclaredChannelPublication, StandingTargets: targets,
	}}
	return use, InboundTarget{
		BundleHash: target.BundleHash, ServiceID: target.ServiceID, FlowPath: target.FlowPath, Alias: target.Alias,
		RunID: target.RunID, Generation: target.Generation, PublicationSequence: target.PublicationSequence,
		Provider: target.Provider, SigningSecret: target.SigningSecret, AdmissionPlan: target.AdmissionPlan,
	}, file, store
}

func publicIngressRegistrationRuntime(t *testing.T, mode string) (*Runtime, *runtimecredentials.FileStore, *standingLearnedCredentialStore) {
	t.Helper()
	const metadata = "version: 1.0.0\nplatform_version: '*'\n"
	members := map[string][]byte{
		"manifest.yaml":             []byte(metadata),
		"schema.yaml":               []byte("stages: []\n"),
		"coordinator/manifest.yaml": []byte(metadata),
		"coordinator/schema.yaml":   []byte("stages: []\npins: {inputs: [inbound.telegram.raw]}\n"),
	}
	entry, present := packfixture.EmbeddedInventory(t).Lookup("telegram")
	if !present {
		t.Fatal("missing captured Telegram provider")
	}
	for name, body := range entry.Provider().Members() {
		members["coordinator/telegram/"+name] = body
	}
	if mode == "mixed" {
		members["coordinator/schema.yaml"] = []byte("stages: []\npins: {inputs: [inbound.telegram.raw, inbound.partner.raw]}\n")
		members["coordinator/partner/manifest.yaml"] = []byte(metadata + "capabilities: {can: {emit_events: [raw], verify_secret: webhook_signing_secret}, cannot: [execute code]}\n")
		members["coordinator/partner/trigger.yaml"] = []byte("secret: {required: true}\nsignature: {type: hmac_sha256, header: X-Signature, prefix: 'sha256=', signed_payload: raw_body}\ndelivery_id: {header: X-Delivery, required: true}\nevent_type: {literal: fixture, required: true}\nevent_name: {literal: raw}\n")
	}
	bundle, rooted, _ := loadS1EffectiveProviderScope(t, members)
	source := semanticview.Wrap(bundle)
	providers, err := packadmission.AdmitProviderSource(rooted, bundle.Platform)
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
	projection, err := packadmission.FromBundle(bundle)
	if err != nil {
		t.Fatalf("exact channel plan=%+v err=%v", projection, err)
	}
	plan := packfixture.ChannelPlanByID(t, projection.ChannelPlans, "telegram")
	registration, ok := plan.Registration()
	if !ok {
		t.Fatal("missing compiled registration")
	}
	file, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	if mode == "mixed" {
		if err := file.Set(t.Context(), "partner.webhook_signing_secret", "independent-author-secret"); err != nil {
			t.Fatal(err)
		}
	}
	owner, err := runtimecredentials.NewSnapshotOwner(file)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := plan.InterfaceIdentity()
	if err != nil {
		t.Fatal(err)
	}
	generation, err := plan.Generation()
	if err != nil {
		t.Fatal(err)
	}
	operation := channelonboarding.Operation{
		OperationID: uuid.NewString(), Provider: "telegram", Interface: identity,
		Phase: channelonboarding.PhaseRetired, TargetSelector: "ingress:coordinator:telegram",
		Coordinate: channelonboarding.ChannelRuntimeContextCoordinate{
			BundleHash: rooted.Artifact().BundleHash(), PackInventoryGeneration: bundle.PackInventory.Digest(), PlanGeneration: generation,
		},
	}
	keys := map[string]string{}
	for _, role := range append(registration.ProviderCredentials(), registration.SigningCredential()) {
		key := "test." + role
		if role == registration.SigningCredential() {
			key = role
		}
		keys[role] = key
		if err := file.Set(t.Context(), key, "public-ingress-test-"+role); err != nil {
			t.Fatal(err)
		}
		evidence, err := owner.SealCurrentValue(t.Context(), key)
		if err != nil {
			t.Fatal(err)
		}
		operation.CredentialAdmissions = append(operation.CredentialAdmissions, channelonboarding.CredentialAdmission{
			Role: role, StoreKey: key, Kind: channelonboarding.CredentialAdmissionObserved, ValueSeal: evidence.Seal,
		})
	}
	bindings := []packs.OutboundBindingPlan{}
	if mode == "declared" || mode == "mixed" {
		binding, err := packs.NewOutboundBindingPlanWithRegistration("telegram", plan, "42", nil, keys, operation.TargetSelector)
		if err != nil {
			t.Fatal(err)
		}
		bindings = append(bindings, binding)
	}
	declared, err := channelonboarding.NewDeclaredOnlyChannelActivationPublication(bindings)
	if err != nil {
		t.Fatal(err)
	}
	store := &standingLearnedCredentialStore{}
	if mode == "learned" || mode == "prebinding" {
		operation.Phase = channelonboarding.PhaseAwaitingOperatorConfirmation
	}
	if mode == "learned" {
		operation.Phase = channelonboarding.PhaseSucceeded
		store.activations = []channelonboarding.ConnectedChannelActivation{{
			OperationID: operation.OperationID, Provider: operation.Provider, Interface: operation.Interface,
			Coordinate: operation.Coordinate, TargetSelector: operation.TargetSelector, CredentialAdmissions: operation.CredentialAdmissions,
		}}
	}
	store.operations = []channelonboarding.Operation{operation}
	rt := &Runtime{Options: RuntimeOptions{
		WorkflowModule: semanticOnlyWorkflowRuntime{source: source}, ProviderTriggerCatalog: catalog,
		ChannelPlans: projection.ChannelPlans, DeclaredChannelPublication: declared,
		ProviderCredentials: file, ChannelOnboardingStore: store, SourceArtifactFact: testSourceArtifactFact(t, rooted.Artifact().BundleHash()),
	}}
	return rt, file, store
}
