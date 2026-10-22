package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/packs"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestDormantIngressReadbackCannotHotActivate(t *testing.T) {
	members := map[string][]byte{
		"manifest.yaml":             []byte("version: 1.0.0\nplatform_version: '*'\n"),
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
	declarations, err := ResolveStandingTargetDeclarations(source, catalog)
	if err != nil || len(declarations) != 1 {
		t.Fatalf("captured standing declarations: %+v, %v", declarations, err)
	}
	declaration := declarations[0]
	if declaration.FlowPath != "coordinator" || len(declaration.Ingress) != 1 || declaration.Ingress[0].Provider != "telegram" {
		t.Fatalf("captured coordinator/Telegram declaration: %+v", declaration)
	}
	signingKey := declaration.Ingress[0].SigningSecret
	if signingKey == "" {
		t.Fatal("captured Telegram declaration has no signing credential role")
	}
	contextDef := rootedContextForSource(t, source, rooted)
	contextDef.ProviderTriggerCatalog = catalog
	applyRuntimeAdmissionCatalog(t, &contextDef, catalog)
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
	if err != nil {
		t.Fatal(err)
	}
	file, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	owner, err := runtimecredentials.NewSnapshotOwner(file)
	if err != nil {
		t.Fatal(err)
	}
	for _, supplied := range []bool{false, true} {
		if supplied {
			if err := file.Set(context.Background(), signingKey, "new-signing-value"); err != nil {
				t.Fatal(err)
			}
		}
		subjects, err := manager.EvaluatedCapabilitySubjects(context.Background(), owner)
		if err != nil {
			t.Fatal(err)
		}
		var effective []packs.Subject
		for _, subject := range subjects {
			if subject.Applicability == "effective" {
				effective = append(effective, subject)
			}
		}
		if len(effective) != 1 || effective[0].Status != packs.StatusNotReady || effective[0].TriggerAdmission.BindingEnabled == nil || *effective[0].TriggerAdmission.BindingEnabled {
			t.Fatalf("dormant readback supplied=%t = %#v", supplied, effective)
		}
		want := packs.RequirementStatusUnbound
		if supplied {
			want = packs.RequirementStatusBound
		}
		if len(effective[0].Requirements) != 1 || effective[0].Requirements[0].Status != want {
			t.Fatalf("credential observation = %#v, want %s", effective[0].Requirements, want)
		}
		if len(manager.LoadedContexts()[0].StandingTargets) != 0 {
			t.Fatal("readback installed an executable target")
		}
	}
}

func TestStandingIngressAdmissionDistinguishesAbsentFromInvalid(t *testing.T) {
	for _, state := range []string{"absent", "empty", "whitespace", "read_error", "valid"} {
		t.Run(state, func(t *testing.T) {
			members := map[string][]byte{
				"manifest.yaml":             []byte("version: 1.0.0\nplatform_version: '*'\n"),
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
			declarations, err := ResolveStandingTargetDeclarations(source, catalog)
			if err != nil || len(declarations) != 1 {
				t.Fatalf("captured standing declarations: %+v, %v", declarations, err)
			}
			declaration := declarations[0]
			if declaration.FlowPath != "coordinator" || len(declaration.Ingress) != 1 || declaration.Ingress[0].Provider != "telegram" {
				t.Fatalf("captured coordinator/Telegram declaration: %+v", declaration)
			}
			signingKey := declaration.Ingress[0].SigningSecret
			if signingKey == "" {
				t.Fatal("captured Telegram declaration has no signing credential role")
			}
			file, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
			if err != nil {
				t.Fatal(err)
			}
			key := signingKey
			values := map[string]string{"empty": "", "whitespace": " \t\n", "valid": "valid-signing-value"}
			if value, present := values[state]; present {
				if err := file.Set(context.Background(), key, value); err != nil {
					t.Fatal(err)
				}
			}
			var credentials runtimecredentials.Store = file
			lookupErr := errors.New("credential observation failed")
			if state == "read_error" {
				credentials = &standingCredentialErrorStore{FileStore: file, err: lookupErr}
			}
			rt := &Runtime{Options: RuntimeOptions{
				WorkflowModule: semanticOnlyWorkflowRuntime{source: source}, ProviderTriggerCatalog: catalog,
				ProviderCredentials: credentials, SourceArtifactFact: sourceartifactfixture.FactFor(rooted.Artifact()),
			}}
			targets, err := rt.PlanStandingTargets()
			if state == "empty" || state == "whitespace" || state == "read_error" {
				if err == nil || len(targets) != 0 {
					t.Fatalf("invalid credential produced executable targets: %#v, %v", targets, err)
				}
				if state == "read_error" && !errors.Is(err, lookupErr) {
					t.Fatalf("observation error was lost: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			candidates, err := rt.PlanStandingServiceCandidates()
			if err != nil || len(candidates) != 1 || candidates[0].BindingEnabled != (state == "valid") {
				t.Fatalf("complete declaration census = %#v, %v", candidates, err)
			}
			if state == "valid" {
				if len(targets) != 1 {
					t.Fatalf("admitted target census = %#v", targets)
				}
				return
			}
			if len(targets) != 0 {
				t.Fatalf("absence granted execution: %#v", targets)
			}
			if err := file.Set(context.Background(), key, "newly-provisioned"); err != nil {
				t.Fatal(err)
			}
			// Merely rereading a plan cannot become an activation boundary.
			if targets, err = rt.PlanStandingTargets(); err != nil || len(targets) != 0 {
				t.Fatalf("planning hot-activated after secrets set: %#v, %v", targets, err)
			}
		})
	}
}

func TestStandingRegistrationRequiresEveryCompiledCredentialRole(t *testing.T) {
	for _, state := range []string{"all_absent", "provider_absent", "signing_absent", "all_present", "invalid_provider_signing_absent", "provider_read_error", "corrupt_file"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			members := map[string][]byte{
				"manifest.yaml":             []byte("version: 1.0.0\nplatform_version: '*'\n"),
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
			declarations, err := ResolveStandingTargetDeclarations(source, catalog)
			if err != nil || len(declarations) != 1 {
				t.Fatalf("captured standing declarations: %+v, %v", declarations, err)
			}
			declaration := declarations[0]
			if declaration.FlowPath != "coordinator" || len(declaration.Ingress) != 1 || declaration.Ingress[0].Provider != "telegram" {
				t.Fatalf("captured coordinator/Telegram declaration: %+v", declaration)
			}
			signingKey := declaration.Ingress[0].SigningSecret
			if signingKey == "" {
				t.Fatal("captured Telegram declaration has no signing credential role")
			}
			definition, present := scope.Provider("telegram")
			if !present {
				t.Fatal("captured scope has no Telegram provider")
			}
			plan, present := definition.ChannelPlan()
			if !present {
				t.Fatal("captured Telegram provider has no channel plan")
			}
			profile, present := plan.OnboardingProfile()
			if !present {
				t.Fatal("captured Telegram channel has no onboarding profile")
			}
			providerKey := profile.ProviderCredential()
			if providerKey == "" || profile.SigningCredential() == "" || providerKey == signingKey || providerKey == profile.SigningCredential() {
				t.Fatal("captured onboarding requires distinct provider and signing credential roles")
			}
			keys := map[string]string{profile.ProviderCredential(): providerKey, profile.SigningCredential(): signingKey}
			binding, err := packs.NewOutboundBindingPlanWithRegistration("telegram", plan, "42", nil, keys, "ingress:coordinator:telegram")
			if err != nil {
				t.Fatal(err)
			}
			declared, err := channelonboarding.NewDeclaredOnlyChannelActivationPublication([]packs.OutboundBindingPlan{binding})
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "credentials.json")
			file, err := runtimecredentials.NewFileStore(path)
			if err != nil {
				t.Fatal(err)
			}
			if state == "all_present" || state == "signing_absent" {
				if err := file.Set(ctx, providerKey, "provider-token"); err != nil {
					t.Fatal(err)
				}
			}
			if state == "all_present" || state == "provider_absent" {
				if err := file.Set(ctx, signingKey, "signing-value"); err != nil {
					t.Fatal(err)
				}
			}
			var selected runtimecredentials.Store = file
			sentinel := errors.New("provider credential unavailable")
			switch state {
			case "invalid_provider_signing_absent":
				err = file.Set(ctx, providerKey, " \t\n")
			case "provider_read_error":
				selected = &standingRoleObservationErrorStore{FileStore: file, key: providerKey, err: sentinel}
			case "corrupt_file":
				err = os.WriteFile(path, []byte("{invalid credential document"), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			rt := &Runtime{Options: RuntimeOptions{WorkflowModule: semanticOnlyWorkflowRuntime{source: source}, ProviderTriggerCatalog: catalog,
				ProviderCredentials: selected, DeclaredChannelPublication: declared, SourceArtifactFact: sourceartifactfixture.FactFor(rooted.Artifact())}}
			targets, err := rt.PlanStandingTargets()
			if state == "invalid_provider_signing_absent" || state == "provider_read_error" || state == "corrupt_file" {
				if err == nil || len(targets) != 0 || rt.standingCredentialAdmission != nil {
					t.Fatalf("invalid role granted a partial deployment decision: %#v, %v", targets, err)
				}
				if state == "provider_read_error" && !errors.Is(err, sentinel) {
					t.Fatalf("role observation lost its error: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			candidates, err := rt.PlanStandingServiceCandidates()
			if err != nil || len(candidates) != 1 || candidates[0].BindingEnabled != (state == "all_present") {
				t.Fatalf("registration enabledness = %#v, %v", candidates, err)
			}
			if state == "all_present" {
				if len(targets) != 1 || targets[0].SigningSecret != signingKey {
					t.Fatalf("complete registered target = %#v", targets)
				}
				return
			}
			ineligible, err := rt.IneligibleStandingIngress()
			wantMissing := 1
			if state == "all_absent" {
				wantMissing = 2
			}
			if err != nil || len(targets) != 0 || len(ineligible) != 1 || len(ineligible[0].MissingCredentials) != wantMissing {
				t.Fatalf("incomplete registered target = %#v, %#v, %v", targets, ineligible, err)
			}
		})
	}
}

type standingCredentialErrorStore struct {
	*runtimecredentials.FileStore
	err error
}

func (s *standingCredentialErrorStore) Snapshot(context.Context, string) (runtimecredentials.AtomicSnapshot, error) {
	return runtimecredentials.AtomicSnapshot{}, s.err
}
