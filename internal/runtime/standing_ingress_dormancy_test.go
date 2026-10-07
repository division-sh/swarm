package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestStandingIngressCredentialAbsenceAcrossVerifiedProviders(t *testing.T) {
	for _, tc := range []struct{ provider, event string }{
		{"github", "inbound.github.raw.issues"}, {"intercom", "inbound.intercom"},
		{"shopify", "inbound.shopify"}, {"slack", "inbound.slack.message"},
		{"stripe", "inbound.stripe"}, {"telegram", "inbound.telegram"},
		{"typeform", "inbound.typeform"}, {"twilio", "inbound.twilio"},
	} {
		t.Run(tc.provider, func(t *testing.T) {
			source, catalog := standingProviderDeclarationSource(t, tc.provider, tc.event)
			file, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
			if err != nil {
				t.Fatal(err)
			}
			rt := &Runtime{Options: RuntimeOptions{
				WorkflowModule: semanticOnlyWorkflowRuntime{source: source}, ProviderTriggerCatalog: catalog,
				ProviderCredentials: file, SourceArtifactFact: testSourceArtifactFact(t, runtimeContextTestHashA),
			}}
			if targets, err := rt.PlanStandingTargets(); err != nil || len(targets) != 0 {
				t.Fatalf("absent credential granted execution = %#v, %v", targets, err)
			}
			candidates, err := rt.PlanStandingServiceCandidates()
			if err != nil || len(candidates) != 1 || candidates[0].BindingEnabled {
				t.Fatalf("complete dormant declaration census = %#v, %v", candidates, err)
			}
			if err := file.Set(context.Background(), "webhook_signing."+tc.provider, "supplied-signing-value"); err != nil {
				t.Fatal(err)
			}
			restarted := &Runtime{Options: rt.Options}
			if targets, err := restarted.PlanStandingTargets(); err != nil || len(targets) != 1 || targets[0].Provider != tc.provider {
				t.Fatalf("provisioned restart targets = %#v, %v", targets, err)
			}
		})
	}
}

func TestDormantIngressReadbackCannotHotActivate(t *testing.T) {
	source, catalog := standingTelegramDeclarationSource(t, "inbound.telegram")
	contextDef := testBundleContext(t, runtimeContextTestHashA, "inbound.telegram")
	contextDef.Source = source
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
			if err := file.Set(context.Background(), "webhook_signing.telegram", "new-signing-value"); err != nil {
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

func TestStandingIngressUnsignedAdmissionNeedsNoCredentialOwner(t *testing.T) {
	source, _ := standingTelegramDeclarationSource(t, "inbound.partner")
	bundle, _ := semanticview.Bundle(source)
	mutateStandingCoordinatorSchema(t, bundle, func(schema *runtimecontracts.FlowSchemaDocument) {
		schema.Ingress.Providers[0].Provider = "partner"
		schema.Ingress.Providers[0].SigningSecret = ""
		schema.Ingress.Providers[0].Admission = runtimecontracts.ProjectFlowIngressAdmission{
			Kind: "raw", Acknowledge: providertriggers.UnsignedWebhookAcknowledgement, Event: "inbound.partner", Payload: "json",
			Authentication: &runtimecontracts.ProjectFlowIngressAuthentication{Kind: "none"},
			DeliveryID:     &runtimecontracts.ProjectFlowIngressDeliveryID{Source: "json_path", JSONPath: "$.id"},
		}
	})
	catalog, err := providertriggers.NewCatalogSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	rt := &Runtime{Options: RuntimeOptions{WorkflowModule: semanticOnlyWorkflowRuntime{source: source}, ProviderTriggerCatalog: catalog,
		SourceArtifactFact: testSourceArtifactFact(t, runtimeContextTestHashA)}}
	if targets, err := rt.PlanStandingTargets(); err != nil || len(targets) != 1 {
		t.Fatalf("explicit unsigned admission = %#v, %v", targets, err)
	}
}

func TestStandingIngressAdmissionDistinguishesAbsentFromInvalid(t *testing.T) {
	for _, state := range []string{"absent", "empty", "whitespace", "read_error", "valid"} {
		t.Run(state, func(t *testing.T) {
			source, catalog := standingTelegramDeclarationSource(t, "inbound.telegram")
			file, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
			if err != nil {
				t.Fatal(err)
			}
			key := "webhook_signing.telegram"
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
				ProviderCredentials: credentials, SourceArtifactFact: testSourceArtifactFact(t, "bundle-v2:sha256:"+strings.Repeat("a", 64)),
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
			source, catalog := standingTelegramDeclarationSource(t, "inbound.telegram")
			bundle, _ := semanticview.Bundle(source)
			projection, err := packadmission.FromBundle(bundle)
			if err != nil {
				t.Fatal(err)
			}
			plan := projection.ChannelPlans[0]
			profile, _ := plan.OnboardingProfile()
			keys := map[string]string{profile.ProviderCredential(): "provider.bot", profile.SigningCredential(): "webhook_signing.telegram"}
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
				if err := file.Set(ctx, "provider.bot", "provider-token"); err != nil {
					t.Fatal(err)
				}
			}
			if state == "all_present" || state == "provider_absent" {
				if err := file.Set(ctx, "webhook_signing.telegram", "signing-value"); err != nil {
					t.Fatal(err)
				}
			}
			var selected runtimecredentials.Store = file
			sentinel := errors.New("provider credential unavailable")
			switch state {
			case "invalid_provider_signing_absent":
				err = file.Set(ctx, "provider.bot", " \t\n")
			case "provider_read_error":
				selected = &standingRoleObservationErrorStore{FileStore: file, key: "provider.bot", err: sentinel}
			case "corrupt_file":
				err = os.WriteFile(path, []byte("{invalid credential document"), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			rt := &Runtime{Options: RuntimeOptions{WorkflowModule: semanticOnlyWorkflowRuntime{source: source}, ProviderTriggerCatalog: catalog,
				ProviderCredentials: selected, DeclaredChannelPublication: declared, SourceArtifactFact: testSourceArtifactFact(t, runtimeContextTestHashA)}}
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
				if len(targets) != 1 || targets[0].SigningSecret != "webhook_signing.telegram" {
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

func TestDormantIngressCannotMaskStructuralRefusal(t *testing.T) {
	for _, defect := range []string{"missing_pin", "unknown_provider", "duplicate_provider", "malformed_raw", "non_singleton", "invalid_alias"} {
		t.Run(defect, func(t *testing.T) {
			event := "inbound.telegram"
			if defect == "missing_pin" {
				event = "unrelated.event"
			}
			source, catalog := standingTelegramDeclarationSource(t, event)
			bundle, _ := semanticview.Bundle(source)
			mutateStandingCoordinatorSchema(t, bundle, func(schema *runtimecontracts.FlowSchemaDocument) {
				switch defect {
				case "unknown_provider":
					schema.Ingress.Providers[0].Provider = "unknown-provider"
				case "duplicate_provider":
					schema.Ingress.Providers = append(schema.Ingress.Providers, schema.Ingress.Providers[0])
				case "malformed_raw":
					schema.Ingress.Providers[0].Admission.Kind = "raw"
				case "non_singleton":
					field, err := runtimecontracts.ParseTemplateInstanceField("customer_id")
					if err != nil {
						t.Fatal(err)
					}
					schema.Instance = field
				case "invalid_alias":
					schema.Ingress.Alias = "unreachable/alias"
				}
			})
			if defect == "invalid_alias" {
				if err := runtimecontracts.CompileWorkflowSemantics(bundle); err == nil {
					t.Fatal("invalid alias escaped the compiled declaration owner")
				}
				return
			}
			rt := &Runtime{Options: RuntimeOptions{WorkflowModule: semanticOnlyWorkflowRuntime{source: source}, ProviderTriggerCatalog: catalog,
				SourceArtifactFact: testSourceArtifactFact(t, runtimeContextTestHashA)}}
			if targets, err := rt.PlanStandingTargets(); err == nil || len(targets) != 0 || rt.standingCredentialAdmission != nil {
				t.Fatalf("missing credential masked invalid declaration: %#v, %v", targets, err)
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
