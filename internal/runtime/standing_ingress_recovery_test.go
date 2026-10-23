package runtime

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/packmodel"
	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type standingLearnedCredentialStore struct {
	channelonboarding.Store
	operations  []channelonboarding.Operation
	activations []channelonboarding.ConnectedChannelActivation
	err         error
}

func (s *standingLearnedCredentialStore) GetChannelOnboarding(_ context.Context, id string) (channelonboarding.Operation, error) {
	for _, operation := range s.operations {
		if operation.OperationID == id {
			return operation, s.err
		}
	}
	return channelonboarding.Operation{}, channelonboarding.ErrNotFound
}

func (s *standingLearnedCredentialStore) ListChannelOnboardingOperations(context.Context) ([]channelonboarding.Operation, error) {
	return s.operations, s.err
}

func (s *standingLearnedCredentialStore) ListCurrentConnectedChannelActivations(context.Context) ([]channelonboarding.ConnectedChannelActivation, error) {
	return s.activations, s.err
}

type standingRoleObservationErrorStore struct {
	*runtimecredentials.FileStore
	key string
	err error
}

func (s *standingRoleObservationErrorStore) Snapshot(ctx context.Context, key string) (runtimecredentials.AtomicSnapshot, error) {
	if key == s.key {
		return runtimecredentials.AtomicSnapshot{}, s.err
	}
	return s.FileStore.Snapshot(ctx, key)
}

func TestStandingLearnedAuthorityCredentialMatrix(t *testing.T) {
	for _, role := range []string{"provider", "signing"} {
		for _, state := range []string{"current", "changed_value", "same_value_new_receipt", "deleted", "empty", "whitespace", "read_error", "other_role_invalid", "other_role_read_error", "preparing_after_reset", "duplicate_role", "missing_role", "owner_read_error", "healthy_same_service_sibling", "completed_activation_pending_reconnect", "competing_pending_owners", "after_boot_rotation", "after_boot_receipt", "after_boot_deletion", "after_boot_ABA"} {
			t.Run(role+"/"+state, func(t *testing.T) {
				ctx := context.Background()
				members := map[string][]byte{
					"manifest.yaml":             []byte("version: 1.0.0\nplatform_version: '*'\n"),
					"schema.yaml":               []byte("stages: []\n"),
					"coordinator/manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
					"coordinator/schema.yaml":   []byte("stages: []\npins: {inputs: [inbound.telegram.raw]}\n"),
				}
				if state == "healthy_same_service_sibling" {
					members["coordinator/schema.yaml"] = []byte("stages: []\npins: {inputs: [inbound.telegram.raw, inbound.partner.raw.issues]}\n")
				}
				for _, entry := range packfixture.EmbeddedInventory(t).Entries() {
					directory := ""
					switch entry.ID() {
					case "telegram":
						directory = "telegram"
					case "github":
						if state == "healthy_same_service_sibling" {
							directory = "partner"
						}
					}
					if directory == "" {
						continue
					}
					members["coordinator/"+directory+"/manifest.yaml"] = entry.ManifestBody()
					for _, role := range []packmodel.BodyRole{packmodel.BodyTrigger, packmodel.BodyConnector, packmodel.BodyChannel} {
						if body, present := entry.Provider().Body(role); present {
							members["coordinator/"+directory+"/"+role.FileName()] = body.Bytes()
						}
					}
				}
				bundle, rooted, _ := loadS1EffectiveProviderScope(t, members)
				source := semanticview.Wrap(bundle)
				fact := sourceartifactfixture.FactFor(rooted.Artifact())
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
				definition, present := scope.Provider("telegram")
				if !present {
					t.Fatal("captured Telegram provider is unavailable")
				}
				plan, present := definition.ChannelPlan()
				if !present {
					t.Fatal("captured Telegram channel plan is unavailable")
				}
				declarations, err := ResolveStandingTargetDeclarations(source, catalog)
				if err != nil || len(declarations) != 1 || declarations[0].FlowPath != "coordinator" {
					t.Fatalf("captured coordinator declaration: %+v, %v", declarations, err)
				}
				declaration := declarations[0]
				var telegramBinding, partnerBinding StandingIngressBinding
				for _, binding := range declaration.Ingress {
					switch binding.Provider {
					case "telegram":
						telegramBinding = binding
					case "partner":
						partnerBinding = binding
					}
				}
				if telegramBinding.SigningSecret == "" || (state == "healthy_same_service_sibling" && partnerBinding.SigningSecret == "") {
					t.Fatalf("captured provider signing roles: %+v", declaration.Ingress)
				}
				alias, present := bundle.FlowIngressAlias("coordinator")
				if !present || alias != declaration.Alias {
					t.Fatalf("captured coordinator alias: %q, %+v", alias, declaration)
				}
				identity, err := plan.InterfaceIdentity()
				if err != nil {
					t.Fatal(err)
				}
				generation, err := plan.Generation()
				if err != nil {
					t.Fatal(err)
				}
				registration, ok := plan.Registration()
				if !ok || len(registration.ProviderCredentials()) != 1 {
					t.Fatal("fixture requires exact provider and signing registration roles")
				}
				file, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
				if err != nil {
					t.Fatal(err)
				}
				owner, err := runtimecredentials.NewSnapshotOwner(file)
				if err != nil {
					t.Fatal(err)
				}
				operation := channelonboarding.Operation{
					OperationID: uuid.NewString(), Provider: "telegram", Interface: identity,
					Phase: channelonboarding.PhaseAwaitingOperatorConfirmation, TargetSelector: "ingress:" + declaration.FlowPath + ":telegram",
					Coordinate: channelonboarding.ChannelRuntimeContextCoordinate{
						BundleHash: fact.BundleHash(), PackInventoryGeneration: bundle.PackInventory.Digest(), PlanGeneration: generation,
					},
				}
				roles := []string{registration.ProviderCredentials()[0], registration.SigningCredential()}
				for _, credentialRole := range roles {
					key, value, receipt := "learned."+credentialRole, "original-"+credentialRole, "receipt-"+credentialRole
					if _, err := file.AdmitWithReceipt(ctx, key, value, receipt); err != nil {
						t.Fatal(err)
					}
					evidence, err := owner.SealCurrentValue(ctx, key)
					if err != nil {
						t.Fatal(err)
					}
					operation.CredentialAdmissions = append(operation.CredentialAdmissions, channelonboarding.CredentialAdmission{
						Role: credentialRole, StoreKey: key, Kind: channelonboarding.CredentialAdmissionWritten, Receipt: receipt, ValueSeal: evidence.Seal,
					})
				}
				index := 0
				if role == "signing" {
					index = 1
				}
				selected, sibling := operation.CredentialAdmissions[index], operation.CredentialAdmissions[1-index]
				var credentials runtimecredentials.Store = file
				observationErr := errors.New("required role could not be observed")
				store := &standingLearnedCredentialStore{}
				wantError, wantRecovery := false, false
				switch state {
				case "changed_value", "other_role_invalid", "other_role_read_error", "healthy_same_service_sibling":
					wantRecovery = true
					if err := file.Set(ctx, selected.StoreKey, "replacement-value"); err != nil {
						t.Fatal(err)
					}
					if state == "other_role_invalid" {
						wantError = true
						if err := file.Set(ctx, sibling.StoreKey, " \t\n"); err != nil {
							t.Fatal(err)
						}
					}
					if state == "other_role_read_error" {
						wantError = true
						credentials = &standingRoleObservationErrorStore{FileStore: file, key: sibling.StoreKey, err: observationErr}
					}
					if state == "healthy_same_service_sibling" {
						if err := file.Set(ctx, partnerBinding.SigningSecret, "independent-sibling-secret"); err != nil {
							t.Fatal(err)
						}
					}
				case "same_value_new_receipt":
					wantRecovery = true
					if _, err := file.AdmitWithReceipt(ctx, selected.StoreKey, "original-"+selected.Role, "replacement-receipt"); err != nil {
						t.Fatal(err)
					}
				case "deleted":
					wantRecovery = true
					if err := file.Delete(ctx, selected.StoreKey); err != nil {
						t.Fatal(err)
					}
				case "empty", "whitespace":
					wantError = true
					value := ""
					if state == "whitespace" {
						value = " \t\n"
					}
					if err := file.Set(ctx, selected.StoreKey, value); err != nil {
						t.Fatal(err)
					}
				case "read_error":
					wantError = true
					credentials = &standingRoleObservationErrorStore{FileStore: file, key: selected.StoreKey, err: observationErr}
				case "preparing_after_reset":
					wantRecovery = true
					operation.Phase, operation.CredentialAdmissions = channelonboarding.PhasePreparing, nil
					if err := file.Set(ctx, telegramBinding.SigningSecret, "unrelated-declaration-value"); err != nil {
						t.Fatal(err)
					}
				case "duplicate_role":
					wantError = true
					operation.CredentialAdmissions = append(operation.CredentialAdmissions, selected)
				case "missing_role":
					wantError = true
					operation.CredentialAdmissions = []channelonboarding.CredentialAdmission{selected}
				case "owner_read_error":
					wantError, store.err = true, observationErr
				case "completed_activation_pending_reconnect", "competing_pending_owners":
					wantRecovery = true
					if err := file.Set(ctx, selected.StoreKey, "replacement-value"); err != nil {
						t.Fatal(err)
					}
					if state == "completed_activation_pending_reconnect" {
						operation.Phase = channelonboarding.PhaseSucceeded
						store.activations = []channelonboarding.ConnectedChannelActivation{{
							OperationID: operation.OperationID, Provider: operation.Provider, Interface: operation.Interface,
							Coordinate: operation.Coordinate, TargetSelector: operation.TargetSelector, CredentialAdmissions: operation.CredentialAdmissions,
						}}
					} else {
						wantError = true
					}
					pending := operation
					pending.OperationID, pending.Phase, pending.CredentialAdmissions = uuid.NewString(), channelonboarding.PhasePreparing, nil
					store.operations = append(store.operations, operation)
					operation = pending
				}
				store.operations = append(store.operations, operation)
				rt := &Runtime{Options: RuntimeOptions{WorkflowModule: semanticOnlyWorkflowRuntime{source: source},
					ProviderTriggerCatalog: catalog, ChannelPlans: []packs.SatisfactionPlan{plan}, ProviderCredentials: credentials,
					ChannelOnboardingStore: store, SourceArtifactFact: fact}}
				targets, err := rt.PlanStandingTargets()
				if wantError {
					if err == nil || len(targets) != 0 {
						t.Fatalf("invalid learned authority granted a successful disposition: %#v, %v", targets, err)
					}
					if (state == "read_error" || state == "other_role_read_error" || state == "owner_read_error") && !errors.Is(err, observationErr) {
						t.Fatalf("typed observation error lost: %v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if !wantRecovery {
					if len(targets) != 1 || targets[0].SigningSecret != operation.CredentialAdmissions[1].StoreKey {
						t.Fatalf("learned exact target = %#v", targets)
					}
					if strings.HasPrefix(state, "after_boot_") {
						proveLearnedIngressMutationAfterBoot(t, rt, targets[0], file, selected, operation, state)
					}
					return
				}
				ineligible, err := rt.IneligibleStandingIngress()
				wantTargets := 0
				if state == "healthy_same_service_sibling" {
					wantTargets = 1
				}
				if err != nil || len(targets) != wantTargets || len(ineligible) != 1 || ineligible[0].BlockReason != runtimerunlifecycle.StandingBindingRecoveryRequired || ineligible[0].RecoveryOperationID != operation.OperationID {
					t.Fatalf("stale learned recovery authority = %#v, %#v, %v", targets, ineligible, err)
				}
				if state == "healthy_same_service_sibling" {
					candidates, err := rt.PlanStandingServiceCandidates()
					if err != nil || len(candidates) != 1 || !candidates[0].BindingEnabled || candidates[0].BindingBlockReason != "" || targets[0].Provider != "partner" {
						t.Fatalf("independent sibling retention = %#v, %#v, %v", targets, candidates, err)
					}
					contextDef := rootedContextForSource(t, source, rooted)
					contextDef.ProviderTriggerCatalog = catalog
					contextDef.StandingTargets = bindStandingContextFixtureTargets(t, source, targets, uuid.NewString())
					contextDef.Runtime.Options.SourceArtifactFact = contextDef.SourceArtifactFact
					contextDef.Runtime.standingCredentialAdmission = rt.standingCredentialAdmission
					applyRuntimeAdmissionCatalog(t, &contextDef, catalog)
					manager, err := newTestRuntimeContextManager(t, nil, contextDef)
					if err != nil {
						t.Fatal(err)
					}
					if lookup := manager.LookupIngress(alias, "telegram"); lookup.Loaded() {
						t.Fatal("stale binding executed through its sibling's active service")
					}
					if lookup := manager.LookupIngress(alias, "partner"); !lookup.Loaded() {
						t.Fatalf("healthy binding was suppressed: %#v", lookup)
					}
				}
				if state == "deleted" && (len(ineligible[0].MissingCredentials) != 1 || ineligible[0].MissingCredentials[0] != selected.StoreKey) {
					t.Fatalf("key absence lost retained responsibility: %#v", ineligible)
				}
			})
		}
	}
}

func proveLearnedIngressMutationAfterBoot(t *testing.T, rt *Runtime, target StandingTarget, file *runtimecredentials.FileStore, selected channelonboarding.CredentialAdmission, operation channelonboarding.Operation, state string) {
	t.Helper()
	ctx := context.Background()
	inbound := InboundTarget{BundleHash: target.BundleHash, FlowPath: target.FlowPath, Alias: target.Alias, Provider: target.Provider, SigningSecret: target.SigningSecret, AdmissionPlan: target.AdmissionPlan}
	_, validate, err := rt.AdmitInboundCredentials(ctx, inbound)
	if err != nil {
		t.Fatal(err)
	}
	switch state {
	case "after_boot_rotation":
		err = file.Set(ctx, selected.StoreKey, "unadmitted-replacement")
	case "after_boot_receipt":
		_, err = file.AdmitWithReceipt(ctx, selected.StoreKey, "original-"+selected.Role, "unadmitted-receipt")
	case "after_boot_deletion", "after_boot_ABA":
		err = file.Delete(ctx, selected.StoreKey)
		if err == nil && state == "after_boot_ABA" {
			_, err = file.AdmitWithReceipt(ctx, selected.StoreKey, "original-"+selected.Role, selected.Receipt)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := validate(ctx); err == nil {
		t.Fatal("in-flight request kept stale learned credential authority")
	}
	if _, _, err := rt.AdmitInboundCredentials(ctx, inbound); err == nil {
		t.Fatal("request adopted changed learned value or receipt")
	}
	owner, err := runtimecredentials.NewSnapshotOwner(file)
	if err != nil {
		t.Fatal(err)
	}
	subject, err := target.CapabilitySubject()
	if err != nil {
		t.Fatal(err)
	}
	current, _, err := rt.evaluateStandingIngressAdmission(ctx, target, subject, owner.BeginSecretBindingProjection())
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := packs.NormalizeSubjects([]packs.Subject{current})
	if err != nil {
		t.Fatal(err)
	}
	current = normalized[0]
	if current.Status != packs.StatusNotReady || current.TriggerAdmission.BindingBlockReason != string(runtimerunlifecycle.StandingBindingRecoveryRequired) || current.TriggerAdmission.RecoveryOperationID != operation.OperationID || current.TriggerAdmission.RecoveryCommand != operation.CredentialRecoveryCommand() {
		t.Fatalf("stale learned readback lost exact recovery: %#v, %v", current, err)
	}
}

func TestStandingRecoveryReadbackDoesNotObserveDeclarationFallback(t *testing.T) {
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
	contextDef := rootedContextForSource(t, source, rooted)
	contextDef.ProviderTriggerCatalog = catalog
	applyRuntimeAdmissionCatalog(t, &contextDef, catalog)
	rt := contextDef.Runtime
	declarations, err := ResolveStandingTargetDeclarations(source, catalog)
	if err != nil || len(declarations) != 1 || declarations[0].FlowPath != "coordinator" {
		t.Fatalf("captured coordinator declaration: %+v, %v", declarations, err)
	}
	var telegramBinding StandingIngressBinding
	for _, binding := range declarations[0].Ingress {
		if binding.Provider == "telegram" {
			telegramBinding = binding
		}
	}
	if telegramBinding.SigningSecret == "" {
		t.Fatal("captured Telegram declaration has no signing credential role")
	}
	rt.Options.SourceArtifactFact = contextDef.SourceArtifactFact
	rt.standingCredentialAdmission = &standingCredentialAdmission{
		declarations: declarations,
		bindings: map[string]standingBindingCredentials{"ingress:" + declarations[0].FlowPath + ":telegram": {
			blockReason: runtimerunlifecycle.StandingBindingRecoveryRequired, operationID: "retained-operation", recoveryCommand: "swarm channel resume retained-operation --credential-stdin",
		}},
	}
	manager, err := newTestRuntimeContextManager(t, nil, contextDef)
	if err != nil {
		t.Fatal(err)
	}
	file, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Set(context.Background(), telegramBinding.SigningSecret, "unrelated-default"); err != nil {
		t.Fatal(err)
	}
	owner, err := runtimecredentials.NewSnapshotOwner(file)
	if err != nil {
		t.Fatal(err)
	}
	subjects, err := manager.EvaluatedCapabilitySubjects(context.Background(), owner)
	if err != nil {
		t.Fatal(err)
	}
	for _, subject := range subjects {
		if subject.Kind != packs.SubjectProviderTrigger || subject.Applicability != "effective" {
			continue
		}
		if subject.Status != packs.StatusNotReady || len(subject.Requirements) != 1 || subject.Requirements[0].Satisfied != nil {
			t.Fatalf("recovery readback adopted declaration credential: %#v", subject)
		}
		if subject.TriggerAdmission.BindingBlockReason != "recovery_required" || subject.TriggerAdmission.RecoveryOperationID != "retained-operation" {
			t.Fatalf("exact recovery remedy disappeared: %#v", subject.TriggerAdmission)
		}
		return
	}
	t.Fatal("recovery declaration disappeared from capability readback")
}

func TestExplicitStandingAdmissionCannotRefreshSiblingAuthority(t *testing.T) {
	for _, change := range []string{"rotated", "deleted", "provisioned_after_boot", "invalid", "read_error"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			members := map[string][]byte{
				"manifest.yaml":             []byte("version: 1.0.0\nplatform_version: '*'\n"),
				"schema.yaml":               []byte("stages: []\n"),
				"coordinator/manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
				"coordinator/schema.yaml":   []byte("stages: []\npins: {inputs: [inbound.telegram.raw, inbound.partner.raw.issues]}\n"),
			}
			for _, entry := range packfixture.EmbeddedInventory(t).Entries() {
				directory := ""
				switch entry.ID() {
				case "telegram":
					directory = "telegram"
				case "github":
					directory = "partner"
				}
				if directory == "" {
					continue
				}
				members["coordinator/"+directory+"/manifest.yaml"] = entry.ManifestBody()
				for _, role := range []packmodel.BodyRole{packmodel.BodyTrigger, packmodel.BodyConnector, packmodel.BodyChannel} {
					if body, present := entry.Provider().Body(role); present {
						members["coordinator/"+directory+"/"+role.FileName()] = body.Bytes()
					}
				}
			}
			bundle, rooted, _ := loadS1EffectiveProviderScope(t, members)
			source := semanticview.Wrap(bundle)
			fact := sourceartifactfixture.FactFor(rooted.Artifact())
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
			definition, present := scope.Provider("telegram")
			if !present {
				t.Fatal("captured Telegram provider is unavailable")
			}
			plan, present := definition.ChannelPlan()
			if !present {
				t.Fatal("captured Telegram channel plan is unavailable")
			}
			identity, err := plan.InterfaceIdentity()
			if err != nil {
				t.Fatal(err)
			}
			generation, err := plan.Generation()
			if err != nil {
				t.Fatal(err)
			}
			profile, present := plan.OnboardingProfile()
			if !present {
				t.Fatal("captured Telegram onboarding profile is unavailable")
			}
			registration, present := plan.Registration()
			if !present {
				t.Fatal("captured Telegram registration is unavailable")
			}
			declarations, err := ResolveStandingTargetDeclarations(source, catalog)
			if err != nil || len(declarations) != 1 || declarations[0].FlowPath != "coordinator" {
				t.Fatalf("captured coordinator declaration: %+v, %v", declarations, err)
			}
			declaration := declarations[0]
			var telegramBinding, partnerBinding StandingIngressBinding
			for _, binding := range declaration.Ingress {
				switch binding.Provider {
				case "telegram":
					telegramBinding = binding
				case "partner":
					partnerBinding = binding
				}
			}
			if telegramBinding.SigningSecret == "" || partnerBinding.SigningSecret == "" || telegramBinding.SigningSecret == partnerBinding.SigningSecret {
				t.Fatalf("captured distinct provider signing roles: %+v", declaration.Ingress)
			}
			bundleIdentity, err := runtimecontracts.BootBundleIdentity(bundle)
			if err != nil {
				t.Fatal(err)
			}
			file, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
			if err != nil {
				t.Fatal(err)
			}
			if change != "provisioned_after_boot" {
				if err := file.Set(ctx, partnerBinding.SigningSecret, "partner-original"); err != nil {
					t.Fatal(err)
				}
			}
			operation := channelonboarding.Operation{OperationID: uuid.NewString(), Provider: "telegram", Interface: identity,
				Phase: channelonboarding.PhasePreparing, Revision: 1, TargetSelector: "ingress:" + declaration.FlowPath + ":telegram",
				Coordinate: channelonboarding.ChannelRuntimeContextCoordinate{BundleHash: fact.BundleHash(), BundleIdentity: fmt.Sprintf("%s@%s#%s", bundleIdentity.WorkflowName, bundleIdentity.WorkflowVersion, bundleIdentity.BundleHash),
					PackInventoryGeneration: bundle.PackInventory.Digest(), PlanGeneration: generation, RuntimeInstanceID: uuid.NewString(), ContextPublicationGeneration: 1}}
			store := &standingLearnedCredentialStore{operations: []channelonboarding.Operation{operation}}
			rt := &Runtime{Options: RuntimeOptions{WorkflowModule: semanticOnlyWorkflowRuntime{source: source},
				ProviderTriggerCatalog: catalog, ChannelPlans: []packs.SatisfactionPlan{plan}, ProviderCredentials: file,
				ChannelOnboardingStore: store, SourceArtifactFact: fact}}
			frozen, err := rt.standingCredentials(ctx)
			if err != nil {
				t.Fatal(err)
			}
			owner, err := runtimecredentials.NewSnapshotOwner(file)
			if err != nil {
				t.Fatal(err)
			}
			for _, role := range append(registration.ProviderCredentials(), registration.SigningCredential()) {
				key, receipt := "learned."+role, "receipt-"+role
				if _, err := file.AdmitWithReceipt(ctx, key, "fresh-"+role, receipt); err != nil {
					t.Fatal(err)
				}
				evidence, err := owner.SealCurrentValue(ctx, key)
				if err != nil {
					t.Fatal(err)
				}
				operation.CredentialAdmissions = append(operation.CredentialAdmissions, channelonboarding.CredentialAdmission{
					Role: role, StoreKey: key, Kind: channelonboarding.CredentialAdmissionWritten, Receipt: receipt, ValueSeal: evidence.Seal})
			}
			operation.Phase = channelonboarding.PhaseCredentialsAdmitted
			store.operations = []channelonboarding.Operation{operation}
			switch change {
			case "rotated", "provisioned_after_boot":
				err = file.Set(ctx, partnerBinding.SigningSecret, "unadmitted-partner")
			case "deleted":
				err = file.Delete(ctx, partnerBinding.SigningSecret)
			case "invalid":
				err = file.Set(ctx, partnerBinding.SigningSecret, " \t\n")
			case "read_error":
				rt.Options.ProviderCredentials = &standingRoleObservationErrorStore{FileStore: file, key: partnerBinding.SigningSecret, err: errors.New("sibling observation unavailable")}
			}
			if err != nil {
				t.Fatal(err)
			}
			candidate := channelonboarding.Candidate{Provider: "telegram", Interface: identity, Plan: plan, Coordinate: operation.Coordinate,
				Target: channelonboarding.CandidateTarget{Selector: operation.TargetSelector, ServiceID: runtimeflowidentity.StandingServiceID(declaration.FlowPath),
					FlowPath: declaration.FlowPath, Alias: declaration.Alias, Provider: "telegram", AdmissionGeneration: telegramBinding.AdmissionPlan.Generation()},
				Posture: channelonboarding.ActivationPosture(profile.ActivationPosture()), Ceremony: channelonboarding.IdentityCeremony(profile.IdentityCeremony()),
				ProviderCredentialRole: profile.ProviderCredential(), SigningCredentialRole: profile.SigningCredential(), ConfirmationOperation: profile.ConfirmationOperation()}
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("sibling preflight reached standing mutation: %v", recovered)
				}
			}()
			if _, _, err := rt.AdmitChannelStandingTarget(ctx, operation, candidate, operatorchannel.ProviderAuthority{}); err == nil {
				t.Fatal("explicit admission adopted unrequested sibling authority")
			}
			if rt.standingCredentialAdmission != frozen {
				t.Fatal("refused admission replaced the frozen sibling projection")
			}
		})
	}
}
