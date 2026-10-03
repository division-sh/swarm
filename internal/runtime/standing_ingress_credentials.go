package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type standingBindingCredentials struct {
	enabled         bool
	blockReason     runtimerunlifecycle.StandingBindingBlockReason
	operationID     string
	recoveryCommand string
	signingKey      string
	credentialKeys  []string
	missing         []string
}

type standingCredentialAdmission struct {
	declarations []StandingTargetDeclaration
	bindings     map[string]standingBindingCredentials
	projection   *runtimecredentials.SecretBindingProjection
}

type StandingIngressIneligibility struct {
	SourcePath          string
	FlowPath            string
	Provider            string
	MissingCredentials  []string
	BlockReason         runtimerunlifecycle.StandingBindingBlockReason
	RecoveryOperationID string
	RecoveryCommand     string
}

type standingIngressReadback struct {
	subject        packs.Subject
	observeSigning bool
}

func (rt *Runtime) ineligibleStandingCapabilitySubjects() (map[string]standingIngressReadback, error) {
	out := map[string]standingIngressReadback{}
	rt.standingCredentialMu.Lock()
	defer rt.standingCredentialMu.Unlock()
	admission := rt.standingCredentialAdmission
	if admission == nil {
		return out, nil
	}
	for _, declaration := range admission.declarations {
		for _, binding := range declaration.Ingress {
			credential := admission.bindings[standingIngressSelector(declaration.FlowPath, binding.Provider)]
			if credential.enabled {
				continue
			}
			signingKey := credential.signingKey
			observe := signingKey != "" || !binding.AdmissionPlan.RequiresSecret()
			if !observe {
				// This is an unevaluated declaration requirement, not an observed
				// default credential or replacement for the cleared admission.
				signingKey = binding.SigningSecret
			}
			subject, err := binding.AdmissionPlan.EffectiveCapabilitySubject(providertriggers.EffectiveSubjectRequest{
				BundleHash: rt.Options.SourceArtifactFact.BundleHash(), Alias: declaration.Alias,
				SigningSecret: signingKey, SourcePath: declaration.SourcePath,
			})
			if err != nil {
				return nil, err
			}
			enabled := false
			subject.Status = ""
			subject.TriggerAdmission.BindingEnabled = &enabled
			subject.TriggerAdmission.BindingBlockReason = string(credential.blockReason)
			if credential.blockReason == runtimerunlifecycle.StandingBindingRecoveryRequired {
				subject.TriggerAdmission.RecoveryOperationID = credential.operationID
				subject.TriggerAdmission.RecoveryCommand = credential.recoveryCommand
			}
			normalized, err := packs.NormalizeSubjects([]packs.Subject{subject})
			if err != nil {
				return nil, err
			}
			out[subject.ID] = standingIngressReadback{subject: normalized[0], observeSigning: observe}
		}
	}
	return out, nil
}

// Diagnostics consume the frozen admission, not fresh presence as authority.
func (rt *Runtime) IneligibleStandingIngress() ([]StandingIngressIneligibility, error) {
	admission, err := rt.standingCredentials(context.Background())
	if err != nil {
		return nil, err
	}
	var out []StandingIngressIneligibility
	for _, declaration := range admission.declarations {
		for _, binding := range declaration.Ingress {
			credentials := admission.bindings[standingIngressSelector(declaration.FlowPath, binding.Provider)]
			if credentials.enabled {
				continue
			}
			out = append(out, StandingIngressIneligibility{SourcePath: declaration.SourcePath, FlowPath: declaration.FlowPath,
				Provider: binding.Provider, MissingCredentials: append([]string(nil), credentials.missing...),
				BlockReason: credentials.blockReason, RecoveryOperationID: credentials.operationID, RecoveryCommand: credentials.recoveryCommand})
		}
	}
	return out, nil
}

func standingIngressSelector(flowPath, provider string) string {
	return "ingress:" + flowPath + ":" + provider
}

// Freeze one deployment decision. Readback never reprovisions or promotes it;
// only a new runtime or the explicit channel credential-admission boundary does.
func (rt *Runtime) standingCredentials(ctx context.Context) (*standingCredentialAdmission, error) {
	rt.standingCredentialMu.Lock()
	defer rt.standingCredentialMu.Unlock()
	if rt.standingCredentialAdmission != nil {
		return rt.standingCredentialAdmission, nil
	}
	admission, err := rt.observeStandingCredentials(ctx)
	if err != nil {
		return nil, err
	}
	rt.standingCredentialAdmission = admission
	return admission, nil
}

func (rt *Runtime) observeStandingCredentials(ctx context.Context) (*standingCredentialAdmission, error) {
	source := rt.Options.WorkflowModule.SemanticSource()
	declarations, err := ResolveStandingTargetDeclarations(source, rt.Options.ProviderTriggerCatalog)
	if err != nil {
		return nil, err
	}
	admission := &standingCredentialAdmission{declarations: declarations, bindings: map[string]standingBindingCredentials{}}
	var owner *runtimecredentials.SnapshotOwner
	if rt.Options.ProviderCredentials != nil {
		owner, err = runtimecredentials.NewSnapshotOwner(rt.Options.ProviderCredentials)
		if err != nil {
			return nil, fmt.Errorf("standing ingress credential owner: %w", err)
		}
	}
	admission.projection = owner.BeginSecretBindingProjection()
	learned, err := rt.currentStandingCredentialAdmissions(ctx)
	if err != nil {
		return nil, err
	}
	for _, declaration := range declarations {
		for _, binding := range declaration.Ingress {
			selector := standingIngressSelector(declaration.FlowPath, binding.Provider)
			keys := map[string]string{}
			if binding.AdmissionPlan.RequiresSecret() {
				keys["signing"] = binding.SigningSecret
			}
			signingRole := "signing"
			for _, outbound := range rt.Options.DeclaredChannelPublication.Bindings() {
				if outbound.RegistrationTarget() != selector {
					continue
				}
				registration, present := outbound.Registration()
				if !present {
					return nil, fmt.Errorf("channel binding %s selects registration without a compiled recipe", outbound.BindingID())
				}
				roleKeys := outbound.CredentialStoreKeys()
				keys = map[string]string{}
				signingRole = registration.SigningCredential()
				for _, role := range append(registration.ProviderCredentials(), signingRole) {
					key := strings.TrimSpace(roleKeys[role])
					if key == "" {
						return nil, fmt.Errorf("channel binding %s registration credential role %s has no exact key", outbound.BindingID(), role)
					}
					keys[role] = key
				}
			}
			sealed := map[string]channelonboarding.CredentialAdmission{}
			result := standingBindingCredentials{enabled: true, signingKey: binding.SigningSecret}
			if current, present := learned[selector]; present {
				keys = map[string]string{}
				for _, credential := range current.admissions {
					keys[credential.Role] = credential.StoreKey
					sealed[credential.Role] = credential
				}
				signingRole = current.signingRole
				result.operationID = current.operationID
				result.recoveryCommand = current.recoveryCommand
				if current.awaitingAdmission {
					result.enabled, result.blockReason = false, runtimerunlifecycle.StandingBindingRecoveryRequired
					result.signingKey = ""
				}
			}
			roles := make([]string, 0, len(keys))
			for role := range keys {
				roles = append(roles, role)
			}
			sort.Strings(roles)
			for _, role := range roles {
				key := keys[role]
				result.credentialKeys = append(result.credentialKeys, key)
				observed, err := admission.projection.ObserveActivationCredential(ctx, key)
				if err != nil {
					return nil, fmt.Errorf("%s %s credential %q: %w", declaration.SourcePath, selector, key, err)
				}
				if !observed.Present {
					result.enabled = false
					if result.blockReason == "" {
						result.blockReason = runtimerunlifecycle.StandingBindingCredentialsAbsent
					}
					result.missing = append(result.missing, key)
				}
				if credential, present := sealed[role]; present {
					current, err := owner.CurrentValueMatchesSeal(ctx, runtimecredentials.ValueEvidence{Key: key, Seal: credential.ValueSeal})
					if err != nil {
						return nil, err
					}
					if !current {
						result.enabled, result.blockReason = false, runtimerunlifecycle.StandingBindingRecoveryRequired
					}
					if credential.Kind == channelonboarding.CredentialAdmissionWritten {
						observer, ok := rt.Options.ProviderCredentials.(runtimecredentials.ReceiptObserver)
						if !ok {
							return nil, fmt.Errorf("learned ingress %s requires exact credential receipt ownership", selector)
						}
						_, current, err := observer.ObserveReceipt(ctx, key, credential.Receipt)
						if err != nil {
							return nil, fmt.Errorf("learned ingress %s credential receipt observation: %w", selector, err)
						}
						if !current {
							result.enabled, result.blockReason = false, runtimerunlifecycle.StandingBindingRecoveryRequired
						}
					}
				}
				if role == signingRole {
					result.signingKey = key
				}
			}
			admission.bindings[selector] = result
		}
	}
	if err := admission.projection.ValidateCurrent(ctx); err != nil {
		return nil, err
	}
	return admission, nil
}

type standingLearnedCredentials struct {
	admissions        []channelonboarding.CredentialAdmission
	signingRole       string
	operationID       string
	recoveryCommand   string
	awaitingAdmission bool
}

func (rt *Runtime) currentStandingCredentialAdmissions(ctx context.Context) (map[string]standingLearnedCredentials, error) {
	out := map[string]standingLearnedCredentials{}
	store := rt.Options.ChannelOnboardingStore
	if store == nil {
		return out, nil
	}
	activations, err := store.ListCurrentConnectedChannelActivations(ctx)
	if err != nil {
		return nil, err
	}
	operations, err := store.ListChannelOnboardingOperations(ctx)
	if err != nil {
		return nil, err
	}
	if len(activations) == 0 && len(operations) == 0 {
		return out, nil
	}
	operationsByID := make(map[string]channelonboarding.Operation, len(operations))
	for _, operation := range operations {
		if _, duplicate := operationsByID[operation.OperationID]; duplicate {
			return nil, fmt.Errorf("learned ingress has competing operation owners %s", operation.OperationID)
		}
		operationsByID[operation.OperationID] = operation
	}
	bundle, ok := semanticview.Bundle(rt.Options.WorkflowModule.SemanticSource())
	if !ok || bundle == nil || bundle.PackInventory == nil {
		return nil, fmt.Errorf("learned standing credentials require the exact admitted source inventory")
	}
	for _, plan := range rt.Options.ChannelPlans {
		profile, present := plan.OnboardingProfile()
		if !present || channelonboarding.ActivationPosture(profile.ActivationPosture()) != channelonboarding.ActivationWebhookRegistration {
			continue
		}
		identity, err := plan.InterfaceIdentity()
		if err != nil {
			return nil, err
		}
		generation, err := plan.Generation()
		if err != nil {
			return nil, err
		}
		registration, present := plan.Registration()
		if !present {
			return nil, fmt.Errorf("webhook channel plan has no compiled registration credential owner")
		}
		required := append(registration.ProviderCredentials(), registration.SigningCredential())
		admitLearned := func(admissions []channelonboarding.CredentialAdmission) (standingLearnedCredentials, error) {
			byRole := make(map[string]channelonboarding.CredentialAdmission, len(admissions))
			for _, credential := range admissions {
				if err := credential.Validate(); err != nil {
					return standingLearnedCredentials{}, err
				}
				if _, duplicate := byRole[credential.Role]; duplicate {
					return standingLearnedCredentials{}, fmt.Errorf("learned ingress has duplicate credential role %q", credential.Role)
				}
				byRole[credential.Role] = credential
			}
			if len(byRole) != len(required) {
				return standingLearnedCredentials{}, fmt.Errorf("learned ingress credential census contradicts its compiled registration")
			}
			for _, role := range required {
				if _, present := byRole[role]; !present {
					return standingLearnedCredentials{}, fmt.Errorf("learned ingress is missing compiled credential role %q", role)
				}
			}
			return standingLearnedCredentials{admissions: admissions, signingRole: registration.SigningCredential()}, nil
		}
		matches := func(coordinate channelonboarding.ChannelRuntimeContextCoordinate) bool {
			return coordinate.BundleHash == rt.Options.SourceArtifactFact.BundleHash() &&
				coordinate.PackInventoryGeneration == bundle.PackInventory.Digest() && coordinate.PlanGeneration.Equal(generation)
		}
		for _, activation := range activations {
			if activation.Provider != profile.Provider() || activation.Interface.Normalized() != identity.Normalized() || !matches(activation.Coordinate) {
				continue
			}
			if _, exists := out[activation.TargetSelector]; exists {
				return nil, fmt.Errorf("ingress %s has competing learned credential owners", activation.TargetSelector)
			}
			admitted, err := admitLearned(activation.CredentialAdmissions)
			if err != nil {
				return nil, fmt.Errorf("ingress %s learned activation: %w", activation.TargetSelector, err)
			}
			admitted.operationID = activation.OperationID
			operation, found := operationsByID[activation.OperationID]
			if !found || operation.Provider != activation.Provider || operation.Interface.Normalized() != activation.Interface.Normalized() || operation.TargetSelector != activation.TargetSelector || !matches(operation.Coordinate) {
				return nil, fmt.Errorf("ingress %s activation contradicts its exact onboarding responsibility", activation.TargetSelector)
			}
			admitted.recoveryCommand = operation.CredentialRecoveryCommand()
			out[activation.TargetSelector] = admitted
		}
		pendingOwners := make(map[string]string)
		for _, operation := range operations {
			if operation.Provider != profile.Provider() || operation.Interface.Normalized() != identity.Normalized() || !matches(operation.Coordinate) {
				continue
			}
			if !operation.Phase.Terminal() {
				if predecessor, found := pendingOwners[operation.TargetSelector]; found {
					return nil, fmt.Errorf("ingress %s has competing pending responsibilities %s and %s", operation.TargetSelector, predecessor, operation.OperationID)
				}
				pendingOwners[operation.TargetSelector] = operation.OperationID
			}
			switch operation.Phase {
			case channelonboarding.PhasePreparing:
				// Cleanup must not turn a retained responsibility into permission
				// to adopt the declaration's unrelated credential keys.
				if _, current := out[operation.TargetSelector]; !current {
					out[operation.TargetSelector] = standingLearnedCredentials{
						signingRole: registration.SigningCredential(), operationID: operation.OperationID, recoveryCommand: operation.CredentialRecoveryCommand(), awaitingAdmission: true,
					}
				} else {
					// Retain the predecessor's exact credential evidence, but
					// direct recovery to the already-created reconnect owner.
					admitted := out[operation.TargetSelector]
					admitted.operationID, admitted.recoveryCommand = operation.OperationID, operation.CredentialRecoveryCommand()
					out[operation.TargetSelector] = admitted
				}
			case channelonboarding.PhaseCredentialsAdmitted, channelonboarding.PhaseActivatingProvider,
				channelonboarding.PhaseAwaitingExternalIdentity, channelonboarding.PhaseAwaitingOperatorConfirmation,
				channelonboarding.PhasePublishingActivation, channelonboarding.PhasePublishingProcessActivation,
				channelonboarding.PhasePromotingRegistration, channelonboarding.PhaseRetiringPredecessor,
				channelonboarding.PhaseDeliveringConfirmation:
				admitted, err := admitLearned(operation.CredentialAdmissions)
				if err != nil {
					return nil, fmt.Errorf("ingress %s pending activation: %w", operation.TargetSelector, err)
				}
				admitted.operationID = operation.OperationID
				admitted.recoveryCommand = operation.CredentialRecoveryCommand()
				out[operation.TargetSelector] = admitted
			}
		}
	}
	return out, nil
}

func (rt *Runtime) ValidateStandingIngressCredentials(ctx context.Context) error {
	admission, err := rt.standingCredentials(ctx)
	if err != nil {
		return err
	}
	return admission.projection.ValidateCurrent(ctx)
}

// Explicit connect is the only in-process credential-producing activation
// boundary. Ordinary planning and diagnostic reads retain the frozen decision.
func (rt *Runtime) AdmitChannelStandingTarget(ctx context.Context, operation channelonboarding.Operation, candidate channelonboarding.Candidate) ([]StandingTarget, error) {
	if rt == nil || rt.Options.ChannelOnboardingStore == nil || rt.Options.WorkflowModule == nil {
		return nil, fmt.Errorf("channel target promotion requires runtime and selected onboarding owner")
	}
	if err := candidate.ValidateDeclaration(); err != nil {
		return nil, err
	}
	current, err := rt.Options.ChannelOnboardingStore.GetChannelOnboarding(ctx, operation.OperationID)
	if err != nil {
		return nil, err
	}
	if current.Phase != channelonboarding.PhaseCredentialsAdmitted || current.Revision != operation.Revision || !current.Coordinate.MatchesDeclaration(candidate.Coordinate) || current.TargetSelector != candidate.Target.Selector || current.Coordinate.BundleHash != rt.Options.SourceArtifactFact.BundleHash() {
		return nil, fmt.Errorf("%w: channel target promotion has no exact admitted credential responsibility", channelonboarding.ErrRevisionConflict)
	}
	rt.standingCredentialMu.Lock()
	var siblingKeys []string
	frozen := rt.standingCredentialAdmission
	if frozen != nil {
		for selector, binding := range frozen.bindings {
			if selector != candidate.Target.Selector {
				siblingKeys = append(siblingKeys, binding.credentialKeys...)
			}
		}
		if err := frozen.projection.ValidateCurrentKeys(ctx, siblingKeys); err != nil {
			rt.standingCredentialMu.Unlock()
			return nil, fmt.Errorf("channel target admission cannot refresh sibling authority: %w", err)
		}
	}
	admission, err := rt.observeStandingCredentials(ctx)
	if err == nil && frozen != nil {
		err = frozen.projection.ValidateCurrentKeys(ctx, siblingKeys)
	}
	if err == nil && !admission.bindings[candidate.Target.Selector].enabled {
		err = fmt.Errorf("channel target %s remains credential-dormant", candidate.Target.Selector)
	}
	if err == nil {
		rt.standingCredentialAdmission = admission
	}
	rt.standingCredentialMu.Unlock()
	if err != nil {
		return nil, err
	}
	plans, err := rt.standingTargetPlans()
	if err != nil {
		return nil, err
	}
	for _, plan := range plans {
		if plan.serviceID != candidate.Target.ServiceID {
			continue
		}
		current, found, err := rt.Pipeline.LoadReconciledStandingService(ctx, runtimepipeline.StandingServiceCandidate{
			BindingEnabled: plan.bindingEnabled, BindingBlockReason: plan.blockReason, ServiceID: plan.serviceID, FlowPath: plan.declaration.FlowPath,
			InstanceID: plan.instance.InstanceID, EntityID: plan.instance.EntityID, Source: rt.Options.SourceArtifactFact,
		})
		if err != nil {
			return nil, err
		}
		if found && current.RestartDisposition.Executable() && current.PublicationSequence > 0 {
			if candidate.Target.Generation > 0 && (candidate.Target.Generation != uint64(current.Generation) || candidate.Target.PublicationSequence != current.PublicationSequence) {
				return nil, fmt.Errorf("%w: channel standing target changed before promotion", channelonboarding.ErrRevisionConflict)
			}
			targets := append([]StandingTarget(nil), plan.targets...)
			for i := range targets {
				targets[i].RunID, targets[i].Generation, targets[i].PublicationSequence = current.RunID, current.Generation, current.PublicationSequence
			}
			return targets, nil
		}
	}
	targets, _, err := rt.EnsureStandingServiceTargets(ctx, candidate.Target.ServiceID)
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("channel target %s has no executable standing owner; inspect standing service %s", candidate.Target.Selector, candidate.Target.ServiceID)
	}
	return targets, nil
}
