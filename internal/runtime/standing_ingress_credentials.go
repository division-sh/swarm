package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
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
			result, err := rt.observeStandingBindingCredentials(ctx, admission.projection, declaration, binding, learned)
			if err != nil {
				return nil, err
			}
			admission.bindings[selector] = result
		}
	}
	if err := admission.projection.ValidateCurrent(ctx); err != nil {
		return nil, err
	}
	return admission, nil
}

func (rt *Runtime) standingBindingCredentialRoles(selector string, binding StandingIngressBinding) (map[string]string, string, error) {
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
			return nil, "", fmt.Errorf("channel binding %s selects registration without a compiled recipe", outbound.BindingID())
		}
		roleKeys := outbound.CredentialStoreKeys()
		keys = map[string]string{}
		signingRole = registration.SigningCredential()
		for _, role := range append(registration.ProviderCredentials(), signingRole) {
			key := strings.TrimSpace(roleKeys[role])
			if key == "" {
				return nil, "", fmt.Errorf("channel binding %s registration credential role %s has no exact key", outbound.BindingID(), role)
			}
			keys[role] = key
		}
	}
	return keys, signingRole, nil
}

func (rt *Runtime) observeStandingBindingCredentials(ctx context.Context, projection *runtimecredentials.SecretBindingProjection, declaration StandingTargetDeclaration, binding StandingIngressBinding, learned map[string]standingLearnedCredentials) (standingBindingCredentials, error) {
	selector := standingIngressSelector(declaration.FlowPath, binding.Provider)
	keys, signingRole, err := rt.standingBindingCredentialRoles(selector, binding)
	if err != nil {
		return standingBindingCredentials{}, err
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
		result.operationID, result.recoveryCommand = current.operationID, current.recoveryCommand
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
		credential, hasSeal := sealed[role]
		observed, current, err := observeStandingCredentialRole(ctx, projection, key, credential, hasSeal)
		if err != nil {
			return standingBindingCredentials{}, fmt.Errorf("%s %s credential %q: %w", declaration.SourcePath, selector, key, err)
		}
		if !current {
			result.enabled, result.blockReason = false, runtimerunlifecycle.StandingBindingRecoveryRequired
		}
		if !observed.Present {
			result.enabled = false
			if result.blockReason == "" {
				result.blockReason = runtimerunlifecycle.StandingBindingCredentialsAbsent
			}
			result.missing = append(result.missing, key)
		}
		if role == signingRole {
			result.signingKey = key
		}
	}
	return result, nil
}

func observeStandingCredentialRole(ctx context.Context, projection *runtimecredentials.SecretBindingProjection, key string, credential channelonboarding.CredentialAdmission, hasSeal bool) (runtimecredentials.AdmittedSnapshot, bool, error) {
	if hasSeal {
		return projection.ObserveAdmittedActivationCredential(ctx, runtimecredentials.ValueEvidence{Key: key, Seal: credential.ValueSeal}, credential.Receipt)
	}
	observed, err := projection.ObserveActivationCredential(ctx, key)
	return observed, true, err
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
		scope, present, err := rt.standingCredentialPlanScope(plan, bundle.PackInventory.Digest())
		if err != nil {
			return nil, err
		}
		if !present {
			continue
		}
		if err := scope.addCurrentActivations(activations, operationsByID, out); err != nil {
			return nil, err
		}
		if err := scope.addPendingOperations(operations, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

type standingCredentialPlanScope struct {
	provider    string
	identity    operatorchannel.InterfaceIdentity
	coordinate  channelonboarding.ChannelRuntimeContextCoordinate
	required    []string
	signingRole string
}

func (rt *Runtime) standingCredentialPlanScope(plan packs.SatisfactionPlan, inventory string) (standingCredentialPlanScope, bool, error) {
	profile, present := plan.OnboardingProfile()
	if !present || channelonboarding.ActivationPosture(profile.ActivationPosture()) != channelonboarding.ActivationWebhookRegistration {
		return standingCredentialPlanScope{}, false, nil
	}
	identity, err := plan.InterfaceIdentity()
	if err != nil {
		return standingCredentialPlanScope{}, false, err
	}
	generation, err := plan.Generation()
	if err != nil {
		return standingCredentialPlanScope{}, false, err
	}
	registration, present := plan.Registration()
	if !present {
		return standingCredentialPlanScope{}, false, fmt.Errorf("webhook channel plan has no compiled registration credential owner")
	}
	return standingCredentialPlanScope{provider: profile.Provider(), identity: identity,
		coordinate: channelonboarding.ChannelRuntimeContextCoordinate{BundleHash: rt.Options.SourceArtifactFact.BundleHash(), PackInventoryGeneration: inventory, PlanGeneration: generation},
		required:   append(registration.ProviderCredentials(), registration.SigningCredential()), signingRole: registration.SigningCredential()}, true, nil
}

func (s standingCredentialPlanScope) matches(provider string, identity operatorchannel.InterfaceIdentity, coordinate channelonboarding.ChannelRuntimeContextCoordinate) bool {
	return provider == s.provider && identity.Normalized() == s.identity.Normalized() &&
		coordinate.BundleHash == s.coordinate.BundleHash && coordinate.PackInventoryGeneration == s.coordinate.PackInventoryGeneration && coordinate.PlanGeneration.Equal(s.coordinate.PlanGeneration)
}

func (s standingCredentialPlanScope) admit(admissions []channelonboarding.CredentialAdmission) (standingLearnedCredentials, error) {
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
	if len(byRole) != len(s.required) {
		return standingLearnedCredentials{}, fmt.Errorf("learned ingress credential census contradicts its compiled registration")
	}
	for _, role := range s.required {
		if _, present := byRole[role]; !present {
			return standingLearnedCredentials{}, fmt.Errorf("learned ingress is missing compiled credential role %q", role)
		}
	}
	return standingLearnedCredentials{admissions: admissions, signingRole: s.signingRole}, nil
}

func (s standingCredentialPlanScope) addCurrentActivations(activations []channelonboarding.ConnectedChannelActivation, operations map[string]channelonboarding.Operation, out map[string]standingLearnedCredentials) error {
	for _, activation := range activations {
		if !s.matches(activation.Provider, activation.Interface, activation.Coordinate) {
			continue
		}
		if _, exists := out[activation.TargetSelector]; exists {
			return fmt.Errorf("ingress %s has competing learned credential owners", activation.TargetSelector)
		}
		admitted, err := s.admit(activation.CredentialAdmissions)
		if err != nil {
			return fmt.Errorf("ingress %s learned activation: %w", activation.TargetSelector, err)
		}
		operation, found := operations[activation.OperationID]
		if !found || !s.matches(operation.Provider, operation.Interface, operation.Coordinate) || operation.TargetSelector != activation.TargetSelector {
			return fmt.Errorf("ingress %s activation contradicts its exact onboarding responsibility", activation.TargetSelector)
		}
		admitted.operationID, admitted.recoveryCommand = activation.OperationID, operation.CredentialRecoveryCommand()
		out[activation.TargetSelector] = admitted
	}
	return nil
}

func (s standingCredentialPlanScope) addPendingOperations(operations []channelonboarding.Operation, out map[string]standingLearnedCredentials) error {
	pendingOwners := make(map[string]string)
	for _, operation := range operations {
		if !s.matches(operation.Provider, operation.Interface, operation.Coordinate) {
			continue
		}
		if !operation.Phase.Terminal() {
			if predecessor, found := pendingOwners[operation.TargetSelector]; found {
				return fmt.Errorf("ingress %s has competing pending responsibilities %s and %s", operation.TargetSelector, predecessor, operation.OperationID)
			}
			pendingOwners[operation.TargetSelector] = operation.OperationID
		}
		switch operation.Phase {
		case channelonboarding.PhasePreparing:
			// Keep predecessor evidence while directing recovery to the exact
			// reconnect owner; cleanup never authorizes declaration fallback.
			admitted, current := out[operation.TargetSelector]
			if !current {
				admitted = standingLearnedCredentials{signingRole: s.signingRole, awaitingAdmission: true}
			}
			admitted.operationID, admitted.recoveryCommand = operation.OperationID, operation.CredentialRecoveryCommand()
			out[operation.TargetSelector] = admitted
		case channelonboarding.PhaseCredentialsAdmitted, channelonboarding.PhaseActivatingProvider,
			channelonboarding.PhaseAwaitingExternalIdentity, channelonboarding.PhaseAwaitingOperatorConfirmation,
			channelonboarding.PhasePublishingActivation, channelonboarding.PhasePublishingProcessActivation,
			channelonboarding.PhasePromotingRegistration, channelonboarding.PhaseRetiringPredecessor,
			channelonboarding.PhaseDeliveringConfirmation:
			admitted, err := s.admit(operation.CredentialAdmissions)
			if err != nil {
				return fmt.Errorf("ingress %s pending activation: %w", operation.TargetSelector, err)
			}
			admitted.operationID, admitted.recoveryCommand = operation.OperationID, operation.CredentialRecoveryCommand()
			out[operation.TargetSelector] = admitted
		}
	}
	return nil
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
	if err := rt.refreshStandingCredentialAdmission(ctx, candidate.Target.Selector); err != nil {
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

func (rt *Runtime) refreshStandingCredentialAdmission(ctx context.Context, selected string) error {
	rt.standingCredentialMu.Lock()
	defer rt.standingCredentialMu.Unlock()
	var siblingKeys []string
	frozen := rt.standingCredentialAdmission
	if frozen != nil {
		for selector, binding := range frozen.bindings {
			if selector != selected {
				siblingKeys = append(siblingKeys, binding.credentialKeys...)
			}
		}
		if err := frozen.projection.ValidateCurrentKeys(ctx, siblingKeys); err != nil {
			return fmt.Errorf("channel target admission cannot refresh sibling authority: %w", err)
		}
	}
	admission, err := rt.observeStandingCredentials(ctx)
	if err == nil && frozen != nil {
		err = frozen.projection.ValidateCurrentKeys(ctx, siblingKeys)
	}
	if err == nil && !admission.bindings[selected].enabled {
		err = fmt.Errorf("channel target %s remains credential-dormant", selected)
	}
	if err == nil {
		rt.standingCredentialAdmission = admission
	}
	return err
}
