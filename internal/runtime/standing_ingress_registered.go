package runtime

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type registeredStandingCredential struct {
	operation channelonboarding.Operation
	holder    channelonboarding.AccountHolder
}

func registeredStandingSelector(selector, operation string) string {
	if operation == "" {
		return selector
	}
	return selector + "\x00" + operation
}

func (rt *Runtime) validateRegisteredStandingCredential(ctx context.Context, evidence registeredStandingCredential, exact bool) error {
	store, ok := rt.Options.ChannelOnboardingStore.(interface {
		DeploymentChannelOperations() channelonboarding.OperationStore
	})
	if !ok {
		return channelonboarding.ErrInvalidRequest
	}
	original := evidence.operation
	ref := channelonboarding.AccountReference{Provider: original.Provider, Account: original.AccountReference}
	current, err := channelonboarding.RegisteredAccount(ctx, store.DeploymentChannelOperations(), ref)
	if err != nil {
		return err
	}
	if current.OperationID != original.OperationID || current.Revision < original.Revision || exact && current.Revision != original.Revision ||
		current.PrincipalID != original.PrincipalID || current.Interface.Normalized() != original.Interface.Normalized() ||
		!slices.Equal(current.CredentialAdmissions, original.CredentialAdmissions) || evidence.holder.BundleHash != rt.Options.SourceArtifactFact.BundleHash() {
		return channelonboarding.ErrRevisionConflict
	}
	holders, err := rt.AccountHolders(ctx, evidence.holder.RunID, ref)
	if err != nil {
		return err
	}
	matched := 0
	for _, holder := range holders {
		if holder.Flow != evidence.holder.Flow {
			continue
		}
		if holder != evidence.holder {
			return channelonboarding.ErrRevisionConflict
		}
		matched++
	}
	if matched != 1 {
		return channelonboarding.ErrRevisionConflict
	}
	return nil
}

// This is invoked only by explicit startup admission, never registration
// reconciliation or readback. Each entry retains one operation's authority.
func (rt *Runtime) admitRegisteredStandingChannelTarget(ctx context.Context, op channelonboarding.Operation, candidate channelonboarding.Candidate, holder channelonboarding.AccountHolder) ([]StandingTarget, []StandingActivation, error) {
	evidence := registeredStandingCredential{operation: op, holder: holder}
	if err := rt.validateRegisteredStandingCredential(ctx, evidence, true); err != nil {
		return nil, nil, fmt.Errorf("registered ingress account/holder currentness: %w", err)
	}
	bundle, ok := semanticview.Bundle(rt.Options.WorkflowModule.SemanticSource())
	if !ok || bundle == nil || bundle.PackInventory == nil {
		return nil, nil, channelonboarding.ErrInvalidRequest
	}
	scope, present, err := rt.standingCredentialPlanScope(candidate.Plan, bundle.PackInventory.Digest())
	if err != nil {
		return nil, nil, err
	}
	if !present || !scope.matches(candidate.Provider, candidate.Interface, candidate.Coordinate) {
		return nil, nil, fmt.Errorf("registered ingress source plan: %w", channelonboarding.ErrRevisionConflict)
	}
	// Deployment proof keeps its own interface above. The source must hold its
	// independently admitted plan; registration is not authority to replace it.
	captured := false
	for _, plan := range rt.Options.ChannelPlans {
		identity, err := plan.InterfaceIdentity()
		if err != nil {
			return nil, nil, err
		}
		if plan.MatchesCapturedHolder(holder.Flow) && identity.Normalized() == candidate.Interface.Normalized() {
			captured = true
		}
	}
	if !captured || op.Provider != scope.provider || op.Posture != channelonboarding.ActivationWebhookRegistration {
		return nil, nil, fmt.Errorf("registered ingress captured provider/interface: %w", channelonboarding.ErrRevisionConflict)
	}
	if holder.Flow != candidate.Target.FlowPath || holder.RunID == "" || holder.FlowInstance == "" {
		return nil, nil, fmt.Errorf("registered ingress source holder: %w", channelonboarding.ErrRevisionConflict)
	}
	sealed, err := scope.admit(op.CredentialAdmissions)
	if err != nil {
		return nil, nil, err
	}
	if err := rt.freezeRegisteredStandingCredential(ctx, candidate, sealed, evidence); err != nil {
		return nil, nil, fmt.Errorf("registered ingress sealed admission: %w", err)
	}
	return rt.EnsureFlowTargets(ctx, holder.Flow)
}

func (rt *Runtime) freezeRegisteredStandingCredential(ctx context.Context, candidate channelonboarding.Candidate, sealed standingLearnedCredentials, evidence registeredStandingCredential) error {
	rt.standingCredentialMu.Lock()
	defer rt.standingCredentialMu.Unlock()
	frozen := rt.standingCredentialAdmission
	if frozen == nil {
		return fmt.Errorf("registered ingress requires its frozen source admission")
	}
	selector := candidate.Target.Selector
	base, found := frozen.bindings[selector]
	if !found || base.alias != candidate.Target.Alias || !base.plan.Generation().Equal(candidate.Target.AdmissionGeneration) {
		return channelonboarding.ErrRevisionConflict
	}
	key := registeredStandingSelector(selector, evidence.operation.OperationID)
	if existing, found := frozen.bindings[key]; found {
		if existing.registered == nil || existing.registered.holder != evidence.holder || !slices.Equal(existing.admissions, sealed.admissions) {
			return channelonboarding.ErrRevisionConflict
		}
		return frozen.projection.ValidateCurrentKeys(ctx, existing.credentialKeys)
	}
	var siblings []string
	for sibling, binding := range frozen.bindings {
		if sibling != selector {
			siblings = append(siblings, binding.credentialKeys...)
		}
	}
	if err := frozen.projection.ValidateCurrentKeys(ctx, siblings); err != nil {
		return err
	}
	if err := rt.validateStandingSessionBindings(ctx, frozen.bindings, selector); err != nil {
		return err
	}
	bindings := maps.Clone(frozen.bindings)
	child := standingBindingCredentials{alias: base.alias, plan: base.plan, enabled: true,
		operationID: evidence.operation.OperationID, recoveryCommand: evidence.operation.CredentialRecoveryCommand(),
		admissions: slices.Clone(sealed.admissions), registered: &evidence}
	for _, admission := range sealed.admissions {
		child.credentialKeys = append(child.credentialKeys, admission.StoreKey)
		if admission.Role == sealed.signingRole {
			child.signingKey = admission.StoreKey
		}
	}
	if child.signingKey == "" {
		return channelonboarding.ErrInvalidRequest
	}
	bindings[key] = child
	// The base selects only the native registration route. It owns no default
	// account or signing value and cannot authenticate a request on its own.
	base.enabled, base.registeredOnly, base.blockReason = true, true, ""
	base.credentialKeys, base.admissions, base.operationID, base.recoveryCommand = nil, nil, "", ""
	bindings[selector] = base
	projection, err := rt.sealStandingCredentialBindings(ctx, bindings, key)
	if err != nil {
		return err
	}
	if err := frozen.projection.ValidateCurrentKeys(ctx, siblings); err != nil {
		return err
	}
	rt.standingCredentialAdmission = &standingCredentialAdmission{declarations: frozen.declarations, bindings: bindings, projection: projection}
	return nil
}

func (rt *Runtime) sealStandingCredentialBindings(ctx context.Context, bindings map[string]standingBindingCredentials, selected string) (*credentials.SecretBindingProjection, error) {
	owner, err := credentials.NewSnapshotOwner(rt.Options.ProviderCredentials)
	if err != nil {
		return nil, err
	}
	projection := owner.BeginSecretBindingProjection()
	for entry, binding := range bindings {
		byKey := map[string]channelonboarding.CredentialAdmission{}
		for _, admission := range binding.admissions {
			byKey[admission.StoreKey] = admission
		}
		for _, credential := range binding.credentialKeys {
			admitted, sealed := byKey[credential]
			observed, current, err := observeStandingCredentialRole(ctx, projection, credential, admitted, sealed)
			if err != nil {
				return nil, err
			}
			if entry == selected && (!current || !observed.Present) {
				return nil, &credentials.SecretBindingProjectionStaleError{Key: credential}
			}
		}
	}
	if err := projection.ValidateCurrent(ctx); err != nil {
		return nil, err
	}
	return projection, nil
}

// Source health aggregates the captured accounts; request admission above still
// selects exactly one operation and never authenticates through this aggregate.
func (rt *Runtime) evaluateRegisteredStandingIngress(ctx context.Context, target StandingTarget, subject packs.Subject, presence *credentials.SecretBindingProjection) (packs.Subject, func(context.Context) error, error) {
	expected, err := target.CapabilitySubject()
	if err != nil {
		return packs.Subject{}, nil, err
	}
	normalized, err := packs.NormalizeSubjects([]packs.Subject{subject})
	if err != nil || len(normalized) != 1 || !reflect.DeepEqual(normalized[0], expected) {
		return packs.Subject{}, nil, fmt.Errorf("registered ingress subject contradicts its source admission")
	}
	current := expected
	current.Requirements = nil
	current.Capabilities = nil
	for _, capability := range expected.Capabilities {
		if capability.Code != packs.CapabilityVerifySecret {
			current.Capabilities = append(current.Capabilities, capability)
		}
	}
	rt.standingCredentialMu.Lock()
	var selected []standingBindingCredentials
	for _, binding := range rt.standingCredentialAdmission.bindings {
		if binding.registered != nil && binding.registered.holder.Flow == target.FlowPath && binding.registered.operation.Provider == target.Provider {
			selected = append(selected, binding)
		}
	}
	rt.standingCredentialMu.Unlock()
	if len(selected) == 0 {
		return packs.Subject{}, nil, channelonboarding.ErrRevisionConflict
	}
	enabled := true
	var validators []func(context.Context) error
	for _, binding := range selected {
		accountTarget := target
		accountTarget.SigningSecret = binding.signingKey
		accountSubject, err := accountTarget.CapabilitySubject()
		if err != nil {
			return packs.Subject{}, nil, err
		}
		observed, err := evaluateStandingIngressCapabilitySubject(ctx, accountTarget, accountSubject, presence)
		if err != nil {
			return packs.Subject{}, nil, err
		}
		_, _, validate, err := rt.standingIngressCredentialScope(InboundTarget{BundleHash: target.BundleHash, FlowPath: target.FlowPath,
			RunID: binding.registered.holder.RunID, Alias: target.Alias, Provider: target.Provider,
			SigningSecret: binding.signingKey, RegisteredOperationID: binding.operationID, AdmissionPlan: target.AdmissionPlan})
		if err != nil {
			return packs.Subject{}, nil, err
		}
		if err := validate(ctx); err != nil {
			var stale *credentials.SecretBindingProjectionStaleError
			if !errors.As(err, &stale) {
				return packs.Subject{}, nil, err
			}
			enabled = false
			if len(selected) != 1 {
				return packs.Subject{}, nil, err
			}
			current.TriggerAdmission.BindingBlockReason = string(runlifecycle.StandingBindingRecoveryRequired)
			current.TriggerAdmission.RecoveryOperationID = binding.operationID
			current.TriggerAdmission.RecoveryCommand = binding.recoveryCommand
		}
		validators = append(validators, validate)
		current.Requirements = append(current.Requirements, observed.Requirements...)
		current.Capabilities = append(current.Capabilities, packs.Capability{Code: packs.CapabilityVerifySecret, Target: binding.signingKey})
	}
	current.Status, current.TriggerAdmission.BindingEnabled = "", &enabled
	if !enabled {
		return current, nil, nil
	}
	return current, func(ctx context.Context) error {
		for _, validate := range validators {
			if err := validate(ctx); err != nil {
				return err
			}
		}
		return nil
	}, nil
}

// RegisteredIngressCurrent observes the exact route/operation identity. Value
// currentness belongs to native readiness and request admission, not route
// selection. This observation never refreshes the captured credentials.
func (rt *Runtime) RegisteredIngressCurrent(ctx context.Context, target InboundTarget) (bool, error) {
	rt.standingCredentialMu.Lock()
	admission := rt.standingCredentialAdmission
	present := admission != nil && admission.bindings[registeredStandingSelector(standingIngressSelector(target.FlowPath, target.Provider), target.RegisteredOperationID)].registered != nil
	rt.standingCredentialMu.Unlock()
	if !present || target.RegisteredOperationID == "" {
		return false, nil
	}
	binding, _, _, err := rt.standingIngressCredentialScope(target)
	if err != nil {
		return false, err
	}
	err = rt.validateRegisteredStandingCredential(ctx, *binding.registered, false)
	return err == nil, err
}
