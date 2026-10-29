package serveapp

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime"
	runtimechannelactivation "github.com/division-sh/swarm/internal/runtime/channelactivation"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimepublicingress "github.com/division-sh/swarm/internal/runtime/publicingress"
)

func serveRegistrationSelectionCurrent(ctx context.Context, manager *runtime.RuntimeContextManager, store channelonboarding.Store, pair runtimepublicingress.RegistrationPair, exactPendingRevision bool) (bool, error) {
	if pair.ActivationSource == channelonboarding.ActivationSourceDeclared {
		return pair.OnboardingOperationID == "" && pair.ChannelActivationGeneration.Valid(), nil
	}
	if pair.ActivationSource == channelonboarding.ActivationSourceRegistered {
		scoped, ok := store.(interface {
			DeploymentChannelOperations() channelonboarding.OperationStore
		})
		if !ok {
			return false, channelonboarding.ErrInvalidRequest
		}
		op, err := channelonboarding.RegisteredAccount(ctx, scoped.DeploymentChannelOperations(), channelonboarding.AccountReference{
			Provider: pair.Target.Provider, Account: pair.OperatorAccount,
		})
		if err == channelonboarding.ErrNotFound {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		current := op.OperationID == pair.OnboardingOperationID && pair.OnboardingRevision > 0 && op.Revision >= pair.OnboardingRevision &&
			(!exactPendingRevision || op.Revision == pair.OnboardingRevision) && pair.ActivationRevision == 0 &&
			slices.Equal(op.CredentialAdmissions, pair.CredentialAdmissions)
		if !current {
			return false, nil
		}
		holders, err := manager.AccountHolders(ctx, channelonboarding.AccountReference{Provider: op.Provider, Account: op.AccountReference})
		if err != nil || len(holders) != 1 || holders[0] != pair.AccountHolder {
			return false, err
		}
		return serveRegisteredIngressCurrent(ctx, manager, pair.AccountHolder, pair.Target)
	}
	return channelonboarding.AdmissionResponsibilityCurrent(ctx, store, channelonboarding.AdmissionResponsibility{
		OperationID: pair.OnboardingOperationID, OperationRevision: pair.OnboardingRevision,
		ActivationRevision: pair.ActivationRevision, Coordinate: pair.OnboardingCoordinate,
		TargetSelector: pair.Target.Selector, Provider: pair.Target.Provider, Credentials: pair.CredentialAdmissions,
	}, exactPendingRevision)
}

func serveRegisteredIngressCurrent(ctx context.Context, manager *runtime.RuntimeContextManager, holder channelonboarding.AccountHolder, target runtimepublicingress.RegistrationTarget) (current bool, resultErr error) {
	use, _, availability, err := manager.AcquireRun(ctx, holder.RunID)
	if err != nil || use == nil {
		return false, err
	}
	defer func() { resultErr = errors.Join(resultErr, use.Done()) }()
	if !availability.Available() || use.Context.BundleHash() != holder.BundleHash || holder.BundleHash != target.BundleHash || holder.Flow != target.FlowPath {
		return false, channelonboarding.ErrRevisionConflict
	}
	for _, owned := range use.Context.StandingTargets {
		if owned.FlowPath != holder.Flow || owned.Provider != target.Provider || owned.Alias != target.Alias {
			continue
		}
		if !owned.AdmissionPlan.Generation().Equal(target.AdmissionPlanGeneration) || owned.RunID != holder.RunID {
			return false, channelonboarding.ErrRevisionConflict
		}
		return use.Runtime().RegisteredIngressCurrent(use.WorkContext(), runtime.InboundTarget{
			BundleHash: target.BundleHash, FlowPath: target.FlowPath, RunID: holder.RunID,
			Alias: target.Alias, Provider: target.Provider, SigningSecret: target.SigningCredentialKey,
			RegisteredOperationID: target.RegisteredOperationID, AdmissionPlan: owned.AdmissionPlan,
		})
	}
	return false, nil
}

type serveRegistrationSelection struct {
	Pairs  []runtimepublicingress.RegistrationPair
	leases []*runtimechannelactivation.Lease
	once   sync.Once
}

func (s *serveRegistrationSelection) Release() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		for _, lease := range s.leases {
			lease.Release()
		}
		s.leases = nil
	})
}

func resolveServeRegistrationPairs(ctx context.Context, snapshot serveChannelActivationSnapshot, manager *runtime.RuntimeContextManager) (*serveRegistrationSelection, error) {
	if manager == nil {
		return nil, fmt.Errorf("runtime context manager is required for provider registration")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	contexts := manager.LoadedContexts()
	selection := &serveRegistrationSelection{}
	fail := func(err error) (*serveRegistrationSelection, error) {
		selection.Release()
		return nil, err
	}
	pairs := make([]runtimepublicingress.RegistrationPair, 0)
	activations := []channelonboarding.CompiledActivation{}
	activationGenerations := map[string]channelonboarding.ChannelActivationGeneration{}
	for _, contextDef := range contexts {
		lease, current, err := manager.AcquireChannelActivationPublication(contextDef.BundleHash(), contextDef.PublicationGeneration)
		if err != nil {
			return fail(err)
		}
		if current {
			selection.leases = append(selection.leases, lease)
			activations = append(activations, lease.Activations()...)
			activationGenerations[contextDef.BundleHash()+"\x00"+contextDef.RuntimeInstanceID+"\x00"+fmt.Sprint(contextDef.PublicationGeneration)] = lease.Generation()
		}
	}
	for _, activation := range activations {
		generation := activationGenerations[activation.Coordinate.BundleHash+"\x00"+activation.Coordinate.RuntimeInstanceID+"\x00"+fmt.Sprint(activation.Coordinate.ContextPublicationGeneration)]
		pair, present, err := resolveServeActivationRegistrationPair(ctx, manager, contexts, activation, generation)
		if err != nil {
			return fail(err)
		}
		if present {
			pairs = append(pairs, pair)
		}
	}
	for _, intent := range snapshot.Prebinding {
		pair, err := resolveServePrebindingRegistrationPair(intent)
		if err != nil {
			return fail(err)
		}
		pairs = append(pairs, pair)
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].BindingID != pairs[j].BindingID {
			return pairs[i].BindingID < pairs[j].BindingID
		}
		return pairs[i].Target.Selector < pairs[j].Target.Selector
	})
	selection.Pairs = pairs
	return selection, nil
}

func resolveServeActivationRegistrationPair(ctx context.Context, manager *runtime.RuntimeContextManager, contexts []runtime.BundleContext, activation channelonboarding.CompiledActivation, generation channelonboarding.ChannelActivationGeneration) (runtimepublicingress.RegistrationPair, bool, error) {
	var absent runtimepublicingress.RegistrationPair
	binding := activation.Plan
	rawSelector := binding.RegistrationTarget()
	if binding.Transport() == packs.ChannelTransportSession || rawSelector == "" {
		return absent, false, nil
	}
	selector, err := runtimepublicingress.ParseTargetSelector(rawSelector)
	if err != nil {
		return absent, false, fmt.Errorf("channels.bindings.%s.register: %w", binding.BindingID(), err)
	}
	registration, ok := binding.Registration()
	if !ok {
		return absent, false, fmt.Errorf("channels.bindings.%s.register has no compiled channel registration owner", binding.BindingID())
	}
	contextDef, err := exactActivationContext(contexts, activation.Coordinate)
	if err != nil {
		return absent, false, fmt.Errorf("channels.bindings.%s.register: %w", binding.BindingID(), err)
	}
	target, targetErr := exactContextTarget(contextDef, rawSelector)
	if targetErr == nil && !target.AdmissionPlan.RequiresSecret() {
		return absent, false, fmt.Errorf("channels.bindings.%s.register target %q requires a signing credential role but the exact ingress target is %s", binding.BindingID(), rawSelector, target.AdmissionPlan.RequestAuthentication())
	}
	if activation.Source == channelonboarding.ActivationSourceDeclared {
		enabled, err := serveDeclaredRegistrationEnabled(ctx, manager, contextDef, binding)
		if err != nil || !enabled {
			return absent, false, err
		}
	}
	var holder channelonboarding.AccountHolder
	registeredOperationID := ""
	if activation.Source == channelonboarding.ActivationSourceRegistered {
		holders, err := manager.AccountHolders(ctx, channelonboarding.AccountReference{Provider: registration.Provider(), Account: activation.OperatorAccount})
		if err != nil {
			return absent, false, err
		}
		if len(holders) != 1 || holders[0].BundleHash != contextDef.BundleHash() || holders[0].Flow != selector.FlowPath {
			return absent, false, nil
		}
		holder = holders[0]
		registeredOperationID = activation.OnboardingOperationID
		registeredTarget := runtimepublicingress.RegistrationTarget{
			BundleHash: contextDef.BundleHash(), FlowPath: selector.FlowPath, Alias: target.Alias, Provider: registration.Provider(),
			RegisteredOperationID: activation.OnboardingOperationID, SigningCredentialKey: binding.CredentialStoreKeys()[registration.SigningCredential()],
			AdmissionPlanGeneration: target.AdmissionPlan.Generation(),
		}
		current, err := serveRegisteredIngressCurrent(ctx, manager, holder, registeredTarget)
		if err != nil || !current {
			return absent, false, err
		}
		if holder.RunID != target.RunID {
			return absent, false, channelonboarding.ErrRevisionConflict
		}
	}
	if targetErr != nil {
		return absent, false, fmt.Errorf("channels.bindings.%s.register: %w", binding.BindingID(), targetErr)
	}
	planGeneration, err := binding.PlanGeneration()
	if err != nil {
		return absent, false, err
	}
	if strings.TrimSpace(target.SigningSecret) == "" {
		return absent, false, fmt.Errorf("channels.bindings.%s.register target %q has no signing credential binding", binding.BindingID(), rawSelector)
	}
	credentialKeys := binding.CredentialStoreKeys()
	signingCredentialKey := strings.TrimSpace(credentialKeys[registration.SigningCredential()])
	if signingCredentialKey == "" {
		return absent, false, fmt.Errorf("channels.bindings.%s.register signing credential role %q has no admitted store key", binding.BindingID(), registration.SigningCredential())
	}
	return runtimepublicingress.RegistrationPair{
		BindingID: binding.BindingID(), PlanGeneration: planGeneration, ActivationSource: activation.Source,
		OperatorAccount: activation.OperatorAccount, AccountHolder: holder, OnboardingOperationID: activation.OnboardingOperationID,
		OnboardingRevision: activation.OnboardingRevision, ActivationRevision: activation.ActivationRevision,
		OnboardingCoordinate: activation.Coordinate, ChannelActivationGeneration: generation,
		Registration: registration, CredentialKeys: credentialKeys,
		CredentialAdmissions: append([]channelonboarding.CredentialAdmission(nil), activation.CredentialAdmissions...),
		Target: runtimepublicingress.RegistrationTarget{
			Selector: rawSelector, BundleHash: target.BundleHash, FlowPath: target.FlowPath, Alias: target.Alias, Provider: target.Provider,
			AdmissionPlanGeneration: target.AdmissionPlan.Generation(), SigningCredentialKey: signingCredentialKey,
			RegisteredOperationID: registeredOperationID,
		},
	}, true, nil
}

func resolveServePrebindingRegistrationPair(intent servePrebindingActivation) (runtimepublicingress.RegistrationPair, error) {
	registration, ok := intent.Candidate.Plan.Registration()
	if !ok {
		return runtimepublicingress.RegistrationPair{}, fmt.Errorf("channel onboarding operation %s has no compiled registration owner", intent.Operation.OperationID)
	}
	generation, err := intent.Candidate.Plan.Generation()
	if err != nil {
		return runtimepublicingress.RegistrationPair{}, err
	}
	credentials := map[string]string{}
	for _, admission := range intent.Operation.CredentialAdmissions {
		credentials[admission.Role] = admission.StoreKey
	}
	if len(credentials) == 0 {
		return runtimepublicingress.RegistrationPair{}, fmt.Errorf("channel onboarding operation %s has no admitted credentials", intent.Operation.OperationID)
	}
	target := intent.Candidate.Target
	return runtimepublicingress.RegistrationPair{
		BindingID: channelonboarding.LearnedBindingID(intent.Operation.SlotKey), PlanGeneration: generation,
		OnboardingOperationID: intent.Operation.OperationID, PrebindingOperationID: intent.Operation.OperationID,
		OnboardingRevision: intent.Operation.Revision, OnboardingCoordinate: intent.Operation.Coordinate,
		Registration: registration, CredentialKeys: credentials,
		CredentialAdmissions: append([]channelonboarding.CredentialAdmission(nil), intent.Operation.CredentialAdmissions...),
		Target: runtimepublicingress.RegistrationTarget{
			Selector: target.Selector, BundleHash: intent.Candidate.Coordinate.BundleHash,
			FlowPath: target.FlowPath, Alias: target.Alias, Provider: target.Provider,
			AdmissionPlanGeneration: target.AdmissionGeneration, SigningCredentialKey: credentials[intent.Candidate.SigningCredentialRole],
		},
	}, nil
}

func exactActivationContext(contexts []runtime.BundleContext, coordinate channelonboarding.ChannelRuntimeContextCoordinate) (runtime.BundleContext, error) {
	matches := []runtime.BundleContext{}
	for _, contextDef := range contexts {
		bundleHash := contextDef.SourceArtifactFact.BundleHash()
		if bundleHash == coordinate.BundleHash &&
			contextDef.RuntimeInstanceID == coordinate.RuntimeInstanceID && contextDef.PublicationGeneration == coordinate.ContextPublicationGeneration {
			matches = append(matches, contextDef)
		}
	}
	if len(matches) != 1 {
		return runtime.BundleContext{}, fmt.Errorf("channel activation resolves to %d exact current runtime contexts; require one", len(matches))
	}
	return matches[0], nil
}

func exactContextTarget(contextDef runtime.BundleContext, rawSelector string) (runtime.StandingTarget, error) {
	target, found, err := findContextTarget(contextDef, rawSelector)
	if err != nil {
		return runtime.StandingTarget{}, err
	}
	if !found {
		return runtime.StandingTarget{}, fmt.Errorf("registration target %q has no enabled target in exact runtime context %s", rawSelector, contextDef.BundleHash())
	}
	return target, nil
}

func findContextTarget(contextDef runtime.BundleContext, rawSelector string) (runtime.StandingTarget, bool, error) {
	selector, err := runtimepublicingress.ParseTargetSelector(rawSelector)
	if err != nil {
		return runtime.StandingTarget{}, false, err
	}
	matches := []runtime.StandingTarget{}
	for _, target := range contextDef.StandingTargets {
		if strings.TrimSpace(target.FlowPath) == selector.FlowPath && strings.TrimSpace(target.Provider) == selector.Provider {
			matches = append(matches, target)
		}
	}
	if len(matches) > 1 {
		return runtime.StandingTarget{}, false, fmt.Errorf("registration target %q resolves to %d targets in exact runtime context %s; require one", rawSelector, len(matches), contextDef.BundleHash())
	}
	if len(matches) == 0 {
		return runtime.StandingTarget{}, false, nil
	}
	return matches[0], true, nil
}

func startServePublicIngressRenewal(
	ctx context.Context,
	owner *worklifetime.Process,
	exposure *runtimepublicingress.Controller,
	reconcile func(context.Context, runtimepublicingress.Generation) error,
) error {
	if owner == nil || exposure == nil || reconcile == nil {
		return fmt.Errorf("public ingress renewal dependencies are incomplete")
	}
	lease, err := owner.Begin(ctx)
	if err != nil {
		return err
	}
	go func() {
		defer func() { _ = lease.Done() }()
		ticker := time.NewTicker(runtimepublicingress.RenewalInterval)
		defer ticker.Stop()
		for {
			select {
			case <-lease.Context().Done():
				return
			case <-ticker.C:
				if err := exposure.Renew(lease.Context()); err != nil {
					log.Printf("public ingress reachability renewal failed: %v", err)
					continue
				}
				if err := reconcile(lease.Context(), exposure.Generation()); err != nil {
					log.Printf("provider registration renewal failed: %v", err)
				}
			}
		}
	}()
	return nil
}

func sessionIngressFact(fact serveLifecycleIngressFact) bool {
	return fact.Subject.TriggerAdmission != nil && fact.Subject.TriggerAdmission.Transport == packs.ChannelTransportSession
}

func requiresPublicIngressPresentation(facts []serveLifecycleIngressFact) bool {
	for _, fact := range facts {
		if !sessionIngressFact(fact) {
			return true
		}
	}
	return false
}

func publicIngressPresentation(facts []serveLifecycleIngressFact, snapshot runtimepublicingress.Snapshot) []serveLifecycleIngressFact {
	result := append([]serveLifecycleIngressFact(nil), facts...)
	registrationURLs := make(map[string]string, len(snapshot.Registrations))
	for _, registration := range snapshot.Registrations {
		registrationURLs[registration.Alias+"\x00"+strings.TrimSpace(registration.Provider)] = strings.TrimSpace(registration.CallbackURL)
	}
	origin := ""
	if snapshot.Exposure != nil {
		origin = strings.TrimRight(strings.TrimSpace(snapshot.Exposure.PublicOrigin), "/")
	}
	for index := range result {
		if sessionIngressFact(result[index]) {
			result[index].URL = "in-process session"
			continue
		}
		key := result[index].Alias + "\x00" + strings.TrimSpace(result[index].Provider)
		if callback := registrationURLs[key]; callback != "" {
			result[index].URL = callback
		} else if snapshot.PublicIngressEnabled && origin != "" {
			result[index].URL = origin + "/webhooks/" + result[index].Alias + "/" + result[index].Provider
		} else {
			result[index].URL = "not exposed"
		}
	}
	return result
}
