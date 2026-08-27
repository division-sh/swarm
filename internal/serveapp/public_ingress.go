package serveapp

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime"
	runtimechannelactivation "github.com/division-sh/swarm/internal/runtime/channelactivation"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimepublicingress "github.com/division-sh/swarm/internal/runtime/publicingress"
)

func resolveServePublicIngressMode(opts cliapp.ServeOptions) (string, bool, error) {
	externalOrigin := strings.TrimSpace(opts.PublicWebhookBaseURL)
	externalListen := strings.TrimSpace(opts.PublicWebhookListen)
	enabled := opts.Expose || externalOrigin != "" || externalListen != ""
	if enabled && !opts.Dev {
		return "", false, fmt.Errorf("--expose and external public webhook options require --dev")
	}
	if opts.Expose && (externalOrigin != "" || externalListen != "") {
		return "", false, fmt.Errorf("--expose is mutually exclusive with --public-webhook-base-url and --public-webhook-listen")
	}
	if (externalOrigin == "") != (externalListen == "") {
		return "", false, fmt.Errorf("--public-webhook-base-url and --public-webhook-listen must be set together")
	}
	if opts.Expose {
		return runtimepublicingress.ModeManagedQuickTunnel, true, nil
	}
	if externalOrigin != "" {
		return runtimepublicingress.ModeExternalOrigin, true, nil
	}
	return "", false, nil
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

func resolveServeRegistrationPairs(snapshot serveChannelActivationSnapshot, manager *runtime.RuntimeContextManager) (*serveRegistrationSelection, error) {
	if manager == nil {
		return nil, fmt.Errorf("runtime context manager is required for provider registration")
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
			activationGenerations[contextDef.BundleHash()+"\x00"+fmt.Sprint(contextDef.PublicationGeneration)] = lease.Generation()
		}
	}
	for _, activation := range activations {
		binding := activation.Plan
		rawSelector := binding.RegistrationTarget()
		if rawSelector == "" {
			continue
		}
		if _, err := runtimepublicingress.ParseTargetSelector(rawSelector); err != nil {
			return fail(fmt.Errorf("channels.bindings.%s.register: %w", binding.BindingID(), err))
		}
		registration, ok := binding.Registration()
		if !ok {
			return fail(fmt.Errorf("channels.bindings.%s.register has no compiled channel registration owner", binding.BindingID()))
		}
		contextDef, err := exactActivationContext(contexts, activation.Coordinate)
		if err != nil {
			return fail(fmt.Errorf("channels.bindings.%s.register: %w", binding.BindingID(), err))
		}
		target, err := exactContextTarget(contextDef, rawSelector)
		if err != nil {
			return fail(fmt.Errorf("channels.bindings.%s.register: %w", binding.BindingID(), err))
		}
		planGeneration, err := binding.PlanGeneration()
		if err != nil {
			return fail(err)
		}
		if !target.AdmissionPlan.RequiresSecret() {
			return fail(fmt.Errorf("channels.bindings.%s.register target %q requires a signing credential role but the exact ingress target is %s", binding.BindingID(), rawSelector, target.AdmissionPlan.RequestAuthentication()))
		}
		if strings.TrimSpace(target.SigningSecret) == "" {
			return fail(fmt.Errorf("channels.bindings.%s.register target %q has no signing credential binding", binding.BindingID(), rawSelector))
		}
		credentialKeys := binding.CredentialStoreKeys()
		signingCredentialKey := strings.TrimSpace(credentialKeys[registration.SigningCredential()])
		if signingCredentialKey == "" {
			return fail(fmt.Errorf("channels.bindings.%s.register signing credential role %q has no admitted store key", binding.BindingID(), registration.SigningCredential()))
		}
		pairs = append(pairs, runtimepublicingress.RegistrationPair{
			BindingID: binding.BindingID(), PlanGeneration: planGeneration,
			ChannelActivationGeneration: activationGenerations[activation.Coordinate.BundleHash+"\x00"+fmt.Sprint(activation.Coordinate.ContextPublicationGeneration)],
			Registration:                registration,
			CredentialKeys:              credentialKeys,
			Target: runtimepublicingress.RegistrationTarget{
				Selector: rawSelector, BundleHash: target.BundleHash, ServiceID: target.ServiceID,
				PackageKey: target.PackageKey, FlowID: target.FlowID, Alias: target.Alias, Provider: target.Provider,
				Generation: target.Generation, PublicationSequence: target.PublicationSequence,
				AdmissionPlanGeneration: target.AdmissionPlan.Generation(), SigningCredentialKey: signingCredentialKey,
			},
		})
	}
	pairIndexes := make(map[string]int, len(pairs))
	for index, pair := range pairs {
		pairIndexes[serveRegistrationPairKey(pair)] = index
	}
	for _, intent := range snapshot.Prebinding {
		pair, err := resolveServePrebindingRegistrationPair(intent)
		if err != nil {
			return fail(err)
		}
		key := serveRegistrationPairKey(pair)
		if index, replacing := pairIndexes[key]; replacing {
			pairs[index] = pair
			continue
		}
		pairIndexes[key] = len(pairs)
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
		PrebindingOperationID: intent.Operation.OperationID,
		Registration:          registration, CredentialKeys: credentials,
		Target: runtimepublicingress.RegistrationTarget{
			Selector: target.Selector, BundleHash: intent.Candidate.Coordinate.BundleHash, ServiceID: target.ServiceID,
			PackageKey: target.PackageKey, FlowID: target.FlowID, Alias: target.Alias, Provider: target.Provider,
			Generation: int64(target.Generation), PublicationSequence: target.PublicationSequence,
			AdmissionPlanGeneration: target.AdmissionGeneration, SigningCredentialKey: credentials[intent.Candidate.SigningCredentialRole],
		},
	}, nil
}

func serveRegistrationPairKey(pair runtimepublicingress.RegistrationPair) string {
	return strings.TrimSpace(pair.BindingID) + "\x00" + strings.TrimSpace(pair.Target.Selector)
}

func exactActivationContext(contexts []runtime.BundleContext, coordinate channelonboarding.ChannelRuntimeContextCoordinate) (runtime.BundleContext, error) {
	matches := []runtime.BundleContext{}
	for _, contextDef := range contexts {
		bundleHash, bundleSource := contextDef.BundleSourceFact.StorageValues()
		if bundleHash == coordinate.BundleHash && bundleSource == coordinate.BundleSource && contextDef.PublicationGeneration == coordinate.ContextPublicationGeneration {
			matches = append(matches, contextDef)
		}
	}
	if len(matches) != 1 {
		return runtime.BundleContext{}, fmt.Errorf("channel activation resolves to %d exact current runtime contexts; require one", len(matches))
	}
	return matches[0], nil
}

func exactContextTarget(contextDef runtime.BundleContext, rawSelector string) (runtime.StandingTarget, error) {
	selector, err := runtimepublicingress.ParseTargetSelector(rawSelector)
	if err != nil {
		return runtime.StandingTarget{}, err
	}
	matches := []runtime.StandingTarget{}
	for _, target := range contextDef.StandingTargets {
		if strings.TrimSpace(target.PackageKey) == selector.PackageKey && strings.TrimSpace(target.FlowID) == selector.FlowID && strings.TrimSpace(target.Provider) == selector.Provider {
			matches = append(matches, target)
		}
	}
	if len(matches) != 1 {
		return runtime.StandingTarget{}, fmt.Errorf("registration target %q resolves to %d targets in exact runtime context %s; require one", rawSelector, len(matches), contextDef.BundleHash())
	}
	return matches[0], nil
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

func publicIngressPresentation(facts []serveLifecycleIngressFact, snapshot runtimepublicingress.Snapshot) []serveLifecycleIngressFact {
	result := append([]serveLifecycleIngressFact(nil), facts...)
	registrationURLs := make(map[string]string, len(snapshot.Registrations))
	for _, registration := range snapshot.Registrations {
		registrationURLs[strings.TrimSpace(registration.Alias)+"\x00"+strings.TrimSpace(registration.Provider)] = strings.TrimSpace(registration.CallbackURL)
	}
	origin := ""
	if snapshot.Exposure != nil {
		origin = strings.TrimRight(strings.TrimSpace(snapshot.Exposure.PublicOrigin), "/")
	}
	for index := range result {
		key := strings.TrimSpace(result[index].Alias) + "\x00" + strings.TrimSpace(result[index].Provider)
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
