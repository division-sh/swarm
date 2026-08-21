package serveapp

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime"
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

func resolveServeRegistrationPairs(bindings []packs.OutboundBindingPlan, manager *runtime.RuntimeContextManager) ([]runtimepublicingress.RegistrationPair, error) {
	targets := []runtime.StandingTarget{}
	if manager != nil {
		for _, contextDef := range manager.LoadedContexts() {
			targets = append(targets, contextDef.StandingTargets...)
		}
	}
	pairs := make([]runtimepublicingress.RegistrationPair, 0)
	for _, binding := range bindings {
		rawSelector := binding.RegistrationTarget()
		if rawSelector == "" {
			continue
		}
		selector, err := runtimepublicingress.ParseTargetSelector(rawSelector)
		if err != nil {
			return nil, fmt.Errorf("channels.bindings.%s.register: %w", binding.BindingID(), err)
		}
		registration, ok := binding.Registration()
		if !ok {
			return nil, fmt.Errorf("channels.bindings.%s.register has no compiled channel registration owner", binding.BindingID())
		}
		matches := make([]runtime.StandingTarget, 0, 1)
		for _, target := range targets {
			if strings.TrimSpace(target.PackageKey) == selector.PackageKey && strings.TrimSpace(target.FlowID) == selector.FlowID && strings.TrimSpace(target.Provider) == selector.Provider {
				matches = append(matches, target)
			}
		}
		if len(matches) != 1 {
			return nil, fmt.Errorf("channels.bindings.%s.register %q resolves to %d loaded ingress targets; require exactly one", binding.BindingID(), rawSelector, len(matches))
		}
		target := matches[0]
		planGeneration, err := binding.PlanGeneration()
		if err != nil {
			return nil, err
		}
		if !target.AdmissionPlan.RequiresSecret() {
			return nil, fmt.Errorf("channels.bindings.%s.register target %q requires a signing credential role but the exact ingress target is %s", binding.BindingID(), rawSelector, target.AdmissionPlan.RequestAuthentication())
		}
		if strings.TrimSpace(target.SigningSecret) == "" {
			return nil, fmt.Errorf("channels.bindings.%s.register target %q has no signing credential binding", binding.BindingID(), rawSelector)
		}
		pairs = append(pairs, runtimepublicingress.RegistrationPair{
			BindingID: binding.BindingID(), PlanGeneration: planGeneration, Registration: registration,
			CredentialKeys: binding.CredentialStoreKeys(),
			Target: runtimepublicingress.RegistrationTarget{
				Selector: rawSelector, BundleHash: target.BundleHash, ServiceID: target.ServiceID,
				PackageKey: target.PackageKey, FlowID: target.FlowID, Alias: target.Alias, Provider: target.Provider,
				Generation: target.Generation, PublicationSequence: target.PublicationSequence,
				AdmissionPlanGeneration: target.AdmissionPlan.Generation(), SigningCredentialKey: target.SigningSecret,
			},
		})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].BindingID != pairs[j].BindingID {
			return pairs[i].BindingID < pairs[j].BindingID
		}
		return pairs[i].Target.Selector < pairs[j].Target.Selector
	})
	return pairs, nil
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
