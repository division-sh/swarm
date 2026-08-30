package serveapp

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime"
	runtimepublicingress "github.com/division-sh/swarm/internal/runtime/publicingress"
)

type runtimeProcessInboundHandler struct {
	contexts *runtime.RuntimeContextManager
}

func (h runtimeProcessInboundHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	alias, provider, ok := parseProcessWebhookPath(r.URL.Path)
	if !ok {
		http.Error(w, "expected /webhooks/{alias}/{provider}", http.StatusBadRequest)
		return
	}
	use, lookup, acquireErr := h.contexts.AcquireIngress(r.Context(), alias, providertriggers.NormalizeProviderName(provider))
	if !lookup.Found {
		if lookup.AliasFound {
			http.Error(w, fmt.Sprintf("ingress target %q does not declare provider %q; add that provider binding to the standing singleton flow", alias, provider), http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("no ingress target %q is declared; add ingress to a standing singleton flow", alias), http.StatusNotFound)
		return
	}
	if acquireErr != nil {
		http.Error(w, fmt.Sprintf("ingress target %q provider %q cannot admit work: %v", alias, provider, acquireErr), http.StatusServiceUnavailable)
		return
	}
	if !lookup.Loaded() || use == nil || use.Runtime() == nil || use.Runtime().InboundGateway == nil {
		http.Error(w, fmt.Sprintf("ingress target %q provider %q is unavailable: %s", alias, provider, lookup.Cause), http.StatusServiceUnavailable)
		return
	}
	defer func() { _ = use.Done() }()
	target := lookup.Target
	if registrationTarget, managed := runtimepublicingress.CurrentRegistrationTarget(r.Context()); managed {
		if !sameRuntimeRegistrationTarget(registrationTarget, target) {
			http.Error(w, fmt.Sprintf("ingress target %q provider %q contradicts the current provider registration", alias, provider), http.StatusServiceUnavailable)
			return
		}
		target.SigningSecret = registrationTarget.SigningCredentialKey
	}
	selectedRuntime := use.Runtime()
	selectedRuntime.InboundGateway.HandleResolvedWebhook(w, r.WithContext(use.WorkContext()), runtime.InboundTarget{
		BundleHash: target.BundleHash, ServiceID: target.ServiceID, PackageKey: target.PackageKey,
		FlowID: target.FlowID, RunID: target.RunID, Generation: target.Generation,
		PublicationSequence: target.PublicationSequence, InstanceID: target.InstanceID,
		FlowInstance: target.FlowInstance, EntityID: target.EntityID, EntitySlug: target.Alias,
		Alias: target.Alias, Provider: target.Provider, SigningSecret: target.SigningSecret, AdmissionPlan: target.AdmissionPlan,
	}, use.Context.Source)
}

func sameRuntimeRegistrationTarget(registration runtimepublicingress.RegistrationTarget, target runtime.StandingTarget) bool {
	return strings.TrimSpace(registration.BundleHash) == strings.TrimSpace(target.BundleHash) &&
		strings.TrimSpace(registration.ServiceID) == strings.TrimSpace(target.ServiceID) &&
		strings.TrimSpace(registration.PackageKey) == strings.TrimSpace(target.PackageKey) &&
		strings.TrimSpace(registration.FlowID) == strings.TrimSpace(target.FlowID) &&
		strings.TrimSpace(registration.Alias) == strings.TrimSpace(target.Alias) &&
		strings.TrimSpace(registration.Provider) == strings.TrimSpace(target.Provider) &&
		registration.Generation == target.Generation &&
		registration.PublicationSequence == target.PublicationSequence &&
		registration.AdmissionPlanGeneration.Equal(target.AdmissionPlan.Generation()) &&
		strings.TrimSpace(registration.SigningCredentialKey) != ""
}

func parseProcessWebhookPath(path string) (string, string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 3 || parts[0] != "webhooks" {
		return "", "", false
	}
	alias := strings.TrimSpace(parts[1])
	provider := strings.TrimSpace(parts[2])
	return alias, provider, alias != "" && provider != ""
}
