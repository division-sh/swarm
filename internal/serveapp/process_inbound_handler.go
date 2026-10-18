package serveapp

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimepublicingress "github.com/division-sh/swarm/internal/runtime/publicingress"
)

type runtimeProcessInboundHandler struct {
	contexts           *runtime.RuntimeContextManager
	publicRegistration *runtimepublicingress.ProviderRegistrationController
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
		diaglog.ProcessLog(diaglog.LevelWarn, "serve-ingress", "runtime acquisition refused",
			"alias", alias, "provider", provider, "error", acquireErr.Error())
		http.Error(w, "ingress target is unavailable", http.StatusServiceUnavailable)
		return
	}
	if !lookup.Loaded() || use == nil || use.Runtime() == nil || use.Runtime().InboundGateway == nil {
		diaglog.ProcessLog(diaglog.LevelWarn, "serve-ingress", "runtime target unavailable",
			"alias", alias, "provider", provider, "state", lookup.State, "cause", lookup.Cause)
		http.Error(w, "ingress target is unavailable", http.StatusServiceUnavailable)
		return
	}
	defer func() { _ = use.Done() }()
	target := lookup.Target
	inbound := runtime.InboundTarget{
		BundleHash: target.BundleHash, ServiceID: target.ServiceID, FlowPath: target.FlowPath,
		RunID: target.RunID, Generation: target.Generation, PublicationSequence: target.PublicationSequence,
		Alias: target.Alias, Provider: target.Provider, SigningSecret: target.SigningSecret, AdmissionPlan: target.AdmissionPlan,
	}
	var required bool
	if h.publicRegistration != nil {
		var err error
		inbound, required, err = use.AdmitPublicIngressTarget(inbound)
		if err != nil {
			http.Error(w, "ingress registration ownership is unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	forward := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveAcquiredProcessWebhook(w, r, use, target, inbound, alias, provider)
	})
	if h.publicRegistration != nil {
		if !h.publicRegistration.ExposureCurrent(use.WorkContext()) {
			http.NotFound(w, r)
			return
		}
		if required || h.publicRegistration.OwnsRoute(target.Alias, target.Provider) {
			h.publicRegistration.Handler(forward).ServeHTTP(w, r)
			return
		}
	}
	forward.ServeHTTP(w, r)
}

func serveAcquiredProcessWebhook(w http.ResponseWriter, r *http.Request, use *runtime.RuntimeContextUse, target runtime.StandingTarget, inbound runtime.InboundTarget, alias, provider string) {
	if registrationTarget, managed := runtimepublicingress.CurrentRegistrationTarget(r.Context()); managed {
		if !sameRuntimeRegistrationTarget(registrationTarget, target) {
			http.Error(w, fmt.Sprintf("ingress target %q provider %q contradicts the current provider registration", alias, provider), http.StatusServiceUnavailable)
			return
		}
		inbound.SigningSecret = registrationTarget.SigningCredentialKey
	}
	selectedRuntime := use.Runtime()
	selectedRuntime.InboundGateway.HandleResolvedWebhook(w, r.WithContext(use.WorkContext()), inbound, use.Context.Source)
}

func sameRuntimeRegistrationTarget(registration runtimepublicingress.RegistrationTarget, target runtime.StandingTarget) bool {
	return strings.TrimSpace(registration.BundleHash) == strings.TrimSpace(target.BundleHash) &&
		strings.TrimSpace(registration.ServiceID) == strings.TrimSpace(target.ServiceID) &&
		strings.TrimSpace(registration.FlowPath) == strings.TrimSpace(target.FlowPath) &&
		registration.Alias == target.Alias &&
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
	alias := parts[1]
	provider := strings.TrimSpace(parts[2])
	return alias, provider, alias != "" && provider != ""
}
