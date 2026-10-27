package serveapp

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimepublicingress "github.com/division-sh/swarm/internal/runtime/publicingress"
)

type runtimeProcessInboundHandler struct {
	contexts           *runtime.RuntimeContextManager
	publicRegistration *runtimepublicingress.ProviderRegistrationController
	pendingClaim       runtimepublicingress.PendingDeploymentClaim
}

func (h runtimeProcessInboundHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	alias, provider, ok := parseProcessWebhookPath(r.URL.Path)
	if !ok {
		http.Error(w, "expected /webhooks/{alias}/{provider}", http.StatusBadRequest)
		return
	}
	lookup := h.contexts.LookupIngress(alias, provider)
	forward := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.pendingClaim != nil && h.publicRegistration != nil && provider == "telegram" {
			consumed, err := h.publicRegistration.ConsumePendingDeploymentClaim(r, lookup.Target.AdmissionPlan, h.pendingClaim)
			if err != nil {
				status := http.StatusServiceUnavailable
				var refusal providertriggers.Error
				if errors.As(err, &refusal) {
					status = refusal.Status
				}
				http.Error(w, "deployment claim admission refused", status)
				return
			}
			if consumed {
				w.WriteHeader(http.StatusOK)
				return
			}
		}
		use, lookup, acquireErr := h.contexts.AcquireIngress(r.Context(), alias, provider)
		if use != nil {
			defer func() { _ = use.Done() }()
		}
		h.serveAcquiredIngress(w, r, use, lookup, acquireErr, alias, provider)
	})
	if h.publicRegistration != nil {
		query, queryErr := url.ParseQuery(r.URL.RawQuery)
		// A callback URL never becomes author traffic after withdrawal/reset.
		// Missing native author evidence is not an authorization shortcut.
		if queryErr != nil || query.Has("swarm_callback_generation") || h.publicRegistration.OwnsRoute(alias, provider) || !lookup.DeclaresAuthorIngress() {
			h.publicRegistration.Handler(forward).ServeHTTP(w, r)
			return
		}
		ctx := r.Context()
		if !h.publicRegistration.ExposureCurrent(ctx) {
			http.NotFound(w, r)
			return
		}
	}
	forward.ServeHTTP(w, r)
}

func (h runtimeProcessInboundHandler) serveAcquiredIngress(w http.ResponseWriter, r *http.Request, use *runtime.RuntimeContextUse, lookup runtime.RuntimeIngressContextLookup, acquireErr error, alias, provider string) {
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
	target := lookup.Target
	inbound := runtime.InboundTarget{
		BundleHash: target.BundleHash, FlowPath: target.FlowPath,
		RunID: target.RunID,
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
		if _, authenticated := runtimepublicingress.CurrentRegistrationTarget(r.Context()); !authenticated && (required || h.publicRegistration.OwnsRoute(target.Alias, target.Provider)) {
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
		strings.TrimSpace(registration.FlowPath) == strings.TrimSpace(target.FlowPath) &&
		registration.Alias == target.Alias &&
		registration.Provider == target.Provider &&
		registration.AdmissionPlanGeneration.Equal(target.AdmissionPlan.Generation()) &&
		strings.TrimSpace(registration.SigningCredentialKey) != ""
}

func parseProcessWebhookPath(path string) (string, string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 3 || parts[0] != "webhooks" {
		return "", "", false
	}
	alias := parts[1]
	provider := parts[2]
	return alias, provider, alias != "" && provider != ""
}
