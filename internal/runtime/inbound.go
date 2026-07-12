package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

const inboundWebhookMaxBodyBytes = 1 << 20

type InboundPersistence interface {
	RecordInboundEvent(ctx context.Context, providerEventID, entityID, provider string) (bool, error)
	PurgeInboundEventsBefore(ctx context.Context, before time.Time, limit int) (int, error)
}

type InboundFailureRollback interface {
	DeleteInboundEvent(ctx context.Context, providerEventID, entityID, provider string) error
}

type InboundTarget struct {
	BundleHash    string
	FlowID        string
	RunID         string
	FlowInstance  string
	EntityID      string
	EntitySlug    string
	Alias         string
	Provider      string
	SigningSecret string
	AdmissionPlan providertriggers.InboundAdmissionPlan
}

func (t InboundTarget) EffectiveEntityID() string {
	return firstNonEmpty(t.EntityID)
}

func (t InboundTarget) EffectiveEntitySlug() string {
	return firstNonEmpty(t.EntitySlug)
}

func (t *InboundTarget) NormalizeEntity() {
	if t == nil {
		return
	}
	entityID := t.EffectiveEntityID()
	entitySlug := t.EffectiveEntitySlug()
	t.EntityID = entityID
	t.EntitySlug = entitySlug
}

type InboundGateway struct {
	bus                     *runtimebus.EventBus
	store                   InboundPersistence
	logger                  *RuntimeLogger
	shutdownAdmissionClosed func() bool
	beginAdmission          func(context.Context) (context.Context, func(), bool)
	runtimeIngress          *runtimeingress.Controller
	credentials             runtimecredentials.Store
}

func (g *InboundGateway) SetAdmissionGuard(begin func(context.Context) (context.Context, func(), bool)) {
	if g != nil {
		g.beginAdmission = begin
	}
}

func NewInboundGateway(bus *runtimebus.EventBus, logger *RuntimeLogger, shutdownAdmissionClosed func() bool, stores ...InboundPersistence) *InboundGateway {
	var store InboundPersistence
	if len(stores) > 0 {
		store = stores[0]
	}
	g := &InboundGateway{
		bus:                     bus,
		store:                   store,
		logger:                  logger,
		shutdownAdmissionClosed: shutdownAdmissionClosed,
	}
	return g
}

func (g *InboundGateway) SetCredentialStore(store runtimecredentials.Store) {
	if g != nil {
		g.credentials = store
	}
}

func (g *InboundGateway) SetRuntimeIngress(controller *runtimeingress.Controller) {
	if g == nil {
		return
	}
	g.runtimeIngress = controller
}

func (g *InboundGateway) HandleResolvedWebhook(w http.ResponseWriter, r *http.Request, target InboundTarget, source semanticview.Source) {
	if g == nil {
		http.Error(w, "runtime ingress unavailable", http.StatusServiceUnavailable)
		return
	}
	g.handleResolvedWebhook(w, r, target, source)
}

func (g *InboundGateway) handleResolvedWebhook(w http.ResponseWriter, r *http.Request, target InboundTarget, source semanticview.Source) {
	if g.beginAdmission != nil {
		admissionCtx, release, admitted := g.beginAdmission(r.Context())
		if !admitted {
			http.Error(w, "runtime shutting down", http.StatusServiceUnavailable)
			return
		}
		defer release()
		r = r.WithContext(admissionCtx)
	} else if g.shutdownAdmissionClosed != nil && g.shutdownAdmissionClosed() {
		http.Error(w, "runtime shutting down", http.StatusServiceUnavailable)
		return
	}
	if g.runtimeIngress != nil {
		if err := g.runtimeIngress.AdmitQueueableIngress(r.Context(), "inbound.webhook"); err != nil {
			http.Error(w, "runtime ingress unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	provider := providertriggers.NormalizeProviderName(target.Provider)
	if provider == "" {
		_, provider, _ = parseWebhookPath(r.URL.Path)
	}
	if provider == "" {
		http.Error(w, "provider is required", http.StatusBadRequest)
		return
	}
	if !target.AdmissionPlan.Valid() {
		http.Error(w, fmt.Sprintf("ingress target %q provider %q has no compiled admission plan; request rejected before provider admission", target.Alias, provider), http.StatusServiceUnavailable)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, inboundWebhookMaxBodyBytes+1))
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}
	if len(body) > inboundWebhookMaxBodyBytes {
		http.Error(w, "webhook body too large", http.StatusRequestEntityTooLarge)
		return
	}
	requestURL := inboundRequestURL(r)
	queryValues, queryParseError := inboundQueryValues(r)
	formValues, formParsed, formParseError := inboundFormValues(r.Header.Get("Content-Type"), body)

	signingValue := ""
	if target.AdmissionPlan.RequiresSecret() {
		if target.SigningSecret == "" {
			http.Error(w, fmt.Sprintf("ingress alias %q provider %q requires a signing secret for %s request authentication", target.Alias, provider, target.AdmissionPlan.RequestAuthentication()), http.StatusServiceUnavailable)
			return
		}
		if g.credentials == nil {
			http.Error(w, fmt.Sprintf("signing secret %s is UNBOUND; run `swarm secrets set %s`", target.SigningSecret, target.SigningSecret), http.StatusServiceUnavailable)
			return
		}
		resolved, ok, err := g.credentials.Get(r.Context(), target.SigningSecret)
		if err != nil {
			http.Error(w, "read signing secret failed", http.StatusServiceUnavailable)
			return
		}
		if !ok || strings.TrimSpace(resolved) == "" {
			http.Error(w, fmt.Sprintf("signing secret %s is UNBOUND; run `swarm secrets set %s`", target.SigningSecret, target.SigningSecret), http.StatusServiceUnavailable)
			return
		}
		signingValue = resolved
	}
	target.NormalizeEntity()
	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		payload = map[string]any{"raw": string(body)}
	}
	entityID := target.EffectiveEntityID()
	entitySlug := target.EffectiveEntitySlug()
	now := time.Now().UTC()
	delivery, err := target.AdmissionPlan.Accept(providertriggers.Request{
		Provider: provider,
		Target: providertriggers.Target{
			EntityID:      target.EntityID,
			EntitySlug:    target.EntitySlug,
			WebhookSecret: signingValue,
		},
		Method:          r.Method,
		URL:             requestURL,
		Body:            body,
		Headers:         r.Header,
		Payload:         payload,
		ContentType:     r.Header.Get("Content-Type"),
		Query:           queryValues,
		QueryParseError: queryParseError,
		Form:            formValues,
		FormParsed:      formParsed,
		FormParseError:  formParseError,
		Received:        now,
		UserAgent:       r.UserAgent(),
	})
	if err != nil {
		status := http.StatusBadRequest
		if providerErr, ok := err.(providertriggers.Error); ok {
			status = providerErr.Status
		}
		http.Error(w, err.Error(), status)
		return
	}
	if delivery.Response != nil {
		status := delivery.Response.Status
		if status == 0 {
			status = http.StatusOK
		}
		contentType := strings.TrimSpace(delivery.Response.ContentType)
		if contentType == "" {
			contentType = "text/plain; charset=utf-8"
		}
		w.Header().Set("content-type", contentType)
		w.WriteHeader(status)
		_, _ = w.Write(delivery.Response.Body)
		return
	}
	if source != nil && !standingInputPinAdmitted(source, target.FlowID, string(delivery.EventName)) {
		http.Error(w, fmt.Sprintf("ingress target %q provider %q resolved event %q, but flow %q in bundle %s has no exact external input pin; add that pin", target.Alias, provider, delivery.EventName, target.FlowID, target.BundleHash), http.StatusUnprocessableEntity)
		return
	}
	providerEventID := delivery.ProviderEventID
	requestCtx := r.Context()
	if strings.TrimSpace(target.RunID) != "" {
		requestCtx = runtimecorrelation.WithRunID(requestCtx, target.RunID)
	}

	if g.store != nil {
		inserted, err := g.store.RecordInboundEvent(requestCtx, providerEventID, entityID, provider)
		if err != nil {
			http.Error(w, "record inbound failed", http.StatusInternalServerError)
			return
		}
		if !inserted {
			writeJSON(w, http.StatusOK, map[string]any{
				"status":              "duplicate",
				"provider":            provider,
				"provider_event_id":   providerEventID,
				"provider_event_type": delivery.ProviderEventType,
				"event_name":          string(delivery.EventName),
			})
			return
		}
	}

	pubType, pubPayload := delivery.EventName, delivery.Payload
	envelopeBytes := mustJSON(pubPayload)
	if g.bus != nil {
		pubCtx := runtimebus.WithCurrentRuntimeEpoch(requestCtx)
		envelope := events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: entityID, FlowInstance: target.FlowInstance})
		published := events.NewRootIngressEvent(uuid.NewString(), pubType, "inbound-gateway", "", envelopeBytes, 0, target.RunID, "", envelope, now)
		var err error
		if delivery.AcknowledgeBeforeDispatch {
			err = g.bus.PublishAcknowledged(pubCtx, published)
		} else {
			err = g.bus.Publish(pubCtx, published)
		}
		if err != nil {
			if g.logger != nil {
				handleRuntimeLogPersistenceError("inbound-gateway", "publish_failed", g.logger.Error(requestCtx, "inbound-gateway", "publish_failed", map[string]any{
					"provider":          provider,
					"entity_id":         entityID,
					"provider_event_id": providerEventID,
				}, err))
			}
			if rollback, ok := g.store.(InboundFailureRollback); ok && rollback != nil {
				if rollbackErr := rollback.DeleteInboundEvent(requestCtx, providerEventID, entityID, provider); rollbackErr != nil {
					if g.logger != nil {
						handleRuntimeLogPersistenceError("inbound-gateway", "rollback_failed", g.logger.Error(requestCtx, "inbound-gateway", "rollback_failed", map[string]any{
							"provider":          provider,
							"entity_id":         entityID,
							"provider_event_id": providerEventID,
						}, rollbackErr))
					}
				}
			}
			http.Error(w, "publish inbound failed", http.StatusServiceUnavailable)
			return
		}
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":              "accepted",
		"entity_id":           entityID,
		"entity_slug":         entitySlug,
		"provider":            provider,
		"provider_event_id":   providerEventID,
		"provider_event_type": delivery.ProviderEventType,
		"event_name":          string(delivery.EventName),
	})
}

func parseWebhookPath(path string) (entityID, provider string, ok bool) {
	p := strings.Trim(path, "/")
	parts := strings.Split(p, "/")
	if len(parts) != 3 || parts[0] != "webhooks" {
		return "", "", false
	}
	entityID = strings.TrimSpace(parts[1])
	provider = strings.TrimSpace(parts[2])
	if entityID == "" || provider == "" {
		return "", "", false
	}
	return entityID, provider, true
}

func inboundRequestURL(r *http.Request) string {
	if r == nil || r.URL == nil || strings.TrimSpace(r.URL.Fragment) != "" {
		return ""
	}
	scheme := "http"
	host := strings.TrimSpace(r.Host)
	if r.URL.IsAbs() {
		if strings.TrimSpace(r.URL.Scheme) == "" {
			return ""
		}
		scheme = strings.TrimSpace(r.URL.Scheme)
		if host == "" {
			host = strings.TrimSpace(r.URL.Host)
		}
	} else if r.TLS != nil {
		scheme = "https"
	}
	if host == "" {
		return ""
	}
	uri := r.URL.RequestURI()
	if strings.TrimSpace(uri) == "" {
		uri = "/"
	}
	return scheme + "://" + host + uri
}

func inboundQueryValues(r *http.Request) (url.Values, string) {
	if r == nil || r.URL == nil || strings.TrimSpace(r.URL.RawQuery) == "" {
		return nil, ""
	}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, err.Error()
	}
	return values, ""
}

func inboundFormValues(contentType string, body []byte) (url.Values, bool, string) {
	contentType = strings.TrimSpace(contentType)
	if contentType == "" {
		return nil, false, ""
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, false, err.Error()
	}
	if !strings.EqualFold(mediaType, "application/x-www-form-urlencoded") {
		return nil, false, ""
	}
	values, err := url.ParseQuery(string(body))
	if err != nil {
		return nil, true, err.Error()
	}
	return values, true, ""
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
