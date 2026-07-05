package runtime

import (
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	"github.com/google/uuid"
)

const inboundWebhookMaxBodyBytes = 1 << 20

type InboundPersistence interface {
	RecordInboundEvent(ctx context.Context, providerEventID, entityID, provider string) (bool, error)
	ResolveInboundTarget(ctx context.Context, entityKey, provider string) (InboundTarget, error)
	PurgeInboundEventsBefore(ctx context.Context, before time.Time, limit int) (int, error)
}

type InboundFailureRollback interface {
	DeleteInboundEvent(ctx context.Context, providerEventID, entityID, provider string) error
}

type InboundTarget struct {
	EntityID      string
	EntitySlug    string
	WebhookSecret string
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
	mux                     *http.ServeMux
	bus                     *runtimebus.EventBus
	store                   InboundPersistence
	logger                  *RuntimeLogger
	shutdownAdmissionClosed func() bool
	runtimeIngress          *runtimeingress.Controller
	providers               *providertriggers.Registry
}

func NewInboundGateway(bus *runtimebus.EventBus, logger *RuntimeLogger, shutdownAdmissionClosed func() bool, stores ...InboundPersistence) *InboundGateway {
	var store InboundPersistence
	if len(stores) > 0 {
		store = stores[0]
	}
	g := &InboundGateway{
		mux:                     http.NewServeMux(),
		bus:                     bus,
		store:                   store,
		logger:                  logger,
		shutdownAdmissionClosed: shutdownAdmissionClosed,
		providers:               providertriggers.DefaultRegistry(),
	}
	g.mux.HandleFunc("/webhooks/", g.handleWebhook)
	return g
}

func (g *InboundGateway) Handler() http.Handler {
	return g.mux
}

func (g *InboundGateway) SetRuntimeIngress(controller *runtimeingress.Controller) {
	if g == nil {
		return
	}
	g.runtimeIngress = controller
}

func (g *InboundGateway) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if g.shutdownAdmissionClosed != nil && g.shutdownAdmissionClosed() {
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
	entityKey, provider, ok := parseWebhookPath(r.URL.Path)
	if !ok {
		http.Error(w, "expected /webhooks/{entity}/{provider}", http.StatusBadRequest)
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
	if len(body) == 0 {
		body = []byte("{}")
	}
	requestURL := inboundRequestURL(r)
	queryValues, queryParseError := inboundQueryValues(r)
	formValues, formParsed, formParseError := inboundFormValues(r.Header.Get("Content-Type"), body)

	target := InboundTarget{
		EntityID:   entityKey,
		EntitySlug: entityKey,
	}
	if g.store != nil {
		resolved, err := g.store.ResolveInboundTarget(r.Context(), entityKey, provider)
		if err != nil {
			http.Error(w, "unknown entity", http.StatusNotFound)
			return
		}
		target = resolved
	}
	target.NormalizeEntity()
	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		payload = map[string]any{"raw": string(body)}
	}
	entityID := target.EffectiveEntityID()
	entitySlug := target.EffectiveEntitySlug()
	now := time.Now().UTC()
	delivery, err := g.providers.Accept(providertriggers.Request{
		Provider: provider,
		Target: providertriggers.Target{
			EntityID:      target.EntityID,
			EntitySlug:    target.EntitySlug,
			WebhookSecret: target.WebhookSecret,
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
	providerEventID := delivery.ProviderEventID

	if g.store != nil {
		inserted, err := g.store.RecordInboundEvent(r.Context(), providerEventID, entityID, provider)
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
		pubCtx := runtimebus.WithCurrentRuntimeEpoch(r.Context())
		published := events.NewRootIngressEvent(uuid.NewString(), pubType, "inbound-gateway", "", envelopeBytes, 0, "", "", events.EventEnvelope{EntityID: entityID}, now)
		var err error
		if delivery.AcknowledgeBeforeDispatch {
			err = g.bus.PublishAcknowledged(pubCtx, published)
		} else {
			err = g.bus.Publish(pubCtx, published)
		}
		if err != nil {
			if g.logger != nil {
				handleRuntimeLogPersistenceError("inbound-gateway", "publish_failed", g.logger.Error(r.Context(), "inbound-gateway", "publish_failed", map[string]any{
					"provider":          provider,
					"entity_id":         entityID,
					"provider_event_id": providerEventID,
				}, err))
			}
			if rollback, ok := g.store.(InboundFailureRollback); ok && rollback != nil {
				if rollbackErr := rollback.DeleteInboundEvent(r.Context(), providerEventID, entityID, provider); rollbackErr != nil {
					if g.logger != nil {
						handleRuntimeLogPersistenceError("inbound-gateway", "rollback_failed", g.logger.Error(r.Context(), "inbound-gateway", "rollback_failed", map[string]any{
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
