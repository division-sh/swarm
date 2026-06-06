package runtime

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	"github.com/google/uuid"
)

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

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}
	if len(body) == 0 {
		body = []byte("{}")
	}

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
	if !verifyProviderSignature(provider, target.WebhookSecret, body, r.Header) {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		payload = map[string]any{"raw": string(body)}
	}
	entityID := target.EffectiveEntityID()
	entitySlug := target.EffectiveEntitySlug()
	providerEventID := firstNonEmpty(
		r.Header.Get("X-Provider-Event-ID"),
		r.Header.Get("X-Request-ID"),
		extractProviderEventID(payload),
		fingerprintInbound(entityID, provider, body),
	)

	if g.store != nil {
		inserted, err := g.store.RecordInboundEvent(r.Context(), providerEventID, entityID, provider)
		if err != nil {
			http.Error(w, "record inbound failed", http.StatusInternalServerError)
			return
		}
		if !inserted {
			writeJSON(w, http.StatusOK, map[string]any{"status": "duplicate", "provider_event_id": providerEventID})
			return
		}
	}

	now := time.Now().UTC()
	pubType, pubPayload := buildInboundPublishPayload(provider, entityID, providerEventID, payload, now)
	pubPayload["headers"] = map[string]any{
		"user_agent": r.UserAgent(),
	}
	envelopeBytes := mustJSON(pubPayload)
	if g.bus != nil {
		pubCtx := runtimebus.WithCurrentRuntimeEpoch(r.Context())
		if err := g.bus.Publish(pubCtx, events.NewRootIngressEvent(uuid.NewString(), pubType, "inbound-gateway", "", envelopeBytes, 0, "", "", events.EventEnvelope{EntityID: entityID}, now)); err != nil {
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
		"status":            "accepted",
		"entity_id":         entityID,
		"entity_slug":       entitySlug,
		"provider":          provider,
		"provider_event_id": providerEventID,
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

func extractProviderEventID(payload any) string {
	m, ok := payload.(map[string]any)
	if !ok {
		return ""
	}
	for _, key := range []string{"id", "event_id", "message_id"} {
		if v, ok := m[key].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func fingerprintInbound(entityID, provider string, body []byte) string {
	h := sha1.New()
	_, _ = h.Write([]byte(entityID))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write([]byte(provider))
	_, _ = h.Write([]byte("|"))
	_, _ = h.Write(body)
	return "fp:" + hex.EncodeToString(h.Sum(nil))
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func buildInboundPublishPayload(provider, entityID, providerEventID string, rawPayload any, now time.Time) (events.EventType, map[string]any) {
	return events.EventType("inbound." + normalizeEventToken(provider)), map[string]any{
		"entity_id":         strings.TrimSpace(entityID),
		"provider":          strings.TrimSpace(provider),
		"event_type":        resolveProviderEventType(provider, rawPayload),
		"provider_event_id": strings.TrimSpace(providerEventID),
		"payload":           rawPayload,
		"received_at":       now.Format(time.RFC3339),
	}
}

func firstStringByKeys(obj map[string]any, keys ...string) string {
	for _, key := range keys {
		if obj == nil {
			break
		}
		if v, ok := obj[key]; ok {
			switch t := v.(type) {
			case string:
				if s := strings.TrimSpace(t); s != "" {
					return s
				}
			case json.Number:
				if s := strings.TrimSpace(t.String()); s != "" {
					return s
				}
			default:
				if s := strings.TrimSpace(fmt.Sprint(t)); s != "" && s != "<nil>" && s != "map[]" && s != "[]" {
					return s
				}
			}
		}
	}
	return ""
}

func firstStringSliceByKeys(obj map[string]any, keys ...string) []string {
	for _, key := range keys {
		if obj == nil {
			break
		}
		v, ok := obj[key]
		if !ok {
			continue
		}
		switch t := v.(type) {
		case []string:
			out := make([]string, 0, len(t))
			for _, s := range t {
				if trimmed := strings.TrimSpace(s); trimmed != "" {
					out = append(out, trimmed)
				}
			}
			if len(out) > 0 {
				return out
			}
		case []any:
			out := make([]string, 0, len(t))
			for _, item := range t {
				if trimmed := strings.TrimSpace(fmt.Sprint(item)); trimmed != "" && trimmed != "<nil>" {
					out = append(out, trimmed)
				}
			}
			if len(out) > 0 {
				return out
			}
		case string:
			if trimmed := strings.TrimSpace(t); trimmed != "" {
				return []string{trimmed}
			}
		}
	}
	return []string{}
}

func resolveProviderEventType(provider string, payload any) string {
	m, _ := payload.(map[string]any)
	for _, key := range []string{"event_type", "type", "status", "kind", "action"} {
		if v, ok := m[key].(string); ok && strings.TrimSpace(v) != "" {
			return normalizeEventToken(v)
		}
	}
	return "event"
}

func normalizeEventToken(raw string) string {
	token := strings.TrimSpace(strings.ToLower(raw))
	token = strings.ReplaceAll(token, ".", "_")
	token = strings.ReplaceAll(token, "-", "_")
	token = strings.ReplaceAll(token, " ", "_")
	if token == "" {
		return "event"
	}
	return token
}

func verifyProviderSignature(provider, secret string, body []byte, headers http.Header) bool {
	secret = strings.TrimSpace(secret)
	// If no secret is configured, accept unsigned ingress.
	if secret == "" {
		return true
	}
	if sig := strings.TrimSpace(headers.Get("X-Hub-Signature-256")); strings.HasPrefix(strings.ToLower(sig), "sha256=") {
		given := strings.TrimPrefix(sig, "sha256=")
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write(body)
		expected := hex.EncodeToString(mac.Sum(nil))
		return hmac.Equal([]byte(strings.ToLower(given)), []byte(strings.ToLower(expected)))
	}
	if sig := strings.TrimSpace(headers.Get("Stripe-Signature")); sig != "" {
		timestamp := ""
		v1Sigs := make([]string, 0, 2)
		for _, part := range strings.Split(sig, ",") {
			kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
			if len(kv) != 2 {
				continue
			}
			switch strings.TrimSpace(kv[0]) {
			case "t":
				timestamp = strings.TrimSpace(kv[1])
			case "v1":
				v1Sigs = append(v1Sigs, strings.TrimSpace(kv[1]))
			}
		}
		if timestamp == "" || len(v1Sigs) == 0 {
			return false
		}
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write([]byte(timestamp))
		_, _ = mac.Write([]byte("."))
		_, _ = mac.Write(body)
		expected := hex.EncodeToString(mac.Sum(nil))
		for _, candidate := range v1Sigs {
			if hmac.Equal([]byte(strings.ToLower(candidate)), []byte(strings.ToLower(expected))) {
				return true
			}
		}
		return false
	}
	token := strings.TrimSpace(headers.Get("X-Webhook-Token"))
	if token == "" {
		auth := strings.TrimSpace(headers.Get("Authorization"))
		if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			token = strings.TrimSpace(auth[7:])
		}
	}
	return hmac.Equal([]byte(token), []byte(secret))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
