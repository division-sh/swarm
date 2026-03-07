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
	"log"
	"net/http"
	"strings"
	"time"

	"empireai/internal/events"
	runtimebus "empireai/internal/runtime/bus"
	"github.com/google/uuid"
)

type InboundPersistence interface {
	RecordInboundEvent(ctx context.Context, providerEventID, verticalID, provider string) (bool, error)
	ResolveInboundTarget(ctx context.Context, verticalKey, provider string) (InboundTarget, error)
	PurgeInboundEventsBefore(ctx context.Context, before time.Time, limit int) (int, error)
}

type InboundTarget struct {
	VerticalID    string
	VerticalSlug  string
	WebhookSecret string
}

type InboundGateway struct {
	mux   *http.ServeMux
	bus   *EventBus
	store InboundPersistence
}

func NewInboundGateway(bus *EventBus, stores ...InboundPersistence) *InboundGateway {
	var store InboundPersistence
	if len(stores) > 0 {
		store = stores[0]
	}
	g := &InboundGateway{
		mux:   http.NewServeMux(),
		bus:   bus,
		store: store,
	}
	g.mux.HandleFunc("/webhooks/", g.handleWebhook)
	return g
}

func (g *InboundGateway) Handler() http.Handler {
	return g.mux
}

func (g *InboundGateway) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if runtimebus.RuntimeIngressPaused() {
		http.Error(w, "runtime reset in progress", http.StatusServiceUnavailable)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	verticalKey, provider, ok := parseWebhookPath(r.URL.Path)
	if !ok {
		http.Error(w, "expected /webhooks/{vertical}/{provider}", http.StatusBadRequest)
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
		VerticalID:   verticalKey,
		VerticalSlug: verticalKey,
	}
	if g.store != nil {
		resolved, err := g.store.ResolveInboundTarget(r.Context(), verticalKey, provider)
		if err != nil {
			http.Error(w, "unknown vertical", http.StatusNotFound)
			return
		}
		target = resolved
	}
	if !verifyProviderSignature(provider, target.WebhookSecret, body, r.Header) {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		payload = map[string]any{"raw": string(body)}
	}
	providerEventID := firstNonEmpty(
		r.Header.Get("X-Provider-Event-ID"),
		r.Header.Get("X-Request-ID"),
		extractProviderEventID(payload),
		fingerprintInbound(target.VerticalID, provider, body),
	)

	if g.store != nil {
		inserted, err := g.store.RecordInboundEvent(r.Context(), providerEventID, target.VerticalID, provider)
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
	pubType, pubPayload := buildInboundPublishPayload(provider, target.VerticalID, providerEventID, payload, now)
	pubPayload["headers"] = map[string]any{
		"user_agent": r.UserAgent(),
	}
	envelopeBytes := mustJSON(pubPayload)
	if g.bus != nil {
		pubCtx := runtimebus.WithCurrentRuntimeEpoch(r.Context())
		if err := g.bus.Publish(pubCtx, events.Event{
			ID:          uuid.NewString(),
			Type:        pubType,
			SourceAgent: "inbound-gateway",
			VerticalID:  target.VerticalID,
			Payload:     envelopeBytes,
			CreatedAt:   now,
		}); err != nil {
			log.Printf("inbound publish failed provider=%s vertical=%s err=%v", provider, target.VerticalID, err)
		}
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":            "accepted",
		"vertical_id":       target.VerticalID,
		"vertical_slug":     target.VerticalSlug,
		"provider":          provider,
		"provider_event_id": providerEventID,
	})
}

func parseWebhookPath(path string) (verticalID, provider string, ok bool) {
	p := strings.Trim(path, "/")
	parts := strings.Split(p, "/")
	if len(parts) != 3 || parts[0] != "webhooks" {
		return "", "", false
	}
	verticalID = strings.TrimSpace(parts[1])
	provider = strings.TrimSpace(parts[2])
	if verticalID == "" || provider == "" {
		return "", "", false
	}
	return verticalID, provider, true
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

func fingerprintInbound(verticalID, provider string, body []byte) string {
	h := sha1.New()
	_, _ = h.Write([]byte(verticalID))
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

func buildInboundPublishPayload(provider, verticalID, providerEventID string, rawPayload any, now time.Time) (events.EventType, map[string]any) {
	obj, _ := rawPayload.(map[string]any)
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "whatsapp":
		return events.EventType("inbound.whatsapp_message"), map[string]any{
			"vertical_id":       strings.TrimSpace(verticalID),
			"from":              firstStringByKeys(obj, "from", "sender", "wa_id", "phone", "contact"),
			"message":           firstStringByKeys(obj, "message", "text", "body"),
			"timestamp":         firstNonEmpty(firstStringByKeys(obj, "timestamp", "time"), now.Format(time.RFC3339)),
			"media_urls":        firstStringSliceByKeys(obj, "media_urls", "media", "attachments"),
			"provider_event_id": strings.TrimSpace(providerEventID),
			"provider":          "whatsapp",
			"payload":           rawPayload,
			"received_at":       now.Format(time.RFC3339),
		}
	case "email":
		return events.EventType("inbound.email"), map[string]any{
			"vertical_id":       strings.TrimSpace(verticalID),
			"from":              firstStringByKeys(obj, "from", "sender", "email"),
			"subject":           firstStringByKeys(obj, "subject"),
			"body":              firstStringByKeys(obj, "body", "text", "message"),
			"attachments":       firstStringSliceByKeys(obj, "attachments"),
			"provider_event_id": strings.TrimSpace(providerEventID),
			"provider":          "email",
			"payload":           rawPayload,
			"received_at":       now.Format(time.RFC3339),
		}
	default:
		return events.EventType("inbound." + normalizeEventToken(provider)), map[string]any{
			"vertical_id":       strings.TrimSpace(verticalID),
			"provider":          strings.TrimSpace(provider),
			"event_type":        resolveProviderEventType(provider, rawPayload),
			"provider_event_id": strings.TrimSpace(providerEventID),
			"payload":           rawPayload,
			"received_at":       now.Format(time.RFC3339),
		}
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
	switch provider {
	case "whatsapp":
		return "message"
	case "email":
		return "received"
	case "domain":
		m, _ := payload.(map[string]any)
		if v, ok := m["status"].(string); ok && strings.TrimSpace(v) != "" {
			return normalizeEventToken(v)
		}
		return "confirmed"
	case "stripe":
		m, _ := payload.(map[string]any)
		if v, ok := m["type"].(string); ok && strings.TrimSpace(v) != "" {
			return normalizeEventToken(v)
		}
		return "payment_event"
	default:
		return "event"
	}
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
	// If no secret is configured, accept only explicitly unsigned providers.
	if secret == "" {
		return provider == "email"
	}
	switch provider {
	case "whatsapp":
		sig := strings.TrimSpace(headers.Get("X-Hub-Signature-256"))
		if !strings.HasPrefix(strings.ToLower(sig), "sha256=") {
			return false
		}
		given := strings.TrimPrefix(sig, "sha256=")
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write(body)
		expected := hex.EncodeToString(mac.Sum(nil))
		return hmac.Equal([]byte(strings.ToLower(given)), []byte(strings.ToLower(expected)))
	case "stripe":
		sig := strings.TrimSpace(headers.Get("Stripe-Signature"))
		if sig == "" {
			return false
		}
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
	default:
		token := strings.TrimSpace(headers.Get("X-Webhook-Token"))
		if token == "" {
			auth := strings.TrimSpace(headers.Get("Authorization"))
			if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
				token = strings.TrimSpace(auth[7:])
			}
		}
		return hmac.Equal([]byte(token), []byte(secret))
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
