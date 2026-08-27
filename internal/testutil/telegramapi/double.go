package telegramapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
)

// Double implements the Telegram API operations used by connected-channel
// onboarding and records every externally visible effect.
type Double struct {
	mu                           sync.Mutex
	callbackURL                  string
	signingSecret                string
	registrationRequests         []map[string]any
	deliveries                   []map[string]any
	rejectNextCredential         bool
	loseNextRegistrationResponse bool
}

func (p *Double) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case strings.HasSuffix(request.URL.Path, "/getMe"):
		p.mu.Lock()
		reject := p.rejectNextCredential
		p.rejectNextCredential = false
		p.mu.Unlock()
		if reject {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"ok":false,"error_code":401,"description":"Unauthorized"}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":true,"result":{"id":420079}}`))
	case strings.HasSuffix(request.URL.Path, "/setWebhook"):
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		p.callbackURL = strings.TrimSpace(fmt.Sprint(payload["url"]))
		p.signingSecret = strings.TrimSpace(fmt.Sprint(payload["secret_token"]))
		p.registrationRequests = append(p.registrationRequests, clonePayload(payload))
		loseResponse := p.loseNextRegistrationResponse
		p.loseNextRegistrationResponse = false
		p.mu.Unlock()
		if loseResponse {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				http.Error(w, "provider response-loss injection requires HTTP hijacking", http.StatusInternalServerError)
				return
			}
			connection, _, err := hijacker.Hijack()
			if err == nil {
				_ = connection.Close()
			}
			return
		}
		_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
	case strings.HasSuffix(request.URL.Path, "/getWebhookInfo"):
		p.mu.Lock()
		callbackURL := p.callbackURL
		p.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"url": callbackURL}})
	case strings.HasSuffix(request.URL.Path, "/sendMessage"):
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		p.deliveries = append(p.deliveries, clonePayload(payload))
		messageID := len(p.deliveries)
		p.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": messageID}})
	default:
		http.Error(w, `{"ok":false}`, http.StatusNotFound)
	}
}

func (p *Double) Registration() (string, string, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.callbackURL, p.signingSecret, len(p.registrationRequests)
}

func (p *Double) Delivery(index int) map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	if index < 0 || index >= len(p.deliveries) {
		return nil
	}
	return clonePayload(p.deliveries[index])
}

func (p *Double) Counts() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.registrationRequests), len(p.deliveries)
}

func (p *Double) RejectNextCredentialPreflight() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rejectNextCredential = true
}

func (p *Double) LoseNextRegistrationAcknowledgment() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.loseNextRegistrationResponse = true
}

func clonePayload(payload map[string]any) map[string]any {
	cloned := make(map[string]any, len(payload))
	for key, value := range payload {
		cloned[key] = value
	}
	return cloned
}
