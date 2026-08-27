package releasee2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
)

// releaseTelegramAPIDouble is deliberately local to the public-process suite:
// release tests may not import Swarm implementation or internal test packages.
type releaseTelegramAPIDouble struct {
	mu                   sync.Mutex
	callbackURL          string
	signingSecret        string
	registrationRequests []map[string]any
	deliveries           []map[string]any
}

func (p *releaseTelegramAPIDouble) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case strings.HasSuffix(request.URL.Path, "/getMe"):
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
		p.registrationRequests = append(p.registrationRequests, cloneReleaseTelegramPayload(payload))
		p.mu.Unlock()
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
		p.deliveries = append(p.deliveries, cloneReleaseTelegramPayload(payload))
		messageID := len(p.deliveries)
		p.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": messageID}})
	default:
		http.Error(w, `{"ok":false}`, http.StatusNotFound)
	}
}

func (p *releaseTelegramAPIDouble) Registration() (string, string, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.callbackURL, p.signingSecret, len(p.registrationRequests)
}

func (p *releaseTelegramAPIDouble) Delivery(index int) map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	if index < 0 || index >= len(p.deliveries) {
		return nil
	}
	return cloneReleaseTelegramPayload(p.deliveries[index])
}

func (p *releaseTelegramAPIDouble) Counts() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.registrationRequests), len(p.deliveries)
}

func cloneReleaseTelegramPayload(payload map[string]any) map[string]any {
	cloned := make(map[string]any, len(payload))
	for key, value := range payload {
		cloned[key] = value
	}
	return cloned
}
