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
	registrations                map[string]registration
	resourceIDs                  map[string]int64
	commands                     map[string][]map[string]any
	commandWrites                []map[string]any
	registrationRequests         []map[string]any
	deliveries                   []map[string]any
	edits                        []map[string]any
	acknowledgments              []map[string]any
	rejectNextCredential         bool
	loseNextRegistrationResponse bool
	loseNextEditResponse         bool
	loseNextAckResponse          bool
	registrationResponseBarrier  *responseBarrier
	deliveryResponseBarrier      *responseBarrier
	editResponseBarrier          *responseBarrier
}

type registration struct {
	callbackURL   string
	signingSecret string
}

type responseBarrier struct {
	arrived chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *Double) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	credential := credentialFromPath(request.URL.Path)
	switch {
	case strings.HasSuffix(request.URL.Path, "/getMe"):
		p.mu.Lock()
		reject := p.rejectNextCredential
		p.rejectNextCredential = false
		resourceID := p.resourceIDs[credential]
		p.mu.Unlock()
		if reject {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"ok":false,"error_code":401,"description":"Unauthorized"}`))
			return
		}
		if resourceID == 0 {
			resourceID = 420079
		}
		_, _ = fmt.Fprintf(w, `{"ok":true,"result":{"id":%d,"username":"SwarmTestBot"}}`, resourceID)
	case strings.HasSuffix(request.URL.Path, "/setMyCommands"):
		var payload struct {
			Scope        map[string]any   `json:"scope"`
			LanguageCode string           `json:"language_code"`
			Commands     []map[string]any `json:"commands"`
		}
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		key, err := commandScopeKey(credential, payload.Scope, payload.LanguageCode)
		if err != nil || len(payload.Commands) > 100 {
			http.Error(w, "invalid command scope", http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		if p.commands == nil {
			p.commands = map[string][]map[string]any{}
		}
		p.commands[key] = payload.Commands
		p.commandWrites = append(p.commandWrites, map[string]any{"scope": payload.Scope, "commands": payload.Commands, "language_code": payload.LanguageCode})
		p.mu.Unlock()
		_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
	case strings.HasSuffix(request.URL.Path, "/getMyCommands"):
		var payload struct {
			Scope        map[string]any `json:"scope"`
			LanguageCode string         `json:"language_code"`
		}
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		key, err := commandScopeKey(credential, payload.Scope, payload.LanguageCode)
		if err != nil {
			http.Error(w, "invalid command scope", http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		commands := append([]map[string]any(nil), p.commands[key]...)
		p.mu.Unlock()
		if commands == nil {
			commands = []map[string]any{}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": commands})
	case strings.HasSuffix(request.URL.Path, "/setWebhook"):
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		p.callbackURL = strings.TrimSpace(fmt.Sprint(payload["url"]))
		p.signingSecret = strings.TrimSpace(fmt.Sprint(payload["secret_token"]))
		if p.registrations == nil {
			p.registrations = map[string]registration{}
		}
		p.registrations[credential] = registration{callbackURL: p.callbackURL, signingSecret: p.signingSecret}
		p.registrationRequests = append(p.registrationRequests, clonePayload(payload))
		loseResponse := p.loseNextRegistrationResponse
		p.loseNextRegistrationResponse = false
		barrier := p.registrationResponseBarrier
		p.registrationResponseBarrier = nil
		p.mu.Unlock()
		if barrier != nil {
			close(barrier.arrived)
			<-barrier.release
		}
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
		callbackURL := p.registrations[credential].callbackURL
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
		barrier := p.deliveryResponseBarrier
		p.deliveryResponseBarrier = nil
		p.mu.Unlock()
		if barrier != nil {
			close(barrier.arrived)
			<-barrier.release
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": messageID}})
	case strings.HasSuffix(request.URL.Path, "/editMessageText"):
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		messageID, ok := payload["message_id"].(float64)
		p.mu.Lock()
		if !ok || messageID < 1 || int(messageID) > len(p.deliveries) ||
			payload["chat_id"] != p.deliveries[int(messageID)-1]["chat_id"] {
			p.mu.Unlock()
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"ok":false,"description":"message not found"}`))
			return
		}
		p.edits = append(p.edits, clonePayload(payload))
		loseResponse := p.loseNextEditResponse
		p.loseNextEditResponse = false
		barrier := p.editResponseBarrier
		p.editResponseBarrier = nil
		p.mu.Unlock()
		if barrier != nil {
			close(barrier.arrived)
			<-barrier.release
		}
		if loseResponse {
			loseProviderResponse(w)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": int(messageID)}})
	case strings.HasSuffix(request.URL.Path, "/answerCallbackQuery"):
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if payload["callback_query_id"] == nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"ok":false,"description":"callback query required"}`))
			return
		}
		p.mu.Lock()
		p.acknowledgments = append(p.acknowledgments, clonePayload(payload))
		loseResponse := p.loseNextAckResponse
		p.loseNextAckResponse = false
		p.mu.Unlock()
		if loseResponse {
			loseProviderResponse(w)
			return
		}
		_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
	default:
		http.Error(w, `{"ok":false}`, http.StatusNotFound)
	}
}

func loseProviderResponse(w http.ResponseWriter) {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "provider response-loss injection requires HTTP hijacking", http.StatusInternalServerError)
		return
	}
	connection, _, err := hijacker.Hijack()
	if err == nil {
		_ = connection.Close()
	}
}

func commandScopeKey(credential string, scope map[string]any, language string) (string, error) {
	if scope == nil {
		return "", fmt.Errorf("command scope is required")
	}
	typeName, ok := scope["type"].(string)
	if !ok || (typeName != "chat" && typeName != "chat_member") {
		return "", fmt.Errorf("unsupported command scope")
	}
	chat, ok := scope["chat_id"].(string)
	if !ok || chat == "" {
		return "", fmt.Errorf("command chat is required")
	}
	member := ""
	if typeName == "chat_member" {
		member, ok = scope["user_id"].(string)
		if !ok || member == "" {
			return "", fmt.Errorf("command member is required")
		}
	}
	return credential + "\x00" + typeName + "\x00" + chat + "\x00" + member + "\x00" + language, nil
}

func (p *Double) CommandWrites() []map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]map[string]any, len(p.commandWrites))
	for index, write := range p.commandWrites {
		out[index] = clonePayload(write)
	}
	return out
}

// SetResourceID makes one credential represent a distinct provider resource.
func (p *Double) SetResourceID(credential string, resourceID int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.resourceIDs == nil {
		p.resourceIDs = map[string]int64{}
	}
	p.resourceIDs[strings.TrimSpace(credential)] = resourceID
}

// RegistrationForCredential returns the current provider registration for one
// exact credential and the total number of registration effects.
func (p *Double) RegistrationForCredential(credential string) (string, string, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	current := p.registrations[strings.TrimSpace(credential)]
	return current.callbackURL, current.signingSecret, len(p.registrationRequests)
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

func (p *Double) Edits() []map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]map[string]any, len(p.edits))
	for index, edit := range p.edits {
		out[index] = clonePayload(edit)
	}
	return out
}

func (p *Double) Acknowledgments() []map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]map[string]any, len(p.acknowledgments))
	for index, ack := range p.acknowledgments {
		out[index] = clonePayload(ack)
	}
	return out
}

func (p *Double) LoseNextEditAcknowledgment() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.loseNextEditResponse = true
}

func (p *Double) LoseNextCallbackAcknowledgment() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.loseNextAckResponse = true
}

func (p *Double) PauseNextEditResponse() (<-chan struct{}, func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	barrier := newResponseBarrier()
	p.editResponseBarrier = barrier
	return barrier.arrived, barrier.releaseResponse
}

func (p *Double) Confirmation(index int) map[string]any {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, delivery := range p.deliveries {
		text, _ := delivery["text"].(string)
		if !strings.HasPrefix(text, "Swarm channel connected.") {
			continue
		}
		if index == 0 {
			return clonePayload(delivery)
		}
		index--
	}
	return nil
}

func (p *Double) OnboardingCounts() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	confirmations := 0
	for _, delivery := range p.deliveries {
		text, _ := delivery["text"].(string)
		if strings.HasPrefix(text, "Swarm channel connected.") {
			confirmations++
		}
	}
	return len(p.registrationRequests), confirmations
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

func (p *Double) PauseNextRegistrationResponse() (<-chan struct{}, func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	barrier := newResponseBarrier()
	p.registrationResponseBarrier = barrier
	return barrier.arrived, barrier.releaseResponse
}

func (p *Double) PauseNextDeliveryResponse() (<-chan struct{}, func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	barrier := newResponseBarrier()
	p.deliveryResponseBarrier = barrier
	return barrier.arrived, barrier.releaseResponse
}

func newResponseBarrier() *responseBarrier {
	return &responseBarrier{arrived: make(chan struct{}), release: make(chan struct{})}
}

func (b *responseBarrier) releaseResponse() {
	if b == nil {
		return
	}
	b.once.Do(func() { close(b.release) })
}

func clonePayload(payload map[string]any) map[string]any {
	cloned := make(map[string]any, len(payload))
	for key, value := range payload {
		cloned[key] = value
	}
	return cloned
}

func credentialFromPath(path string) string {
	return strings.SplitN(strings.TrimPrefix(path, "/bot"), "/", 2)[0]
}
