package connectorapi

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

type MicrosoftGraph struct {
	Server        *httptest.Server
	TokenCalls    atomic.Int64
	Calls         atomic.Int64
	RefreshTokens []string
	SideEffects   chan Call
}

func NewMicrosoftGraph(t testing.TB) *MicrosoftGraph {
	t.Helper()
	fake := &MicrosoftGraph{SideEffects: make(chan Call, 8)}
	mux := http.NewServeMux()
	mux.HandleFunc("/tenant/oauth2/v2.0/token", fake.serveToken)
	mux.HandleFunc("/v1.0/users/user@example.com/sendMail", fake.serveMail)
	fake.Server = httptest.NewServer(mux)
	return fake
}

func (fake *MicrosoftGraph) serveToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "bad method", http.StatusMethodNotAllowed)
		return
	}
	if got := r.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/x-www-form-urlencoded") {
		http.Error(w, "bad content type", http.StatusBadRequest)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if got := r.Form.Get("grant_type"); got != "client_credentials" {
		http.Error(w, "unexpected grant_type "+got, http.StatusBadRequest)
		return
	}
	if got := r.Form.Get("client_id"); got != "graph-client" {
		http.Error(w, "unexpected client_id", http.StatusBadRequest)
		return
	}
	if got := r.Form.Get("client_secret"); got != "graph-client-secret" {
		http.Error(w, "unexpected client_secret", http.StatusBadRequest)
		return
	}
	if got := r.Form.Get("scope"); got != "https://graph.microsoft.com/.default" {
		http.Error(w, "unexpected scope "+got, http.StatusBadRequest)
		return
	}
	if refresh := strings.TrimSpace(r.Form.Get("refresh_token")); refresh != "" {
		fake.RefreshTokens = append(fake.RefreshTokens, refresh)
		http.Error(w, "client_credentials must not send refresh_token", http.StatusBadRequest)
		return
	}
	call := fake.TokenCalls.Add(1)
	access := "graph-fresh-token"
	if call >= 2 {
		access = "graph-after-401-token"
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token": access,
		"token_type":   "Bearer",
		"expires_in":   3600,
	})
}

func (fake *MicrosoftGraph) serveMail(w http.ResponseWriter, r *http.Request) {
	call := fake.Calls.Add(1)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "bad method", http.StatusMethodNotAllowed)
		return
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if recipients := graphMessageRecipients(body); len(recipients) != 1 {
		http.Error(w, "toRecipients must be a JSON array", http.StatusBadRequest)
		return
	}
	auth := r.Header.Get("Authorization")
	subject := graphMessageSubject(body)
	if auth == "Bearer graph-fresh-token" && subject == "needs 401 refresh" {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": "InvalidAuthenticationToken", "message": "token expired"}})
		return
	}
	if auth != "Bearer graph-fresh-token" && auth != "Bearer graph-after-401-token" {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": "InvalidAuthenticationToken", "message": "bad token"}})
		return
	}
	if subject == "provider 429 fixture" {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": "TooManyRequests", "message": "slow down"}})
		return
	}
	if call < 1 {
		http.Error(w, "unreachable", http.StatusInternalServerError)
		return
	}
	fake.SideEffects <- Call{Method: r.Method, Path: r.URL.Path, Auth: auth, Body: body, Raw: string(raw)}
	w.WriteHeader(http.StatusAccepted)
}
