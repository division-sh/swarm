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

type Notion struct {
	Server      *httptest.Server
	TokenCalls  atomic.Int64
	Calls       atomic.Int64
	SideEffects chan Call
}

func NewNotion(t testing.TB) *Notion {
	t.Helper()
	fake := &Notion{SideEffects: make(chan Call, 8)}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/oauth/token", fake.serveToken)
	mux.HandleFunc("/v1/blocks/block-123/children", fake.serveChildren)
	fake.Server = httptest.NewServer(mux)
	return fake
}

func (fake *Notion) serveToken(w http.ResponseWriter, r *http.Request) {
	user, pass, ok := r.BasicAuth()
	if !ok || user != "notion-client" || pass != "notion-secret" {
		http.Error(w, "bad basic auth", http.StatusUnauthorized)
		return
	}
	if got := r.Header.Get("Notion-Version"); got != "2026-03-11" {
		http.Error(w, "missing Notion-Version", http.StatusBadRequest)
		return
	}
	if got := r.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		http.Error(w, "bad content type", http.StatusBadRequest)
		return
	}
	var body map[string]string
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	grant := body["grant_type"]
	if grant != "refresh_token" && grant != "authorization_code" {
		http.Error(w, "unexpected grant_type "+body["grant_type"], http.StatusBadRequest)
		return
	}
	call := fake.TokenCalls.Add(1)
	wantRefresh := "refresh-secret"
	access := "fresh-token"
	refresh := "rotated-refresh"
	if call >= 2 {
		wantRefresh = "rotated-refresh"
		access = "after-401-token"
		refresh = "after-401-refresh"
	}
	if grant == "authorization_code" && body["code"] != "fixture-code" {
		http.Error(w, "bad authorization code", http.StatusUnauthorized)
		return
	}
	if grant == "refresh_token" && body["refresh_token"] != wantRefresh {
		http.Error(w, "unexpected refresh token", http.StatusBadRequest)
		return
	}
	for _, forbidden := range []string{"client_id", "client_secret"} {
		if body[forbidden] != "" {
			http.Error(w, "body carried "+forbidden, http.StatusBadRequest)
			return
		}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token":  access,
		"refresh_token": refresh,
		"token_type":    "Bearer",
		"expires_in":    3600,
	})
}

func (fake *Notion) serveChildren(w http.ResponseWriter, r *http.Request) {
	call := fake.Calls.Add(1)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if r.Method != http.MethodPatch {
		http.Error(w, "bad method", http.StatusMethodNotAllowed)
		return
	}
	if got := r.Header.Get("Notion-Version"); got != "2026-03-11" {
		http.Error(w, "missing Notion-Version", http.StatusBadRequest)
		return
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	auth := r.Header.Get("Authorization")
	if call == 2 && auth == "Bearer fresh-token" {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{"object": "error", "status": 401, "code": "unauthorized", "message": "token expired"})
		return
	}
	if call == 4 {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Header().Set("Retry-After", "1")
		_ = json.NewEncoder(w).Encode(map[string]any{"object": "error", "status": 429, "code": "rate_limited", "message": "slow down"})
		return
	}
	if auth != "Bearer fresh-token" && auth != "Bearer after-401-token" {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{"object": "error", "status": 401, "code": "unauthorized", "message": "bad token"})
		return
	}
	fake.SideEffects <- Call{Method: r.Method, Path: r.URL.Path, Auth: auth, Body: body, Raw: string(raw)}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"object": "list",
		"results": []map[string]any{
			{"object": "block", "id": "child-1"},
		},
	})
}
