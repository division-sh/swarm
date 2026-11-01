package connectorapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

type Slack struct {
	Server      *httptest.Server
	TokenCalls  atomic.Int64
	Calls       atomic.Int64
	SideEffects chan Call
}

func NewSlack(t testing.TB) *Slack {
	t.Helper()
	fake := &Slack{SideEffects: make(chan Call, 8)}
	mux := http.NewServeMux()
	mux.HandleFunc("/oauth.v2.access", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		grant := r.Form.Get("grant_type")
		if grant != "refresh_token" && grant != "authorization_code" {
			http.Error(w, "unexpected grant_type "+grant, http.StatusBadRequest)
			return
		}
		if grant == "authorization_code" && (r.Form.Get("code") != "fixture-code" || r.Form.Get("client_id") != "slack-client" || r.Form.Get("client_secret") != "slack-secret") {
			http.Error(w, "bad authorization code or client", http.StatusUnauthorized)
			return
		}
		if got := r.Form.Get("refresh_token"); grant == "refresh_token" && got != "refresh-secret" {
			http.Error(w, "unexpected refresh token", http.StatusBadRequest)
			return
		}
		call := fake.TokenCalls.Add(1)
		token := "fresh-token"
		if call >= 2 {
			token = "after-401-token"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  token,
			"refresh_token": "refresh-secret",
			"token_type":    "Bearer",
			"expires_in":    3600,
			"scope":         "chat:write",
		})
	})
	mux.HandleFunc("/api/chat.postMessage", func(w http.ResponseWriter, r *http.Request) {
		fake.Calls.Add(1)
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var body map[string]any
		if err := json.Unmarshal(raw, &body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		auth := r.Header.Get("Authorization")
		text := fmt.Sprint(body["text"])
		if auth == "Bearer fresh-token" && text == "needs 401 refresh" {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "token_expired"})
			return
		}
		if auth != "Bearer fresh-token" && auth != "Bearer after-401-token" {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "bad_token"})
			return
		}
		if text == "provider ok false" {
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "channel_not_found"})
			return
		}
		call := Call{Method: r.Method, Path: r.URL.Path, Auth: auth, Body: body, Raw: string(raw)}
		fake.SideEffects <- call
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":      true,
			"channel": body["channel"],
			"message": map[string]any{
				"text": body["text"],
			},
		})
	})
	fake.Server = httptest.NewServer(mux)
	return fake
}
