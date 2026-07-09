package managedcredentials

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	managedcredentialmodel "github.com/division-sh/swarm/internal/runtime/managedcredentials/model"
)

func TestTokenSourceAuthCodePKCEPersistsStructuredTokenRecord(t *testing.T) {
	ctx := context.Background()
	var sawGrant string
	var sawVerifier string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/token" {
			http.NotFound(w, r)
			return
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm: %v", err)
		}
		sawGrant = r.Form.Get("grant_type")
		sawVerifier = r.Form.Get("code_verifier")
		if got := r.Form.Get("code"); got != "auth-code" {
			t.Fatalf("code = %q, want auth-code", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "access-secret",
			"refresh_token": "refresh-secret",
			"expires_in":    3600,
			"token_type":    "Bearer",
			"scope":         "drive.read",
		})
	}))
	defer server.Close()

	path := filepath.Join(t.TempDir(), "managed.json")
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	source := TokenSource{Store: store}
	begin, err := source.BeginAuthCodePKCE(ctx, BeginAuthCodeRequest{
		Key:         "google_drive",
		Provider:    "google",
		AuthURL:     server.URL + "/auth",
		TokenURL:    server.URL + "/token",
		ClientID:    "client-id",
		RedirectURL: "http://127.0.0.1/callback",
		Scopes:      []string{"drive.read"},
		Account:     "ops@example.test",
	})
	if err != nil {
		t.Fatalf("BeginAuthCodePKCE: %v", err)
	}
	authorizeURL, err := url.Parse(begin.AuthorizeURL)
	if err != nil {
		t.Fatalf("Parse authorize URL: %v", err)
	}
	if got := authorizeURL.Query().Get("code_challenge_method"); got != "S256" {
		t.Fatalf("code_challenge_method = %q, want S256", got)
	}
	if authorizeURL.Query().Get("code_challenge") == "" || authorizeURL.Query().Get("state") == "" {
		t.Fatalf("authorize URL missing PKCE challenge/state: %s", begin.AuthorizeURL)
	}
	if begin.CodeVerifier == "" {
		t.Fatal("CodeVerifier is empty")
	}

	record, err := source.CompleteAuthCode(ctx, CompleteAuthCodeRequest{
		Key:   "google_drive",
		State: begin.State,
		Code:  "auth-code",
	})
	if err != nil {
		t.Fatalf("CompleteAuthCode: %v", err)
	}
	if sawGrant != "authorization_code" || sawVerifier == "" {
		t.Fatalf("token request grant/verifier = (%q, %q), want authorization_code and verifier", sawGrant, sawVerifier)
	}
	if record.Status != StatusConnected || record.AccessToken != "access-secret" || record.RefreshToken != "refresh-secret" {
		t.Fatalf("record = %#v, want connected token record", record)
	}
	if record.PKCEVerifier != "" || record.OAuthState != "" {
		t.Fatalf("record retained transient PKCE data: %#v", record)
	}
	reopened, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore(reopen): %v", err)
	}
	persisted, ok, err := reopened.Get(ctx, "google_drive")
	if err != nil || !ok {
		t.Fatalf("reopened.Get = (%#v, %v, %v), want persisted record", persisted, ok, err)
	}
	if persisted.AccessToken != "access-secret" || persisted.RefreshToken != "refresh-secret" {
		t.Fatalf("persisted tokens = (%q, %q), want stored tokens", persisted.AccessToken, persisted.RefreshToken)
	}
	desc := persisted.Descriptor()
	raw, err := json.Marshal(desc)
	if err != nil {
		t.Fatalf("Marshal descriptor: %v", err)
	}
	if strings.Contains(string(raw), "access-secret") || strings.Contains(string(raw), "refresh-secret") {
		t.Fatalf("descriptor leaked token material: %s", string(raw))
	}
}

func TestTokenSourceAuthCodeFailureRedactsCallbackCode(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm: %v", err)
		}
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":             "invalid_grant",
			"error_description": "authorization code callback-code was rejected",
		})
	}))
	defer server.Close()

	store := NewMemoryStore()
	source := TokenSource{Store: store}
	begin, err := source.BeginAuthCodePKCE(ctx, BeginAuthCodeRequest{
		Key:         "google_drive",
		Provider:    "google",
		AuthURL:     server.URL + "/auth",
		TokenURL:    server.URL,
		ClientID:    "client-id",
		RedirectURL: "http://127.0.0.1/callback",
		Scopes:      []string{"drive.read"},
	})
	if err != nil {
		t.Fatalf("BeginAuthCodePKCE: %v", err)
	}
	_, err = source.CompleteAuthCode(ctx, CompleteAuthCodeRequest{
		Key:   "google_drive",
		State: begin.State,
		Code:  "callback-code",
	})
	if err == nil {
		t.Fatal("CompleteAuthCode error = nil, want redacted token endpoint failure")
	}
	if strings.Contains(err.Error(), "callback-code") {
		t.Fatalf("CompleteAuthCode error leaked callback code: %v", err)
	}
	stored, ok, getErr := store.Get(ctx, "google_drive")
	if getErr != nil || !ok {
		t.Fatalf("store.Get = (%#v, %v, %v)", stored, ok, getErr)
	}
	if stored.Status != StatusRefreshFailed {
		t.Fatalf("stored status = %q, want refresh_failed", stored.Status)
	}
	if strings.Contains(stored.Failure, "callback-code") {
		t.Fatalf("stored failure leaked callback code: %q", stored.Failure)
	}
}

func TestTokenSourceAuthCodeUsesBasicJSONTokenRequestProfile(t *testing.T) {
	ctx := context.Background()
	var sawBody map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/token" {
			http.NotFound(w, r)
			return
		}
		user, pass, ok := r.BasicAuth()
		if !ok || user != "notion-client" || pass != "notion-secret" {
			t.Fatalf("BasicAuth = (%q, %q, %v), want notion client credentials", user, pass, ok)
		}
		if got := r.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
			t.Fatalf("Content-Type = %q, want application/json", got)
		}
		if got := r.Header.Get("Notion-Version"); got != "2026-03-11" {
			t.Fatalf("Notion-Version = %q, want 2026-03-11", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&sawBody); err != nil {
			t.Fatalf("decode token body: %v", err)
		}
		for _, forbidden := range []string{"client_id", "client_secret", "code_verifier"} {
			if _, exists := sawBody[forbidden]; exists {
				t.Fatalf("token JSON body carried %s: %#v", forbidden, sawBody)
			}
		}
		if sawBody["grant_type"] != "authorization_code" || sawBody["code"] != "auth-code" || sawBody["redirect_uri"] != "http://127.0.0.1/callback" {
			t.Fatalf("token JSON body = %#v, want Notion auth-code body", sawBody)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "notion-access",
			"refresh_token": "notion-refresh",
			"expires_in":    3600,
			"token_type":    "Bearer",
		})
	}))
	defer server.Close()

	store := NewMemoryStore()
	source := TokenSource{Store: store}
	begin, err := source.BeginAuthCode(ctx, BeginAuthCodeRequest{
		Key:          "notion_oauth",
		Provider:     "notion",
		AuthURL:      server.URL + "/auth",
		TokenURL:     server.URL + "/token",
		ClientID:     "notion-client",
		ClientSecret: "notion-secret",
		RedirectURL:  "http://127.0.0.1/callback",
		GrantModel:   managedcredentialmodel.GrantModelWorkspace,
		TokenRequest: managedcredentialmodel.TokenRequestProfile{
			ClientAuth: managedcredentialmodel.TokenClientAuthBasic,
			Body:       managedcredentialmodel.TokenBodyJSON,
			StaticHeaders: map[string]string{
				"Notion-Version": "2026-03-11",
			},
		},
	})
	if err != nil {
		t.Fatalf("BeginAuthCode: %v", err)
	}
	authorizeURL, err := url.Parse(begin.AuthorizeURL)
	if err != nil {
		t.Fatalf("Parse authorize URL: %v", err)
	}
	if authorizeURL.Query().Get("code_challenge") != "" || authorizeURL.Query().Get("code_challenge_method") != "" {
		t.Fatalf("non-PKCE authorize URL carried PKCE params: %s", begin.AuthorizeURL)
	}
	if begin.CodeVerifier != "" {
		t.Fatalf("CodeVerifier = %q, want empty for authorization_code", begin.CodeVerifier)
	}

	record, err := source.CompleteAuthCode(ctx, CompleteAuthCodeRequest{
		Key:   "notion_oauth",
		State: begin.State,
		Code:  "auth-code",
	})
	if err != nil {
		t.Fatalf("CompleteAuthCode: %v", err)
	}
	if record.GrantType != GrantAuthorizationCode || record.GrantModel != managedcredentialmodel.GrantModelWorkspace {
		t.Fatalf("record grant = (%q, %q), want authorization_code workspace_grant", record.GrantType, record.GrantModel)
	}
	if !managedcredentialmodel.TokenRequestProfileEqual(record.TokenRequest, managedcredentialmodel.TokenRequestProfile{
		ClientAuth: managedcredentialmodel.TokenClientAuthBasic,
		Body:       managedcredentialmodel.TokenBodyJSON,
		StaticHeaders: map[string]string{
			"Notion-Version": "2026-03-11",
		},
	}) {
		t.Fatalf("record token_request = %#v, want Basic JSON Notion header", record.TokenRequest)
	}
	if record.AccessToken != "notion-access" || record.RefreshToken != "notion-refresh" {
		t.Fatalf("record tokens = (%q, %q), want Notion tokens", record.AccessToken, record.RefreshToken)
	}
}

func TestTokenSourceAuthCodeBasicProfileRequiresClientSecretBeforePersisting(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	source := TokenSource{Store: store}
	_, err := source.BeginAuthCode(ctx, BeginAuthCodeRequest{
		Key:         "notion_oauth",
		Provider:    "notion",
		AuthURL:     "https://example.invalid/auth",
		TokenURL:    "https://example.invalid/token",
		ClientID:    "notion-client",
		RedirectURL: "http://127.0.0.1/callback",
		GrantModel:  managedcredentialmodel.GrantModelWorkspace,
		TokenRequest: managedcredentialmodel.TokenRequestProfile{
			ClientAuth: managedcredentialmodel.TokenClientAuthBasic,
			Body:       managedcredentialmodel.TokenBodyJSON,
		},
	})
	if err == nil {
		t.Fatal("BeginAuthCode error = nil, want missing client_secret failure")
	}
	if !strings.Contains(err.Error(), "client_secret is required") {
		t.Fatalf("BeginAuthCode error = %v, want missing client_secret failure", err)
	}
	if _, ok, getErr := store.Get(ctx, "notion_oauth"); getErr != nil || ok {
		t.Fatalf("store.Get after failed BeginAuthCode = (%v, %v), want no persisted pending record", ok, getErr)
	}
}

func TestTokenSourceClientCredentialsRefreshBeforeUseAndFailureEvidence(t *testing.T) {
	ctx := context.Background()
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm: %v", err)
		}
		if got := r.Form.Get("grant_type"); got != "client_credentials" {
			t.Fatalf("grant_type = %q, want client_credentials", got)
		}
		if requests == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "first-token",
				"expires_in":   1,
				"scope":        "repo.read",
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "refreshed-token",
			"expires_in":   3600,
			"scope":        "repo.read",
		})
	}))
	defer server.Close()

	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	store := NewMemoryStore()
	source := TokenSource{
		Store: store,
		Now: func() time.Time {
			return now
		},
	}
	record, err := source.ConnectClientCredentials(ctx, ClientCredentialsRequest{
		Key:      "github",
		Provider: "github",
		TokenURL: server.URL,
		ClientID: "client-id",
		Scopes:   []string{"repo.read"},
	})
	if err != nil {
		t.Fatalf("ConnectClientCredentials: %v", err)
	}
	if record.Status != StatusConnected || record.AccessToken != "first-token" {
		t.Fatalf("record = %#v, want connected first token", record)
	}
	now = now.Add(2 * time.Second)
	token, record, err := source.AccessToken(ctx, AccessTokenRequest{Key: "github", Scopes: []string{"repo.read"}})
	if err != nil {
		t.Fatalf("AccessToken refresh-before-use: %v", err)
	}
	if token != "refreshed-token" || record.AccessToken != "refreshed-token" {
		t.Fatalf("refreshed token = (%q, %q), want refreshed-token", token, record.AccessToken)
	}
	if requests != 2 {
		t.Fatalf("token endpoint requests = %d, want 2", requests)
	}
}

func TestTokenSourceClientCredentialsUsesMicrosoftDefaultScopeAndNeverRefreshToken(t *testing.T) {
	ctx := context.Background()
	var requests []url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm: %v", err)
		}
		form := url.Values{}
		for key, values := range r.Form {
			form[key] = append([]string(nil), values...)
		}
		requests = append(requests, form)
		token := "graph-first-token"
		if len(requests) > 1 {
			token = "graph-second-token"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": token,
			"expires_in":   1,
		})
	}))
	defer server.Close()

	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	store := NewMemoryStore()
	source := TokenSource{
		Store: store,
		Now: func() time.Time {
			return now
		},
	}
	record, err := source.ConnectClientCredentials(ctx, ClientCredentialsRequest{
		Key:          "microsoft_graph_app",
		Provider:     "microsoft_graph",
		TokenURL:     server.URL,
		ClientID:     "graph-client",
		ClientSecret: "graph-secret",
		Scopes:       []string{"https://graph.microsoft.com/.default"},
	})
	if err != nil {
		t.Fatalf("ConnectClientCredentials: %v", err)
	}
	if record.RefreshToken != "" {
		t.Fatalf("client_credentials record refresh_token = %q, want none", record.RefreshToken)
	}
	now = now.Add(2 * time.Second)
	token, record, err := source.AccessToken(ctx, AccessTokenRequest{
		Key:    "microsoft_graph_app",
		Scopes: []string{"https://graph.microsoft.com/.default"},
	})
	if err != nil {
		t.Fatalf("AccessToken re-acquire: %v", err)
	}
	if token != "graph-second-token" || record.AccessToken != "graph-second-token" {
		t.Fatalf("re-acquired token = (%q, %q), want graph-second-token", token, record.AccessToken)
	}
	if record.RefreshToken != "" {
		t.Fatalf("re-acquired client_credentials record refresh_token = %q, want none", record.RefreshToken)
	}
	if len(requests) != 2 {
		t.Fatalf("token endpoint requests = %d, want 2", len(requests))
	}
	for i, form := range requests {
		if got := form.Get("grant_type"); got != "client_credentials" {
			t.Fatalf("request %d grant_type = %q, want client_credentials", i+1, got)
		}
		if got := form.Get("client_id"); got != "graph-client" {
			t.Fatalf("request %d client_id = %q, want graph-client", i+1, got)
		}
		if got := form.Get("client_secret"); got != "graph-secret" {
			t.Fatalf("request %d client_secret = %q, want graph-secret", i+1, got)
		}
		if got := form.Get("scope"); got != "https://graph.microsoft.com/.default" {
			t.Fatalf("request %d scope = %q, want Graph .default resource scope", i+1, got)
		}
		if got := form.Get("refresh_token"); got != "" {
			t.Fatalf("request %d refresh_token = %q, want none for client_credentials", i+1, got)
		}
	}
}

func TestTokenSourceGitHubAppInstallationExchangesJWTAndRequiresInstallationSelection(t *testing.T) {
	ctx := context.Background()
	privateKeyPEM, publicKey := testGitHubAppPrivateKey(t)
	now := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	var tokenRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/app/installations/1001/access_tokens" {
			t.Fatalf("path = %s, want installation token endpoint", r.URL.Path)
		}
		if got := r.Header.Get("Accept"); got != "application/vnd.github+json" {
			t.Fatalf("Accept = %q, want GitHub JSON media type", got)
		}
		jwt := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if strings.TrimSpace(jwt) == "" || jwt == r.Header.Get("Authorization") {
			t.Fatalf("Authorization = %q, want Bearer JWT", r.Header.Get("Authorization"))
		}
		testVerifyGitHubAppJWT(t, jwt, publicKey, "github-app-client-id", now)
		tokenRequests++
		token := "github-install-token-1"
		expires := now.Add(time.Second)
		if tokenRequests > 1 {
			token = "github-install-token-2"
			expires = now.Add(time.Hour)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"token":      token,
			"expires_at": expires.Format(time.RFC3339),
		})
	}))
	defer server.Close()

	store := NewMemoryStore()
	source := TokenSource{
		Store: store,
		Now: func() time.Time {
			return now
		},
	}
	record, err := source.ConnectGitHubAppInstallation(ctx, GitHubAppInstallationRequest{
		Key:            "github_app",
		Provider:       "github",
		APIBaseURL:     server.URL,
		ClientID:       "github-app-client-id",
		InstallationID: "1001",
		PrivateKey:     privateKeyPEM,
		Account:        "octo-org/octo-repo",
	})
	if err != nil {
		t.Fatalf("ConnectGitHubAppInstallation: %v", err)
	}
	if record.Status != StatusConnected || record.AccessToken != "github-install-token-1" || record.GrantType != GrantGitHubAppInstallation {
		t.Fatalf("connected record = %#v, want github app installation token", record)
	}
	if record.GrantModel != managedcredentialmodel.GrantModelInstallation || record.InstallationID != "1001" {
		t.Fatalf("record grant/install = (%q, %q), want installation_grant/1001", record.GrantModel, record.InstallationID)
	}

	now = now.Add(2 * time.Second)
	token, record, err := source.AccessToken(ctx, AccessTokenRequest{
		Key:            "github_app",
		GrantType:      GrantGitHubAppInstallation,
		GrantModel:     managedcredentialmodel.GrantModelInstallation,
		InstallationID: "1001",
	})
	if err != nil {
		t.Fatalf("AccessToken refresh-before-use: %v", err)
	}
	if token != "github-install-token-2" || record.AccessToken != "github-install-token-2" {
		t.Fatalf("refreshed token = (%q, %q), want github-install-token-2", token, record.AccessToken)
	}
	if tokenRequests != 2 {
		t.Fatalf("installation token requests = %d, want 2", tokenRequests)
	}

	if _, _, err := source.AccessToken(ctx, AccessTokenRequest{
		Key:            "github_app",
		GrantType:      GrantGitHubAppInstallation,
		GrantModel:     managedcredentialmodel.GrantModelInstallation,
		InstallationID: "2002",
	}); err == nil || !strings.Contains(err.Error(), "installation_id mismatch") {
		t.Fatalf("AccessToken wrong installation err = %v, want mismatch", err)
	}
	if _, _, err := source.AccessToken(ctx, AccessTokenRequest{
		Key:            "github_app",
		GrantType:      GrantClientCredentials,
		InstallationID: "1001",
	}); err == nil || !strings.Contains(err.Error(), "grant-type-insufficient") {
		t.Fatalf("AccessToken wrong grant err = %v, want grant-type-insufficient", err)
	}

	desc := record.Descriptor()
	raw, err := json.Marshal(desc)
	if err != nil {
		t.Fatalf("Marshal descriptor: %v", err)
	}
	if strings.Contains(string(raw), privateKeyPEM) || strings.Contains(string(raw), "github-install-token") {
		t.Fatalf("descriptor leaked GitHub App secret material: %s", string(raw))
	}
	if desc.InstallationID != "1001" || desc.APIBaseURL != server.URL {
		t.Fatalf("descriptor installation/api = (%q, %q), want 1001/%s", desc.InstallationID, desc.APIBaseURL, server.URL)
	}
}

func TestTokenSourceGitHubAppInstallationExchangeErrorRedactsGeneratedJWT(t *testing.T) {
	ctx := context.Background()
	privateKeyPEM, _ := testGitHubAppPrivateKey(t)
	var rawAuth string
	store := NewMemoryStore(Record{
		Key:            "github_app",
		Provider:       "github",
		GrantType:      GrantGitHubAppInstallation,
		APIBaseURL:     "https://api.github.test",
		ClientID:       "github-app-client-id",
		InstallationID: "1001",
		PrivateKey:     privateKeyPEM,
		GrantModel:     managedcredentialmodel.GrantModelInstallation,
		AccessToken:    "expired-install-token",
		Status:         StatusConnected,
		ExpiresAt:      time.Now().Add(-time.Hour),
	})
	source := TokenSource{
		Store: store,
		HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			rawAuth = r.Header.Get("Authorization")
			return nil, errors.New("proxy captured Authorization=" + rawAuth)
		})},
	}
	_, _, err := source.AccessToken(ctx, AccessTokenRequest{
		Key:            "github_app",
		GrantType:      GrantGitHubAppInstallation,
		InstallationID: "1001",
	})
	if err == nil {
		t.Fatal("AccessToken error = nil, want transport failure")
	}
	jwt := strings.TrimPrefix(rawAuth, "Bearer ")
	if jwt == "" || jwt == rawAuth {
		t.Fatalf("captured auth header = %q, want bearer GitHub App JWT", rawAuth)
	}
	if strings.Contains(err.Error(), jwt) || strings.Contains(err.Error(), rawAuth) {
		t.Fatalf("AccessToken error leaked generated JWT: %v", err)
	}
	stored, ok, getErr := store.Get(ctx, "github_app")
	if getErr != nil || !ok {
		t.Fatalf("store.Get = (%#v, %v, %v)", stored, ok, getErr)
	}
	if strings.Contains(stored.Failure, jwt) || strings.Contains(stored.Failure, rawAuth) {
		t.Fatalf("stored failure leaked generated JWT: %q", stored.Failure)
	}
}

func TestTokenSourceGitHubAppInstallationHTTPErrorBodyRedactsGeneratedJWT(t *testing.T) {
	ctx := context.Background()
	privateKeyPEM, _ := testGitHubAppPrivateKey(t)
	var rawAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": "provider echoed Authorization=" + rawAuth,
		})
	}))
	defer server.Close()

	store := NewMemoryStore(Record{
		Key:            "github_app",
		Provider:       "github",
		GrantType:      GrantGitHubAppInstallation,
		APIBaseURL:     server.URL,
		ClientID:       "github-app-client-id",
		InstallationID: "1001",
		PrivateKey:     privateKeyPEM,
		GrantModel:     managedcredentialmodel.GrantModelInstallation,
		AccessToken:    "expired-install-token",
		Status:         StatusConnected,
		ExpiresAt:      time.Now().Add(-time.Hour),
	})
	source := TokenSource{Store: store}
	_, _, err := source.AccessToken(ctx, AccessTokenRequest{
		Key:            "github_app",
		GrantType:      GrantGitHubAppInstallation,
		InstallationID: "1001",
	})
	if err == nil {
		t.Fatal("AccessToken error = nil, want provider HTTP failure")
	}
	jwt := strings.TrimPrefix(rawAuth, "Bearer ")
	if jwt == "" || jwt == rawAuth {
		t.Fatalf("captured auth header = %q, want bearer GitHub App JWT", rawAuth)
	}
	if strings.Contains(err.Error(), jwt) || strings.Contains(err.Error(), rawAuth) {
		t.Fatalf("AccessToken error leaked generated JWT: %v", err)
	}
	stored, ok, getErr := store.Get(ctx, "github_app")
	if getErr != nil || !ok {
		t.Fatalf("store.Get = (%#v, %v, %v)", stored, ok, getErr)
	}
	if strings.Contains(stored.Failure, jwt) || strings.Contains(stored.Failure, rawAuth) {
		t.Fatalf("stored failure leaked generated JWT: %q", stored.Failure)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestTokenSourceRefreshUsesBasicJSONTokenRequestProfileAndRotatesRefreshToken(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "notion-client" || pass != "notion-secret" {
			t.Fatalf("BasicAuth = (%q, %q, %v), want notion client credentials", user, pass, ok)
		}
		if got := r.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
			t.Fatalf("Content-Type = %q, want application/json", got)
		}
		if got := r.Header.Get("Notion-Version"); got != "2026-03-11" {
			t.Fatalf("Notion-Version = %q, want 2026-03-11", got)
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode refresh body: %v", err)
		}
		for _, forbidden := range []string{"client_id", "client_secret"} {
			if _, exists := body[forbidden]; exists {
				t.Fatalf("refresh JSON body carried %s: %#v", forbidden, body)
			}
		}
		if body["grant_type"] != "refresh_token" || body["refresh_token"] != "old-refresh" {
			t.Fatalf("refresh body = %#v, want old refresh token", body)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "refreshed-access",
			"refresh_token": "rotated-refresh",
			"expires_in":    3600,
		})
	}))
	defer server.Close()

	store := NewMemoryStore(Record{
		Key:          "notion_oauth",
		Provider:     "notion",
		GrantType:    GrantAuthorizationCode,
		TokenURL:     server.URL,
		ClientID:     "notion-client",
		ClientSecret: "notion-secret",
		GrantModel:   managedcredentialmodel.GrantModelWorkspace,
		TokenRequest: managedcredentialmodel.TokenRequestProfile{
			ClientAuth: managedcredentialmodel.TokenClientAuthBasic,
			Body:       managedcredentialmodel.TokenBodyJSON,
			StaticHeaders: map[string]string{
				"Notion-Version": "2026-03-11",
			},
		},
		AccessToken:  "expired-access",
		RefreshToken: "old-refresh",
		Status:       StatusConnected,
	})
	source := TokenSource{Store: store}
	token, record, err := source.Refresh(ctx, "notion_oauth")
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if token != "refreshed-access" || record.AccessToken != "refreshed-access" || record.RefreshToken != "rotated-refresh" {
		t.Fatalf("refreshed record = (%q, %#v), want rotated Notion tokens", token, record)
	}
	stored, ok, err := store.Get(ctx, "notion_oauth")
	if err != nil || !ok {
		t.Fatalf("store.Get = (%#v, %v, %v)", stored, ok, err)
	}
	if stored.RefreshToken != "rotated-refresh" {
		t.Fatalf("stored refresh token = %q, want rotated-refresh", stored.RefreshToken)
	}
}

func TestTokenSourceAccessTokenFailsClosedWhenTokenRequestProfileDoesNotSatisfyRequirement(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(Record{
		Key:         "notion_oauth",
		Provider:    "notion",
		GrantType:   GrantAuthorizationCode,
		TokenURL:    "https://api.notion.test/oauth/token",
		ClientID:    "notion-client",
		GrantModel:  managedcredentialmodel.GrantModelWorkspace,
		AccessToken: "fresh-token",
		Status:      StatusConnected,
		ExpiresAt:   time.Now().Add(time.Hour),
	})
	source := TokenSource{Store: store}
	token, _, err := source.AccessToken(ctx, AccessTokenRequest{
		Key:        "notion_oauth",
		GrantModel: managedcredentialmodel.GrantModelWorkspace,
		TokenRequest: managedcredentialmodel.TokenRequestProfile{
			ClientAuth: managedcredentialmodel.TokenClientAuthBasic,
			Body:       managedcredentialmodel.TokenBodyJSON,
			StaticHeaders: map[string]string{
				"Notion-Version": "2026-03-11",
			},
		},
	})
	if err == nil {
		t.Fatal("AccessToken error = nil, want token-request-insufficient")
	}
	if token != "" {
		t.Fatalf("token = %q, want no token on token_request mismatch", token)
	}
	if !strings.Contains(err.Error(), "token-request-insufficient") {
		t.Fatalf("AccessToken error = %v, want token-request-insufficient", err)
	}
}

func TestTokenSourceClientCredentialsFailureRedactsProviderSecretEcho(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":             "invalid_client",
			"error_description": "client secret cli-secret was rejected",
		})
	}))
	defer server.Close()

	store := NewMemoryStore()
	source := TokenSource{Store: store}
	_, err := source.ConnectClientCredentials(ctx, ClientCredentialsRequest{
		Key:          "github",
		Provider:     "github",
		TokenURL:     server.URL,
		ClientID:     "client-id",
		ClientSecret: "cli-secret",
		Scopes:       []string{"repo.read"},
	})
	if err == nil {
		t.Fatal("ConnectClientCredentials error = nil, want redacted token endpoint failure")
	}
	if strings.Contains(err.Error(), "cli-secret") {
		t.Fatalf("ConnectClientCredentials error leaked client secret: %v", err)
	}
	stored, ok, getErr := store.Get(ctx, "github")
	if getErr != nil || !ok {
		t.Fatalf("store.Get = (%#v, %v, %v)", stored, ok, getErr)
	}
	if stored.Status != StatusRefreshFailed {
		t.Fatalf("stored status = %q, want refresh_failed", stored.Status)
	}
	if strings.Contains(stored.Failure, "cli-secret") {
		t.Fatalf("stored failure leaked client secret: %q", stored.Failure)
	}
}

func TestTokenSourceClientCredentialsFailurePropagatesFailureStatePersistenceError(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":             "invalid_client",
			"error_description": "client secret cli-secret was rejected",
		})
	}))
	defer server.Close()

	store := failingPutStore{
		Store: NewMemoryStore(),
		err:   errors.New("permission denied while writing cli-secret"),
	}
	source := TokenSource{Store: store}
	record, err := source.ConnectClientCredentials(ctx, ClientCredentialsRequest{
		Key:          "github",
		Provider:     "github",
		TokenURL:     server.URL,
		ClientID:     "client-id",
		ClientSecret: "cli-secret",
		Scopes:       []string{"repo.read"},
	})
	if err == nil {
		t.Fatal("ConnectClientCredentials error = nil, want persistence failure surfaced")
	}
	if !strings.Contains(err.Error(), "persist refresh_failed state") {
		t.Fatalf("ConnectClientCredentials error = %v, want persistence failure context", err)
	}
	if strings.Contains(err.Error(), "cli-secret") {
		t.Fatalf("ConnectClientCredentials error leaked client secret: %v", err)
	}
	if record.Status != StatusRefreshFailed {
		t.Fatalf("returned record status = %q, want refresh_failed", record.Status)
	}
	if strings.Contains(record.Failure, "cli-secret") {
		t.Fatalf("returned record failure leaked client secret: %q", record.Failure)
	}
	if _, ok, getErr := store.Store.Get(ctx, "github"); getErr != nil || ok {
		t.Fatalf("underlying store.Get = (_, %v, %v), want no silently persisted record", ok, getErr)
	}
}

func TestTokenSourceAccessTokenFailsClosedWhenRefreshNarrowsScopes(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm: %v", err)
		}
		if got := r.Form.Get("grant_type"); got != "client_credentials" {
			t.Fatalf("grant_type = %q, want client_credentials", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "narrow-token",
			"expires_in":   3600,
			"scope":        "repo.read",
		})
	}))
	defer server.Close()

	now := time.Date(2026, 7, 3, 12, 0, 0, 0, time.UTC)
	store := NewMemoryStore(Record{
		Key:         "github",
		GrantType:   GrantClientCredentials,
		TokenURL:    server.URL,
		ClientID:    "client-id",
		AccessToken: "expired-token",
		ExpiresAt:   now.Add(-time.Second),
		Scopes:      []string{"repo.read", "repo.write"},
		Status:      StatusConnected,
	})
	source := TokenSource{
		Store: store,
		Now: func() time.Time {
			return now
		},
	}
	token, record, err := source.AccessToken(ctx, AccessTokenRequest{Key: "github", Scopes: []string{"repo.write"}})
	if err == nil {
		t.Fatal("AccessToken error = nil, want scope-insufficient after narrowed refresh")
	}
	if token != "" {
		t.Fatalf("token = %q, want empty token on narrowed scope", token)
	}
	if !strings.Contains(err.Error(), "scope-insufficient") || !strings.Contains(err.Error(), "repo.write") {
		t.Fatalf("AccessToken error = %v, want scope-insufficient repo.write", err)
	}
	if record.AccessToken != "narrow-token" {
		t.Fatalf("returned record token = %q, want refreshed narrow token retained only in record", record.AccessToken)
	}
	if record.Status != StatusConnected {
		t.Fatalf("returned record status = %q, want connected for per-request scope failure", record.Status)
	}
	stored, ok, getErr := store.Get(ctx, "github")
	if getErr != nil || !ok {
		t.Fatalf("store.Get = (%#v, %v, %v)", stored, ok, getErr)
	}
	if got := strings.Join(stored.Scopes, " "); got != "repo.read" {
		t.Fatalf("stored scopes = %q, want provider-narrowed repo.read", got)
	}
	if stored.Status != StatusConnected {
		t.Fatalf("stored status = %q, want connected for per-request scope failure", stored.Status)
	}
}

func TestTokenSourceRefreshFailureFailsClosedAndRedactsSecrets(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":             "invalid_client",
			"error_description": "client-secret was rejected with refresh-secret",
		})
	}))
	defer server.Close()

	store := NewMemoryStore(Record{
		Key:          "github",
		GrantType:    GrantAuthorizationCodePKCE,
		TokenURL:     server.URL,
		ClientID:     "client-id",
		ClientSecret: "client-secret",
		AccessToken:  "access-secret",
		RefreshToken: "refresh-secret",
		Status:       StatusConnected,
	})
	source := TokenSource{Store: store}
	_, record, err := source.Refresh(ctx, "github")
	if err == nil {
		t.Fatal("Refresh error = nil, want fail-closed error")
	}
	if strings.Contains(err.Error(), "client-secret") || strings.Contains(err.Error(), "refresh-secret") {
		t.Fatalf("refresh error leaked secret material: %v", err)
	}
	stored, ok, getErr := store.Get(ctx, "github")
	if getErr != nil || !ok {
		t.Fatalf("store.Get = (%#v, %v, %v)", stored, ok, getErr)
	}
	if stored.Status != StatusRefreshFailed {
		t.Fatalf("stored status = %q, want refresh_failed", stored.Status)
	}
	if strings.Contains(stored.Failure, "client-secret") || strings.Contains(stored.Failure, "refresh-secret") {
		t.Fatalf("stored failure leaked secret material: %q", stored.Failure)
	}
	if record.Status != StatusRefreshFailed {
		t.Fatalf("returned record status = %q, want refresh_failed", record.Status)
	}
}

func TestTokenSourceRefreshFailurePropagatesFailureStatePersistenceError(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":             "invalid_client",
			"error_description": "client-secret was rejected with refresh-secret",
		})
	}))
	defer server.Close()

	inner := NewMemoryStore(Record{
		Key:          "github",
		GrantType:    GrantAuthorizationCodePKCE,
		TokenURL:     server.URL,
		ClientID:     "client-id",
		ClientSecret: "client-secret",
		AccessToken:  "access-secret",
		RefreshToken: "refresh-secret",
		Status:       StatusConnected,
	})
	store := failingPutStore{
		Store: inner,
		err:   errors.New("database locked while writing client-secret and refresh-secret"),
	}
	source := TokenSource{Store: store}
	_, record, err := source.Refresh(ctx, "github")
	if err == nil {
		t.Fatal("Refresh error = nil, want persistence failure surfaced")
	}
	if !strings.Contains(err.Error(), "persist refresh_failed state") {
		t.Fatalf("Refresh error = %v, want persistence failure context", err)
	}
	if strings.Contains(err.Error(), "client-secret") || strings.Contains(err.Error(), "refresh-secret") {
		t.Fatalf("Refresh error leaked secret material: %v", err)
	}
	if record.Status != StatusRefreshFailed {
		t.Fatalf("returned record status = %q, want refresh_failed", record.Status)
	}
	if strings.Contains(record.Failure, "client-secret") || strings.Contains(record.Failure, "refresh-secret") {
		t.Fatalf("returned record failure leaked secret material: %q", record.Failure)
	}
	stored, ok, getErr := inner.Get(ctx, "github")
	if getErr != nil || !ok {
		t.Fatalf("inner.Get = (%#v, %v, %v), want original stale record", stored, ok, getErr)
	}
	if stored.Status != StatusConnected {
		t.Fatalf("stale stored status = %q, want connected to prove write failure was not hidden", stored.Status)
	}
}

func testGitHubAppPrivateKey(t *testing.T) (string, *rsa.PublicKey) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	raw := x509.MarshalPKCS1PrivateKey(key)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: raw})
	if len(pemBytes) == 0 {
		t.Fatal("EncodeToMemory returned empty PEM")
	}
	return string(pemBytes), &key.PublicKey
}

func testVerifyGitHubAppJWT(t *testing.T, token string, publicKey *rsa.PublicKey, wantIss string, now time.Time) {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("JWT parts = %d, want 3", len(parts))
	}
	decode := func(name, raw string, target any) {
		t.Helper()
		decoded, err := base64.RawURLEncoding.DecodeString(raw)
		if err != nil {
			t.Fatalf("decode JWT %s: %v", name, err)
		}
		if err := json.Unmarshal(decoded, target); err != nil {
			t.Fatalf("unmarshal JWT %s: %v", name, err)
		}
	}
	var header struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
	}
	decode("header", parts[0], &header)
	if header.Alg != "RS256" || header.Typ != "JWT" {
		t.Fatalf("JWT header = %#v, want RS256 JWT", header)
	}
	var claims struct {
		Iss string `json:"iss"`
		Iat int64  `json:"iat"`
		Exp int64  `json:"exp"`
	}
	decode("claims", parts[1], &claims)
	if claims.Iss != wantIss {
		t.Fatalf("JWT iss = %q, want %q", claims.Iss, wantIss)
	}
	if claims.Iat > now.Unix() || claims.Exp <= now.Unix() || claims.Exp-claims.Iat > 10*60+60 {
		t.Fatalf("JWT time claims = %#v, want active short-lived app JWT around %s", claims, now)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatalf("decode JWT signature: %v", err)
	}
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(publicKey, crypto.SHA256, sum[:], signature); err != nil {
		t.Fatalf("VerifyPKCS1v15: %v", err)
	}
}

type failingPutStore struct {
	Store
	err error
}

func (s failingPutStore) Put(context.Context, Record) error {
	if s.err != nil {
		return s.err
	}
	return errors.New("put failed")
}
