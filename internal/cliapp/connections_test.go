package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	managedcredentialmodel "github.com/division-sh/swarm/internal/runtime/managedcredentials/model"
)

func TestConnectionsClientCredentialsStatusDisconnectRedactsTokenMaterial(t *testing.T) {
	repo := t.TempDir()
	t.Setenv("SWARM_MANAGED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "managed.json"))
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm: %v", err)
		}
		if got := r.Form.Get("grant_type"); got != "client_credentials" {
			t.Fatalf("grant_type = %q, want client_credentials", got)
		}
		if got := r.Form.Get("client_secret"); got != "cli-secret" {
			t.Fatalf("client_secret = %q, want cli-secret", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "cli-access-token",
			"expires_in":   3600,
			"scope":        "repo.read",
		})
	}))
	defer tokenServer.Close()

	code, stdout, stderr := executeRootCommandWithInput(context.Background(), repo, []string{
		"connections", "connect", "github",
		"--grant", "client_credentials",
		"--provider", "github",
		"--token-url", tokenServer.URL,
		"--client-id", "client-id",
		"--client-secret-stdin",
		"--scope", "repo.read",
		"--json",
	}, "cli-secret\n")
	if code != 0 {
		t.Fatalf("connections connect exit = %d err=%s", code, stderr)
	}
	if outputLeaksManagedSecret(stdout) {
		t.Fatalf("connect output leaked secret material: %s", stdout)
	}
	if !strings.Contains(stdout, `"status": "connected"`) {
		t.Fatalf("connect output = %s, want connected status", stdout)
	}

	var out bytes.Buffer
	var errOut bytes.Buffer
	out.Reset()
	errOut.Reset()
	code = executeRootCommand(context.Background(), repo, []string{"connections", "status", "github", "--json"}, &out, &errOut)
	if code != 0 {
		t.Fatalf("connections status exit = %d err=%s", code, errOut.String())
	}
	if outputLeaksManagedSecret(out.String()) {
		t.Fatalf("status output leaked secret material: %s", out.String())
	}
	if !strings.Contains(out.String(), `"present": true`) {
		t.Fatalf("status output = %s, want present true", out.String())
	}

	out.Reset()
	errOut.Reset()
	code = executeRootCommand(context.Background(), repo, []string{"connections", "disconnect", "github"}, &out, &errOut)
	if code != 0 {
		t.Fatalf("connections disconnect exit = %d err=%s", code, errOut.String())
	}

	out.Reset()
	errOut.Reset()
	code = executeRootCommand(context.Background(), repo, []string{"connections", "status", "github", "--json"}, &out, &errOut)
	if code != 0 {
		t.Fatalf("connections status after disconnect exit = %d err=%s", code, errOut.String())
	}
	if !strings.Contains(out.String(), `"present": false`) {
		t.Fatalf("status after disconnect output = %s, want present false", out.String())
	}
}

func TestConnectionsAuthCodeCallbackCompletesPKCERecord(t *testing.T) {
	repo := t.TempDir()
	t.Setenv("SWARM_MANAGED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "managed.json"))
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatalf("ParseForm: %v", err)
		}
		if got := r.Form.Get("grant_type"); got != "authorization_code" {
			t.Fatalf("grant_type = %q, want authorization_code", got)
		}
		if got := r.Form.Get("code"); got != "callback-code" {
			t.Fatalf("code = %q, want callback-code", got)
		}
		if r.Form.Get("code_verifier") == "" {
			t.Fatal("code_verifier is empty")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "callback-access-token",
			"refresh_token": "callback-refresh-token",
			"expires_in":    3600,
			"scope":         "drive.read",
		})
	}))
	defer tokenServer.Close()

	var out bytes.Buffer
	var errOut bytes.Buffer
	code := executeRootCommand(context.Background(), repo, []string{
		"connections", "connect", "drive",
		"--grant", "authorization_code_pkce",
		"--provider", "google",
		"--auth-url", tokenServer.URL + "/auth",
		"--token-url", tokenServer.URL,
		"--client-id", "client-id",
		"--redirect-url", "http://127.0.0.1/callback",
		"--scope", "drive.read",
		"--json",
	}, &out, &errOut)
	if code != 0 {
		t.Fatalf("connections connect exit = %d err=%s", code, errOut.String())
	}
	if outputLeaksManagedSecret(out.String()) {
		t.Fatalf("auth connect output leaked secret material: %s", out.String())
	}
	var begin struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(out.Bytes(), &begin); err != nil {
		t.Fatalf("decode connect output: %v\n%s", err, out.String())
	}
	if begin.State == "" {
		t.Fatalf("connect output missing state: %s", out.String())
	}

	out.Reset()
	errOut.Reset()
	code, stdout, stderr := executeRootCommandWithInput(context.Background(), repo, []string{
		"connections", "callback", "drive",
		"--state", begin.State,
		"--code-stdin",
		"--json",
	}, "callback-code\n")
	if code != 0 {
		t.Fatalf("connections callback exit = %d err=%s", code, stderr)
	}
	if outputLeaksManagedSecret(stdout) {
		t.Fatalf("callback output leaked secret material: %s", stdout)
	}
	if !strings.Contains(stdout, `"status": "connected"`) {
		t.Fatalf("callback output = %s, want connected status", stdout)
	}
}

func TestConnectionsAuthCodeCallbackCompletesBasicJSONWorkspaceGrantRecord(t *testing.T) {
	repo := t.TempDir()
	t.Setenv("SWARM_MANAGED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "managed.json"))
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			t.Fatalf("decode token JSON body: %v", err)
		}
		if body["grant_type"] != "authorization_code" || body["code"] != "notion-code" {
			t.Fatalf("token body = %#v, want Notion auth-code exchange", body)
		}
		if body["client_id"] != "" || body["client_secret"] != "" || body["code_verifier"] != "" {
			t.Fatalf("token body leaked post-auth fields for Basic auth: %#v", body)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "notion-access-token",
			"refresh_token": "notion-refresh-token",
			"expires_in":    3600,
		})
	}))
	defer tokenServer.Close()

	code, stdout, stderr := executeRootCommandWithInput(context.Background(), repo, []string{
		"connections", "connect", "notion_oauth",
		"--grant", "authorization_code",
		"--provider", "notion",
		"--auth-url", tokenServer.URL + "/auth",
		"--token-url", tokenServer.URL + "/v1/oauth/token",
		"--client-id", "notion-client",
		"--client-secret-stdin",
		"--redirect-url", "http://127.0.0.1/callback",
		"--grant-model", "workspace_grant",
		"--token-client-auth", "basic",
		"--token-body", "json",
		"--token-header", "Notion-Version=2026-03-11",
		"--json",
	}, "notion-secret\n")
	if code != 0 {
		t.Fatalf("connections connect exit = %d err=%s", code, stderr)
	}
	if outputLeaksManagedSecret(stdout) {
		t.Fatalf("connect output leaked secret material: %s", stdout)
	}
	var begin connectionsConnectResult
	if err := json.Unmarshal([]byte(stdout), &begin); err != nil {
		t.Fatalf("decode connect output: %v\n%s", err, stdout)
	}
	authorizeURL, err := url.Parse(begin.AuthorizeURL)
	if err != nil {
		t.Fatalf("parse authorize_url: %v", err)
	}
	if begin.State == "" || authorizeURL.Query().Get("state") == "" {
		t.Fatalf("connect output missing state: %#v", begin)
	}
	if authorizeURL.Query().Get("code_challenge") != "" || authorizeURL.Query().Get("code_challenge_method") != "" {
		t.Fatalf("authorization_code connect emitted PKCE params: %s", begin.AuthorizeURL)
	}
	if begin.Connection.GrantModel != managedcredentialmodel.GrantModelWorkspace || begin.Connection.TokenRequest.ClientAuth != managedcredentialmodel.TokenClientAuthBasic || begin.Connection.TokenRequest.Body != managedcredentialmodel.TokenBodyJSON {
		t.Fatalf("connect record = %#v, want workspace Basic JSON profile", begin.Connection)
	}

	code, stdout, stderr = executeRootCommandWithInput(context.Background(), repo, []string{
		"connections", "callback", "notion_oauth",
		"--state", begin.State,
		"--code-stdin",
		"--json",
	}, "notion-code\n")
	if code != 0 {
		t.Fatalf("connections callback exit = %d err=%s", code, stderr)
	}
	if outputLeaksManagedSecret(stdout) {
		t.Fatalf("callback output leaked secret material: %s", stdout)
	}
	var completed connectionsConnectResult
	if err := json.Unmarshal([]byte(stdout), &completed); err != nil {
		t.Fatalf("decode callback output: %v\n%s", err, stdout)
	}
	if completed.Connection.Status != "connected" || completed.Connection.GrantModel != managedcredentialmodel.GrantModelWorkspace || completed.Connection.TokenRequest.StaticHeaders["Notion-Version"] != "2026-03-11" {
		t.Fatalf("completed record = %#v, want connected Notion profile", completed.Connection)
	}

	var out bytes.Buffer
	var errOut bytes.Buffer
	code = executeRootCommand(context.Background(), repo, []string{"connections", "status", "notion_oauth", "--json"}, &out, &errOut)
	if code != 0 {
		t.Fatalf("connections status exit = %d err=%s", code, errOut.String())
	}
	if outputLeaksManagedSecret(out.String()) {
		t.Fatalf("status output leaked secret material: %s", out.String())
	}
	var status connectionsStatusResult
	if err := json.Unmarshal(out.Bytes(), &status); err != nil {
		t.Fatalf("decode status output: %v\n%s", err, out.String())
	}
	if len(status.Connections) != 1 || status.Connections[0].GrantModel != managedcredentialmodel.GrantModelWorkspace || status.Connections[0].TokenRequest.ClientAuth != managedcredentialmodel.TokenClientAuthBasic {
		t.Fatalf("status = %#v, want Notion grant/profile readback", status)
	}
}

func TestConnectionsRejectSecretBearingArgvFlags(t *testing.T) {
	repo := t.TempDir()
	t.Setenv("SWARM_MANAGED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "managed.json"))

	code, stdout, stderr := executeRootCommandWithInput(context.Background(), repo, []string{
		"connections", "connect", "github",
		"--grant", "client_credentials",
		"--token-url", "https://example.invalid/token",
		"--client-id", "client-id",
		"--client-secret", "argv-secret",
	}, "")
	if code == 0 {
		t.Fatalf("connections connect with --client-secret exit = 0, want failure")
	}
	if strings.Contains(stdout+stderr, "argv-secret") {
		t.Fatalf("--client-secret failure leaked secret stdout=%q stderr=%q", stdout, stderr)
	}

	code, stdout, stderr = executeRootCommandWithInput(context.Background(), repo, []string{
		"connections", "callback", "github",
		"--state", "state",
		"--code", "argv-code",
	}, "")
	if code == 0 {
		t.Fatalf("connections callback with --code exit = 0, want failure")
	}
	if strings.Contains(stdout+stderr, "argv-code") {
		t.Fatalf("--code failure leaked code stdout=%q stderr=%q", stdout, stderr)
	}
}

func outputLeaksManagedSecret(raw string) bool {
	for _, secret := range []string{
		"cli-access-token",
		"cli-secret",
		"callback-access-token",
		"callback-refresh-token",
		"callback-code",
		"notion-secret",
		"notion-access-token",
		"notion-refresh-token",
		"notion-code",
	} {
		if strings.Contains(raw, secret) {
			return true
		}
	}
	return false
}
