package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
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
	} {
		if strings.Contains(raw, secret) {
			return true
		}
	}
	return false
}
