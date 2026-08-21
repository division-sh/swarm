package serveapp

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packartifact"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/packfixture"
)

func TestInboundAdmissionSupportedSurfacePolicyMatrixSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runInboundAdmissionSupportedSurfacePolicyMatrix(t, backend)
		})
	}
}

func TestInboundAdmissionSupportedSurfaceStartupFailuresSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			var postgresStore *store.PostgresStore
			var postgresDSN string
			if backend == "postgres" {
				dsn, _, cleanup := testutil.StartPostgres(t)
				postgresDSN = dsn
				t.Cleanup(cleanup)
				oldBuildStores := buildStoresForServe
				oldWorkspace := cliapp.ConfiguredWorkspaceLifecycleForServe
				buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (storeBundle, error) {
					storetest.BootstrapPostgresRuntimeStore(t, postgresStore)
					return selectedPostgresStoreBundle(postgresStore, storetest.DatabaseForTest(postgresStore), cfg), nil
				}
				cliapp.ConfiguredWorkspaceLifecycleForServe = func(workspace.Lookup, *config.Config, string, semanticview.Source, cliapp.WorkspaceMountSources, cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
					return serveRuntimeWorkspaceStub{}, nil
				}
				t.Cleanup(func() {
					buildStoresForServe = oldBuildStores
					cliapp.ConfiguredWorkspaceLifecycleForServe = oldWorkspace
				})
			}

			for _, tc := range []struct {
				name          string
				mutatePackage func(string) string
				want          string
			}{
				{
					name: "missing pinned pack",
					mutatePackage: func(body string) string {
						return strings.Replace(body, "pack: {id: provider.telegram}", "pack: {id: provider.telegram_missing}", 1)
					},
					want: `verified pack for "telegram" is "provider.telegram"`,
				},
				{
					name: "provider pack mismatch",
					mutatePackage: func(body string) string {
						return strings.Replace(body, "pack: {id: provider.telegram}", "pack: {id: provider.slack}", 1)
					},
					want: `which provides "slack"`,
				},
				{
					name: "missing project pack",
					mutatePackage: func(body string) string {
						return strings.Replace(body, `        - provider: acme_public
          admission:
            kind: raw
            acknowledge: unsigned_webhook
            authentication: {kind: none}
            event: inbound.acme_public
            delivery_id: {source: body_sha256}
            payload: json`, `        - provider: acme_public
          admission:
            pack: {id: provider.acme_public}`, 1)
					},
					want: `pins pack "provider.acme_public", but that id is not selected`,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					contractsRoot := writeInboundAdmissionPolicyMatrixFixture(t)
					packagePath := filepath.Join(contractsRoot, "package.yaml")
					body, err := os.ReadFile(packagePath)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(packagePath, []byte(tc.mutatePackage(string(body))), 0o600); err != nil {
						t.Fatal(err)
					}
					if backend == "postgres" {
						postgresStore, err = store.NewPostgresStore(postgresDSN)
						if err != nil {
							t.Fatal(err)
						}
					}
					configPath := writeInboundAdmissionRuntimeConfig(t, backend, filepath.Join(t.TempDir(), "failure.sqlite"))
					process := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
						ConfigPath: configPath, ContractsPath: contractsRoot, PlatformSpecPath: defaultPlatformSpecPath,
						StoreMode: backend, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
						SelfCheck: true, RequireBundleMatch: false, Dev: true, Verbose: true,
					})
					code, exited := process.waitForExit(15 * time.Second)
					if !exited {
						process.cleanup()
						t.Fatal("invalid admission candidate reached a live served runtime")
					}
					process.recordStopped(code)
					output := process.outputString()
					if code == 0 || strings.Contains(output, "swarm runtime ready") || !strings.Contains(output, tc.want) {
						t.Fatalf("exit=%d want=%q\n%s", code, tc.want, output)
					}
				})
			}
		})
	}
}

func runInboundAdmissionSupportedSurfacePolicyMatrix(t *testing.T, backend string) {
	t.Helper()
	isolateCLIAPIConfigEnv(t)
	contractsRoot := writeInboundAdmissionPolicyMatrixFixture(t)
	importProjectTelegramTrigger(t, contractsRoot)
	dataRoot := t.TempDir()
	credentialPath := filepath.Join(dataRoot, "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	credentialStore, err := runtimecredentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{
		"webhook_signing.telegram": "telegram-secret",
		"webhook_signing.intercom": "intercom-secret",
		"webhook_signing.partner":  "partner-secret",
	} {
		if err := credentialStore.Set(context.Background(), key, value); err != nil {
			t.Fatalf("set credential %s: %v", key, err)
		}
	}

	var sqliteStore *store.SQLiteRuntimeStore
	var postgresStore *store.PostgresStore
	var postgresDSN string
	configPath := ""
	if backend == "sqlite" {
		sqlitePath := filepath.Join(dataRoot, "admission.sqlite")
		configPath = writeInboundAdmissionRuntimeConfig(t, backend, sqlitePath)
	} else {
		dsn, _, cleanup := testutil.StartPostgres(t)
		postgresDSN = dsn
		t.Cleanup(cleanup)
		postgresStore, err = store.NewPostgresStore(dsn)
		if err != nil {
			t.Fatal(err)
		}
		oldBuildStores := buildStoresForServe
		oldWorkspace := cliapp.ConfiguredWorkspaceLifecycleForServe
		buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (storeBundle, error) {
			storetest.BootstrapPostgresRuntimeStore(t, postgresStore)
			return selectedPostgresStoreBundle(postgresStore, storetest.DatabaseForTest(postgresStore), cfg), nil
		}
		cliapp.ConfiguredWorkspaceLifecycleForServe = func(workspace.Lookup, *config.Config, string, semanticview.Source, cliapp.WorkspaceMountSources, cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
			return serveRuntimeWorkspaceStub{}, nil
		}
		t.Cleanup(func() {
			buildStoresForServe = oldBuildStores
			cliapp.ConfiguredWorkspaceLifecycleForServe = oldWorkspace
		})
		configPath = writeInboundAdmissionRuntimeConfig(t, backend, "")
	}

	opts := cliapp.ServeOptions{
		ConfigPath: configPath, ContractsPath: contractsRoot, PlatformSpecPath: defaultPlatformSpecPath,
		StoreMode: backend, StoreModeSet: true, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, RequireBundleMatch: false, Dev: true, Verbose: true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}
	process := startServeRuntimeTestProcess(t, opts)
	process.waitForReadyLine()
	waitForInboundAdmissionServeOutput(t, process, "[WARN] inbound_unsigned_webhook")
	serveOutput := process.outputString()
	for _, want := range []string{"packs                      embedded · 14 packs", "effective sha256:", "project provider.telegram"} {
		if !strings.Contains(serveOutput, want) {
			t.Fatalf("serve readback omitted imported Telegram inventory fact %q:\n%s", want, serveOutput)
		}
	}
	var unsignedWarningLine string
	for _, line := range strings.Split(serveOutput, "\n") {
		if strings.Contains(line, "[WARN] inbound_unsigned_webhook") {
			if unsignedWarningLine != "" {
				t.Fatalf("serve emitted duplicate unsigned warning:\n%s", serveOutput)
			}
			unsignedWarningLine = line
		}
	}
	if unsignedWarningLine == "" || !strings.Contains(unsignedWarningLine, `provider "partner_open" accepts unsigned webhooks`) || strings.Contains(unsignedWarningLine, "partner_ack") || !strings.Contains(serveOutput, "remediation: add admission.acknowledge: unsigned_webhook") {
		t.Fatalf("serve unsigned warning line=%q\noutput:\n%s", unsignedWarningLine, serveOutput)
	}
	for _, provider := range []string{"partner_open", "partner_ack"} {
		found := false
		for _, line := range strings.Split(serveOutput, "\n") {
			if strings.Contains(line, provider+" webhook") {
				found = true
				break
			}
		}
		for _, forbidden := range []string{"request_authentication=", "catalog_generation=", "manifest_hash=", "policy_source=", "provenance=", "source_path=", "standing ingress admitted:"} {
			if strings.Contains(serveOutput, forbidden) {
				t.Fatalf("serve output leaked diagnostic field %q:\n%s", forbidden, serveOutput)
			}
		}
		if !found {
			t.Fatalf("serve readback omitted %s UNAUTHENTICATED truth:\n%s", provider, serveOutput)
		}
	}
	baseURL := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString())

	tests := []struct {
		provider   string
		body       []byte
		headers    map[string]string
		eventNames []string
	}{
		{provider: "telegram", body: []byte(`{"update_id":901,"message":{"message_id":901,"from":{"id":7},"chat":{"id":42,"type":"private"},"text":"hello"}}`), headers: map[string]string{"X-Telegram-Bot-Api-Secret-Token": "telegram-secret"}, eventNames: []string{"inbound.telegram", "inbound.telegram.text_message"}},
		{provider: "intercom", body: []byte(`{"id":"platform-unsigned-1","topic":"contact.created"}`), eventNames: []string{"inbound.intercom"}},
		{provider: "acme_public", body: []byte(`{"id":"external-unsigned-1"}`), eventNames: []string{"inbound.acme_public"}},
		{provider: "partner_auth", body: []byte(`{"value":1}`), headers: map[string]string{"X-Partner-Delivery": "partner-auth-1"}, eventNames: []string{"inbound.partner_auth"}},
		{provider: "partner_open", body: []byte(`{"delivery":{"id":"partner-open-1"},"value":2}`), eventNames: []string{"inbound.partner_open"}},
		{provider: "partner_ack", body: []byte("raw-open-body"), eventNames: []string{"inbound.partner_ack"}},
	}
	for i := range tests {
		test := &tests[i]
		if test.headers == nil {
			test.headers = map[string]string{}
		}
		if test.provider == "intercom" {
			mac := hmac.New(sha1.New, []byte("intercom-secret"))
			_, _ = mac.Write(test.body)
			test.headers["X-Hub-Signature"] = "sha1=" + hex.EncodeToString(mac.Sum(nil))
		}
		if test.provider == "partner_auth" {
			mac := hmac.New(sha256.New, []byte("partner-secret"))
			_, _ = mac.Write(test.body)
			test.headers["X-Partner-Signature"] = hex.EncodeToString(mac.Sum(nil))
		}
		status, response := sendInboundAdmissionSupportedRequest(t, baseURL, test.provider, test.body, test.headers)
		if status != http.StatusAccepted {
			t.Fatalf("%s status=%d response=%s\nserve output:\n%s", test.provider, status, response, process.outputString())
		}
		accepted := decodeInboundAdmissionPublicationResponse(t, response)
		if strings.Join(accepted.EventNames, "\x00") != strings.Join(test.eventNames, "\x00") || len(accepted.EventIDs) != len(test.eventNames) {
			t.Fatalf("%s accepted ordered children ids=%v names=%v, want names=%v", test.provider, accepted.EventIDs, accepted.EventNames, test.eventNames)
		}
		status, response = sendInboundAdmissionSupportedRequest(t, baseURL, test.provider, test.body, test.headers)
		if status != http.StatusOK {
			t.Fatalf("%s duplicate status=%d response=%s", test.provider, status, response)
		}
		duplicate := decodeInboundAdmissionPublicationResponse(t, response)
		if duplicate.Status != "duplicate" || strings.Join(duplicate.EventIDs, "\x00") != strings.Join(accepted.EventIDs, "\x00") || strings.Join(duplicate.EventNames, "\x00") != strings.Join(accepted.EventNames, "\x00") {
			t.Fatalf("%s duplicate readback=%#v, want original %#v", test.provider, duplicate, accepted)
		}
	}
	status, _ := sendInboundAdmissionSupportedRequest(t, baseURL, "partner_auth", []byte(`{"value":1}`), map[string]string{
		"X-Partner-Delivery":  "partner-auth-invalid",
		"X-Partner-Signature": "invalid",
	})
	if status != http.StatusUnauthorized {
		t.Fatalf("invalid authenticated raw status=%d, want 401", status)
	}
	status, response := sendInboundAdmissionSupportedRequest(t, baseURL, "telegram", []byte(`[]`), map[string]string{
		"X-Telegram-Bot-Api-Secret-Token": "telegram-secret",
	})
	if status != http.StatusBadRequest || !strings.Contains(response, "project telegram update object is required") {
		t.Fatalf("project Telegram invalid-payload status=%d response=%s", status, response)
	}
	if code := process.stop(); code != 0 {
		t.Fatalf("serve exit=%d\n%s", code, process.outputString())
	}
	if backend == "postgres" {
		postgresStore, err = store.NewPostgresStore(postgresDSN)
		if err != nil {
			t.Fatal(err)
		}
	}
	restarted := startServeRuntimeTestProcess(t, opts)
	restarted.waitForReadyLine()
	restartURL := "http://" + serveRuntimeAPIListenerFromOutput(t, restarted.outputString())
	status, response = sendInboundAdmissionSupportedRequest(t, restartURL, "telegram", []byte(`{"update_id":902,"message":{"message_id":902,"from":{"id":7},"chat":{"id":42,"type":"private"},"text":"after restart"}}`), map[string]string{
		"X-Telegram-Bot-Api-Secret-Token": "telegram-secret",
	})
	if status != http.StatusAccepted {
		t.Fatalf("project Telegram after restart status=%d response=%s\nserve output:\n%s", status, response, restarted.outputString())
	}
	if code := restarted.stop(); code != 0 {
		t.Fatalf("restarted serve exit=%d\n%s", code, restarted.outputString())
	}

	if backend == "sqlite" {
		sqlitePath := inboundAdmissionSQLitePathFromConfig(t, configPath)
		sqliteStore, err = store.NewSQLiteRuntimeStore(sqlitePath)
		if err != nil {
			t.Fatal(err)
		}
		defer sqliteStore.Close()
	} else {
		postgresStore, err = store.NewPostgresStore(postgresDSN)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, test := range tests {
		for _, eventName := range test.eventNames {
			var count int
			if backend == "sqlite" {
				err = storetest.DatabaseForTest(sqliteStore).QueryRow(`SELECT COUNT(*) FROM events WHERE event_name = ?`, eventName).Scan(&count)
			} else {
				err = storetest.DatabaseForTest(postgresStore).QueryRow(`SELECT COUNT(*) FROM events WHERE event_name = $1`, eventName).Scan(&count)
			}
			want := 1
			if test.provider == "telegram" {
				want = 2
			}
			if err != nil || count != want {
				t.Fatalf("%s persisted count=%d err=%v, want %d", eventName, count, err, want)
			}
		}
	}
}

func importProjectTelegramTrigger(t testing.TB, contractsRoot string) {
	t.Helper()
	base := packfixture.EmbeddedBase(t)
	if changed, err := packartifact.ImportEmbeddedPack(contractsRoot, "provider.telegram", base); err != nil || !changed {
		t.Fatalf("import project Telegram trigger changed=%t: %v", changed, err)
	}
	path := filepath.Join(contractsRoot, packartifact.ProjectPackDirectory, "provider.telegram", packartifact.TriggerManifestFileName)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	edited := strings.Replace(string(body), "telegram update object is required", "project telegram update object is required", 1)
	if edited == string(body) {
		t.Fatal("project Telegram trigger edit found no canonical validation message")
	}
	if err := os.WriteFile(path, []byte(edited), 0o644); err != nil {
		t.Fatal(err)
	}
}

func waitForInboundAdmissionServeOutput(t *testing.T, process *serveRuntimeTestProcess, evidence string) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for {
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for serve output %q:\n%s", evidence, process.outputString())
		case <-ticker.C:
			if strings.Contains(process.outputString(), evidence) {
				return
			}
		}
	}
}

type inboundAdmissionPublicationResponse struct {
	Status     string   `json:"status"`
	EventIDs   []string `json:"event_ids"`
	EventNames []string `json:"event_names"`
}

func decodeInboundAdmissionPublicationResponse(t testing.TB, raw string) inboundAdmissionPublicationResponse {
	t.Helper()
	var response inboundAdmissionPublicationResponse
	if err := json.Unmarshal([]byte(raw), &response); err != nil {
		t.Fatalf("decode inbound publication response: %v body=%s", err, raw)
	}
	return response
}

func sendInboundAdmissionSupportedRequest(t testing.TB, baseURL, provider string, body []byte, headers map[string]string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, strings.TrimRight(baseURL, "/")+"/webhooks/matrix/"+provider, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	response := new(bytes.Buffer)
	_, _ = response.ReadFrom(resp.Body)
	return resp.StatusCode, response.String()
}

func writeInboundAdmissionPolicyMatrixFixture(t testing.TB) string {
	t.Helper()
	return canonicalrouting.CopyInboundAdmissionPolicyMatrix(t)
}

func writeInboundAdmissionRuntimeConfig(t testing.TB, backend, sqlitePath string) string {
	t.Helper()
	lines := []string{"runtime:", "  execution_posture: live", "  recovery_on_startup: true", "workspace:", "  data_source: " + t.TempDir()}
	if backend == "sqlite" {
		lines = append(lines, "store:", "  backend: sqlite", "  sqlite:", "    path: "+sqlitePath)
	}
	lines = append(lines, "llm:", "  backend: anthropic")
	path := filepath.Join(t.TempDir(), "swarm.yaml")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func inboundAdmissionSQLitePathFromConfig(t testing.TB, configPath string) string {
	t.Helper()
	body, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "path: ") {
			return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "path: "))
		}
	}
	t.Fatal("SQLite path missing from test config")
	return ""
}
