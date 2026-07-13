package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestInboundAdmissionSupportedSurfacePolicyMatrixSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			runInboundAdmissionSupportedSurfacePolicyMatrix(t, backend)
		})
	}
}

func TestVerifyRejectsAmbientCheckoutProviderTriggerInventory(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := writeVerifyLintEvidenceFixture(t)
	opts := defaultVerifyCommandOptions()
	opts.contractsPath = root
	opts.platformSpecPath = filepath.Join(repoRoot(), defaultPlatformSpecPath)
	var errors []string
	for _, repo := range []string{repoRoot(), t.TempDir()} {
		var out, errOut bytes.Buffer
		if code := runVerifyCommandWithOutput(context.Background(), repo, opts, &out, &errOut); code == 0 {
			t.Fatalf("verify unexpectedly admitted ambient inventory for repo %q:\n%s", repo, out.String())
		}
		if out.Len() != 0 {
			t.Fatalf("verify repo %q projected capabilities without configured inventory:\n%s", repo, out.String())
		}
		if !strings.Contains(errOut.String(), "provider_triggers.packs.platform_dirs is required") {
			t.Fatalf("verify repo %q error omitted configured inventory requirement:\n%s", repo, errOut.String())
		}
		errors = append(errors, errOut.String())
	}
	if errors[0] != errors[1] {
		t.Fatalf("identical configuration changed meaning by checkout presence:\ncheckout: %s\nempty repo: %s", errors[0], errors[1])
	}
}

func TestVerifyProjectsExplicitConfiguredInventoryWithoutStandingIngress(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	platformDirs, externalDirs := writeInboundAdmissionPackInventory(t)
	root := writeVerifyLintEvidenceFixture(t)
	opts := defaultVerifyCommandOptions()
	opts.contractsPath = root
	opts.platformSpecPath = filepath.Join(repoRoot(), defaultPlatformSpecPath)
	opts.configPath = writeInboundAdmissionRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "verify.sqlite"), platformDirs, externalDirs)
	emptyRepo := t.TempDir()
	var textOut, textErr bytes.Buffer
	if code := runVerifyCommandWithOutput(context.Background(), emptyRepo, opts, &textOut, &textErr); code != 0 {
		t.Fatalf("verify text exit=%d stdout=%s stderr=%s", code, textOut.String(), textErr.String())
	}
	for _, provider := range []string{"acme_public", "github", "intercom", "shopify", "slack", "stripe", "telegram", "twilio", "typeform"} {
		if !strings.Contains(textOut.String(), "provider trigger pack provider."+provider+" AVAILABLE") {
			t.Fatalf("verify text omitted installed %s trigger:\n%s", provider, textOut.String())
		}
	}
	opts.output.asJSON = true
	var jsonOut, jsonErr bytes.Buffer
	if code := runVerifyCommandWithOutput(context.Background(), emptyRepo, opts, &jsonOut, &jsonErr); code != 0 {
		t.Fatalf("verify JSON exit=%d stdout=%s stderr=%s", code, jsonOut.String(), jsonErr.String())
	}
	result := decodeOutputJSON[verifyCommandResult](t, jsonOut.String())
	installed := 0
	for _, subject := range result.CapabilitySubjects {
		if subject.Kind == packs.SubjectProviderTrigger && subject.Applicability == "installed" {
			installed++
		}
	}
	if installed != 9 {
		t.Fatalf("verify installed trigger subjects=%d, want 9: %#v", installed, result.CapabilitySubjects)
	}
}

func TestVerifyConfiguredInventoryProjectsUnsignedWarningAndReadback(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	platformDirs, externalDirs := writeInboundAdmissionPackInventory(t)
	opts := defaultVerifyCommandOptions()
	opts.contractsPath = writeInboundAdmissionPolicyMatrixFixture(t)
	opts.platformSpecPath = filepath.Join(repoRoot(), defaultPlatformSpecPath)
	opts.configPath = writeInboundAdmissionRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "verify.sqlite"), platformDirs, externalDirs)
	emptyRepo := t.TempDir()

	var textOut, textErr bytes.Buffer
	if code := runVerifyCommandWithOutput(context.Background(), emptyRepo, opts, &textOut, &textErr); code != 0 {
		t.Fatalf("verify text exit=%d stdout=%s stderr=%s", code, textOut.String(), textErr.String())
	}
	if got := strings.Count(textErr.String(), "inbound_unsigned_webhook"); got != 1 {
		t.Fatalf("verify text unsigned warning count=%d, want 1:\n%s", got, textErr.String())
	}
	for _, want := range []string{`provider "partner_open" accepts unsigned webhooks`, "add admission.acknowledge: unsigned_webhook"} {
		if !strings.Contains(textErr.String(), want) {
			t.Fatalf("verify text warning omitted %q:\n%s", want, textErr.String())
		}
	}
	if strings.Contains(textErr.String(), `provider "partner_ack" accepts unsigned webhooks`) {
		t.Fatalf("verify text did not suppress acknowledged warning:\n%s", textErr.String())
	}

	opts.output.asJSON = true
	var jsonOut, jsonErr bytes.Buffer
	if code := runVerifyCommandWithOutput(context.Background(), emptyRepo, opts, &jsonOut, &jsonErr); code != 0 {
		t.Fatalf("verify JSON exit=%d stdout=%s stderr=%s", code, jsonOut.String(), jsonErr.String())
	}
	if jsonErr.Len() != 0 {
		t.Fatalf("verify JSON stderr=%s, want empty", jsonErr.String())
	}
	result := decodeOutputJSON[verifyCommandResult](t, jsonOut.String())
	unsignedWarnings := 0
	for _, warning := range result.Warnings {
		if warning.CheckID != "inbound_unsigned_webhook" {
			continue
		}
		unsignedWarnings++
		if !strings.Contains(warning.Message, `provider "partner_open" accepts unsigned webhooks`) || warning.Remediation != "add admission.acknowledge: unsigned_webhook to confirm this intentional public endpoint" {
			t.Fatalf("verify JSON unsigned warning=%#v", warning)
		}
	}
	if unsignedWarnings != 1 {
		t.Fatalf("verify JSON unsigned warnings=%d, want 1: %#v", unsignedWarnings, result.Warnings)
	}

	readback := map[string]packs.Subject{}
	installed, effective := 0, 0
	for _, subject := range result.CapabilitySubjects {
		switch subject.Applicability {
		case "installed":
			installed++
		case "effective":
			effective++
			readback[subject.Provider] = subject
		}
	}
	if installed != 9 || effective != 6 {
		t.Fatalf("verify subject multiplicity installed=%d effective=%d", installed, effective)
	}
	for _, provider := range []string{"partner_open", "partner_ack"} {
		subject, ok := readback[provider]
		if !ok || subject.TriggerAdmission == nil || subject.TriggerAdmission.PolicySource != "raw_declaration" || subject.TriggerAdmission.RequestAuthentication != "UNAUTHENTICATED" {
			t.Fatalf("verify %s readback=%#v", provider, subject)
		}
		if rendered := packs.RenderSubject(subject, false); !strings.Contains(textOut.String(), rendered) {
			t.Fatalf("verify text/JSON readback diverged for %s:\nwant %s\ntext:\n%s", provider, rendered, textOut.String())
		}
	}
}

func TestProviderTriggerCapabilitySubjectsPreserveInstalledEffectiveMultiplicityAndRendering(t *testing.T) {
	platformDirs, externalDirs := writeInboundAdmissionPackInventory(t)
	catalog, _, err := providertriggers.NewCatalogSnapshotFromRequiredPlatformPackDirs("0.7.0", platformDirs, externalDirs)
	if err != nil {
		t.Fatal(err)
	}
	contractsRoot := writeInboundAdmissionPolicyMatrixFixture(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot(), contractsRoot, runtimecontracts.DefaultPlatformSpecFile(repoRoot()))
	if err != nil {
		t.Fatal(err)
	}
	subjects, err := runtime.ProviderTriggerCapabilitySubjects(semanticview.Wrap(bundle), catalog)
	if err != nil {
		t.Fatal(err)
	}
	installed, effective, raw := 0, 0, 0
	textProjection := ""
	for _, subject := range subjects {
		textProjection += packs.RenderSubject(subject, false) + "\n"
		switch subject.Applicability {
		case "installed":
			installed++
		case "effective":
			effective++
			if subject.TriggerAdmission != nil && subject.TriggerAdmission.PolicySource == "raw_declaration" {
				raw++
			}
		}
	}
	if installed != 9 || effective != 6 || raw != 3 {
		t.Fatalf("subject multiplicity installed=%d effective=%d raw=%d", installed, effective, raw)
	}
	body, err := json.Marshal(verifyCommandResult{OK: true, CapabilitySubjects: subjects})
	if err != nil {
		t.Fatal(err)
	}
	var projected verifyCommandResult
	if err := json.Unmarshal(body, &projected); err != nil {
		t.Fatal(err)
	}
	if len(projected.CapabilitySubjects) != len(subjects) {
		t.Fatalf("JSON subjects=%d, want %d", len(projected.CapabilitySubjects), len(subjects))
	}
	for _, subject := range projected.CapabilitySubjects {
		if !strings.Contains(textProjection, subject.ID) {
			t.Fatalf("text projection omitted JSON subject %q", subject.ID)
		}
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
				oldWorkspace := configuredWorkspaceLifecycleForServe
				buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (storeBundle, error) {
					if _, err := postgresStore.BindSchemaCapabilities(ctx); err != nil {
						return storeBundle{}, err
					}
					return selectedPostgresStoreBundle(postgresStore, cfg), nil
				}
				configuredWorkspaceLifecycleForServe = func(*sql.DB, *config.Config, string, semanticview.Source, workspaceMountSources, workspaceBackendSelection) (serveWorkspaceLifecycle, error) {
					return serveRuntimeWorkspaceStub{}, nil
				}
				t.Cleanup(func() {
					buildStoresForServe = oldBuildStores
					configuredWorkspaceLifecycleForServe = oldWorkspace
				})
			}

			for _, tc := range []struct {
				name           string
				mutatePackage  func(string) string
				removeExternal bool
				want           string
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
					name:          "removed external pack",
					mutatePackage: func(body string) string { return body }, removeExternal: true,
					want: `pins pack "provider.acme_public", but that id is not loaded`,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					platformDirs, externalDirs := writeInboundAdmissionPackInventory(t)
					if tc.removeExternal {
						externalDirs = nil
					}
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
					configPath := writeInboundAdmissionRuntimeConfig(t, backend, filepath.Join(t.TempDir(), "failure.sqlite"), platformDirs, externalDirs)
					process := startServeRuntimeTestProcess(t, serveOptions{
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
	platformDirs, externalDirs := writeInboundAdmissionPackInventory(t)
	contractsRoot := writeInboundAdmissionPolicyMatrixFixture(t)
	dataRoot := t.TempDir()
	credentialPath := filepath.Join(dataRoot, "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	credentialStore, err := runtimecredentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{
		"webhook_signing.telegram": "telegram-secret",
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
		configPath = writeInboundAdmissionRuntimeConfig(t, backend, sqlitePath, platformDirs, externalDirs)
	} else {
		dsn, _, cleanup := testutil.StartPostgres(t)
		postgresDSN = dsn
		t.Cleanup(cleanup)
		postgresStore, err = store.NewPostgresStore(dsn)
		if err != nil {
			t.Fatal(err)
		}
		oldBuildStores := buildStoresForServe
		oldWorkspace := configuredWorkspaceLifecycleForServe
		buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (storeBundle, error) {
			if _, err := postgresStore.BindSchemaCapabilities(ctx); err != nil {
				return storeBundle{}, err
			}
			return selectedPostgresStoreBundle(postgresStore, cfg), nil
		}
		configuredWorkspaceLifecycleForServe = func(*sql.DB, *config.Config, string, semanticview.Source, workspaceMountSources, workspaceBackendSelection) (serveWorkspaceLifecycle, error) {
			return serveRuntimeWorkspaceStub{}, nil
		}
		t.Cleanup(func() {
			buildStoresForServe = oldBuildStores
			configuredWorkspaceLifecycleForServe = oldWorkspace
		})
		configPath = writeInboundAdmissionRuntimeConfig(t, backend, "", platformDirs, externalDirs)
	}

	opts := serveOptions{
		ConfigPath: configPath, ContractsPath: contractsRoot, PlatformSpecPath: defaultPlatformSpecPath,
		StoreMode: backend, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, RequireBundleMatch: false, Dev: true, Verbose: true,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
	}
	process := startServeRuntimeTestProcess(t, opts)
	process.waitForReadyLine()
	waitForInboundAdmissionServeOutput(t, process, "[WARN] inbound_unsigned_webhook")
	serveOutput := process.outputString()
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
		provider string
		body     []byte
		headers  map[string]string
		event    string
	}{
		{provider: "telegram", body: []byte(`{"update_id":901,"message":{"chat":{"id":42}}}`), headers: map[string]string{"X-Telegram-Bot-Api-Secret-Token": "telegram-secret"}, event: "inbound.telegram"},
		{provider: "intercom", body: []byte(`{"id":"platform-unsigned-1","topic":"contact.created"}`), event: "inbound.intercom"},
		{provider: "acme_public", body: []byte(`{"id":"external-unsigned-1"}`), event: "inbound.acme_public"},
		{provider: "partner_auth", body: []byte(`{"value":1}`), headers: map[string]string{"X-Partner-Delivery": "partner-auth-1"}, event: "inbound.partner_auth"},
		{provider: "partner_open", body: []byte(`{"delivery":{"id":"partner-open-1"},"value":2}`), event: "inbound.partner_open"},
		{provider: "partner_ack", body: []byte("raw-open-body"), event: "inbound.partner_ack"},
	}
	for i := range tests {
		test := &tests[i]
		if test.provider == "partner_auth" {
			mac := hmac.New(sha256.New, []byte("partner-secret"))
			_, _ = mac.Write(test.body)
			test.headers["X-Partner-Signature"] = hex.EncodeToString(mac.Sum(nil))
		}
		status, response := sendInboundAdmissionSupportedRequest(t, baseURL, test.provider, test.body, test.headers)
		if status != http.StatusAccepted {
			t.Fatalf("%s status=%d response=%s\nserve output:\n%s", test.provider, status, response, process.outputString())
		}
	}
	status, _ := sendInboundAdmissionSupportedRequest(t, baseURL, "partner_auth", []byte(`{"value":1}`), map[string]string{
		"X-Partner-Delivery":  "partner-auth-invalid",
		"X-Partner-Signature": "invalid",
	})
	if status != http.StatusUnauthorized {
		t.Fatalf("invalid authenticated raw status=%d, want 401", status)
	}
	if code := process.stop(); code != 0 {
		t.Fatalf("serve exit=%d\n%s", code, process.outputString())
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
		var count int
		if backend == "sqlite" {
			err = sqliteStore.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE event_name = ?`, test.event).Scan(&count)
		} else {
			err = postgresStore.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE event_name = $1`, test.event).Scan(&count)
		}
		if err != nil || count != 1 {
			t.Fatalf("%s persisted count=%d err=%v, want 1", test.event, count, err)
		}
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
	root := t.TempDir()
	files := map[string]string{
		"package.yaml": `name: inbound-admission-policy-matrix
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: matrix
    flow: matrix
    mode: singleton
    activation: standing
    ingress:
      alias: matrix
      providers:
        - provider: telegram
          signing_secret: webhook_signing.telegram
          admission:
            pack: {id: provider.telegram}
        - provider: intercom
          admission:
            pack: {id: provider.intercom}
            acknowledge: unsigned_webhook
        - provider: acme_public
          admission:
            pack: {id: provider.acme_public}
            acknowledge: unsigned_webhook
        - provider: partner_auth
          signing_secret: webhook_signing.partner
          admission:
            kind: raw
            authentication: {kind: hmac_sha256, header: X-Partner-Signature, encoding: hex}
            event: inbound.partner_auth
            delivery_id: {source: header, header: X-Partner-Delivery}
            payload: json
        - provider: partner_open
          admission:
            kind: raw
            authentication: {kind: none}
            event: inbound.partner_open
            delivery_id: {source: json_path, json_path: $.delivery.id}
            payload: raw
        - provider: partner_ack
          admission:
            kind: raw
            acknowledge: unsigned_webhook
            authentication: {kind: none}
            event: inbound.partner_ack
            delivery_id: {source: body_sha256}
            payload: raw
`,
		"schema.yaml": "name: inbound-admission-policy-matrix\n",
		"policy.yaml": "{}\n", "tools.yaml": "{}\n", "agents.yaml": "{}\n", "events.yaml": "{}\n", "nodes.yaml": "{}\n",
		"flows/matrix/schema.yaml": `name: matrix
mode: singleton
initial_state: active
states: [active]
pins:
  inputs:
    events:
      - {name: telegram, event: inbound.telegram, source: external}
      - {name: intercom, event: inbound.intercom, source: external}
      - {name: acme_public, event: inbound.acme_public, source: external}
      - {name: partner_auth, event: inbound.partner_auth, source: external}
      - {name: partner_open, event: inbound.partner_open, source: external}
      - {name: partner_ack, event: inbound.partner_ack, source: external}
  outputs: {events: []}
`,
		"flows/matrix/entities.yaml": "matrix_service:\n  service_id:\n    type: text\n    initial: standing\n  records:\n    type: map[text]json\n    initial: {}\n",
		"flows/matrix/types.yaml":    "{}\n", "flows/matrix/policy.yaml": "{}\n", "flows/matrix/tools.yaml": "{}\n", "flows/matrix/agents.yaml": "{}\n",
		"flows/matrix/events.yaml": inboundAdmissionMatrixEventsYAML(),
		"flows/matrix/nodes.yaml":  inboundAdmissionMatrixNodesYAML(),
	}
	for name, body := range files {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func inboundAdmissionMatrixEventsYAML() string {
	var out strings.Builder
	for _, event := range []string{"inbound.partner_auth", "inbound.partner_open", "inbound.partner_ack"} {
		fmt.Fprintf(&out, "%s:\n  entity_id: text\n  provider: text\n  event_type: text\n  provider_event_type: text\n  provider_event_id: text\n  provider_delivery_id: text\n  headers: json\n  received_at: text\n", event)
	}
	return out.String()
}

func inboundAdmissionMatrixNodesYAML() string {
	events := []string{"inbound.telegram", "inbound.intercom", "inbound.acme_public", "inbound.partner_auth", "inbound.partner_open", "inbound.partner_ack"}
	var out strings.Builder
	out.WriteString("matrix-sink:\n  id: matrix-sink\n  execution_type: system_node\n  subscribes_to: [" + strings.Join(events, ", ") + "]\n  event_handlers:\n")
	for _, event := range events {
		fmt.Fprintf(&out, `    %s:
      data_accumulation:
        writes:
          - op: set
            target: entity.records
            key: {ref: payload.provider_event_id}
            value: {ref: payload.provider_event_id}
`, event)
	}
	return out.String()
}

func writeInboundAdmissionPackInventory(t *testing.T) ([]string, []string) {
	t.Helper()
	platformRoot := t.TempDir()
	platformDirs := make([]string, 0, len(providertriggers.RequiredPlatformPackIdentities()))
	for _, identity := range providertriggers.RequiredPlatformPackIdentities() {
		dir := filepath.Join(platformRoot, identity.Provider)
		copyProviderTriggerPackFixture(t, identity.Provider, dir, false)
		platformDirs = append(platformDirs, dir)
	}
	writeUnsignedProviderTriggerPack(t, filepath.Join(platformRoot, "intercom"), "provider.intercom", "intercom", "platform", "inbound.intercom")
	externalDir := filepath.Join(t.TempDir(), "acme_public")
	writeUnsignedProviderTriggerPack(t, externalDir, "provider.acme_public", "acme_public", "external", "inbound.acme_public")
	return platformDirs, []string{externalDir}
}

func writeUnsignedProviderTriggerPack(t testing.TB, dir, id, provider, provenance, event string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := []byte(fmt.Sprintf(`provider: %s
payload_object_required: true
secret: {required: false}
delivery_id: {json_path: $.id, required: true}
event_type: {literal: event, required: true}
event_name: {literal: %s}
ack: {mode: durable_before_dispatch}
`, provider, event))
	envelope := []byte(fmt.Sprintf(`id: %s
version: 0.1.0
platform_version: '>=0.7.0 <0.8.0'
type: trigger
manifest_hash: sha256:%s
provenance: {source: %s}
capabilities:
  can:
    receive_https_route: /webhooks/{alias}/%s
    emit_events: [%s]
    persist_dedupe_markers: true
  cannot: [emit_undeclared_events, run_code_before_admission, touch_unbound_resources]
requires: {}
tests: [providertriggers/%s]
`, id, strings.Repeat("0", 64), provenance, provider, event, provider))
	_, stamped, err := providertriggers.StampPackEnvelope(envelope, manifest)
	if err != nil {
		t.Fatalf("stamp %s: %v", id, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "trigger.yaml"), manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "pack.yaml"), stamped, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeInboundAdmissionRuntimeConfig(t testing.TB, backend, sqlitePath string, platformDirs, externalDirs []string) string {
	t.Helper()
	lines := []string{"runtime:", "  recovery_on_startup: false", "workspace:", "  data_source: " + t.TempDir()}
	if backend == "sqlite" {
		lines = append(lines, "store:", "  backend: sqlite", "  sqlite:", "    path: "+sqlitePath)
	}
	lines = append(lines, "llm:", "  backend: anthropic", "provider_triggers:", "  packs:", "    platform_dirs:")
	for _, dir := range platformDirs {
		lines = append(lines, fmt.Sprintf("      - %q", dir))
	}
	lines = append(lines, "    external_dirs:")
	for _, dir := range externalDirs {
		lines = append(lines, fmt.Sprintf("      - %q", dir))
	}
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
