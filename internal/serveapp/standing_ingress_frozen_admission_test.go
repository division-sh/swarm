package serveapp

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/testutil"
	"gopkg.in/yaml.v3"
)

func TestServeIngressFrozenCredentialMutationAndRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, policy := range []string{"pack", "raw", "unsigned"} {
			t.Run(backend+"/"+policy, func(t *testing.T) {
				runServeIngressFrozenCredentialJourney(t, backend, policy, "")
			})
		}
	}
}

func TestServeIngressCredentialMutationDuringRequestBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, point := range []string{"body", "publication_preparation", "observation_error"} {
			t.Run(backend+"/"+point, func(t *testing.T) {
				runServeIngressFrozenCredentialJourney(t, backend, "pack", point)
			})
		}
	}
}

func TestServeIngressFrozenCredentialSiblingIsolationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, point := range []string{"independent_sibling", "shared_key_sibling"} {
			t.Run(backend+"/"+point, func(t *testing.T) {
				runServeIngressFrozenCredentialJourney(t, backend, "pack", point)
			})
		}
	}
}

func runServeIngressFrozenCredentialJourney(t *testing.T, backend, policy, point string) {
	t.Helper()
	ctx := context.Background()
	isolateCLIAPIConfigEnv(t)
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	file, err := credentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Set(ctx, "telegram.telegram_bot_token", "fixture-bot-token"); err != nil {
		t.Fatal(err)
	}
	signingKey := "telegram.webhook_signing_secret"
	if policy != "pack" {
		signingKey = "telegram_raw.webhook_signing_secret"
		if err := file.Set(ctx, "telegram.webhook_signing_secret", "unused-telegram-signing-secret"); err != nil {
			t.Fatal(err)
		}
	}
	set := func(value string) {
		t.Helper()
		if err := file.Set(ctx, signingKey, value); err != nil {
			t.Fatal(err)
		}
	}
	set("original-secret")
	root := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
	disableChannelOnboardingBusinessConsumers(t, root)
	providerName := "telegram"
	if policy != "pack" {
		providerName = "telegram_raw"
		setFrozenIngressRawPolicy(t, root, policy == "unsigned")
	}
	hasSibling := point == "independent_sibling" || point == "shared_key_sibling"
	if hasSibling {
		key := "telegram.webhook_signing_secret"
		if point == "independent_sibling" {
			key = "partner.webhook_signing_secret"
			if err := file.Set(ctx, key, "original-secret"); err != nil {
				t.Fatal(err)
			}
		}
		addFrozenIngressSibling(t, root, key)
	}
	opts := cliapp.ServeOptions{SourceRoot: root, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true,
		WorkspaceBackend: "host", WorkspaceBackendSet: true, StoreMode: backend, StoreModeSet: true}
	var manager *runtime.RuntimeContextManager
	var armed atomic.Bool
	entered := make(chan struct{})
	var signal sync.Once
	opts.TestRuntimeContextsReadyHook = func(m *runtime.RuntimeContextManager) {
		manager = m
		if point == "body" {
			use, _, err := m.AcquireIngress(ctx, "chat", providerName)
			if err != nil || use == nil {
				t.Errorf("capture admitted runtime: %v", err)
				return
			}
			rt := use.Runtime()
			_ = use.Done()
			rt.InboundGateway.SetCredentialAdmission(func(ctx context.Context, target runtime.InboundTarget) (credentials.SecretBinding, func(context.Context) error, error) {
				binding, validate, err := rt.AdmitInboundCredentials(ctx, target)
				if armed.Load() && err == nil {
					signal.Do(func() { close(entered) })
				}
				return binding, validate, err
			})
		}
	}
	var db *sql.DB
	if backend == "sqlite" {
		path := filepath.Join(t.TempDir(), "store.sqlite")
		db, err = sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend, path, channelOnboardingHostWorkspaceFields())
	} else {
		dsn, opened, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		db = opened
		opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, dsn)
	}
	enableChannelOnboardingRecoveryOnStartup(t, opts.ConfigPath)
	if policy == "unsigned" {
		body, err := os.ReadFile(opts.ConfigPath)
		if err != nil {
			t.Fatal(err)
		}
		var deployment map[string]any
		if err := yaml.Unmarshal(body, &deployment); err != nil {
			t.Fatal(err)
		}
		deployment["triggers"] = map[string]any{"telegram_raw/trigger.yaml": map[string]any{"acknowledge": "unsigned_webhook"}}
		body, err = yaml.Marshal(deployment)
		if err != nil {
			t.Fatal(err)
		}
		writeStandingCandidateFile(t, opts.ConfigPath, string(body))
	}
	var process *serveRuntimeTestProcess
	var endpoint string
	start := func() {
		t.Helper()
		process = startServeRuntimeTestProcess(t, opts)
		process.waitForReadyLine()
		endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString())
	}
	start()
	t.Cleanup(func() { process.stop() })
	postTo := func(id int, secret, provider string) int {
		t.Helper()
		alias := "chat"
		if hasSibling && provider == "partner" {
			alias = "chat.partner"
			if point == "shared_key_sibling" {
				provider = "telegram"
			}
		}
		body := fmt.Sprintf(`{"update_id":%d,"message":{"message_id":%d,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"probe"}}`, id, id)
		req, err := http.NewRequest(http.MethodPost, endpoint+"/webhooks/"+alias+"/"+provider, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Telegram-Bot-Api-Secret-Token", secret)
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		return response.StatusCode
	}
	post := func(id int, secret string) int { return postTo(id, secret, providerName) }
	assertReadback := func(ready bool, requirement string) {
		t.Helper()
		owner, err := credentials.NewSnapshotOwner(file)
		if err != nil {
			t.Fatal(err)
		}
		subjects, err := manager.EvaluatedCapabilitySubjects(ctx, owner)
		if err != nil {
			t.Fatal(err)
		}
		var effective []packs.Subject
		siblings := 0
		for _, subject := range subjects {
			if subject.Kind == packs.SubjectProviderTrigger && subject.Applicability == "effective" {
				if subject.Provider == providerName && subject.TriggerAdmission != nil && subject.TriggerAdmission.FlowPath == "." {
					effective = append(effective, subject)
				} else if hasSibling && subject.TriggerAdmission != nil && subject.TriggerAdmission.FlowPath == "partner" {
					siblings++
					wantReady := ready || point == "independent_sibling"
					if (subject.Status == packs.StatusReady) != wantReady || subject.TriggerAdmission.BindingEnabled == nil || *subject.TriggerAdmission.BindingEnabled != wantReady {
						t.Fatalf("sibling readback lost scoped authority: %#v", subject)
					}
				} else {
					t.Fatalf("unexpected effective subject: %#v", subject)
				}
			}
			for _, secret := range []string{"original-secret", "unadmitted-replacement"} {
				if strings.Contains(packs.RenderSubject(subject, true), secret) {
					t.Fatal("capability readback leaked signing bytes")
				}
			}
		}
		if len(effective) != 1 || effective[0].TriggerAdmission.BindingEnabled == nil || *effective[0].TriggerAdmission.BindingEnabled != ready || (effective[0].Status == packs.StatusReady) != ready {
			t.Fatalf("frozen admission readback ready=%t: %#v", ready, effective)
		}
		if hasSibling && siblings != 1 {
			t.Fatal("sibling declaration disappeared from readback")
		}
		if policy != "unsigned" && (len(effective[0].Requirements) != 1 || effective[0].Requirements[0].Status != requirement) {
			t.Fatalf("current presence lost: %#v, want %s", effective[0].Requirements, requirement)
		}
	}
	counts := func() []int {
		t.Helper()
		var result []int
		for _, query := range []string{
			`SELECT COUNT(*) FROM inbound_publications`,
			`SELECT COUNT(*) FROM events WHERE event_name LIKE 'inbound.%'`,
			`SELECT COUNT(*) FROM runtime_external_effect_operations`,
		} {
			var count int
			if err := db.QueryRow(query).Scan(&count); err != nil {
				t.Fatal(err)
			}
			result = append(result, count)
		}
		return result
	}
	assertReadback(true, packs.RequirementStatusBound)
	if code := post(80001, "original-secret"); code != http.StatusAccepted {
		t.Fatalf("original admission HTTP %d", code)
	}
	if code := post(80001, "original-secret"); code != http.StatusOK {
		t.Fatalf("exact duplicate HTTP %d", code)
	}
	if hasSibling {
		if code := postTo(80810, "original-secret", "partner"); code != http.StatusAccepted {
			t.Fatalf("initial sibling HTTP %d", code)
		}
		before := counts()
		set("unadmitted-replacement")
		if code := post(80811, "unadmitted-replacement"); code != http.StatusServiceUnavailable {
			t.Fatalf("rotated main binding HTTP %d", code)
		}
		if after := counts(); fmt.Sprint(after) != fmt.Sprint(before) {
			t.Fatalf("rotated binding published: %v -> %v", before, after)
		}
		want := http.StatusServiceUnavailable
		if point == "independent_sibling" {
			want = http.StatusAccepted
		}
		if code := postTo(80812, "original-secret", "partner"); code != want {
			t.Fatalf("scoped sibling HTTP %d, want %d", code, want)
		}
		if point == "shared_key_sibling" {
			if after := counts(); fmt.Sprint(after) != fmt.Sprint(before) {
				t.Fatalf("shared stale authority published: %v -> %v", before, after)
			}
		}
		assertReadback(false, packs.RequirementStatusBound)
		return
	}
	if point != "" {
		before := counts()
		if point == "observation_error" {
			if err := os.WriteFile(credentialPath, []byte("{invalid credential document"), 0600); err != nil {
				t.Fatal(err)
			}
			if code := post(80800, "original-secret"); code != http.StatusServiceUnavailable {
				t.Fatalf("credential observation error HTTP %d", code)
			}
			owner, err := credentials.NewSnapshotOwner(file)
			if err != nil {
				t.Fatal(err)
			}
			subjects, err := manager.EvaluatedCapabilitySubjects(ctx, owner)
			var observation *credentials.SecretBindingObservationError
			if !errors.As(err, &observation) || len(subjects) != 0 {
				t.Fatalf("credential observation error became a readiness claim: %#v, %v", subjects, err)
			}
			if after := counts(); fmt.Sprint(after) != fmt.Sprint(before) {
				t.Fatalf("observation refusal mutated publication/effects: %v -> %v", before, after)
			}
			return
		}
		armed.Store(true)
		if point == "body" {
			reader, writer := io.Pipe()
			req, err := http.NewRequest(http.MethodPost, endpoint+"/webhooks/chat/telegram", reader)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "original-secret")
			result := make(chan error, 1)
			go func() {
				response, err := http.DefaultClient.Do(req)
				if err == nil {
					_, _ = io.Copy(io.Discard, response.Body)
					_ = response.Body.Close()
					if response.StatusCode != http.StatusServiceUnavailable {
						err = fmt.Errorf("in-flight body mutation HTTP %d", response.StatusCode)
					}
				}
				result <- err
			}()
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				_ = writer.Close()
				t.Fatal("request did not acquire the frozen admission before body processing")
			}
			set("unadmitted-replacement")
			_, writeErr := io.WriteString(writer, `{"update_id":80800,"message":{"message_id":80800,"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"probe"}}`)
			_ = writer.Close()
			if err := <-result; err != nil || writeErr != nil {
				t.Fatalf("body mutation: HTTP=%v body=%v", err, writeErr)
			}
		} else {
			use, _, err := manager.AcquireIngress(ctx, "chat", providerName)
			if err != nil || use == nil {
				t.Fatalf("capture admitted runtime: %v", err)
			}
			rt := use.Runtime()
			_ = use.Done()
			rt.Bus.SetProviderOutputAuthorizationVerifier(&frozenIngressMutationVerifier{verify: rt.Options.ProviderTriggerCatalog.VerifyProviderOutputAuthorization, mutate: func() error {
				return file.Set(ctx, signingKey, "unadmitted-replacement")
			}})
			if code := post(80800, "original-secret"); code != http.StatusServiceUnavailable {
				t.Fatalf("mutation during publication preparation HTTP %d", code)
			}
		}
		if after := counts(); fmt.Sprint(after) != fmt.Sprint(before) {
			t.Fatalf("in-flight refusal published or created effects: %v -> %v", before, after)
		}
		assertReadback(false, packs.RequirementStatusBound)
		return
	}
	for i, mutation := range []struct {
		name, value, presence string
		remove                bool
	}{
		{"rotation", "unadmitted-replacement", packs.RequirementStatusBound, false},
		{"deletion", "", packs.RequirementStatusUnbound, true},
		{"restoration_ABA", "original-secret", packs.RequirementStatusBound, false},
		{"same_value_new_observation", "original-secret", packs.RequirementStatusBound, false},
		{"unusable_value", " \t\n", packs.RequirementStatusUnbound, false},
	} {
		func() {
			t.Log("temporal mutation:", mutation.name)
			before := counts()
			if mutation.remove {
				if err := file.Delete(ctx, signingKey); err != nil {
					t.Fatal(err)
				}
			} else {
				set(mutation.value)
			}
			for j, secret := range []string{"original-secret", "unadmitted-replacement"} {
				want := http.StatusServiceUnavailable
				if policy == "unsigned" {
					want = http.StatusAccepted
				}
				if code := post(80100+i*10+j, secret); code != want {
					t.Fatalf("unadmitted mutation HTTP %d, want %d", code, want)
				}
			}
			if policy != "unsigned" {
				if code := post(80001, "original-secret"); code != http.StatusServiceUnavailable {
					t.Fatalf("stale authentication admitted a historical duplicate: %d", code)
				}
				if after := counts(); fmt.Sprint(after) != fmt.Sprint(before) {
					t.Fatalf("refused admission published or created effects: %v -> %v", before, after)
				}
			}
			beforeReadback := counts()
			assertReadback(policy == "unsigned", mutation.presence)
			if after := counts(); fmt.Sprint(after) != fmt.Sprint(beforeReadback) {
				t.Fatalf("diagnostic read acquired executable authority: %v -> %v", beforeReadback, after)
			}
		}()
	}
	set("unadmitted-replacement")
	if code := process.stop(); code != 0 {
		t.Fatalf("stop before authorized restart=%d", code)
	}
	start()
	assertReadback(true, packs.RequirementStatusBound)
	if code := post(80900, "unadmitted-replacement"); code != http.StatusAccepted {
		t.Fatalf("restart did not admit exact provisioned credentials: %d", code)
	}
}

func setFrozenIngressRawPolicy(t *testing.T, root string, unsigned bool) {
	t.Helper()
	path := filepath.Join(root, "schema.yaml")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var schema map[string]any
	if err := yaml.Unmarshal(body, &schema); err != nil {
		t.Fatal(err)
	}
	schema["pins"] = map[string]any{"inputs": []string{"inbound.telegram_raw.raw"}}
	body, err = yaml.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	writeFrozenIngressTrigger(t, root, "telegram_raw", unsigned)
}

func addFrozenIngressSibling(t *testing.T, root, signingKey string) {
	t.Helper()
	sibling := filepath.Join(root, "partner")
	provider := "partner"
	if signingKey == "telegram.webhook_signing_secret" {
		provider = "telegram"
		copyReleaseFixtureTree(t, filepath.Join(root, "telegram"), filepath.Join(sibling, provider))
	} else {
		writeFrozenIngressTrigger(t, sibling, provider, false)
	}
	event := "inbound." + provider + ".raw"
	if provider == "telegram" {
		event = "inbound.telegram.text_message"
	}
	writeStandingCandidateFile(t, filepath.Join(sibling, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
	writeStandingCandidateFile(t, filepath.Join(sibling, "schema.yaml"), "stages: []\npins:\n  inputs: ["+event+"]\n")
}

func writeFrozenIngressTrigger(t *testing.T, root, provider string, unsigned bool) {
	t.Helper()
	manifest := "version: 1.0.0\nplatform_version: '*'\ncapabilities:\n  can:\n    emit_events: [raw]\n"
	if !unsigned {
		manifest += "    verify_secret: webhook_signing_secret\n"
	}
	manifest += "  cannot: [execute code]\n"
	body := `transport: webhook
payload_object_required: true
secret: {required: true}
signature: {type: token_equality, header: X-Telegram-Bot-Api-Secret-Token}
delivery_id: {json_path: $.update_id, required: true}
event_type: {literal: update, required: true}
event_name: {literal: raw}
`
	if unsigned {
		body = strings.Replace(body, "secret: {required: true}", "secret: {required: false}", 1)
		body = strings.Replace(body, "signature: {type: token_equality, header: X-Telegram-Bot-Api-Secret-Token}\n", "", 1)
	}
	if err := os.MkdirAll(filepath.Join(root, provider), 0700); err != nil {
		t.Fatal(err)
	}
	writeStandingCandidateFile(t, filepath.Join(root, provider, "manifest.yaml"), manifest)
	writeStandingCandidateFile(t, filepath.Join(root, provider, "trigger.yaml"), body)
}

type frozenIngressMutationVerifier struct {
	verify func(runtimeprovideroutput.Authorization) error
	mutate func() error
	once   sync.Once
	err    error
}

func (v *frozenIngressMutationVerifier) VerifyProviderOutputAuthorization(authorization runtimeprovideroutput.Authorization) error {
	if err := v.verify(authorization); err != nil {
		return err
	}
	v.once.Do(func() { v.err = v.mutate() })
	return v.err
}
