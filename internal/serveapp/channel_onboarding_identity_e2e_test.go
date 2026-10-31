package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/telegramapi"
	"gopkg.in/yaml.v3"
)

var channelOnboardingExactChoicePattern = regexp.MustCompile(`source=.+? --interface (\S+) --target (\S+)`)

func registerChannelFixtureAccount(t *testing.T, config, token, account string, hitl ...*telegramapi.Double) channelonboarding.Operation {
	t.Helper()
	if len(hitl) != 0 {
		body, err := os.ReadFile(config)
		if err != nil {
			t.Fatal(err)
		}
		var document map[string]any
		if err := yaml.Unmarshal(body, &document); err != nil {
			t.Fatal(err)
		}
		document["channels"] = map[string]any{"hitl": map[string]any{"provider": "telegram", "account": account}}
		body, err = yaml.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(config, body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	input, err := os.CreateTemp(t.TempDir(), "account-token")
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	if _, err := input.WriteString(token + "\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := input.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	prior := os.Stdin
	os.Stdin = input
	defer func() { os.Stdin = prior }()
	out, errOut := &lockedBuffer{}, &lockedBuffer{}
	args := []string{"--config", config, "channel", "connect", "telegram", "--yes"}
	if len(hitl) == 0 {
		args = append(args, "--json")
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan int, 1)
	progressDone := make(chan int, 1)
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-done
		}
	}()
	go func() {
		code := cliapp.Execute(ctx, args, out, errOut, nil, nil, OpenChannelRegistration)
		done <- code
		progressDone <- code
	}()
	if len(hitl) != 0 {
		challenge := waitChannelOnboardingChallenge(t, out, errOut, progressDone)
		callback, signing, _ := hitl[0].Registration()
		if callback != "" {
			admission := submitChannelOnboardingClaimAs(t, callback, signing, challenge, 1, 7000, 1001, "operator")
			if admission.StatusCode != http.StatusOK || len(admission.EventIDs) != 0 || len(admission.EventNames) != 0 {
				t.Fatalf("live deployment claim admission=%#v out=%s err=%s", admission, out.String(), errOut.String())
			}
		} else {
			hitl[0].QueueUpdate(token, map[string]any{"update_id": 1, "message": map[string]any{
				"message_id": 1, "from": map[string]any{"id": 7000, "username": "operator"},
				"chat": map[string]any{"id": 1001, "type": "private"}, "text": challenge,
			}})
		}
	}
	var code int
	select {
	case code = <-done:
		joined = true
	case <-time.After(20 * time.Second):
		cancel()
		<-done
		joined = true
		t.Fatalf("deployment claim did not finish: out=%s err=%s", out.String(), errOut.String())
	}
	if code != 0 {
		t.Fatalf("fixture account registration exit=%d out=%s err=%s", code, out.String(), errOut.String())
	}
	var result channelonboarding.Result
	if len(hitl) != 0 {
		if !strings.Contains(out.String(), "Claimed by") {
			t.Fatalf("deployment connect did not confirm the operator: %s", out.String())
		}
		out, errOut = &lockedBuffer{}, &lockedBuffer{}
		code = cliapp.Execute(t.Context(), []string{"--config", config, "channel", "list", "--json"}, out, errOut, nil, nil, OpenChannelRegistration)
		var listed struct {
			Accounts []channelonboarding.Result `json:"accounts"`
		}
		if code != 0 || json.Unmarshal([]byte(out.String()), &listed) != nil || len(listed.Accounts) != 1 || listed.Accounts[0].IdentityOperation == nil || listed.Accounts[0].IdentityOperation.State != operatorchannel.StateBound {
			t.Fatalf("deployment claim readback exit=%d out=%s err=%s", code, out.String(), errOut.String())
		}
		result = listed.Accounts[0]
	} else if err := json.Unmarshal([]byte(out.String()), &result); err != nil {
		t.Fatal(err)
	}
	if result.Operation.Provider != "telegram" ||
		result.Operation.AccountReference != account || !result.Operation.AccountConfirmed || result.Operation.Phase != channelonboarding.PhaseSucceeded {
		t.Fatalf("fixture account registration exit=%d err=%s", code, errOut.String())
	}
	return result.Operation
}

type channelOnboardingExactChoice struct {
	source       string
	interfaceRef string
	target       string
}

func TestChannelOnboardingE2E12MultipleExactContexts(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		backend := backend
		t.Run(string(backend), func(t *testing.T) {
			harness := newChannelOnboardingE2EHarness(t, backend, true)
			writeChannelOnboardingLiveToolCredentials(t, harness.credentialPath)
			addSecondChannelOnboardingTarget(t, harness.opts.SourceRoot)
			harness.start(t)

			choices := requireAmbiguousChannelOnboardingChoices(t, harness, 2)
			first, second := choices[0], choices[1]
			firstCommand := startChannelOnboardingCLICommand(t, harness.opts.ConfigPath, harness.endpoint, exactChannelOnboardingArgs("connect", first), "multi-context-token\n")
			challenge := waitChannelOnboardingChallenge(t, firstCommand.stdout, firstCommand.stderr, firstCommand.done)
			callbackURL, signingSecret := waitChannelOnboardingRegistration(t, harness.provider, firstCommand.stdout, firstCommand.stderr, firstCommand.done)
			requireChannelClaimDisposition(t, "E2E-12 first exact claim", submitChannelOnboardingClaim(t, callbackURL, signingSecret, challenge, 7212, "multi_context"), "consumed_by_binding")
			requireChannelOnboardingCommandSuccess(t, firstCommand)
			_ = waitChannelOnboardingDelivery(t, harness.provider, 0)

			firstReady := requireChannelOnboardingTargetRow(t, harness, first.target)
			if firstReady.Operation == nil || firstReady.Activation == nil || firstReady.Readiness == nil || !firstReady.Readiness.Ready {
				t.Fatalf("%s E2E-12 first exact context = %#v", backend, firstReady)
			}

			connectOut, connectErr := &lockedBuffer{}, &lockedBuffer{}
			restoreInput := installChannelOnboardingCLIInput(t, "")
			connectCode := executeCLI(context.Background(), append([]string{"--config", harness.opts.ConfigPath}, append(exactChannelOnboardingArgs("connect", second), "--api-server", harness.endpoint)...), connectOut, connectErr, nil)
			restoreInput()
			if connectCode == 0 || !strings.Contains(connectErr.String(), "provider credential input could not be read") {
				t.Fatalf("%s E2E-12 missing credential input code=%d stdout=%s stderr=%s", backend, connectCode, connectOut.String(), connectErr.String())
			}
			pending := requireChannelOnboardingTargetRow(t, harness, second.target)
			if pending.Operation == nil || pending.Operation.Phase != string(channelonboarding.PhasePreparing) || pending.Activation != nil {
				t.Fatalf("%s E2E-12 credential-blocked operation = %#v", backend, pending)
			}
			if pending.Identity.Status != "unbound" || pending.Identity.BindingRevision != 0 || pending.Identity.AccountReference != "" ||
				pending.Identity.ConversationRef != "" || pending.Identity.ConversationScope != "" || pending.Identity.ProofID != "" || pending.Identity.ProofRevision != 0 {
				t.Fatalf("%s E2E-12 sibling unexpectedly has a bound identity: %#v", backend, pending.Identity)
			}
			blockedOut, blockedErr := &lockedBuffer{}, &lockedBuffer{}
			blockedCode := executeCLI(context.Background(), []string{"--config", harness.opts.ConfigPath, "channel", "resume", pending.Operation.OperationID, "--yes", "--api-server", harness.endpoint}, blockedOut, blockedErr, nil)
			blockedSurface := blockedOut.String() + "\n" + blockedErr.String()
			blockedOperationID := channelOnboardingOperationIDPattern.FindString(blockedSurface)
			if blockedCode == 0 || blockedOperationID != pending.Operation.OperationID || !strings.Contains(blockedSurface, "CHANNEL_CREDENTIAL_REQUIRED") {
				t.Fatalf("%s E2E-12 second exact context code=%d\nstdout:\n%s\nstderr:\n%s", backend, blockedCode, blockedOut.String(), blockedErr.String())
			}

			blocked := requireChannelOnboardingTargetRow(t, harness, second.target)
			if blocked.Operation == nil || blocked.Operation.OperationID != blockedOperationID || blocked.Operation.Phase != string(channelonboarding.PhasePreparing) || blocked.Activation != nil {
				t.Fatalf("%s E2E-12 blocked sibling context = %#v", backend, blocked)
			}
			assertChannelOnboardingIdentityPreserved(t, string(backend)+" E2E-12 blocked sibling identity", pending, blocked)
			if blocked.Identity.Interface != pending.Identity.Interface || blocked.Identity.Status != pending.Identity.Status || blocked.Identity.ProofStatus != pending.Identity.ProofStatus {
				t.Fatalf("%s E2E-12 credential refusal changed sibling identity: before=%#v after=%#v", backend, pending.Identity, blocked.Identity)
			}
			firstAfterBlocked := requireChannelOnboardingTargetRow(t, harness, first.target)
			assertChannelOnboardingIdentityPreserved(t, string(backend)+" E2E-12 first identity after sibling refusal", firstReady, firstAfterBlocked)
			if firstAfterBlocked.Identity.Interface != firstReady.Identity.Interface || firstAfterBlocked.Identity.Status != firstReady.Identity.Status ||
				firstAfterBlocked.Identity.ProofStatus != firstReady.Identity.ProofStatus || firstAfterBlocked.Operation == nil ||
				*firstAfterBlocked.Operation != *firstReady.Operation || firstAfterBlocked.Activation == nil || *firstAfterBlocked.Activation != *firstReady.Activation ||
				firstAfterBlocked.Readiness == nil || *firstAfterBlocked.Readiness != *firstReady.Readiness {
				t.Fatalf("%s E2E-12 blocked sibling mutated first context: before=%#v after=%#v", backend, firstReady, firstAfterBlocked)
			}

			firstReconnect := startChannelOnboardingCLICommand(t, harness.opts.ConfigPath, harness.endpoint, exactChannelOnboardingArgs("reconnect", first), "")
			requireChannelOnboardingCommandSuccess(t, firstReconnect)
			_ = waitChannelOnboardingDelivery(t, harness.provider, 1)

			advanced := requireChannelOnboardingTargetRow(t, harness, first.target)
			stillBlocked := requireChannelOnboardingTargetRow(t, harness, second.target)
			assertChannelOnboardingIdentityPreserved(t, string(backend)+" E2E-12 exact reconnect", firstReady, advanced)
			if advanced.Activation == nil || firstReady.Activation == nil || advanced.Activation.Revision <= firstReady.Activation.Revision ||
				advanced.Readiness == nil || !advanced.Readiness.Ready || advanced.Readiness.ActivationGeneration == firstReady.Readiness.ActivationGeneration {
				t.Fatalf("%s E2E-12 exact reconnect did not advance selected context: before=%#v after=%#v", backend, firstReady, advanced)
			}
			if stillBlocked.Operation == nil || stillBlocked.Operation.OperationID != blocked.Operation.OperationID || stillBlocked.Operation.Phase != blocked.Operation.Phase || stillBlocked.Activation != nil {
				t.Fatalf("%s E2E-12 exact reconnect mutated sibling context: before=%#v after=%#v", backend, blocked, stillBlocked)
			}
			assertChannelOnboardingIdentityPreserved(t, string(backend)+" E2E-12 sibling after first reconnect", blocked, stillBlocked)
			if stillBlocked.Identity.Interface != blocked.Identity.Interface || stillBlocked.Identity.Status != blocked.Identity.Status || stillBlocked.Identity.ProofStatus != blocked.Identity.ProofStatus {
				t.Fatalf("%s E2E-12 exact reconnect changed sibling identity: before=%#v after=%#v", backend, blocked.Identity, stillBlocked.Identity)
			}
			if registrations, confirmations := harness.provider.OnboardingCounts(); registrations != 2 || confirmations != 2 {
				t.Fatalf("%s E2E-12 onboarding effects = %d/%d, want one exact registration and confirmation per completed operation", backend, registrations, confirmations)
			}
			requireAmbiguousChannelOnboardingChoices(t, harness, 2)
			harness.stop(t)
		})
	}
}

func TestChannelOnboardingE2E20FailedReplacementPreservesPredecessor(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		backend := backend
		t.Run(string(backend), func(t *testing.T) {
			harness := newChannelOnboardingE2EHarness(t, backend, true)
			writeChannelOnboardingLiveToolCredentials(t, harness.credentialPath)
			harness.start(t)
			predecessor := runChannelOnboardingCLIJourney(t, harness.opts.ConfigPath, harness.endpoint, harness.provider, "connect", "predecessor-token", 7220, "private", 0)
			if predecessor.Operation == nil || predecessor.Activation == nil || predecessor.Readiness == nil || !predecessor.Readiness.Ready {
				t.Fatalf("%s E2E-20 predecessor = %#v", backend, predecessor)
			}

			replacement := startChannelOnboardingRPC(t, harness, channelonboarding.VerbRebind, "", nil)
			if replacement.IdentityOperation == nil || replacement.Operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity {
				t.Fatalf("%s E2E-20 replacement start = %#v", backend, replacement)
			}
			callbackURL, signingSecret, _ := harness.provider.Registration()
			requireChannelClaimDisposition(t, "E2E-20 replacement claim", submitChannelOnboardingClaim(t, callbackURL, signingSecret, replacement.IdentityOperation.Challenge, 8220, "rejected_replacement"), "consumed_by_binding")
			claimed := getChannelOnboardingRPC(t, harness, replacement.Operation.OperationID)
			if claimed.IdentityOperation == nil || claimed.IdentityOperation.State != "awaiting_confirmation" {
				t.Fatalf("%s E2E-20 replacement claim = %#v", backend, claimed)
			}
			var rejected map[string]any
			requireServedJSONRPCResult(t, harness.rpcEndpoint(), "channel.confirm", map[string]any{
				"operation_id": claimed.IdentityOperation.OperationID, "expected_revision": claimed.IdentityOperation.Revision, "approve": false,
			}, &rejected)
			failed := retryChannelOnboardingRPC(t, harness, replacement.Operation.OperationID, "")
			if failed.Operation.Phase != channelonboarding.PhaseFailed || failed.Operation.FailureCode != "identity_rejected" {
				t.Fatalf("%s E2E-20 replacement terminal result = %#v", backend, failed)
			}

			current := requireChannelOnboardingOperationRow(t, harness, predecessor.Operation.OperationID)
			assertChannelOnboardingIdentityPreserved(t, string(backend)+" E2E-20 predecessor", predecessor, current)
			if current.Activation == nil || current.Readiness == nil || !current.Readiness.Ready || current.Operation == nil || current.Operation.OperationID != predecessor.Operation.OperationID ||
				current.Activation.Revision != predecessor.Activation.Revision || current.Readiness.ActivationGeneration != predecessor.Readiness.ActivationGeneration {
				t.Fatalf("%s E2E-20 failed replacement displaced predecessor: before=%#v after=%#v", backend, predecessor, current)
			}
			for _, row := range readChannelOnboardingRows(t, harness.opts.ConfigPath, harness.endpoint) {
				if row.Operation != nil && row.Operation.OperationID == replacement.Operation.OperationID {
					t.Fatalf("%s E2E-20 terminal replacement replaced current readback: %#v", backend, row)
				}
			}
			if registrations, confirmations := harness.provider.OnboardingCounts(); registrations != 1 || confirmations != 1 {
				t.Fatalf("%s E2E-20 onboarding effects = %d/%d, want retained predecessor registration and confirmation only", backend, registrations, confirmations)
			}
			harness.stop(t)
		})
	}
}

func requireAmbiguousChannelOnboardingChoices(t *testing.T, harness *channelOnboardingE2EHarness, want int) []channelOnboardingExactChoice {
	t.Helper()
	stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
	code := executeCLI(context.Background(), []string{
		"--config", harness.opts.ConfigPath, "channel", "reconnect", "telegram", "--yes", "--api-server", harness.endpoint,
	}, stdout, stderr, nil)
	surface := stdout.String() + "\n" + stderr.String()
	matches := channelOnboardingExactChoicePattern.FindAllStringSubmatch(surface, -1)
	if code == 0 || !strings.Contains(surface, "ambiguous") || len(matches) != want {
		t.Fatalf("ambiguous channel shorthand code=%d choices=%d, want %d\nstdout:\n%s\nstderr:\n%s", code, len(matches), want, stdout.String(), stderr.String())
	}
	choices := make([]channelOnboardingExactChoice, 0, len(matches))
	for _, match := range matches {
		choices = append(choices, channelOnboardingExactChoice{source: harness.opts.SourceRoot, interfaceRef: match[1], target: strings.TrimRight(match[2], ";,)")})
	}
	sort.Slice(choices, func(i, j int) bool { return choices[i].target < choices[j].target })
	return choices
}

func exactChannelOnboardingArgs(verb string, choice channelOnboardingExactChoice) []string {
	return []string{
		"channel", verb, "telegram", "--yes",
		"--source", choice.source, "--interface", choice.interfaceRef, "--target", choice.target,
	}
}

func requireChannelOnboardingTargetRow(t *testing.T, harness *channelOnboardingE2EHarness, target string) channelOnboardingJourneyReadback {
	t.Helper()
	for _, row := range readChannelOnboardingRows(t, harness.opts.ConfigPath, harness.endpoint) {
		if row.Operation != nil && row.Operation.TargetSelector == target {
			return row
		}
	}
	t.Fatalf("channel list has no exact target %s", target)
	return channelOnboardingJourneyReadback{}
}

func addSecondChannelOnboardingTarget(t *testing.T, sourceRoot string) {
	t.Helper()
	sourceDir := sourceRoot
	targetDir := filepath.Join(sourceRoot, "support")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("create second channel onboarding target: %v", err)
	}
	for _, name := range []string{"entities.yaml", "schema.yaml"} {
		contents, err := os.ReadFile(filepath.Join(sourceDir, name))
		if err != nil {
			t.Fatalf("read second channel onboarding target source %s: %v", name, err)
		}
		if name == "schema.yaml" {
			var schema map[string]any
			if err := yaml.Unmarshal(contents, &schema); err != nil {
				t.Fatal(err)
			}
			delete(schema, "connect")
			contents, err = yaml.Marshal(schema)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(targetDir, name), contents, 0o644); err != nil {
			t.Fatalf("write second channel onboarding target %s: %v", name, err)
		}
	}
	writeStandingCandidateFile(t, filepath.Join(targetDir, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
	copyReleaseFixtureTree(t, filepath.Join(sourceDir, "telegram"), filepath.Join(targetDir, "telegram"))
}

type channelOnboardingE2EHarness struct {
	registration   channelonboarding.Operation
	opts           cliapp.ServeOptions
	provider       *channelOnboardingTelegramProvider
	telegram       *httptest.Server
	credentialPath string
	backend        servedparity.Backend
	storeDSN       string
	endpoint       string
	process        *serveRuntimeTestProcess
}

func newChannelOnboardingE2EHarness(t *testing.T, backend servedparity.Backend, publicIngress bool) *channelOnboardingE2EHarness {
	t.Helper()
	isolateCLIAPIConfigEnv(t)
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	provider := &channelOnboardingTelegramProvider{}
	telegram := httptest.NewServer(provider)
	t.Cleanup(telegram.Close)
	sourceRoot := writeStandingTelegramServeFixture(t, telegram.URL)
	disableChannelOnboardingBusinessConsumers(t, sourceRoot)
	opts := cliapp.ServeOptions{
		SourceRoot: sourceRoot, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, AbandonActiveRuns: true, Verbose: true,
		WorkspaceBackend: "host", WorkspaceBackendSet: true, TestLLMRuntime: telegramPhraseBotLLMRuntime{},
	}
	if publicIngress {
		publicListener := reserveChannelOnboardingListener(t)
		publicListen := publicListener.Addr().String()
		redirectExternalHosts(t, map[string]string{"hooks.channel-onboarding.test": "http://" + publicListen, "api.telegram.org": telegram.URL})
		opts.PublicWebhookBaseURL = "https://hooks.channel-onboarding.test"
		opts.PublicWebhookListen = publicListen
		opts.PublicWebhookListener = publicListener
	}
	storeDSN := ""
	switch backend {
	case servedparity.BackendDefaultSQLite:
		sqlitePath := filepath.Join(t.TempDir(), "channel-onboarding-e2e.sqlite")
		storeDSN = sqlitePath
		opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", sqlitePath, channelOnboardingHostWorkspaceFields())
		opts.StoreMode = "sqlite"
	case servedparity.BackendExplicitPostgres:
		dsn, _, _ := testutil.StartPostgres(t)
		storeDSN = dsn
		opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, dsn)
		opts.StoreMode = "postgres"
	default:
		t.Fatalf("unsupported backend %q", backend)
	}
	opts.StoreModeSet = true
	enableChannelOnboardingRecoveryOnStartup(t, opts.ConfigPath)
	return &channelOnboardingE2EHarness{opts: opts, provider: provider, telegram: telegram, credentialPath: credentialPath, backend: backend, storeDSN: storeDSN}
}

func (h *channelOnboardingE2EHarness) start(t *testing.T) {
	t.Helper()
	h.process = startServeRuntimeTestProcess(t, h.opts)
	h.process.waitForReadyLine()
	h.endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, h.process.outputString())
}

func (h *channelOnboardingE2EHarness) stop(t *testing.T) {
	t.Helper()
	if h.process == nil {
		return
	}
	if code := h.process.stop(); code != 0 {
		t.Fatalf("channel onboarding E2E serve exit = %d\n%s", code, h.process.outputString())
	}
	h.process = nil
}

func (h *channelOnboardingE2EHarness) rpcEndpoint() string {
	return strings.TrimRight(h.endpoint, "/") + "/v1/rpc"
}

func startChannelOnboardingRPC(t *testing.T, harness *channelOnboardingE2EHarness, verb channelonboarding.Verb, credential string, selectors map[string]string) channelonboarding.Result {
	t.Helper()
	params := map[string]any{"provider": "telegram", "verb": string(verb), "provider_credential": credential, "save_proof": true}
	for key, value := range selectors {
		params[key] = value
	}
	var result channelonboarding.Result
	requireServedJSONRPCResult(t, harness.rpcEndpoint(), "channel.onboarding_start", params, &result)
	return result
}

func getChannelOnboardingRPC(t *testing.T, harness *channelOnboardingE2EHarness, operationID string) channelonboarding.Result {
	t.Helper()
	var result channelonboarding.Result
	requireServedJSONRPCResult(t, harness.rpcEndpoint(), "channel.onboarding_get", map[string]any{"operation_id": operationID}, &result)
	return result
}

func retryChannelOnboardingRPC(t *testing.T, harness *channelOnboardingE2EHarness, operationID, credential string) channelonboarding.Result {
	t.Helper()
	params := map[string]any{"operation_id": operationID}
	if credential != "" {
		params["provider_credential"] = credential
	}
	var result channelonboarding.Result
	requireServedJSONRPCResult(t, harness.rpcEndpoint(), "channel.onboarding_retry", params, &result)
	return result
}

type channelOnboardingPersistedHandoff struct {
	OperationPhase              string
	OperationActivationRevision int64
	OperationRuntimeInstanceID  string
	ActivationRevision          int64
	ActivationRuntimeInstanceID string
	ActivationStatus            string
}

func channelOnboardingPersistedHandoffAtPublication(t *testing.T, harness *channelOnboardingE2EHarness, operationID string) channelOnboardingPersistedHandoff {
	t.Helper()
	driver, placeholder := "sqlite", "?"
	if harness.backend == servedparity.BackendExplicitPostgres {
		driver, placeholder = "postgres", "$1"
	}
	db, err := sql.Open(driver, harness.storeDSN)
	if err != nil {
		t.Fatalf("open %s channel onboarding store: %v", harness.backend, err)
	}
	defer db.Close()
	query := `SELECT operation.phase,operation.activation_revision,operation.runtime_instance_id,
		activation.activation_revision,activation.runtime_instance_id,activation.status
		FROM channel_onboarding_operations operation
		JOIN connected_channel_activations activation ON activation.operation_id=operation.operation_id
		WHERE operation.operation_id=` + placeholder + ` ORDER BY activation.activation_revision DESC LIMIT 1`
	var out channelOnboardingPersistedHandoff
	if err := db.QueryRowContext(context.Background(), query, operationID).Scan(
		&out.OperationPhase, &out.OperationActivationRevision, &out.OperationRuntimeInstanceID,
		&out.ActivationRevision, &out.ActivationRuntimeInstanceID, &out.ActivationStatus,
	); err != nil {
		t.Fatalf("query %s channel onboarding publication handoff: %v", harness.backend, err)
	}
	return out
}

type channelOnboardingClaimAdmission struct {
	Disposition string   `json:"operator_channel_claim_disposition"`
	EventIDs    []string `json:"event_ids"`
	EventNames  []string `json:"event_names"`
	StatusCode  int      `json:"-"`
	Body        string   `json:"-"`
}

func submitChannelOnboardingClaim(t *testing.T, callbackURL, signingSecret, challenge string, updateID int64, username string) channelOnboardingClaimAdmission {
	t.Helper()
	return submitChannelOnboardingClaimAs(t, callbackURL, signingSecret, challenge, updateID, updateID+1000, updateID+2000, username)
}

func submitChannelOnboardingClaimAs(t *testing.T, callbackURL, signingSecret, challenge string, updateID, claimantID, chatID int64, username string) channelOnboardingClaimAdmission {
	t.Helper()
	return submitChannelOnboardingClaimWithChatType(t, callbackURL, signingSecret, challenge, updateID, claimantID, chatID, "private", username)
}

func submitChannelOnboardingClaimWithChatType(t *testing.T, callbackURL, signingSecret, challenge string, updateID, claimantID, chatID int64, chatType, username string) channelOnboardingClaimAdmission {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"update_id": updateID,
		"message": map[string]any{
			"message_id": updateID, "from": map[string]any{"id": claimantID, "username": username},
			"chat": map[string]any{"id": chatID, "type": chatType}, "text": challenge,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, callbackURL, strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Telegram-Bot-Api-Secret-Token", signingSecret)
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		t.Fatalf("submit Telegram claim: %v", err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	var admission channelOnboardingClaimAdmission
	_ = json.Unmarshal(payload, &admission)
	admission.StatusCode = response.StatusCode
	admission.Body = strings.TrimSpace(string(payload))
	return admission
}

func requireChannelClaimDisposition(t *testing.T, label string, admission channelOnboardingClaimAdmission, disposition string) {
	t.Helper()
	if admission.StatusCode != http.StatusAccepted || admission.Disposition != disposition || len(admission.EventIDs) != 0 || len(admission.EventNames) != 0 {
		t.Fatalf("%s admission = %#v, want status 202 disposition %s and zero business events", label, admission, disposition)
	}
}

func requireChannelOnboardingOperationRow(t *testing.T, harness *channelOnboardingE2EHarness, operationID string) channelOnboardingJourneyReadback {
	t.Helper()
	for _, row := range readChannelOnboardingRows(t, harness.opts.ConfigPath, harness.endpoint) {
		if row.Operation != nil && row.Operation.OperationID == operationID {
			return row
		}
	}
	t.Fatalf("channel list has no operation %s", operationID)
	return channelOnboardingJourneyReadback{}
}

type channelOnboardingCLICommand struct {
	stdout *lockedBuffer
	stderr *lockedBuffer
	done   chan int
}

func startChannelOnboardingCLICommand(t *testing.T, configPath, endpoint string, args []string, input string) channelOnboardingCLICommand {
	t.Helper()
	stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
	done := make(chan int, 1)
	restore := installChannelOnboardingCLIInput(t, input)
	t.Cleanup(restore)
	commandArgs := append([]string{"--config", configPath}, args...)
	commandArgs = append(commandArgs, "--api-server", endpoint)
	go func() {
		done <- executeCLI(context.Background(), commandArgs, stdout, stderr, nil)
	}()
	return channelOnboardingCLICommand{stdout: stdout, stderr: stderr, done: done}
}

func requireChannelOnboardingCommandSuccess(t *testing.T, command channelOnboardingCLICommand) {
	t.Helper()
	select {
	case code := <-command.done:
		if code != 0 {
			t.Fatalf("channel command exited %d\nstdout:\n%s\nstderr:\n%s", code, command.stdout.String(), command.stderr.String())
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("channel command did not complete\nstdout:\n%s\nstderr:\n%s", command.stdout.String(), command.stderr.String())
	}
}

func installChannelOnboardingCLIInput(t *testing.T, contents string) func() {
	t.Helper()
	prior := os.Stdin
	input, err := os.CreateTemp(t.TempDir(), "channel-e2e-input-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := input.WriteString(contents); err != nil {
		t.Fatal(err)
	}
	if _, err := input.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	os.Stdin = input
	var restored bool
	return func() {
		if restored {
			return
		}
		restored = true
		os.Stdin = prior
		_ = input.Close()
	}
}

func (a channelOnboardingClaimAdmission) String() string {
	return fmt.Sprintf("status=%d disposition=%s events=%v/%v body=%q", a.StatusCode, a.Disposition, a.EventIDs, a.EventNames, a.Body)
}
