package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/testpostgres"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/telegramapi"
)

var channelOnboardingChallengePattern = regexp.MustCompile(`SWARM-[A-Z2-7]{16}`)
var channelOnboardingOperationIDPattern = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`)

type channelOnboardingTelegramProvider = telegramapi.Double

func TestChannelConnectTelegramFirstUserJourney(t *testing.T) {
	scenarios := []servedparity.Scenario{
		servedparity.MustScenario(servedparity.ScenarioConnectedChannelOnboardingLifecycle),
		servedparity.MustScenario(servedparity.ScenarioConnectedChannelOnboardingRetryLifecycle),
	}
	servedparity.RunScenarioGroup(t, scenarios, runChannelConnectTelegramFirstUserJourney)
}

func runChannelConnectTelegramFirstUserJourney(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	started := time.Now()
	isolateCLIAPIConfigEnv(t)
	credentialPath := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	provider := &channelOnboardingTelegramProvider{}
	telegram := httptest.NewServer(provider)
	t.Cleanup(telegram.Close)
	contractsRoot := writeStandingTelegramServeFixture(t, telegram.URL)
	disableChannelOnboardingBusinessConsumers(t, contractsRoot)
	publicListen := reserveChannelOnboardingListenAddress(t)
	redirectExternalHosts(t, map[string]string{"hooks.channel-onboarding.test": "http://" + publicListen})

	var db *sql.DB
	var resetSelectedStore func()
	diagnosticDSN := ""
	opts := cliapp.ServeOptions{
		ContractsPath: contractsRoot, PlatformSpecPath: defaultPlatformSpecPath,
		APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		PublicWebhookBaseURL: "https://hooks.channel-onboarding.test", PublicWebhookListen: publicListen,
		SelfCheck: true, RequireBundleMatch: false, AbandonActiveRuns: true, Verbose: true,
		WorkspaceBackend: "host", WorkspaceBackendSet: true, TestLLMRuntime: telegramPhraseBotLLMRuntime{},
	}
	switch backend {
	case servedparity.BackendDefaultSQLite:
		sqlitePath := filepath.Join(t.TempDir(), "channel-onboarding.sqlite")
		diagnosticDSN = sqlitePath
		var err error
		db, err = sql.Open("sqlite", sqlitePath)
		if err != nil {
			t.Fatalf("open SQLite channel diagnostics: %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })
		opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", sqlitePath, channelOnboardingHostWorkspaceFields())
		opts.StoreMode = "sqlite"
		opts.StoreModeSet = true
		resetSelectedStore = func() {
			if err := os.Remove(sqlitePath); err != nil && !os.IsNotExist(err) {
				t.Fatalf("reset SQLite channel selected store: %v", err)
			}
		}
	case servedparity.BackendExplicitPostgres:
		dsn, postgresDB, _ := testutil.StartPostgres(t)
		diagnosticDSN = dsn
		db = postgresDB
		opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, dsn)
		opts.StoreMode = "postgres"
		opts.StoreModeSet = true
		resetSelectedStore = func() {
			resetDB, err := sql.Open("postgres", dsn)
			if err != nil {
				t.Fatalf("open PostgreSQL channel selected store for reset: %v", err)
			}
			if _, err := resetDB.ExecContext(context.Background(), `DROP SCHEMA public CASCADE`); err != nil {
				_ = resetDB.Close()
				t.Fatalf("drop PostgreSQL channel selected-store schema: %v", err)
			}
			if _, err := resetDB.ExecContext(context.Background(), `CREATE SCHEMA public`); err != nil {
				_ = resetDB.Close()
				t.Fatalf("create PostgreSQL channel selected-store schema: %v", err)
			}
			if err := resetDB.Close(); err != nil {
				t.Fatalf("close PostgreSQL channel selected-store reset handle: %v", err)
			}
			assertChannelOnboardingPostgresDSNEmpty(t, dsn, "after selected-store reset")
		}
	default:
		t.Fatalf("unsupported backend %q", backend)
	}
	enableChannelOnboardingRecoveryOnStartup(t, opts.ConfigPath)

	process := startServeRuntimeTestProcess(t, opts)
	process.waitForReadyLine()
	endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString())

	direct := runChannelOnboardingCLIJourney(t, opts.ConfigPath, endpoint, provider, "connect", "bot-token", 1001, "private", 0)
	if direct.Identity.ConversationScope != "direct" || direct.Readiness == nil || !direct.Readiness.Ready {
		t.Fatalf("%s direct channel readback = %#v", backend, direct)
	}
	assertChannelOnboardingReadinessGeneration(t, string(backend)+" direct", direct)
	reconnected := runChannelOnboardingReconnectJourney(t, opts.ConfigPath, endpoint, provider, 1, "")
	assertChannelOnboardingIdentityPreserved(t, string(backend)+" direct reconnect", direct, reconnected)
	assertChannelOnboardingReadinessGeneration(t, string(backend)+" direct reconnect", reconnected)
	if reconnected.Activation == nil || direct.Activation == nil || reconnected.Activation.Revision <= direct.Activation.Revision {
		t.Fatalf("%s reconnect activation revisions = %#v/%#v", backend, direct.Activation, reconnected.Activation)
	}
	if reconnected.Readiness.ActivationGeneration == direct.Readiness.ActivationGeneration {
		t.Fatalf("%s reconnect retained predecessor activation generation %q", backend, direct.Readiness.ActivationGeneration)
	}
	shared := runChannelOnboardingCLIJourney(t, opts.ConfigPath, endpoint, provider, "rebind", "", -2001, "group", 2)
	if shared.Identity.ConversationScope != "shared" || shared.Readiness == nil || !shared.Readiness.Ready {
		t.Fatalf("%s shared channel readback = %#v", backend, shared)
	}
	assertChannelOnboardingReadinessGeneration(t, string(backend)+" shared", shared)
	if shared.Identity.BindingRevision <= direct.Identity.BindingRevision || shared.Activation == nil || shared.Activation.Revision <= reconnected.Activation.Revision {
		t.Fatalf("%s rebind revisions direct/reconnected/shared = %#v/%#v/%#v", backend, direct, reconnected, shared)
	}
	if shared.Readiness.ActivationGeneration == reconnected.Readiness.ActivationGeneration {
		t.Fatalf("%s rebind retained predecessor activation generation %q", backend, reconnected.Readiness.ActivationGeneration)
	}
	runChannelOnboardingUnbindJourney(t, opts.ConfigPath, endpoint, shared.Identity.Interface.Selector)
	unbound := readChannelOnboardingRow(t, opts.ConfigPath, endpoint, "unbound")
	if unbound.Identity.ProofID != shared.Identity.ProofID || unbound.Identity.ProofRevision != shared.Identity.ProofRevision || unbound.Identity.ProofStatus != "active" {
		t.Fatalf("%s unbound proof readback = %#v, want retained active proof %#v", backend, unbound.Identity, shared.Identity)
	}
	assertChannelOnboardingCredentialStoreEmpty(t, credentialPath, string(backend)+" unbind")
	if code := process.stop(); code != 0 {
		t.Fatalf("%s pre-reset serve exit = %d", backend, code)
	}
	assertChannelOnboardingProjectClaimsReleased(t, string(backend)+" graceful shutdown")
	resetSelectedStore()
	crashProcess := startChannelOnboardingCrashServeProcess(t, opts, telegram.URL)
	endpoint = crashProcess.endpoint(t)
	restartedIdentity := readCurrentChannelOnboardingJourney(t, opts.ConfigPath, endpoint)
	if restartedIdentity.Identity.ConversationScope != shared.Identity.ConversationScope || restartedIdentity.Activation != nil || restartedIdentity.Readiness != nil ||
		restartedIdentity.Recovery == nil || restartedIdentity.Recovery.Reason != "activation_not_current" || restartedIdentity.Recovery.Provider != "telegram" ||
		len(restartedIdentity.Recovery.Commands) != 1 || restartedIdentity.Recovery.Commands[0] != "swarm channel reconnect telegram" {
		t.Fatalf("%s restarted reset readback = %#v, want proof-restored identity without activation/readiness", backend, restartedIdentity)
	}
	listOut, listErr := &lockedBuffer{}, &lockedBuffer{}
	if code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{"--config", opts.ConfigPath, "channel", "list", "--api-server", endpoint}, listOut, listErr, nil); code != 0 {
		t.Fatalf("%s reset channel list exited %d: %s", backend, code, listErr.String())
	}
	if !strings.Contains(listOut.String(), "channel telegram: identity verified, activation lost with store - run swarm channel reconnect telegram") {
		t.Fatalf("%s reset channel list lacks reconnect teaching:\n%s", backend, listOut.String())
	}
	registrationsBeforeBlock, deliveriesBeforeBlock := provider.Counts()
	blockedOut, blockedErr := &lockedBuffer{}, &lockedBuffer{}
	blockedCode := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{
		"--config", opts.ConfigPath, "channel", "reconnect", "telegram", "--yes", "--api-server", endpoint,
	}, blockedOut, blockedErr, nil)
	blockedSurface := blockedOut.String() + "\n" + blockedErr.String()
	blockedOperationID := channelOnboardingOperationIDPattern.FindString(blockedSurface)
	blockedResumeCommand := "swarm channel resume " + blockedOperationID + " --credential-stdin"
	if blockedCode == 0 || !strings.Contains(blockedSurface, "CHANNEL_CREDENTIAL_REQUIRED") || blockedOperationID == "" || !strings.Contains(blockedSurface, blockedResumeCommand) {
		t.Fatalf("%s E2E-04 credential-required result code=%d\nstdout:\n%s\nstderr:\n%s", backend, blockedCode, blockedOut.String(), blockedErr.String())
	}
	blockedRow := readCurrentChannelOnboardingJourney(t, opts.ConfigPath, endpoint)
	if blockedRow.Operation == nil || blockedRow.Operation.OperationID != blockedOperationID || blockedRow.Operation.Phase != "preparing" || blockedRow.Activation != nil || blockedRow.Recovery == nil ||
		blockedRow.Operation.Coordinate.RuntimeInstanceID == "" || len(blockedRow.Recovery.Commands) != 1 || blockedRow.Recovery.Commands[0] != blockedResumeCommand {
		t.Fatalf("%s E2E-04 blocked readback = %#v", backend, blockedRow)
	}
	blockedHumanOut, blockedHumanErr := &lockedBuffer{}, &lockedBuffer{}
	if code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{"--config", opts.ConfigPath, "channel", "list", "--api-server", endpoint}, blockedHumanOut, blockedHumanErr, nil); code != 0 || !strings.Contains(blockedHumanOut.String(), blockedResumeCommand) || !strings.Contains(blockedHumanOut.String(), blockedOperationID) {
		t.Fatalf("%s E2E-04 human list code=%d\nstdout:\n%s\nstderr:\n%s", backend, code, blockedHumanOut.String(), blockedHumanErr.String())
	}
	secondOut, secondErr := &lockedBuffer{}, &lockedBuffer{}
	secondCode := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{
		"--config", opts.ConfigPath, "channel", "reconnect", "telegram", "--yes", "--api-server", endpoint,
	}, secondOut, secondErr, nil)
	if secondCode == 0 || !strings.Contains(secondErr.String(), "operation_pending") {
		t.Fatalf("%s E2E-04 second reconnect code=%d\nstdout:\n%s\nstderr:\n%s", backend, secondCode, secondOut.String(), secondErr.String())
	}
	if registrations, deliveries := provider.Counts(); registrations != registrationsBeforeBlock || deliveries != deliveriesBeforeBlock {
		t.Fatalf("%s E2E-04 blocked provider effects registrations=%d/%d deliveries=%d/%d", backend, registrations, registrationsBeforeBlock, deliveries, deliveriesBeforeBlock)
	}

	if err := crashProcess.stop(); err != nil {
		t.Fatalf("%s E2E-06 stop serve: %v", backend, err)
	}
	crashProcess = startChannelOnboardingCrashServeProcess(t, opts, telegram.URL)
	endpoint = crashProcess.endpoint(t)
	restartedBlockedRow := readCurrentChannelOnboardingJourney(t, opts.ConfigPath, endpoint)
	if restartedBlockedRow.Operation == nil || restartedBlockedRow.Operation.OperationID != blockedOperationID || restartedBlockedRow.Operation.Phase != "preparing" || restartedBlockedRow.Recovery == nil ||
		len(restartedBlockedRow.Recovery.Commands) != 1 || restartedBlockedRow.Recovery.Commands[0] != blockedResumeCommand {
		t.Fatalf("%s E2E-06 restarted blocked readback = %#v\nstored operation: %s\nchild output:\n%s", backend, restartedBlockedRow,
			channelOnboardingOperationDiagnostic(t, backend, diagnosticDSN, blockedOperationID), crashProcess.output.String())
	}
	if restartedBlockedRow.Operation.Coordinate.RuntimeInstanceID == blockedRow.Operation.Coordinate.RuntimeInstanceID {
		t.Fatalf("%s E2E-06 retained predecessor runtime instance %s across restart", backend, blockedRow.Operation.Coordinate.RuntimeInstanceID)
	}
	if registrations, deliveries := provider.Counts(); registrations != registrationsBeforeBlock || deliveries != deliveriesBeforeBlock {
		t.Fatalf("%s E2E-06 restart replayed provider effects registrations=%d/%d deliveries=%d/%d", backend, registrations, registrationsBeforeBlock, deliveries, deliveriesBeforeBlock)
	}

	recovered := runChannelOnboardingResumeJourney(t, opts.ConfigPath, endpoint, provider, blockedOperationID, 3, "replacement-bot-token")
	if recovered.Operation == nil || recovered.Operation.OperationID != blockedOperationID || recovered.Operation.Phase != "succeeded" {
		t.Fatalf("%s E2E-05 resumed operation = %#v, want same succeeded operation %s", backend, recovered.Operation, blockedOperationID)
	}
	if recovered.Identity.AccountReference != shared.Identity.AccountReference || recovered.Identity.ConversationRef != shared.Identity.ConversationRef || recovered.Identity.ConversationScope != shared.Identity.ConversationScope {
		t.Fatalf("%s reset reconnect changed external identity: before=%#v after=%#v", backend, shared.Identity, recovered.Identity)
	}
	if recovered.Readiness == nil || !recovered.Readiness.Ready || recovered.Activation == nil {
		t.Fatalf("%s reset reconnect readback = %#v", backend, recovered)
	}
	assertChannelOnboardingReadinessGeneration(t, string(backend)+" reset reconnect", recovered)
	if recovered.Readiness.ActivationGeneration == shared.Readiness.ActivationGeneration {
		t.Fatalf("%s reset reconnect retained unavailable predecessor activation generation %q", backend, shared.Readiness.ActivationGeneration)
	}
	if registrations, deliveries := provider.Counts(); registrations != registrationsBeforeBlock+1 || deliveries != deliveriesBeforeBlock+1 {
		t.Fatalf("%s E2E-05 resume effects registrations=%d deliveries=%d, want %d/%d", backend, registrations, deliveries, registrationsBeforeBlock+1, deliveriesBeforeBlock+1)
	}

	registrationsBeforeLoss, deliveriesBeforeLoss := provider.Counts()
	provider.LoseNextRegistrationAcknowledgment()
	reconciled := runChannelOnboardingReconnectJourney(t, opts.ConfigPath, endpoint, provider, 4, "")
	if reconciled.Operation == nil || reconciled.Operation.Phase != "succeeded" || reconciled.Readiness == nil || !reconciled.Readiness.Ready {
		t.Fatalf("%s E2E-15 response-loss reconciliation = %#v", backend, reconciled)
	}
	if registrations, deliveries := provider.Counts(); registrations != registrationsBeforeLoss+1 || deliveries != deliveriesBeforeLoss+1 {
		t.Fatalf("%s E2E-15 replayed uncertain provider effect registrations=%d deliveries=%d, want %d/%d", backend, registrations, deliveries, registrationsBeforeLoss+1, deliveriesBeforeLoss+1)
	}

	registrationsBeforeReject, deliveriesBeforeReject := provider.Counts()
	provider.RejectNextCredentialPreflight()
	rejectedSurface := runRejectedChannelOnboardingReplacement(t, opts.ConfigPath, endpoint, "rejected-replacement-token")
	if !strings.Contains(rejectedSurface, "CHANNEL_CREDENTIAL_REQUIRED") || !strings.Contains(rejectedSurface, "rejected its credential with HTTP 401") {
		t.Fatalf("%s E2E-10 rejected credential surface lacks typed correction:\n%s", backend, rejectedSurface)
	}
	rows := readChannelOnboardingRows(t, opts.ConfigPath, endpoint)
	var rejectedOperation *channelOnboardingJourneyReadback
	predecessorReady := false
	for index := range rows {
		row := &rows[index]
		if row.Operation != nil && row.Operation.Phase == "preparing" && row.Recovery != nil {
			rejectedOperation = row
		}
		if row.Operation != nil && reconciled.Operation != nil && row.Operation.OperationID == reconciled.Operation.OperationID && row.Readiness != nil && row.Readiness.Ready {
			predecessorReady = true
		}
	}
	if rejectedOperation == nil || rejectedOperation.Operation == nil || !predecessorReady {
		t.Fatalf("%s E2E-10 rejected replacement rows = %#v", backend, rows)
	}
	rejectedOperationID := rejectedOperation.Operation.OperationID
	wantRejectedResume := "swarm channel resume " + rejectedOperationID + " --credential-stdin"
	if len(rejectedOperation.Recovery.Commands) != 1 || rejectedOperation.Recovery.Commands[0] != wantRejectedResume {
		t.Fatalf("%s E2E-10 rejected replacement recovery = %#v, want %q", backend, rejectedOperation.Recovery, wantRejectedResume)
	}
	if registrations, deliveries := provider.Counts(); registrations != registrationsBeforeReject || deliveries != deliveriesBeforeReject {
		t.Fatalf("%s E2E-10 rejected preflight changed provider effects registrations=%d/%d deliveries=%d/%d", backend, registrations, registrationsBeforeReject, deliveries, deliveriesBeforeReject)
	}
	corrected := runChannelOnboardingResumeJourney(t, opts.ConfigPath, endpoint, provider, rejectedOperationID, 5, "corrected-replacement-token")
	if corrected.Operation == nil || corrected.Operation.OperationID != rejectedOperationID || corrected.Operation.Phase != "succeeded" || corrected.Readiness == nil || !corrected.Readiness.Ready {
		t.Fatalf("%s E2E-10 corrected replacement = %#v", backend, corrected)
	}
	if registrations, deliveries := provider.Counts(); registrations != registrationsBeforeReject+1 || deliveries != deliveriesBeforeReject+1 {
		t.Fatalf("%s E2E-10 corrected replacement effects registrations=%d deliveries=%d, want %d/%d", backend, registrations, deliveries, registrationsBeforeReject+1, deliveriesBeforeReject+1)
	}
	recovered = corrected
	runChannelOnboardingUnbindJourney(t, opts.ConfigPath, endpoint, recovered.Identity.Interface.Selector)
	assertChannelOnboardingCredentialStoreEmpty(t, credentialPath, string(backend)+" recovered unbind")
	runChannelOnboardingProofRevokeJourney(t, opts.ConfigPath, endpoint, recovered.Identity.Interface.Selector)
	revoked := readChannelOnboardingRow(t, opts.ConfigPath, endpoint, "unbound")
	if revoked.Identity.ProofStatus != "revoked" || revoked.Identity.ProofID != recovered.Identity.ProofID || revoked.Identity.ProofRevision <= recovered.Identity.ProofRevision {
		t.Fatalf("%s revoked proof readback = %#v, want retained revoked proof identity %#v", backend, revoked.Identity, recovered.Identity)
	}
	if err := crashProcess.stop(); err != nil {
		t.Fatalf("%s final channel serve shutdown: %v", backend, err)
	}
	if db == nil {
		t.Fatalf("%s selected store database was not captured", backend)
	}
	for _, scenario := range scenariosForChannelOnboardingJourney() {
		servedparity.AssertSettlementPostconditions(t, scenario, servedparity.SettlementCounts{})
	}
	if elapsed := time.Since(started); elapsed >= time.Minute {
		t.Fatalf("%s first-user channel journey took %s, want under 60s", backend, elapsed)
	}
}

func channelOnboardingOperationDiagnostic(t *testing.T, backend servedparity.Backend, dsn, operationID string) string {
	t.Helper()
	driver := "sqlite"
	placeholder := "?"
	if backend == servedparity.BackendExplicitPostgres {
		driver = "postgres"
		placeholder = "$1"
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return err.Error()
	}
	defer db.Close()
	var phase, failureCode, failureMessage, bundleHash, bundleSource, bundleIdentity, inventory, runtimeInstance, plan, target string
	var publication, targetGeneration int64
	err = db.QueryRowContext(context.Background(), `SELECT phase,failure_code,failure_message,bundle_hash,bundle_source,bundle_identity,
		pack_inventory_generation,runtime_instance_id,context_publication_generation,plan_generation,target_selector,target_generation
		FROM channel_onboarding_operations WHERE operation_id=`+placeholder, operationID).Scan(
		&phase, &failureCode, &failureMessage, &bundleHash, &bundleSource, &bundleIdentity,
		&inventory, &runtimeInstance, &publication, &plan, &target, &targetGeneration,
	)
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("phase=%s failure=%s:%s bundle=%s source=%s identity=%s inventory=%s runtime_instance=%s publication=%d plan=%s target=%s target_generation=%d",
		phase, failureCode, failureMessage, bundleHash, bundleSource, bundleIdentity, inventory, runtimeInstance, publication, plan, target, targetGeneration)
}

func assertChannelOnboardingProjectClaimsReleased(t *testing.T, label string) {
	t.Helper()
	claimDir := filepath.Join(os.Getenv("HOME"), ".swarm", "contexts", ".project-claims")
	entries, err := os.ReadDir(claimDir)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		t.Fatalf("%s read project claims: %v", label, err)
	}
	if len(entries) == 0 {
		return
	}
	claims := make([]string, 0, len(entries))
	for _, entry := range entries {
		path := filepath.Join(claimDir, entry.Name())
		contents, readErr := os.ReadFile(path)
		if readErr != nil {
			claims = append(claims, entry.Name()+": "+readErr.Error())
			continue
		}
		claims = append(claims, entry.Name()+": "+strings.TrimSpace(string(contents)))
	}
	t.Fatalf("%s retained project claims after serve exit: %s", label, strings.Join(claims, "; "))
}

const channelOnboardingCrashServeHelperEnv = "TEST_CHANNEL_ONBOARDING_CRASH_SERVE_HELPER"

type channelOnboardingCrashServeProcess struct {
	cmd     *exec.Cmd
	output  *lockedBuffer
	exited  chan struct{}
	waitMu  sync.Mutex
	waitErr error
}

func startChannelOnboardingCrashServeProcess(t *testing.T, opts cliapp.ServeOptions, telegramBaseURL string) *channelOnboardingCrashServeProcess {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	output := &lockedBuffer{}
	cmd := exec.Command(executable, "-test.run=^TestChannelOnboardingCrashServeProcessHelper$", "-test.v")
	cmd.Dir = cliapp.RepoRoot()
	cmd.Env = append(os.Environ(),
		channelOnboardingCrashServeHelperEnv+"=1",
		"TEST_CHANNEL_ONBOARDING_CONFIG="+opts.ConfigPath,
		"TEST_CHANNEL_ONBOARDING_CONTRACTS="+opts.ContractsPath,
		"TEST_CHANNEL_ONBOARDING_PLATFORM_SPEC="+opts.PlatformSpecPath,
		"TEST_CHANNEL_ONBOARDING_STORE="+opts.StoreMode,
		"TEST_CHANNEL_ONBOARDING_PUBLIC_ORIGIN="+opts.PublicWebhookBaseURL,
		"TEST_CHANNEL_ONBOARDING_PUBLIC_LISTEN="+opts.PublicWebhookListen,
		"TEST_CHANNEL_ONBOARDING_TELEGRAM_BASE="+telegramBaseURL,
	)
	cmd.Stdout = output
	cmd.Stderr = output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start channel onboarding crash process: %v", err)
	}
	process := &channelOnboardingCrashServeProcess{cmd: cmd, output: output, exited: make(chan struct{})}
	go func() {
		waitErr := cmd.Wait()
		process.waitMu.Lock()
		process.waitErr = waitErr
		process.waitMu.Unlock()
		close(process.exited)
	}()
	t.Cleanup(func() { _ = process.stop() })
	process.endpoint(t)
	return process
}

func (p *channelOnboardingCrashServeProcess) endpoint(t *testing.T) string {
	t.Helper()
	deadline := time.Now().Add(serveRuntimeReadyTimeout)
	for time.Now().Before(deadline) {
		output := p.output.String()
		if serveOutputIsReady(output) {
			if address := serveRuntimeAPIListenerFromOutputIfPresent(output); address != "" {
				return "http://" + address
			}
		}
		select {
		case <-p.exited:
			t.Fatalf("channel onboarding crash process exited before readiness: %v\n%s", p.waitError(), p.output.String())
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for channel onboarding crash process\n%s", p.output.String())
	return ""
}

func (p *channelOnboardingCrashServeProcess) kill() error {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return nil
	}
	if err := p.cmd.Process.Kill(); err != nil {
		select {
		case <-p.exited:
			return nil
		default:
			return err
		}
	}
	select {
	case <-p.exited:
		return nil
	case <-time.After(10 * time.Second):
		return fmt.Errorf("killed channel onboarding process did not exit")
	}
}

func (p *channelOnboardingCrashServeProcess) stop() error {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return nil
	}
	select {
	case <-p.exited:
		return p.waitError()
	default:
	}
	if err := p.cmd.Process.Signal(os.Interrupt); err != nil {
		select {
		case <-p.exited:
			return p.waitError()
		default:
			return err
		}
	}
	timer := time.NewTimer(serveRuntimeStopTimeout)
	defer timer.Stop()
	select {
	case <-p.exited:
		return p.waitError()
	case <-timer.C:
		return fmt.Errorf("channel onboarding process did not stop after interrupt")
	}
}

func (p *channelOnboardingCrashServeProcess) waitError() error {
	p.waitMu.Lock()
	defer p.waitMu.Unlock()
	return p.waitErr
}

func TestChannelOnboardingCrashServeProcessHelper(t *testing.T) {
	if os.Getenv(channelOnboardingCrashServeHelperEnv) != "1" {
		t.Skip("subprocess helper")
	}
	redirectExternalHosts(t, map[string]string{
		"api.telegram.org":              os.Getenv("TEST_CHANNEL_ONBOARDING_TELEGRAM_BASE"),
		"hooks.channel-onboarding.test": "http://" + os.Getenv("TEST_CHANNEL_ONBOARDING_PUBLIC_LISTEN"),
	})
	opts := cliapp.DefaultServeOptions()
	opts.ConfigPath = os.Getenv("TEST_CHANNEL_ONBOARDING_CONFIG")
	opts.ContractsPath = os.Getenv("TEST_CHANNEL_ONBOARDING_CONTRACTS")
	opts.PlatformSpecPath = os.Getenv("TEST_CHANNEL_ONBOARDING_PLATFORM_SPEC")
	opts.StoreMode = os.Getenv("TEST_CHANNEL_ONBOARDING_STORE")
	opts.StoreModeSet = true
	opts.APIListenAddr = "127.0.0.1:0"
	opts.MCPListenAddr = "127.0.0.1:0"
	opts.PublicWebhookBaseURL = os.Getenv("TEST_CHANNEL_ONBOARDING_PUBLIC_ORIGIN")
	opts.PublicWebhookListen = os.Getenv("TEST_CHANNEL_ONBOARDING_PUBLIC_LISTEN")
	opts.WorkspaceBackend = "host"
	opts.WorkspaceBackendSet = true
	opts.SelfCheck = true
	opts.RequireBundleMatch = false
	opts.AbandonActiveRuns = true
	opts.Verbose = true
	opts.Output = os.Stdout
	opts.ErrorOutput = os.Stderr
	opts.TestLLMRuntime = telegramPhraseBotLLMRuntime{}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if code := Run(ctx, cliapp.RepoRoot(), opts); code != 0 {
		t.Fatalf("channel onboarding crash serve exited %d", code)
	}
}

func assertChannelOnboardingPostgresStoreEmpty(t testing.TB, db *sql.DB, label string) {
	t.Helper()
	var tableCount int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'`).Scan(&tableCount); err != nil {
		t.Fatalf("inspect PostgreSQL channel store %s: %v", label, err)
	}
	if tableCount != 0 {
		t.Fatalf("PostgreSQL channel store %s has %d public tables, want 0", label, tableCount)
	}
}

func assertChannelOnboardingPostgresDSNEmpty(t testing.TB, dsn, label string) {
	t.Helper()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL channel store %s: %v", label, err)
	}
	defer db.Close()
	assertChannelOnboardingPostgresStoreEmpty(t, db, label)
}

func serveRuntimeAPIListenerFromOutputIfPresent(output string) string {
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		for index, field := range fields {
			field = strings.Trim(field, "(),")
			if address, ok := strings.CutPrefix(field, "api_listener="); ok && strings.TrimSpace(address) != "" {
				return address
			}
			if strings.TrimSpace(line) != "" && fields[0] == "listeners" && field == "api" && index+1 < len(fields) {
				return strings.Trim(fields[index+1], "(),")
			}
		}
	}
	return ""
}

type channelOnboardingJourneyReadback struct {
	Identity struct {
		Interface struct {
			Selector string `json:"selector"`
		} `json:"interface"`
		Status            string `json:"status"`
		BindingRevision   int64  `json:"binding_revision"`
		AccountReference  string `json:"account_reference"`
		ConversationRef   string `json:"conversation_reference"`
		ConversationScope string `json:"conversation_scope"`
		ProofID           string `json:"proof_id"`
		ProofRevision     int64  `json:"proof_revision"`
		ProofStatus       string `json:"proof_status"`
	} `json:"identity"`
	Operation *struct {
		OperationID    string `json:"operation_id"`
		TargetSelector string `json:"target_selector"`
		Phase          string `json:"phase"`
		Coordinate     struct {
			BundleHash                   string `json:"bundle_hash"`
			RuntimeInstanceID            string `json:"runtime_instance_id"`
			ContextPublicationGeneration uint64 `json:"context_publication_generation"`
			TargetGeneration             uint64 `json:"target_generation"`
		} `json:"coordinate"`
	} `json:"operation"`
	Activation *struct {
		Revision int64 `json:"revision"`
	} `json:"activation"`
	Readiness *struct {
		Ready                bool   `json:"ready"`
		Reason               string `json:"reason"`
		ActivationGeneration string `json:"activation_generation"`
	} `json:"readiness"`
	Recovery *struct {
		Reason   string   `json:"reason"`
		Provider string   `json:"provider"`
		Commands []string `json:"commands"`
	} `json:"recovery"`
}

func assertChannelOnboardingReadinessGeneration(t *testing.T, label string, readback channelOnboardingJourneyReadback) {
	t.Helper()
	if readback.Readiness == nil || readback.Readiness.ActivationGeneration == "" {
		t.Fatalf("%s readiness lacks exact activation generation: %#v", label, readback.Readiness)
	}
}

func runChannelOnboardingReconnectJourney(t *testing.T, configPath, endpoint string, provider *channelOnboardingTelegramProvider, deliveryIndex int, credential string) channelOnboardingJourneyReadback {
	t.Helper()
	stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
	args := []string{"--config", configPath, "channel", "reconnect", "telegram", "--yes", "--api-server", endpoint}
	if credential != "" {
		args = append(args, "--credential-stdin")
		priorStdin := os.Stdin
		input, err := os.CreateTemp(t.TempDir(), "channel-reconnect-input-*")
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			os.Stdin = priorStdin
			_ = input.Close()
		}()
		if _, err := input.WriteString(credential + "\n"); err != nil {
			t.Fatal(err)
		}
		if _, err := input.Seek(0, 0); err != nil {
			t.Fatal(err)
		}
		os.Stdin = input
	}
	code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), args, stdout, stderr, nil)
	if code != 0 {
		t.Fatalf("channel reconnect exited %d\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
	surface := stdout.String() + "\n" + stderr.String()
	if !strings.Contains(surface, "READY") || channelOnboardingChallengePattern.MatchString(surface) {
		t.Fatalf("channel reconnect readiness/ceremony contract violated\n%s", surface)
	}
	delivery := waitChannelOnboardingDelivery(t, provider, deliveryIndex)
	if delivery["text"] != "Swarm channel connected." {
		t.Fatalf("channel reconnect confirmation = %#v", delivery)
	}
	return readCurrentChannelOnboardingJourney(t, configPath, endpoint)
}

func runRejectedChannelOnboardingReplacement(t *testing.T, configPath, endpoint, credential string) string {
	t.Helper()
	stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
	priorStdin := os.Stdin
	input, err := os.CreateTemp(t.TempDir(), "channel-rejected-replacement-input-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		os.Stdin = priorStdin
		_ = input.Close()
	}()
	if _, err := input.WriteString(credential + "\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := input.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	os.Stdin = input
	code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{
		"--config", configPath, "channel", "reconnect", "telegram", "--yes", "--credential-stdin", "--api-server", endpoint,
	}, stdout, stderr, nil)
	if code == 0 {
		t.Fatalf("rejected channel replacement unexpectedly succeeded\nstdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String()+stderr.String(), credential) {
		t.Fatalf("rejected channel replacement leaked credential\nstdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
	}
	return stdout.String() + "\n" + stderr.String()
}

func runChannelOnboardingResumeJourney(t *testing.T, configPath, endpoint string, provider *channelOnboardingTelegramProvider, operationID string, deliveryIndex int, credential string) channelOnboardingJourneyReadback {
	t.Helper()
	stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
	priorStdin := os.Stdin
	input, err := os.CreateTemp(t.TempDir(), "channel-resume-input-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		os.Stdin = priorStdin
		_ = input.Close()
	}()
	if _, err := input.WriteString(credential + "\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := input.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	os.Stdin = input
	code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{
		"--config", configPath, "channel", "resume", operationID, "--yes", "--credential-stdin", "--api-server", endpoint,
	}, stdout, stderr, nil)
	if code != 0 {
		t.Fatalf("channel resume exited %d\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
	surface := stdout.String() + "\n" + stderr.String()
	if !strings.Contains(surface, "READY") || strings.Contains(surface, credential) || channelOnboardingChallengePattern.MatchString(surface) {
		t.Fatalf("channel resume readiness/secret/ceremony contract violated\n%s", surface)
	}
	delivery := waitChannelOnboardingDelivery(t, provider, deliveryIndex)
	if delivery["text"] != "Swarm channel connected." {
		t.Fatalf("channel resume confirmation = %#v", delivery)
	}
	return readCurrentChannelOnboardingJourney(t, configPath, endpoint)
}

func runChannelOnboardingUnbindJourney(t *testing.T, configPath, endpoint, selector string) {
	t.Helper()
	stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
	code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{
		"--config", configPath, "channel", "unbind", selector, "--api-server", endpoint,
	}, stdout, stderr, nil)
	if code != 0 {
		t.Fatalf("channel unbind exited %d\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
}

func runChannelOnboardingProofRevokeJourney(t *testing.T, configPath, endpoint, selector string) {
	t.Helper()
	stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
	code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{
		"--config", configPath, "channel", "revoke-proof", selector, "--api-server", endpoint,
	}, stdout, stderr, nil)
	if code != 0 {
		t.Fatalf("channel proof revoke exited %d\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
}

func assertChannelOnboardingCredentialStoreEmpty(t *testing.T, credentialPath, label string) {
	t.Helper()
	credentialStore, err := runtimecredentials.NewFileStore(credentialPath)
	if err != nil {
		t.Fatalf("%s open credential store: %v", label, err)
	}
	keys, err := credentialStore.List(context.Background())
	if err != nil {
		t.Fatalf("%s list credential store: %v", label, err)
	}
	if len(keys) != 0 {
		t.Fatalf("%s retained channel credentials: %v", label, keys)
	}
}

func assertChannelOnboardingIdentityPreserved(t *testing.T, label string, before, after channelOnboardingJourneyReadback) {
	t.Helper()
	if after.Identity.BindingRevision != before.Identity.BindingRevision ||
		after.Identity.AccountReference != before.Identity.AccountReference ||
		after.Identity.ConversationRef != before.Identity.ConversationRef ||
		after.Identity.ConversationScope != before.Identity.ConversationScope ||
		after.Identity.ProofID != before.Identity.ProofID ||
		after.Identity.ProofRevision != before.Identity.ProofRevision {
		t.Fatalf("%s changed identity: before=%#v after=%#v", label, before.Identity, after.Identity)
	}
}

func runChannelOnboardingCLIJourney(t *testing.T, configPath, endpoint string, provider *channelOnboardingTelegramProvider, verb, credential string, chatID int64, chatType string, deliveryIndex int) channelOnboardingJourneyReadback {
	t.Helper()
	stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
	done := make(chan int, 1)
	args := []string{"--config", configPath, "channel", verb, "telegram", "--yes", "--api-server", endpoint}

	priorStdin := os.Stdin
	input, err := os.CreateTemp(t.TempDir(), "channel-onboarding-input-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := input.WriteString(credential + "\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := input.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	os.Stdin = input
	t.Cleanup(func() {
		os.Stdin = priorStdin
		_ = input.Close()
	})
	go func() {
		done <- cliapp.Execute(context.Background(), cliapp.RepoRoot(), args, stdout, stderr, nil)
	}()

	challenge := waitChannelOnboardingChallenge(t, stdout, stderr, done)
	callbackURL, signingSecret := waitChannelOnboardingRegistration(t, provider, stdout, stderr, done)
	requestBody, err := json.Marshal(map[string]any{
		"update_id": time.Now().UnixNano(),
		"message": map[string]any{
			"message_id": deliveryIndex + 1,
			"from":       map[string]any{"id": 7000 + deliveryIndex, "username": fmt.Sprintf("operator_%d", deliveryIndex)},
			"chat":       map[string]any{"id": chatID, "type": chatType},
			"text":       challenge,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, callbackURL, strings.NewReader(string(requestBody)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Telegram-Bot-Api-Secret-Token", signingSecret)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("publish %s Telegram claim: %v", chatType, err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read %s claim admission: %v", chatType, err)
	}
	var admission map[string]any
	if err := json.Unmarshal(responseBody, &admission); err != nil {
		t.Fatalf("decode %s claim admission status/body %d/%q: %v", chatType, response.StatusCode, responseBody, err)
	}
	eventIDs, eventIDsOK := admission["event_ids"].([]any)
	eventNames, eventNamesOK := admission["event_names"].([]any)
	if response.StatusCode != http.StatusAccepted || admission["operator_channel_claim_disposition"] != "consumed_by_binding" || !eventIDsOK || len(eventIDs) != 0 || !eventNamesOK || len(eventNames) != 0 {
		t.Fatalf("%s claim admission status/body = %d/%#v", chatType, response.StatusCode, admission)
	}

	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("channel %s exited %d\nstdout:\n%s\nstderr:\n%s", verb, code, stdout.String(), stderr.String())
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("channel %s did not complete\nstdout:\n%s\nstderr:\n%s", verb, stdout.String(), stderr.String())
	}
	secretSurface := stdout.String() + "\n" + stderr.String()
	if !strings.Contains(secretSurface, "READY") || strings.Contains(secretSurface, "bot-token") || strings.Contains(secretSurface, signingSecret) {
		t.Fatalf("channel %s output violated readiness/secret contract\n%s", verb, secretSurface)
	}
	delivery := waitChannelOnboardingDelivery(t, provider, deliveryIndex)
	if fmt.Sprint(delivery["chat_id"]) != fmt.Sprint(chatID) || delivery["text"] != "Swarm channel connected." {
		t.Fatalf("channel %s confirmation = %#v", verb, delivery)
	}

	return readCurrentChannelOnboardingJourney(t, configPath, endpoint)
}

func readCurrentChannelOnboardingJourney(t *testing.T, configPath, endpoint string) channelOnboardingJourneyReadback {
	t.Helper()
	for _, row := range readChannelOnboardingRows(t, configPath, endpoint) {
		if row.Identity.Status == "current" {
			return row
		}
	}
	t.Fatal("channel list has no current row")
	return channelOnboardingJourneyReadback{}
}

func readChannelOnboardingRow(t *testing.T, configPath, endpoint, status string) channelOnboardingJourneyReadback {
	t.Helper()
	rows := readChannelOnboardingRows(t, configPath, endpoint)
	for _, row := range rows {
		if row.Identity.Status == status {
			return row
		}
	}
	t.Fatalf("channel list has no %s row: %#v", status, rows)
	return channelOnboardingJourneyReadback{}
}

func readChannelOnboardingRows(t *testing.T, configPath, endpoint string) []channelOnboardingJourneyReadback {
	t.Helper()
	listOut, listErr := &lockedBuffer{}, &lockedBuffer{}
	code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{"--config", configPath, "channel", "list", "--json", "--api-server", endpoint}, listOut, listErr, nil)
	if code != 0 {
		t.Fatalf("channel list exited %d: %s", code, listErr.String())
	}
	var list struct {
		Channels []channelOnboardingJourneyReadback `json:"channels"`
	}
	if err := json.Unmarshal([]byte(listOut.String()), &list); err != nil {
		t.Fatalf("decode channel list: %v\n%s", err, listOut.String())
	}
	return list.Channels
}

func waitChannelOnboardingChallenge(t *testing.T, stdout, stderr *lockedBuffer, done <-chan int) string {
	t.Helper()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if challenge := channelOnboardingChallengePattern.FindString(stdout.String()); challenge != "" {
			return challenge
		}
		select {
		case code := <-done:
			t.Fatalf("channel command exited %d before challenge\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
		case <-deadline.C:
			t.Fatalf("timed out waiting for channel challenge\nstdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
		case <-ticker.C:
		}
	}
}

func waitChannelOnboardingRegistration(t *testing.T, provider *channelOnboardingTelegramProvider, stdout, stderr *lockedBuffer, done <-chan int) (string, string) {
	t.Helper()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		callbackURL, signingSecret, _ := provider.Registration()
		if callbackURL != "" && signingSecret != "" {
			return callbackURL, signingSecret
		}
		select {
		case code := <-done:
			t.Fatalf("channel command exited %d before registration\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
		case <-deadline.C:
			t.Fatalf("timed out waiting for provider registration\nstdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
		case <-ticker.C:
		}
	}
}

func waitChannelOnboardingDelivery(t *testing.T, provider *channelOnboardingTelegramProvider, index int) map[string]any {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if delivery := provider.Delivery(index); delivery != nil {
			return delivery
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for Telegram confirmation %d", index)
	return nil
}

func reserveChannelOnboardingListenAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func channelOnboardingHostWorkspaceFields() []string {
	return []string{
		"  backend: host",
		"  allow_exec_on_host: true",
	}
}

func writeChannelOnboardingPostgresRuntimeConfig(t *testing.T, dsn string) string {
	t.Helper()
	connection, err := testpostgres.ParseConnection(dsn)
	if err != nil {
		t.Fatalf("parse channel onboarding PostgreSQL DSN: %v", err)
	}
	parameters := connection.Parameters()
	t.Setenv("PGPASSWORD", parameters.Password)
	configText := fmt.Sprintf(`runtime:
  execution_posture: live
  recovery_on_startup: false
workspace:
  backend: host
  allow_exec_on_host: true
store:
  backend: postgres
database:
  host: %q
  port: %d
  name: %q
  user: %q
  password_env: PGPASSWORD
  sslmode: %q
  pool_size: 5
llm:
  backend: anthropic
  session:
    lock_ttl: 10s
    rotate_after_turns: 40
    rotate_on_parse_failures: 3
`, parameters.Host, parameters.Port, parameters.Database, parameters.User, parameters.SSLMode)
	path := filepath.Join(t.TempDir(), "swarm-postgres.yaml")
	if err := os.WriteFile(path, []byte(withTestProviderTriggerPlatformInventory(t, configText)), 0o644); err != nil {
		t.Fatalf("write channel onboarding PostgreSQL runtime config: %v", err)
	}
	return path
}

func enableChannelOnboardingRecoveryOnStartup(t *testing.T, configPath string) {
	t.Helper()
	contents, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read channel onboarding runtime config: %v", err)
	}
	updated := strings.Replace(string(contents), "  recovery_on_startup: false", "  recovery_on_startup: true", 1)
	if updated == string(contents) {
		t.Fatalf("channel onboarding runtime config does not declare recovery_on_startup: false")
	}
	if err := os.WriteFile(configPath, []byte(updated), 0o644); err != nil {
		t.Fatalf("enable channel onboarding startup recovery: %v", err)
	}
}

func disableChannelOnboardingBusinessConsumers(t *testing.T, contractsRoot string) {
	t.Helper()
	files := map[string]string{
		"package.yaml": `name: telegram-agent
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - id: bot
    path: bot
flows:
  - id: telegram-ingress
    flow: telegram-ingress
    mode: singleton
    activation: standing
    ingress:
      alias: chat
      providers:
        - provider: telegram
          signing_secret: webhook_signing.telegram
`,
		filepath.Join("bot", "package.yaml"): `name: telegram-channel-onboarding
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
provider_trigger_events:
  imports:
    - provider: telegram
      event: inbound.telegram.text_message
flows:
  - id: telegram-chat
    flow: telegram-chat
    mode: template
`,
	}
	for relative, contents := range files {
		if err := os.WriteFile(filepath.Join(contractsRoot, relative), []byte(contents), 0o644); err != nil {
			t.Fatalf("disable onboarding fixture business consumer %s: %v", relative, err)
		}
	}
	for _, relative := range []string{
		filepath.Join("bot", "flows", "telegram-chat", "agents.yaml"),
		filepath.Join("bot", "flows", "telegram-chat", "nodes.yaml"),
		filepath.Join("bot", "flows", "telegram-chat", "events.yaml"),
	} {
		if err := os.Remove(filepath.Join(contractsRoot, relative)); err != nil && !os.IsNotExist(err) {
			t.Fatalf("remove empty onboarding fixture declaration %s: %v", relative, err)
		}
	}
}

func scenariosForChannelOnboardingJourney() []servedparity.Scenario {
	return []servedparity.Scenario{
		servedparity.MustScenario(servedparity.ScenarioConnectedChannelOnboardingLifecycle),
		servedparity.MustScenario(servedparity.ScenarioConnectedChannelOnboardingRetryLifecycle),
	}
}

func TestWebhookOnboardingAdmissionIsRejectedAtActivationWithoutPublicIngress(t *testing.T) {
	if err := rejectWebhookPrebindingWithoutPublicIngress(serveChannelActivationSnapshot{}); err != nil {
		t.Fatalf("read-only catalog snapshot rejected without ingress: %v", err)
	}
	err := rejectWebhookPrebindingWithoutPublicIngress(serveChannelActivationSnapshot{Prebinding: []servePrebindingActivation{{
		Candidate: channelonboarding.Candidate{Posture: channelonboarding.ActivationWebhookRegistration},
	}}})
	terminal, ok := channelonboarding.AsTerminalActivationError(err)
	if !ok || terminal.Code != "public_ingress_unavailable" {
		t.Fatalf("webhook activation error = %#v, %v", terminal, err)
	}
}

func TestConnectedChannelRecoveryRunsWithoutPublicIngressOwner(t *testing.T) {
	order := []string{}
	teardown := &serveChannelRecoveryProbe{name: "teardown", order: &order}
	onboarding := &serveChannelRecoveryProbe{name: "onboarding", order: &order}
	activate := func() error {
		order = append(order, "runtime_activation")
		return nil
	}
	if err := activateServeAfterConnectedChannelTeardownRecovery(context.Background(), teardown, activate, onboarding); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(order, ","); got != "teardown,runtime_activation,onboarding" {
		t.Fatalf("recovery order = %s, want teardown,runtime_activation,onboarding", got)
	}
}

func TestConnectedChannelLocalRecoveryFailureBlocksChannelPublication(t *testing.T) {
	order := []string{}
	teardown := &serveChannelRecoveryProbe{name: "teardown", order: &order}
	onboarding := &serveChannelRecoveryProbe{name: "onboarding", order: &order, localErr: fmt.Errorf("stale onboarding responsibility")}
	activated := false
	err := activateServeAfterConnectedChannelTeardownRecovery(context.Background(), teardown, func() error {
		activated = true
		return nil
	}, onboarding)
	if err == nil || !strings.Contains(err.Error(), "stale onboarding responsibility") {
		t.Fatalf("local recovery error = %v", err)
	}
	if !activated || strings.Join(order, ",") != "teardown,onboarding" {
		t.Fatalf("local recovery did not run against the loaded runtime context: activated=%v order=%v", activated, order)
	}
}

func TestServeRebindsChannelRecoveryAfterRuntimeContextLoadBeforeChannelPublication(t *testing.T) {
	raw, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	teardownBarrier := strings.Index(source, "activateServeAfterConnectedChannelTeardownRecovery(ctx, channelDestructive")
	apiServing := strings.Index(source, "apiServerLease, err := processWorkOwner.Begin(ctx)")
	mcpServing := strings.Index(source, "mcpServerLease, err := processWorkOwner.Begin(ctx)")
	runtimeActivation := strings.Index(source, "if err := startServeRuntimeContexts(ctx, runtimeContexts, runtimeContextManager)")
	localReconciliation := strings.Index(source, "}, channelOnboarding); err != nil")
	channelPublication := strings.Index(source, "if err := channelActivationRefresher.publishChannelActivations(ctx)")
	effectRecovery := strings.Index(source, "if err := channelOnboarding.Recover(ctx)")
	if teardownBarrier < 0 || apiServing < 0 || mcpServing < 0 || runtimeActivation < 0 || localReconciliation < 0 || channelPublication < 0 || effectRecovery < 0 {
		t.Fatalf("serve startup lifecycle markers missing: teardown=%d api=%d mcp=%d runtime=%d local=%d publication=%d recovery=%d", teardownBarrier, apiServing, mcpServing, runtimeActivation, localReconciliation, channelPublication, effectRecovery)
	}
	if !(teardownBarrier < apiServing && apiServing < mcpServing && mcpServing < runtimeActivation && runtimeActivation < localReconciliation && localReconciliation < channelPublication && channelPublication < effectRecovery) {
		t.Fatalf("serve startup order teardown=%d api=%d mcp=%d runtime=%d local=%d publication=%d recovery=%d", teardownBarrier, apiServing, mcpServing, runtimeActivation, localReconciliation, channelPublication, effectRecovery)
	}
}

type serveChannelRecoveryProbe struct {
	name       string
	order      *[]string
	recoverErr error
	localErr   error
}

func (p *serveChannelRecoveryProbe) Recover(context.Context) error {
	*p.order = append(*p.order, p.name)
	return p.recoverErr
}

func (p *serveChannelRecoveryProbe) ReconcileLocal(context.Context) error {
	*p.order = append(*p.order, p.name)
	return p.localErr
}
