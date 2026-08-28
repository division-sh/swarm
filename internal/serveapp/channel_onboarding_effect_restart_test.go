package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestChannelOnboardingAdmittedEffectRestart(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		backend := backend
		t.Run(string(backend), func(t *testing.T) {
			t.Run("provider_registration", func(t *testing.T) {
				runChannelOnboardingAdmittedEffectRestart(t, backend, "activating_provider")
			})
			t.Run("operator_confirmation", func(t *testing.T) {
				runChannelOnboardingAdmittedEffectRestart(t, backend, "delivering_confirmation")
			})
		})
	}
}

func runChannelOnboardingAdmittedEffectRestart(t *testing.T, backend servedparity.Backend, phase string) {
	t.Helper()
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

	opts := cliapp.ServeOptions{
		ContractsPath: contractsRoot, PlatformSpecPath: defaultPlatformSpecPath,
		APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		PublicWebhookBaseURL: "https://hooks.channel-onboarding.test", PublicWebhookListen: publicListen,
		SelfCheck: true, RequireBundleMatch: false, AbandonActiveRuns: true, Verbose: true,
		WorkspaceBackend: "host", WorkspaceBackendSet: true,
	}
	switch backend {
	case servedparity.BackendDefaultSQLite:
		sqlitePath := filepath.Join(t.TempDir(), "channel-effect-restart.sqlite")
		opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", sqlitePath, channelOnboardingHostWorkspaceFields())
		opts.StoreMode = "sqlite"
		opts.StoreModeSet = true
		enableChannelOnboardingRecoveryOnStartup(t, opts.ConfigPath)
	case servedparity.BackendExplicitPostgres:
		dsn, _, _ := testutil.StartPostgres(t)
		opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, dsn)
		opts.StoreMode = "postgres"
		opts.StoreModeSet = true
		enableChannelOnboardingRecoveryOnStartup(t, opts.ConfigPath)
	default:
		t.Fatalf("unsupported backend %q", backend)
	}
	process := startChannelOnboardingCrashServeProcess(t, opts, telegram.URL)
	endpoint := process.endpoint(t)
	var arrived <-chan struct{}
	var release func()
	if phase == "activating_provider" {
		arrived, release = provider.PauseNextRegistrationResponse()
		t.Cleanup(release)
	}
	command := startChannelOnboardingEffectRestartCommand(t, opts.ConfigPath, endpoint, "effect-restart-token")

	if phase != "activating_provider" {
		challenge := waitChannelOnboardingChallenge(t, command.stdout, command.stderr, command.done)
		callbackURL, signingSecret := waitChannelOnboardingRegistration(t, provider, command.stdout, command.stderr, command.done)
		arrived, release = provider.PauseNextDeliveryResponse()
		t.Cleanup(release)
		_ = submitChannelOnboardingRestartClaim(t, callbackURL, signingSecret, challenge)
	}
	waitChannelOnboardingProviderBarrier(t, arrived, phase)
	predecessor := requireChannelOnboardingOperationPhase(t, opts.ConfigPath, endpoint, phase)

	if err := process.kill(); err != nil {
		t.Fatalf("kill served process at %s: %v", phase, err)
	}
	release()
	select {
	case code := <-command.done:
		if code == 0 {
			t.Fatalf("channel command succeeded across forced %s process loss\nstdout:\n%s\nstderr:\n%s", phase, command.stdout.String(), command.stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("channel command did not settle after forced %s process loss", phase)
	}

	process = startChannelOnboardingCrashServeProcess(t, opts, telegram.URL)
	endpoint = process.endpoint(t)
	restarted := requireChannelOnboardingOperationPhase(t, opts.ConfigPath, endpoint, phase)
	if restarted.Operation.OperationID != predecessor.Operation.OperationID || restarted.Operation.Coordinate != predecessor.Operation.Coordinate {
		t.Fatalf("%s restart rebound admitted effect operation: before=%#v after=%#v", phase, predecessor.Operation, restarted.Operation)
	}
	registrations, deliveries := provider.Counts()
	wantDeliveries := 0
	if phase == "delivering_confirmation" {
		wantDeliveries = 1
	}
	if registrations != 1 || deliveries != wantDeliveries {
		t.Fatalf("%s restart replayed provider effect: registrations=%d deliveries=%d, want 1/%d", phase, registrations, deliveries, wantDeliveries)
	}
	if err := process.stop(); err != nil {
		t.Fatalf("stop restarted %s process: %v", phase, err)
	}
}

type channelOnboardingEffectRestartCommand struct {
	stdout *lockedBuffer
	stderr *lockedBuffer
	done   chan int
}

func startChannelOnboardingEffectRestartCommand(t *testing.T, configPath, endpoint, credential string) channelOnboardingEffectRestartCommand {
	t.Helper()
	stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
	done := make(chan int, 1)
	priorStdin := os.Stdin
	input, err := os.CreateTemp(t.TempDir(), "channel-effect-restart-input-*")
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
		done <- executeCLI(context.Background(), []string{
			"--config", configPath, "channel", "connect", "telegram", "--yes", "--api-server", endpoint,
		}, stdout, stderr, nil)
	}()
	return channelOnboardingEffectRestartCommand{stdout: stdout, stderr: stderr, done: done}
}

func submitChannelOnboardingRestartClaim(t *testing.T, callbackURL, signingSecret, challenge string) <-chan error {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"update_id": time.Now().UnixNano(),
		"message": map[string]any{
			"message_id": 1,
			"from":       map[string]any{"id": 7001, "username": "effect_restart_operator"},
			"chat":       map[string]any{"id": int64(9001), "type": "private"},
			"text":       challenge,
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
	done := make(chan error, 1)
	go func() {
		response, requestErr := http.DefaultClient.Do(request)
		if response != nil {
			_ = response.Body.Close()
		}
		done <- requestErr
	}()
	return done
}

func waitChannelOnboardingProviderBarrier(t *testing.T, arrived <-chan struct{}, phase string) {
	t.Helper()
	select {
	case <-arrived:
	case <-time.After(15 * time.Second):
		t.Fatalf("provider request did not reach admitted %s boundary", phase)
	}
}

func requireChannelOnboardingOperationPhase(t *testing.T, configPath, endpoint, phase string) channelOnboardingJourneyReadback {
	t.Helper()
	rows := readChannelOnboardingRows(t, configPath, endpoint)
	for _, row := range rows {
		if row.Operation != nil && row.Operation.Phase == phase {
			if row.Operation.OperationID == "" || row.Operation.Coordinate.BundleHash == "" || row.Operation.Coordinate.RuntimeInstanceID == "" ||
				row.Operation.Coordinate.ContextPublicationGeneration == 0 || row.Operation.Coordinate.TargetGeneration == 0 {
				t.Fatalf("%s operation lacks exact predecessor coordinate: %#v", phase, row.Operation)
			}
			return row
		}
	}
	t.Fatalf("channel list has no %s operation: %s", phase, fmt.Sprint(rows))
	return channelOnboardingJourneyReadback{}
}
