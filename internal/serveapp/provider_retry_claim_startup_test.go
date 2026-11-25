package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/sourceartifact"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/tests/support/testutil"
	"gopkg.in/yaml.v3"
)

const ordinaryRetryChildEnv = "SWARM_TEST_ORDINARY_RETRY_CHILD"

type ordinaryRetryRequest struct {
	Root, Config, Source, Backend, Launcher string
	Image, MCPListen                        string
	Target                                  uncalledAgentClaim
}

type ordinaryRetryCrashCut struct {
	Claim   uncalledAgentClaim
	Cleanup sourceartifact.RuntimeProjectionCleanup
}

type ordinaryRetryCut struct {
	Claim    uncalledAgentClaim
	Status   deliverylifecycle.Status
	Retries  int
	Reason   string
	Attempts []storetest.DeliveryAttemptDiagnosticRow
	Effects  storetest.ManagedTurnEffectStorage
	Turns    int
}

func TestServedOrdinaryUncalledRetrySettlesAtStartupBothStores(t *testing.T) {
	if raw := os.Getenv(ordinaryRetryChildEnv); raw != "" {
		var req ordinaryRetryRequest
		if err := json.Unmarshal([]byte(raw), &req); err != nil {
			t.Fatal(err)
		}
		runOrdinaryRetryProofChild(t, req)
		return
	}
	image := buildCompletion2270CLIImage(t)
	network, err := exec.Command("docker", "network", "inspect", workspace.DefaultWorkspaceNetwork(), "--format", "{{json .IPAM.Config}}").CombinedOutput()
	var addresses []struct{ Gateway string }
	if err != nil || json.Unmarshal(network, &addresses) != nil || len(addresses) != 1 || net.ParseIP(addresses[0].Gateway) == nil {
		t.Fatalf("inspect exact Docker proof interface: %v: %s", err, network)
	}
	mcpListen := net.JoinHostPort(addresses[0].Gateway, "0")
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			unsetStoreSelectorEnv(t)
			t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
			root := t.TempDir()
			launcher := filepath.Join(root, "docker-proof")
			installOrdinaryRetryLauncher(t, launcher)
			location := filepath.Join(root, "retry.sqlite")
			configPath := writeStandingMockRuntimeConfig(t, backend, location)
			if backend == "postgres" {
				location = testutil.StartEmptyPostgresDSN(t)
				configPath = writeChannelOnboardingPostgresRuntimeConfig(t, location)
			}
			configureOrdinaryRetryProof(t, configPath, launcher, image)
			credentialPath := filepath.Join(root, "credentials.json")
			t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
			secrets, err := credentials.NewFileStore(credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := secrets.Set(t.Context(), "CLAUDE_CODE_OAUTH_TOKEN", "ordinary-retry-fixture-token"); err != nil {
				t.Fatal(err)
			}
			source := writeServedLiveAgentFixture(t)
			req := ordinaryRetryRequest{Root: root, Config: configPath, Source: source, Backend: backend, Launcher: launcher, Image: image, MCPListen: mcpListen}
			first, claims := startOrdinaryRetryChild[ordinaryRetryCrashCut](t, req)
			endpoint := first.endpoint(t) + "/v1/rpc"
			seed := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
				"event_name": "item.received", "bundle_hash": servedEventPublishFixtureBundleHash(t, source),
				"payload": map[string]any{"item_id": "ordinary-retry"}, "idempotency_key": "ordinary-retry-seed",
			})
			waitProviderResumeSeedDelivery(t, endpoint, seed.EventID)
			input := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
				"event_name": "item.processed", "run_id": seed.RunID, "source_event_id": seed.EventID,
				"payload": map[string]any{"item_id": "ordinary-retry"}, "idempotency_key": "ordinary-retry-input",
			})
			crash := awaitOrdinaryRetryChild(t, first, claims)
			claim := crash.Claim
			if claim.Version != 2 || claim.Event != input.EventID || claim.Run != seed.RunID || claim.Delivery == "" {
				t.Fatalf("wrong v2 crash cut: %+v", claim)
			}
			var event operatorread.OperatorEventFull
			requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": claim.Event}, &event)
			before := ordinaryRetryCut{Claim: claim}
			for _, delivery := range event.Deliveries {
				if delivery.DeliveryID == claim.Delivery {
					before.Status, before.Retries = deliverylifecycle.Status(delivery.Status), delivery.RetryCount
				}
			}
			reader, err := storetest.OpenReleaseProcessReadOnlyInspection(backend, location)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reader.Close() })
			if err := reader.InspectSnapshot(t.Context(), func(ctx context.Context) error {
				return before.inspect(ctx, reader)
			}); err != nil {
				t.Fatal(err)
			}
			requireOrdinaryRetryHistory(t, before)
			if before.Status != deliverylifecycle.StatusInProgress || before.Attempts[1].Closure != "open" {
				t.Fatalf("predecessor did not hold v2 before its provider call: %s/%s", before.Status, before.Attempts[1].Closure)
			}
			if err := first.kill(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := workspace.NewDockerManager().DisposeProjectionContainers(context.Background(), crash.Cleanup); err != nil {
					t.Errorf("retire crashed fixture workspace: %v", err)
					return
				}
				if err := sourceartifact.SettleRuntimeProjectionCleanup(crash.Cleanup); err != nil {
					t.Errorf("retire crashed fixture projection: %v", err)
				}
			})
			installOrdinaryRetryLauncher(t, launcher)
			req.Target = claim
			second, cuts := startOrdinaryRetryChild[ordinaryRetryCut](t, req)
			after := awaitOrdinaryRetryChild(t, second, cuts)
			requireOrdinaryRetryHistory(t, after)
			if after.Claim != before.Claim || after.Attempts[0] != before.Attempts[0] ||
				(*after.Effects.Attempts)[0].AttemptID != (*before.Effects.Attempts)[0].AttemptID ||
				(*after.Effects.Attempts)[0].OperationID != (*before.Effects.Attempts)[0].OperationID {
				t.Fatal("replacement startup changed the exact v1 evidence or acquired another delivery")
			}
			if _, err := os.Stat(launcher + ".business"); !os.IsNotExist(err) {
				t.Fatalf("a CLI business subprocess started or its counter is unreadable: %v", err)
			}
			t.Logf("after PrepareLifecycle, before PrepareStart: same v%d status=%s closure=%s retry_count=%d v1=%s/%s effect=%s/%s attempts=%d saved_turns=%d business_calls=0", after.Claim.Version, after.Status, after.Attempts[1].Closure, after.Retries, after.Attempts[0].Outcome, after.Attempts[0].Reason, (*after.Effects.Attempts)[0].AttemptState, (*after.Effects.Attempts)[0].OperationState, after.Effects.ProviderAttempts, after.Turns)
			if after.Status != deliverylifecycle.StatusDeadLetter || after.Reason != "retry_exhausted" || after.Attempts[1].Closure != "settled" || after.Attempts[1].Outcome != "dead_letter" {
				t.Fatalf("startup did not settle the exhausted never-called retry: status=%s reason=%q v2=%s/%s", after.Status, after.Reason, after.Attempts[1].Closure, after.Attempts[1].Outcome)
			}
			second.endpoint(t)
			if err := second.stop(); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(launcher + ".business"); !os.IsNotExist(err) {
				t.Fatalf("the settled turn reached a CLI subprocess after dispatch release: %v", err)
			}
			attempts, err := storetest.ReadDeliveryAttemptDiagnosticRows(t.Context(), reader, claim.Delivery)
			if err != nil || len(attempts) != 2 || attempts[0] != after.Attempts[0] || attempts[1] != after.Attempts[1] {
				t.Fatalf("dispatch changed the two closed claims: count=%d error=%v", len(attempts), err)
			}
		})
	}
}

func (cut *ordinaryRetryCut) inspect(ctx context.Context, selected any) (err error) {
	cut.Attempts, err = storetest.ReadDeliveryAttemptDiagnosticRows(ctx, selected, cut.Claim.Delivery)
	if err != nil {
		return err
	}
	cut.Effects, err = storetest.ReadManagedTurnEffectStorage(ctx, selected, cut.Claim.Run, "")
	if err != nil {
		return err
	}
	turns, err := storetest.ReadManagedAgentTurnStorage(ctx, selected, cut.Claim.Run, "load-agent")
	cut.Turns = len(turns)
	return err
}

func requireOrdinaryRetryHistory(t *testing.T, cut ordinaryRetryCut) {
	t.Helper()
	a, effects := cut.Attempts, cut.Effects
	if cut.Retries != 1 || len(a) != 2 || a[0].Version != 1 || a[0].Closure != "settled" || a[0].Outcome != "retry_scheduled" || a[0].Reason != "handler_failure" || a[1].Version != 2 {
		t.Fatalf("wrong ordinary retry history: status=%s retry_count=%d entries=%d", cut.Status, cut.Retries, len(a))
	}
	failure, err := runtimefailures.UnmarshalEnvelope([]byte(a[0].Failure))
	if err != nil || failure.Detail.Code != "claude_cli_process_start_failed" || !failure.Retryable || failure.Detail.Attributes["launch_rejected"] != true {
		t.Fatal("v1 did not record the ordinary retryable CLI start rejection")
	}
	if effects.ProviderAttempts != 1 || effects.ActiveProviderAttempts != 0 || effects.Attempts == nil || len(*effects.Attempts) != 1 || cut.Turns != 0 {
		t.Fatalf("wrong provider history: attempts=%d active=%d saved_turns=%d", effects.ProviderAttempts, effects.ActiveProviderAttempts, cut.Turns)
	}
	effect := (*effects.Attempts)[0]
	if effect.AttemptID == "" || effect.OperationID == "" || effect.OriginDeliveryID != cut.Claim.Delivery || effect.OriginEventID != cut.Claim.Event || effect.AttemptState != "terminal_failure" || effect.OperationState != "terminal_failure" {
		t.Fatalf("v1 terminal effect not bound to the cut delivery: attempt=%s operation=%s", effect.AttemptState, effect.OperationState)
	}
}

func runOrdinaryRetryProofChild(t *testing.T, req ordinaryRetryRequest) {
	t.Helper()
	witness := os.NewFile(3, "ordinary-retry-witness")
	if witness == nil {
		t.Fatal("missing witness pipe")
	}
	defer witness.Close()
	var projection *sourceartifact.RuntimeProjection
	cliapp.ConfiguredWorkspaceLifecycleForServe = func(_ *config.Config, admitted *sourceartifact.RuntimeProjection, source semanticview.Source, _ cliapp.WorkspaceMountSources, _ cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
		projection = admitted
		manager := workspace.NewDockerManager()
		settings := workspace.DefaultDockerConfig()
		settings.WorkspaceImage, settings.SourceProjection = req.Image, admitted
		manager.SetConfig(settings)
		manager.SetSemanticSource(source)
		return manager, nil
	}
	var selected *selectedStoreOwner
	open := buildStoresForServe
	buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
		var err error
		selected, err = open(ctx, selection, cfg)
		return selected, err
	}
	ctx, stop := signal.NotifyContext(t.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	opts := cliapp.DefaultServeOptions()
	opts.SourceRoot, opts.ConfigPath, opts.StoreMode, opts.StoreModeSet = req.Source, req.Config, req.Backend, true
	opts.APIListenAddr, opts.MCPListenAddr = "127.0.0.1:0", req.MCPListen
	opts.MCPListenAddrSource = cliapp.ListenerAddressFlag
	opts.Output, opts.ErrorOutput, opts.NoColor = os.Stdout, os.Stderr, true
	opts.ShutdownGrace = 5 * time.Second
	if req.Target.Delivery == "" {
		opts.TestLifecycleProbe = issue2564H2CompletionObserver(func(ctx context.Context, s lifecycleprobe.Signal) {
			if s.Kind != lifecycleprobe.DeliveryStatusChanged || s.SubscriberType != "agent" || s.SubscriberID != "load-agent" || s.Status != "in_progress" {
				return
			}
			claim, ok := deliverylifecycle.ClaimFromContext(ctx)
			if !ok || claim.Validate() != nil || claim.SubscriberID() != s.SubscriberID {
				t.Error("in-progress signal has no exact native claim")
				return
			}
			if claim.Version() == 1 {
				// The startup probe used this launcher; now the first business start fails in exec.Cmd.Start.
				if err := os.Remove(req.Launcher); err != nil {
					t.Error(err)
				}
				return
			}
			cleanup, err := projection.CleanupIntent()
			if err != nil {
				t.Error(err)
				return
			}
			cut := ordinaryRetryCrashCut{Claim: uncalledAgentClaim{s.EventID, claim.RunID(), claim.DeliveryID(), claim.Version()}, Cleanup: cleanup}
			if err := json.NewEncoder(witness).Encode(cut); err != nil {
				t.Error(err)
			}
			<-ctx.Done()
		})
	}
	var heldOpen bool
	reconcile := func(ctx context.Context, owner runlifecycle.FlowRunOwner, contexts []serveRuntimeBundleContext) error {
		if err := reconcileServeFlowRuns(ctx, owner, contexts); err != nil || req.Target.Delivery == "" {
			return err
		}
		if len(contexts) != 1 || selected == nil {
			return errors.New("retry proof needs a single selected runtime")
		}
		rt := contexts[0].runtime
		checked, err := rt.PreflightStartupSource(ctx)
		if err != nil {
			return err
		}
		if err := rt.PrepareAuthorActivityCatalog(); err != nil {
			return err
		}
		if err := checked.PrepareLifecycle(); err != nil {
			return err
		}
		deps := selected.RuntimeDeps()
		inspector, ok := deps.EventStore.(interface {
			InspectSnapshot(context.Context, func(context.Context) error) error
		})
		if !ok {
			return errors.New("selected event owner has no inspection snapshot")
		}
		cut := ordinaryRetryCut{Claim: req.Target}
		if err := inspector.InspectSnapshot(ctx, func(ctx context.Context) error {
			row, err := deps.DeliveryStore.Snapshot(ctx, req.Target.Delivery)
			if err != nil {
				return err
			}
			cut.Claim = uncalledAgentClaim{row.EventID, row.RunID, row.DeliveryID, row.ClaimVersion}
			cut.Status, cut.Retries, cut.Reason = row.Status, row.RetryCount, row.ReasonCode
			return cut.inspect(ctx, deps.EventStore)
		}); err != nil {
			return err
		}
		if err := json.NewEncoder(witness).Encode(cut); err != nil {
			return err
		}
		if cut.Status != deliverylifecycle.StatusDeadLetter {
			heldOpen = true
			return errors.New("retry proof stopped before PrepareStart to preserve the observed claim")
		}
		return nil
	}
	code, err := runOwnedLifecycleWithFlowReconciler(ctx, req.Root, req.Root, opts,
		apiv1.AuthTokenResolution{Tokens: []string{apiv1.DefaultLoopbackAPIToken}, Explicit: true, Source: "ordinary-retry-fixture"},
		executionposture.Live, "", nil, reconcile)
	if err != nil || (!heldOpen && code != 0) || (heldOpen && code != 1) {
		t.Fatalf("ordinary-retry child exit=%d: %v", code, err)
	}
}

func startOrdinaryRetryChild[T any](t *testing.T, req ordinaryRetryRequest) (*channelOnboardingCrashServeProcess, <-chan T) {
	t.Helper()
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = read.Close(); _ = write.Close() })
	child := startServedCrashProcessBeforeReadiness(t, "TestServedOrdinaryUncalledRetrySettlesAtStartupBothStores", []string{ordinaryRetryChildEnv + "=" + string(raw)}, write)
	_ = write.Close()
	observed := make(chan T, 1)
	go func() {
		defer close(observed)
		var value T
		if err := json.NewDecoder(read).Decode(&value); err == nil {
			observed <- value
		}
	}()
	t.Cleanup(func() { _ = child.stop(); <-observed })
	return child, observed
}

func awaitOrdinaryRetryChild[T any](t *testing.T, child *channelOnboardingCrashServeProcess, observed <-chan T) T {
	t.Helper()
	select {
	case value, ok := <-observed:
		if ok {
			return value
		}
		t.Fatalf("child did not reach the retry cut:\n%s", child.output.String())
	case <-time.After(servedEventPublishLifecycleProbeWaitTimeout):
		t.Fatalf("timed out waiting for retry cut:\n%s", child.output.String())
	}
	var none T
	return none
}

func configureOrdinaryRetryProof(t *testing.T, configPath, launcher, image string) {
	t.Helper()
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	doc["runtime"] = map[string]any{"recovery_on_startup": true}
	doc["workspace"] = map[string]any{"backend": "docker", "docker_bin": launcher, "image": image}
	doc["llm"] = map[string]any{
		"backend": "claude_cli", "models": map[string]any{"regular": map[string]any{"claude_cli": "request-A"}},
		"claude_cli": map[string]any{"command": "completion-proof-cli", "output_format": "stream-json"},
		"session":    map[string]any{"lock_ttl": "10s", "rotate_on_parse_failures": 3},
	}
	raw, err = yaml.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func installOrdinaryRetryLauncher(t *testing.T, path string) {
	t.Helper()
	// The workspace owner still uses real Docker. The adapter's private launcher
	// forwards the fixture startup probe and detects any unexpected business call.
	script := strings.TrimSpace(`#!/bin/sh
for arg do
    if [ "$arg" = '--model' ]; then
        printf 'unexpected\n' >> "$0.business"
        exit 1
    fi
done
exec docker "$@"
`)
	if err := os.WriteFile(path, []byte(fmt.Sprintln(script)), 0o700); err != nil {
		t.Fatal(err)
	}
}
