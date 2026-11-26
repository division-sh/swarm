package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
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
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/tests/support/testutil"
	"github.com/division-sh/swarm/tests/support/testutil/llmapi"
)

const ordinaryRetryChildEnv = "SWARM_TEST_ORDINARY_RETRY_CHILD"

type ordinaryRetryRequest struct {
	Root, Config, Source, Backend string
	Target                        uncalledAgentClaim
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
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			unsetStoreSelectorEnv(t)
			const profile, key = "openai_compatible", "restart-key"
			provider := llmapi.New(t, llmapi.OpenAICompatible, "/v1/chat/completions", key, func(req llmapi.Request) (llmapi.Reply, error) {
				if req.Number != 1 {
					return llmapi.Reply{}, fmt.Errorf("unexpected provider request %d", req.Number)
				}
				return llmapi.Reply{StatusCode: 429, ErrorBody: `{"error":{"message":"rate limited"}}`}, nil
			})
			root := t.TempDir()
			location := filepath.Join(root, "retry.sqlite")
			configPath := writeStandingMockRuntimeConfig(t, backend, location)
			if backend == "postgres" {
				location = testutil.StartEmptyPostgresDSN(t)
				configPath = writeChannelOnboardingPostgresRuntimeConfig(t, location)
			}
			configureProviderResume(t, configPath, profile, provider.URL)
			credentialPath := filepath.Join(root, "credentials.json")
			t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
			secrets, err := credentials.NewFileStore(credentialPath)
			if err != nil {
				t.Fatal(err)
			}
			selection, err := llmselection.ResolveActiveBackend(profile)
			if err != nil {
				t.Fatal(err)
			}
			if err := secrets.Set(t.Context(), selection.Credential.EnvVar, key); err != nil {
				t.Fatal(err)
			}
			source := writeProviderResumeSource(t, provider.URL+"/unused-tool", false)
			req := ordinaryRetryRequest{Root: root, Config: configPath, Source: source, Backend: backend}
			first, claims := startOrdinaryRetryChild[uncalledAgentClaim](t, req)
			endpoint := first.endpoint(t) + "/v1/rpc"
			seed := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
				"event_name": "item.received", "bundle_hash": servedEventPublishFixtureBundleHash(t, source),
				"payload": map[string]any{"item_id": "ordinary-retry"}, "idempotency_key": "ordinary-retry-seed",
			})
			waitProviderResumeSeedDelivery(t, endpoint, seed.EventID)
			input := requireServedEventPublishRPCResult(t, endpoint, map[string]any{
				"event_name": "item.agent_hold", "run_id": seed.RunID, "source_event_id": seed.EventID,
				"payload": map[string]any{"note": "ordinary-retry"}, "idempotency_key": "ordinary-retry-input",
			})
			claim := awaitOrdinaryRetryChild(t, first, claims)
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
			if count := len(provider.Requests()); count != 1 {
				t.Fatalf("before kill: provider wire requests=%d, want the sole v1 request", count)
			}
			if err := first.kill(); err != nil {
				t.Fatal(err)
			}
			req.Target = claim
			second, cuts := startOrdinaryRetryChild[ordinaryRetryCut](t, req)
			after := awaitOrdinaryRetryChild(t, second, cuts)
			requireOrdinaryRetryHistory(t, after)
			if after.Claim != before.Claim || after.Attempts[0] != before.Attempts[0] {
				t.Fatal("replacement startup changed the exact v1 evidence or acquired another delivery")
			}
			prior, retained := (*before.Effects.Attempts)[0], (*after.Effects.Attempts)[0]
			if prior.AttemptID != retained.AttemptID || prior.OperationID != retained.OperationID ||
				!prior.LaunchedAt.Time.Equal(retained.LaunchedAt.Time) || !prior.ResponseObservedAt.Time.Equal(retained.ResponseObservedAt.Time) ||
				!prior.CompletedAt.Time.Equal(retained.CompletedAt.Time) || before.Turns != after.Turns {
				t.Fatal("replacement changed the retained v1 provider attempt or turn diagnostics")
			}
			if count := len(provider.Requests()); count != 1 {
				t.Fatalf("after startup lifecycle: provider wire requests=%d, want the sole v1 request", count)
			}
			t.Logf("after PrepareLifecycle, before PrepareStart: same v%d status=%s closure=%s reason=%q retry_count=%d v1=%s/%s unchanged_terminal_attempt=%s/%s provider_attempts=%d projected=%d consumed=%d diagnostic_turns=%d wire_requests=1", after.Claim.Version, after.Status, after.Attempts[1].Closure, after.Reason, after.Retries, after.Attempts[0].Outcome, after.Attempts[0].Reason, retained.AttemptState, retained.OperationState, after.Effects.ProviderAttempts, after.Effects.Projected, after.Effects.ResponseConsumed, after.Turns)
			if after.Status != deliverylifecycle.StatusDeadLetter || after.Reason != "retry_exhausted" || after.Attempts[1].Closure != "settled" || after.Attempts[1].Outcome != "dead_letter" {
				t.Fatalf("startup did not settle the exhausted never-called retry: status=%s reason=%q v2=%s/%s", after.Status, after.Reason, after.Attempts[1].Closure, after.Attempts[1].Outcome)
			}
			delivery, letter := awaitExhaustedRetryClaim(t, second.endpoint(t)+"/v1/rpc", claim)
			if delivery.Status != string(deliverylifecycle.StatusDeadLetter) || delivery.RetryCount != 1 ||
				delivery.ReasonCode != "retry_exhausted" || delivery.Failure == nil ||
				delivery.Failure.Class != runtimefailures.ClassRetryExhausted ||
				delivery.Failure.Detail.Code != "delivery_retry_exhausted" || letter.ClaimVersion != 2 {
				t.Fatalf("wrong public exhaustion result: delivery=%+v dead_letter_claim=%d", delivery, letter.ClaimVersion)
			}
			requireExhaustedRetryHistory(t, after.Attempts[0], after.Attempts[1], delivery, letter)
			if err := second.stop(); err != nil {
				t.Fatal(err)
			}
			const notice = "runtime.warn component=startup-recovery message=Agent turns dead-lettered at restart at their retry limit, after a failed attempt and before the retry made a provider call: 1; they are not run again. List dead-lettered deliveries with swarm event list --delivery-status dead_letter"
			if output := second.output.String(); strings.Count(output, notice+"\n") != 1 ||
				strings.Contains(output, "Agent turns dead-lettered at restart before any provider call:") ||
				strings.Contains(output, "before the interrupted call was issued again:") ||
				strings.Contains(output, "attempts abandoned before launch") {
				t.Fatalf("startup did not report the exhausted retry once and separately:\n%s", output)
			}
			if count := len(provider.Requests()); count != 1 {
				t.Fatalf("after dispatch release: provider wire requests=%d, want the sole v1 request", count)
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
	if err != nil || failure.Class != runtimefailures.ClassConnectorFailure || failure.Detail.Code != "provider_http_status" ||
		failure.Component != "llm-provider" || failure.Operation != "http_status" || !failure.Retryable ||
		failure.Detail.Attributes["provider"] != "openai_compatible" || failure.Detail.Attributes["status"] != json.Number("429") {
		t.Fatalf("v1 did not retain retryable HTTP 429: class=%s code=%s component=%s operation=%s status=%v retryable=%t decode=%v",
			failure.Class, failure.Detail.Code, failure.Component, failure.Operation, failure.Detail.Attributes["status"], failure.Retryable, err)
	}
	if effects.ProviderAttempts != 1 || effects.ActiveProviderAttempts != 0 || effects.Attempts == nil || len(*effects.Attempts) != 1 ||
		effects.Settled != 0 || effects.Projected != 0 || effects.ResponseConsumed != 0 {
		t.Fatalf("want one terminal attempt without resumable completion: attempts=%d active=%d settled=%d projected=%d consumed=%d",
			effects.ProviderAttempts, effects.ActiveProviderAttempts, effects.Settled, effects.Projected, effects.ResponseConsumed)
	}
	prior := (*effects.Attempts)[0]
	if prior.AttemptID == "" || prior.OperationID == "" || prior.OriginDeliveryID != cut.Claim.Delivery || prior.OriginEventID != cut.Claim.Event ||
		prior.AttemptState != "terminal_failure" || prior.OperationState != "terminal_failure" ||
		!prior.LaunchedAt.Valid || !prior.ResponseObservedAt.Valid || !prior.CompletedAt.Valid {
		t.Fatalf("v1 terminal provider evidence differs: attempt=%s operation=%s exact_delivery=%t exact_event=%t launch=%t response=%t completed=%t",
			prior.AttemptState, prior.OperationState, prior.OriginDeliveryID == cut.Claim.Delivery, prior.OriginEventID == cut.Claim.Event,
			prior.LaunchedAt.Valid, prior.ResponseObservedAt.Valid, prior.CompletedAt.Valid)
	}
	t.Logf("v1 retained retryable HTTP 429, one terminal provider attempt/operation, launch and response observed, diagnostic_turns=%d", cut.Turns)
}

func runOrdinaryRetryProofChild(t *testing.T, req ordinaryRetryRequest) {
	t.Helper()
	witness := os.NewFile(3, "ordinary-retry-witness")
	if witness == nil {
		t.Fatal("missing witness pipe")
	}
	defer witness.Close()
	configureHostWorkspaceProofLifecycle(t)
	var selected *selectedStoreOwner
	open := buildStoresForServe
	buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config, bootCheck ...*selectedStoreBootCheck) (*selectedStoreOwner, error) {
		var err error
		selected, err = open(ctx, selection, cfg, bootCheck...)
		return selected, err
	}
	ctx, stop := signal.NotifyContext(t.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	opts := cliapp.DefaultServeOptions()
	opts.SourceRoot, opts.ConfigPath, opts.StoreMode, opts.StoreModeSet = req.Source, req.Config, req.Backend, true
	opts.APIListenAddr, opts.MCPListenAddr = "127.0.0.1:0", "127.0.0.1:0"
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
				return
			}
			if err := json.NewEncoder(witness).Encode(uncalledAgentClaim{s.EventID, claim.RunID(), claim.DeliveryID(), claim.Version()}); err != nil {
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
