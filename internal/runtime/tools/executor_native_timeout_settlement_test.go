package tools_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/llm"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

type nativeTimeoutFallbackRuntime struct{ llm.NoopRuntime }

func (nativeTimeoutFallbackRuntime) ProviderContract() llm.ProviderContract {
	contract := llm.AnthropicAPIProviderContract()
	contract.NativeTools.StrictProviderNativeSupport = false
	contract.NativeTools.FallbackToolsAllowed = true
	return contract
}

type nativeTimeoutRuntimeResolver struct{}

func (nativeTimeoutRuntimeResolver) ResolveAgentRuntime(actor models.AgentConfig) (llm.AgentRuntimeResolution, error) {
	return llm.AgentRuntimeResolution{Actor: actor, Runtime: nativeTimeoutFallbackRuntime{}}, nil
}

func nativeTimeoutSelectedSource(url, flowID string) semanticview.Source {
	policy := runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
		"web_search_provider": {Value: map[string]any{
			"provider": "custom", "response_path": "items",
			"field_mapping": map[string]any{"title": "title", "url": "link", "snippet": "summary"},
			"http":          map[string]any{"method": "GET", "url": url, "timeout_seconds": 1},
		}},
	}}
	flow := &runtimecontracts.FlowContractView{Path: flowID, Paths: runtimecontracts.FlowContractPaths{FlowPath: flowID}, Policy: policy}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: policy, FlowTree: runtimecontracts.FlowTree{Root: flow, ByID: map[string]*runtimecontracts.FlowContractView{flowID: flow}, ByPath: map[string]*runtimecontracts.FlowContractView{flowID: flow}},
	})
}

type nativeTimeoutStaleLaunchStore struct{ runtimeeffects.Store }

func (s nativeTimeoutStaleLaunchStore) MarkExternalAttemptLaunched(ctx context.Context, attempt runtimeeffects.Attempt, at time.Time) (runtimeeffects.ExternalAttemptLaunch, error) {
	// Exercise the native launch fence after real authorization, not a fake
	// launch error. The admitted attempt remains unchanged in storage.
	attempt.Authority.Normal.Generation++
	attempt.Token.Generation++
	return s.Store.MarkExternalAttemptLaunched(ctx, attempt, at)
}

func TestNativeWebSearchCustomTimeoutSettlementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, phase := range []string{"success", "timeout", "prelaunch_cancel", "stale_launch", "acknowledged_cleanup", "unacknowledged_settlement"} {
				t.Run(phase, func(t *testing.T) { proveNativeTimeoutSelectedSettlement(t, backend, phase) })
			}
		})
	}
}

func proveNativeTimeoutSelectedSettlement(t *testing.T, backend, phase string) {
	t.Helper()
	var selected httpSettlementSelectedStore
	if backend == "sqlite" {
		selected = storetest.StartSQLiteRuntimeStore(t)
	} else {
		native, _ := storetest.StartPostgresRuntimeStoreWithReopen(t)
		selected = native
	}
	ctx, actor := selectedHTTPSettlementContext(t, selected, "native-search-"+phase)
	actor.NativeTools.WebSearch = true
	ctx = runtimetools.WithActor(ctx, actor)
	fault := errors.New("injected native settlement diagnostic")
	sink := &failingHTTPCompletionSink{err: fault}
	if phase == "acknowledged_cleanup" {
		registration, err := selected.RegisterCompletionCandidateSink(ctx, runtimerunlifecycle.CandidateScope{BundleHash: sourceartifactfixture.BundleHash}, sink)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(registration.Release)
	}
	if phase == "stale_launch" {
		ctx = runtimeeffects.WithController(ctx, runtimeeffects.NewController(nativeTimeoutStaleLaunchStore{Store: selected}).WithExecutionPosture(executionposture.Live))
	}
	if phase == "unacknowledged_settlement" {
		ctx = runtimeeffects.WithController(ctx, runtimeeffects.NewController(refusingHTTPSettlementStore{Store: selected, err: fault}).WithExecutionPosture(executionposture.Live))
	}
	if phase == "prelaunch_cancel" {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		ctx = canceled
	}
	var calls atomic.Int32
	joined := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		defer func() { joined <- struct{}{} }()
		if phase == "timeout" {
			timer := time.NewTimer(2 * time.Second)
			defer timer.Stop()
			select {
			case <-req.Context().Done():
				return
			case <-timer.C:
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"items":[{"title":"Swarm","link":"https://swarm.test/","summary":"A result"}]}`))
	}))
	defer server.Close()
	executor := runtimetools.NewExecutorWithOptions(nil, runtimetools.ExecutorOptions{
		WorkflowSource: nativeTimeoutSelectedSource(server.URL, actor.FlowID), ModelRuntimes: nativeTimeoutRuntimeResolver{},
	})
	started := time.Now()
	out, err := executor.Execute(ctx, "web_search", map[string]any{"query": "swarm"})
	t.Logf("native selected phase=%s elapsed=%s requests=%d output=%#v err=%v", phase, time.Since(started), calls.Load(), out, err)
	if calls.Load() != 0 {
		<-joined
	}
	state := runtimeeffects.StateSettled
	switch phase {
	case "success":
		want := map[string]any{"results": []map[string]any{{"title": "Swarm", "url": "https://swarm.test/", "snippet": "A result"}}}
		if err != nil || !reflect.DeepEqual(out, want) {
			t.Fatalf("selected native result=%#v err=%v", out, err)
		}
	case "timeout":
		state = runtimeeffects.StateOutcomeUncertain
		failure, ok := failures.As(err)
		if out != nil || !ok || failure.Failure.Class != failures.ClassOutcomeUncertain || failure.Failure.Detail.Code != "native_web_search_outcome_unconfirmed" ||
			failure.Failure.Component != "tool-executor" || failure.Failure.Operation != "web_search" || failure.Failure.Detail.Attributes["stage"] != "transport" || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("selected native timeout=%#v/%v", out, err)
		}
	case "prelaunch_cancel":
		rows, readErr := storetest.ReadExternalAttemptStorage(context.Background(), selected)
		if out != nil || !errors.Is(err, context.Canceled) || calls.Load() != 0 || readErr != nil || len(rows) != 0 {
			t.Fatalf("prelaunch cancel=%#v/%v calls=%d rows=%+v read=%v", out, err, calls.Load(), rows, readErr)
		}
		return
	case "stale_launch":
		rows, readErr := storetest.ReadExternalAttemptStorage(context.Background(), selected)
		failure, ok := failures.As(err)
		if out != nil || !ok || failure.Failure.Class != failures.ClassSupersededGeneration || calls.Load() != 0 || readErr != nil || len(rows) != 1 || rows[0].State != string(runtimeeffects.StateAuthorized) {
			t.Fatalf("native launch fence=%#v/%v calls=%d rows=%+v read=%v", out, err, calls.Load(), rows, readErr)
		}
		return
	case "acknowledged_cleanup":
		var committed *runtimeeffects.PostCommitMutationError
		if out != nil || !errors.Is(err, fault) || !errors.As(err, &committed) || committed.Phase != runtimeeffects.MutationSettlement || sink.submits != 1 {
			t.Fatalf("native acknowledged settlement=%#v/%v handoffs=%d", out, err, sink.submits)
		}
	case "unacknowledged_settlement":
		state = runtimeeffects.StateLaunched
		if out != nil || !errors.Is(err, fault) || sink.submits != 0 {
			t.Fatalf("native unacknowledged settlement=%#v/%v handoffs=%d", out, err, sink.submits)
		}
	}
	rows, readErr := storetest.ReadExternalAttemptStorage(context.Background(), selected)
	if readErr != nil || len(rows) != 1 || rows[0].State != string(state) || calls.Load() != 1 {
		t.Fatalf("native persisted attempts=%+v err=%v calls=%d state=%s", rows, readErr, calls.Load(), state)
	}
	requireHTTPSettlementOutcome(t, selected, rows[0].OperationID, state, 1)
	if replay, replayErr := executor.Execute(context.WithoutCancel(ctx), "web_search", map[string]any{"query": "swarm"}); replay != nil || replayErr == nil || calls.Load() != 1 {
		t.Fatalf("native replay=%#v/%v calls=%d", replay, replayErr, calls.Load())
	}
	requireHTTPSettlementOutcome(t, selected, rows[0].OperationID, state, 1)
}
