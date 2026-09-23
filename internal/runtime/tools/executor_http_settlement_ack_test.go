package tools_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type httpSettlementSelectedStore interface {
	storetest.AgentFixtureStore
	runtimeeffects.Store
	runtimeeffects.OutcomeStore
	runtimerunlifecycle.CandidateRegistrar
}

type failingHTTPCompletionSink struct {
	err     error
	submits int
}

func (s *failingHTTPCompletionSink) ReserveCompletionCandidate(context.Context) (runtimerunlifecycle.CandidateAdmission, error) {
	return s, nil
}

func (s *failingHTTPCompletionSink) Submit(runtimerunlifecycle.Candidate) error {
	s.submits++
	return s.err
}

func (*failingHTTPCompletionSink) Cancel() error { return nil }

type refusingHTTPSettlementStore struct {
	runtimeeffects.Store
	err error
}

func (s refusingHTTPSettlementStore) SettleExternalAttempt(context.Context, runtimeeffects.Settlement) error {
	return s.err
}

func selectedHTTPSettlementContext(t *testing.T, selected httpSettlementSelectedStore, identity string) (context.Context, models.AgentConfig) {
	t.Helper()
	harness := effecttest.New()
	intent, err := runtimeagentintent.Resolve(runtimeagentintent.SourceInline, "inline", "agents.yaml#agents.effect-test-agent.intent", "Exercise HTTP effect settlement.")
	if err != nil {
		t.Fatal(err)
	}
	prompt, err := runtimeagentintent.IntentOnlyPrompt(intent)
	if err != nil {
		t.Fatal(err)
	}
	actor := models.AgentConfig{
		ID: harness.Token.AgentID, Identity: harness.Token.Identity, Role: "tester",
		FlowID: "effect-test", FlowPath: harness.Token.Identity.FlowInstance(), Type: "stub", Model: "regular",
		ResolvedLLMBackend: "anthropic", ExecutionMode: "live", Intent: intent, Prompt: prompt, Config: []byte(`{}`),
		Tools: []string{"settlement-proof"}, Permissions: []string{"settlement-proof"},
	}
	runtimeID := uuid.NewString()
	fixtureCtx := runtimecorrelation.WithRuntimeInstanceID(context.Background(), runtimeID)
	fixtureCtx = runtimeauthoractivity.WithScope(fixtureCtx, runtimeauthoractivity.BundleScope(runtimeID, sourceartifactfixture.BundleHash))
	if err := storetest.UpsertStaticAgentFixture(t, fixtureCtx, selected, runtimemanager.PersistedAgent{
		Config: actor, Status: "active", HiredBy: "http-settlement-test", StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed current agent: %v", err)
	}
	lifecycle, found, err := selected.LoadAgentLifecycleState(context.Background(), harness.Token.Identity)
	if err != nil || !found || lifecycle.Phase != runtimemanager.AgentLifecycleRunning {
		t.Fatalf("current agent lifecycle: %+v found=%v err=%v", lifecycle, found, err)
	}
	harness.Token.RuntimeEpoch = lifecycle.RuntimeEpoch
	harness.Token.Generation = lifecycle.Generation
	ctx := harness.CompletionContext(identity)
	ctx = runtimeeffects.WithController(ctx, runtimeeffects.NewController(selected).WithExecutionPosture(executionposture.Live))
	ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, runtimeID)
	ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(runtimeID, sourceartifactfixture.BundleHash))
	return runtimetools.WithActor(ctx, actor), actor
}

func settledHTTPToolSource(url string) semanticview.Source {
	output := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject,
		runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
			"receipt": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaString),
		}), runtimecontracts.ToolSchemaRequired("receipt"))
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{
		"settlement-proof": runtimecontracts.MustToolSchemaEntry(
			runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
			runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), output),
			runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: http.MethodPost, URL: url}),
			runtimecontracts.WithToolResponseSuccess(runtimecontracts.HTTPResponseSuccess{Kind: "json_field_equals", Path: "response.body.ok", Equals: true}),
			runtimecontracts.WithToolResponseMapping(map[string]any{"receipt": "{{response.body.receipt}}"}),
		),
	}})
}

func requireHTTPSettlementOutcome(t *testing.T, selected httpSettlementSelectedStore, operationID string, state runtimeeffects.State, count int) {
	t.Helper()
	db := storetest.DatabaseForTest(selected)
	var actual int
	if err := db.QueryRow(`SELECT COUNT(*) FROM runtime_external_effect_attempts`).Scan(&actual); err != nil || actual != count {
		t.Fatalf("effect attempt count=%d err=%v, want %d", actual, err, count)
	}
	outcome, found, err := selected.GetExternalEffectOutcome(context.Background(), operationID)
	if err != nil || !found || outcome.State != state || outcome.AttemptState != state || outcome.OperationID != operationID {
		t.Fatalf("durable effect outcome=%+v found=%v err=%v", outcome, found, err)
	}
}

func TestHTTPToolAcknowledgedSettlementCleanupPreservesResponseBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected httpSettlementSelectedStore
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStore(t)
			} else {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected = storetest.AdmitPostgresRuntimeStore(t, db)
			}
			ctx, _ := selectedHTTPSettlementContext(t, selected, "http-tool-settlement-ack")
			fault := errors.New("injected post-commit completion handoff failure")
			sink := &failingHTTPCompletionSink{err: fault}
			registration, err := selected.RegisterCompletionCandidateSink(ctx, runtimerunlifecycle.CandidateScope{BundleHash: sourceartifactfixture.BundleHash}, sink)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(registration.Release)
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"ok":true,"receipt":"provider-42"}`))
			}))
			t.Cleanup(server.Close)
			bus := &humanTaskRuntimeLogBus{}
			executor := runtimetools.NewExecutorWithOptions(bus, runtimetools.ExecutorOptions{WorkflowSource: settledHTTPToolSource(server.URL)})
			out, err := executor.Execute(ctx, "settlement-proof", map[string]any{})
			response, ok := out.(map[string]any)
			if err != nil || !ok || len(response) != 1 || response["receipt"] != "provider-42" || calls.Load() != 1 || sink.submits != 1 {
				t.Fatalf("acknowledged HTTP result=%#v err=%v calls=%d handoffs=%d", out, err, calls.Load(), sink.submits)
			}
			var operationID string
			if err := storetest.DatabaseForTest(selected).QueryRow(`SELECT CAST(operation_id AS TEXT) FROM runtime_external_effect_attempts`).Scan(&operationID); err != nil {
				t.Fatal(err)
			}
			requireHTTPSettlementOutcome(t, selected, operationID, runtimeeffects.StateSettled, 1)
			var diagnosticFound bool
			for _, entry := range bus.logs {
				if entry.Action != "http_tool_settlement_post_commit_failure" {
					continue
				}
				detail, ok := entry.Detail.(map[string]any)
				if !ok || detail["post_commit_error"] == nil || !strings.Contains(detail["post_commit_error"].(string), fault.Error()) || detail["operation_id"] != operationID || detail["attempt_id"] == "" {
					t.Fatalf("settlement diagnostic=%+v", entry)
				}
				diagnosticFound = true
			}
			if !diagnosticFound {
				t.Fatalf("missing internal settlement diagnostic: %+v", bus.logs)
			}
			if replayed, replayErr := executor.Execute(ctx, "settlement-proof", map[string]any{}); replayed != nil || replayErr == nil || calls.Load() != 1 || sink.submits != 1 {
				t.Fatalf("same-operation replay=%#v err=%v calls=%d handoffs=%d", replayed, replayErr, calls.Load(), sink.submits)
			}
			requireHTTPSettlementOutcome(t, selected, operationID, runtimeeffects.StateSettled, 1)

			unack := errors.New("injected unacknowledged settlement refusal")
			unackCtx := runtimeeffects.WithLogicalOperationIdentity(ctx, "http-tool-settlement-unack")
			unackCtx = runtimeeffects.WithController(unackCtx, runtimeeffects.NewController(refusingHTTPSettlementStore{Store: selected, err: unack}).WithExecutionPosture(executionposture.Live))
			unackOut, unackErr := executor.Execute(unackCtx, "settlement-proof", map[string]any{})
			if unackOut != nil || unackErr == nil || !errors.Is(unackErr, unack) || calls.Load() != 2 || sink.submits != 1 {
				t.Fatalf("unacknowledged settlement=%#v err=%v calls=%d handoffs=%d", unackOut, unackErr, calls.Load(), sink.submits)
			}
			requireHTTPSettlementOutcome(t, selected, operationID, runtimeeffects.StateSettled, 2)
			var settledCount int
			if err := storetest.DatabaseForTest(selected).QueryRow(`SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE state='settled'`).Scan(&settledCount); err != nil || settledCount != 1 {
				t.Fatalf("settled attempt count=%d err=%v, want only acknowledged settlement", settledCount, err)
			}
			for _, entry := range bus.logs {
				if entry.Action == "http_tool_settlement_post_commit_failure" && entry.Detail.(map[string]any)["operation_id"] != operationID {
					t.Fatalf("unacknowledged settlement reported as committed: %+v", entry)
				}
			}
		})
	}
}
