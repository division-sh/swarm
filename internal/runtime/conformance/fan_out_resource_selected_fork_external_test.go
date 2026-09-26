package conformance

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/google/uuid"
)

func selectedExternalResourceFixture(t *testing.T, backend string) *deploymentResourceFixture {
	t.Helper()
	repo := conformanceRepoRoot(t)
	root := filepath.Join(t.TempDir(), "contracts")
	if err := os.CopyFS(root, os.DirFS(filepath.Join(repo, "tests/tier7-composition/test-agent-emits-to-node"))); err != nil {
		t.Fatal(err)
	}
	schemaPath := filepath.Join(root, "schema.yaml")
	schema, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatal(err)
	}
	const outputPins = "  outputs:\n    events:\n"
	if strings.Count(string(schema), outputPins) != 1 {
		t.Fatalf("external fixture output pins changed: %s", schema)
	}
	schema = []byte(strings.Replace(string(schema), outputPins, outputPins+"      - event: task.assigned\n        sink: harness\n", 1))
	if err := os.WriteFile(schemaPath, schema, 0o600); err != nil {
		t.Fatal(err)
	}
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	f := newDeploymentResourceFixtureWithSource(t, backend, semanticview.Wrap(bundle))
	f.runtime.fanOutServing.Close()
	return f
}

func selectedExternalForkServer(t *testing.T, f *deploymentResourceFixture, owner runforkexecution.SelectedContractExecutionOwner, providerURL string) *httptest.Server {
	t.Helper()
	credentials, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "provider-credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := credentials.Set(f.ctx, llmselection.OpenAICompatibleCredentialEnv, "selected-external-test-key"); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{LLM: config.LLMConfig{
		Backend: llmselection.BackendOpenAICompatible,
		Models: llmselection.ModelAliases{llmselection.ModelAliasRegular: {
			llmselection.BackendOpenAICompatible: "gpt-selected-external-proof",
		}},
		Session: config.LLMSessionConfig{LockTTL: time.Second, RotateAfterTurns: 40, RotateOnParseFailures: 3},
	}}
	cfg.LLM.OpenAICompatible.BaseURL = providerURL
	artifacts, ok := f.selected.(runforkexecution.SourceArtifactSelectedContractSourceStore)
	if !ok {
		t.Fatalf("selected store %T lacks source artifact loader", f.selected)
	}
	availability, ok := f.selected.(apiv1.RunForkAvailabilityStore)
	if !ok {
		t.Fatalf("selected store %T lacks fork availability", f.selected)
	}
	operations, ok := f.selected.(apiv1.RunForkOperationReader)
	if !ok {
		t.Fatalf("selected store %T lacks fork operations", f.selected)
	}
	repo := conformanceRepoRoot(t)
	methods := apiv1.OperatorRunForkHandlers(apiv1.RunForkHandlerOptions{
		Now: time.Now, Availability: availability, Operations: operations,
		Executor: apiv1.SelectedContractRunForkExecutor{
			ExecuteSelectedContractRunFork: func(ctx context.Context, req runforkexecution.SelectedContractExecutionRequest) (runforkexecution.SelectedContractExecutionResult, error) {
				req.Owner = owner
				result, err := runforkexecution.ExecuteSelectedContractRunFork(ctx, req)
				if err != nil {
					t.Logf("selected external executor error: %+v materialization=%+v activation=%+v", err, result.Materialization, result.Activation)
				}
				return result, err
			},
			SourceLoader: runforkexecution.SourceArtifactSelectedContractSourceLoader{
				RepoRoot: repo, PlatformSpecPath: filepath.Join(repo, "platform-spec.yaml"), Store: artifacts,
			},
			AgentRuntime: runforkexecution.SelectedContractAgentRuntimeOptions{
				Config: cfg, ProviderCredentials: credentials, ExecutionPosture: executionposture.Live,
				ProcessCapability: f.topology.capability, QuiescenceTimeout: 15 * time.Second,
			},
		},
	})
	handler, err := apiv1.NewHandler(apiv1.Options{
		PlatformSpecPath: filepath.Join(repo, "platform-spec.yaml"), AuthTokens: []string{apiv1.DefaultLoopbackAPIToken},
		ProcessWorkOwner: conformanceTestProcessOwner(t), Handlers: methods,
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := authoractivity.WithScope(r.Context(), authoractivity.RuntimeScope(authorActivityTestRuntimeInstanceID))
		handler.ServeHTTP(w, r.WithContext(ctx))
	}))
	t.Cleanup(server.Close)
	return server
}

func TestSelectedExternalEffectFixtureControlBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := selectedExternalResourceFixture(t, backend)
			server := f.operatorServer(t)
			path := filepath.Join(t.TempDir(), "row.jsonl")
			if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			sourceRunID := startDeploymentResourceRun(t, f, server, "--data", "task.assigned="+path)
			var calls atomic.Int64
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				var input map[string]any
				if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
					t.Errorf("decode provider request: %v", err)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"model":"gpt-selected-external-proof","choices":[{"message":{"role":"assistant","content":"complete","tool_calls":[{"id":"emit-1","type":"function","function":{"name":"emit_task_completed","arguments":"{}"}}]}}],"usage":{"prompt_tokens":11,"completion_tokens":4,"total_tokens":15}}`))
			}))
			t.Cleanup(provider.Close)
			owner, recovered := deploymentForkOwner(t, f)
			if len(recovered) != 0 {
				t.Fatalf("fresh external source has selected recovery: %+v", recovered)
			}
			forkServer := selectedExternalForkServer(t, f, owner, provider.URL)
			params := map[string]any{
				"source_run_id": sourceRunID, "bundle_hash": f.runtime.sourceArtifactFact.BundleHash(),
				"allow_source_freeze": true, "idempotency_key": uuid.NewString(),
			}
			result, rpcErr := deploymentForkRPC(t, f.ctx, forkServer, params)
			if len(rpcErr) != 0 || result.ForkRunID == "" {
				t.Fatalf("selected external fixture fork: result=%+v error=%s", result, rpcErr)
			}
			if calls.Load() != 1 {
				t.Fatalf("selected external provider calls=%d want one", calls.Load())
			}
			attempts := selectedExternalAttempts(t, f, result.ForkRunID)
			if len(attempts) != 1 || attempts[0].State != effects.StateSettled {
				t.Fatalf("selected external attempts=%+v want one settled provider attempt", attempts)
			}
		})
	}
}

type selectedExternalAttempt struct {
	OperationID string
	AttemptID   string
	State       effects.State
}

func selectedExternalAttempts(t *testing.T, f *deploymentResourceFixture, runID string) []selectedExternalAttempt {
	t.Helper()
	rows, err := f.db.QueryContext(f.ctx, `SELECT CAST(o.operation_id AS TEXT),CAST(a.attempt_id AS TEXT),a.state,o.lineage FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.effect_kind='provider_turn' ORDER BY a.attempt_ordinal`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var attempts []selectedExternalAttempt
	for rows.Next() {
		var attempt selectedExternalAttempt
		var lineageJSON []byte
		if err := rows.Scan(&attempt.OperationID, &attempt.AttemptID, &attempt.State, &lineageJSON); err != nil {
			t.Fatal(err)
		}
		var lineage struct {
			RunID string `json:"run_id"`
		}
		if err := json.Unmarshal(lineageJSON, &lineage); err != nil {
			t.Fatal(err)
		}
		if lineage.RunID == runID {
			attempts = append(attempts, attempt)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return attempts
}

type selectedExternalEffectCut struct {
	effects.Store
	effects.CompletionStore
	effects.CompletionContinuationStore
	stage   string
	entered chan effects.Attempt
	cut     atomic.Bool
}

func (c *selectedExternalEffectCut) interrupt(stage string, attempt effects.Attempt) {
	if c.stage != stage || !c.cut.CompareAndSwap(false, true) {
		return
	}
	c.entered <- attempt
	if stage == "settled" {
		// A panic here is caught by AgentManager and would falsely dead-letter
		// the delivery. Interrupt at the API activation boundary instead.
		return
	}
	panic("test-only process interruption after durable external-effect " + stage)
}

func (c *selectedExternalEffectCut) AuthorizeExternalAttempt(ctx context.Context, authority effects.Authority, req effects.AuthorizeRequest) (effects.Attempt, error) {
	attempt, err := c.Store.AuthorizeExternalAttempt(ctx, authority, req)
	if err == nil && req.Kind == effects.KindProviderTurn {
		c.interrupt("authorized", attempt)
	}
	return attempt, err
}

func (c *selectedExternalEffectCut) MarkExternalAttemptResponseObserved(ctx context.Context, attempt effects.Attempt, evidence map[string]any, at time.Time) error {
	err := c.Store.MarkExternalAttemptResponseObserved(ctx, attempt, evidence, at)
	if err == nil && attempt.Kind == effects.KindProviderTurn {
		c.interrupt("response_observed", attempt)
	}
	return err
}

func (c *selectedExternalEffectCut) SettleCompletion(ctx context.Context, attempt effects.Attempt, settlement effects.CompletionSettlement) (effects.CompletionSettlementResult, error) {
	result, err := c.CompletionStore.SettleCompletion(ctx, attempt, settlement)
	if err == nil && attempt.Kind == effects.KindProviderTurn && settlement.Settlement.State == effects.StateSettled {
		c.interrupt("settled", attempt)
	}
	return result, err
}

func selectedExternalOwnerWithCut(t *testing.T, f *deploymentResourceFixture, cut *selectedExternalEffectCut, fork runforkexecution.SelectedContractForkLifecycle) runforkexecution.SelectedContractExecutionOwner {
	t.Helper()
	var owner runforkexecution.SelectedContractExecutionOwner
	var err error
	switch selected := f.selected.(type) {
	case *store.SQLiteRuntimeStore:
		if fork == nil {
			fork = selected
		}
		cut.Store, cut.CompletionStore, cut.CompletionContinuationStore = selected, selected, selected
		durable := bus.DurableDependencies{
			ReplyContext: selected, RunLifecycle: selected, DeliveryLifecycle: selected,
			FlowRoutes: selected, FlowRouteRecords: selected, FlowRouteSets: selected,
			FlowRouteTopology: selected, FlowRouteRollback: selected,
			ActiveAgents: selected, ActiveFlows: selected, TargetOwners: selected,
			PreparedEvents: selected, TargetFailureRecorder: selected,
			RunOrigins: selected, StandingRestarts: selected,
		}
		roles := manager.PersistenceRoles{
			LifecycleState: selected, LifecycleEffects: selected, LifecycleDiagnostics: selected,
			EffectsRecovery: selected, DeliveryQuiescence: selected, EventExistence: selected,
			DirectiveOperations: selected, DirectiveTargets: selected, FlowRoutes: selected,
			StandingRestarts: selected,
		}
		owner, err = runforkexecution.NewSelectedContractExecutionOwner(
			pipeline.NewWorkflowPersistence(selected), fork, selected, selected, selected,
			durable, selected.PipelineObligations(), selected, roles,
			cut, cut, selected, selected, selected, selected, selected,
			selected, selected, selected, selected, selected,
		)
	case *store.PostgresStore:
		if fork == nil {
			fork = selected
		}
		cut.Store, cut.CompletionStore, cut.CompletionContinuationStore = selected, selected, selected
		durable := bus.DurableDependencies{
			ReplyContext: selected, RunLifecycle: selected, DeliveryLifecycle: selected,
			FlowRoutes: selected, FlowRouteRecords: selected, FlowRouteSets: selected,
			FlowRouteTopology: selected, FlowRouteRollback: selected,
			ActiveAgents: selected, ActiveFlows: selected, TargetOwners: selected,
			PreparedEvents: selected, TargetFailureRecorder: selected,
			RunOrigins: selected, StandingRestarts: selected,
		}
		roles := manager.PersistenceRoles{
			LifecycleState: selected, LifecycleEffects: selected, LifecycleDiagnostics: selected,
			EffectsRecovery: selected, DeliveryQuiescence: selected, EventExistence: selected,
			DirectiveOperations: selected, DirectiveTargets: selected, FlowRoutes: selected,
			StandingRestarts: selected,
		}
		owner, err = runforkexecution.NewSelectedContractExecutionOwner(
			pipeline.NewWorkflowPersistence(selected), fork, selected, selected, selected,
			durable, selected.PipelineObligations(), selected, roles,
			cut, cut, selected, selected, selected, selected, selected,
			selected, selected, selected, selected, selected,
		)
	default:
		t.Fatalf("unsupported selected store %T", f.selected)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.BindSelectedProcess(f.ctx, conformanceTestProcessOwner(t), f.topology.capability); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.RecoverSelectedForkContexts(f.ctx, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.Live), runforkexecution.SelectedForkRecoveryEnvironment{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.RetireSelectedContexts(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return owner
}

// R5 interrupts only after the real effect owner has committed each named
// transition. No test row or synthetic operation substitutes for the provider.
func TestSelectedDeploymentExternalEffectRecoveryBothStores(t *testing.T) {
	for _, stage := range []struct {
		name          string
		wantState     effects.State
		providerCalls int64
	}{
		{"authorized", effects.StateAuthorized, 0},
		{"response_observed", effects.StateResponseObserved, 1},
		{"settled", effects.StateSettled, 1},
	} {
		for _, backend := range []string{"sqlite", "postgres"} {
			t.Run(stage.name+"/"+backend, func(t *testing.T) {
				f := selectedExternalResourceFixture(t, backend)
				server := f.operatorServer(t)
				path := filepath.Join(t.TempDir(), "row.jsonl")
				if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				sourceRunID := startDeploymentResourceRun(t, f, server, "--data", "task.assigned="+path)
				var calls atomic.Int64
				provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					var input map[string]any
					if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
						t.Errorf("decode provider request: %v", err)
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"model":"gpt-selected-external-proof","choices":[{"message":{"role":"assistant","content":"complete","tool_calls":[{"id":"emit-1","type":"function","function":{"name":"emit_task_completed","arguments":"{}"}}]}}],"usage":{"prompt_tokens":11,"completion_tokens":4,"total_tokens":15}}`))
				}))
				t.Cleanup(provider.Close)
				cut := &selectedExternalEffectCut{stage: stage.name, entered: make(chan effects.Attempt, 1)}
				var fork runforkexecution.SelectedContractForkLifecycle
				var activation *selectedDeploymentBeforeActivationCrash
				if stage.name == "settled" {
					selected, ok := f.selected.(runforkexecution.SelectedContractForkLifecycle)
					if !ok {
						t.Fatalf("selected store lacks fork lifecycle: %T", f.selected)
					}
					activation = &selectedDeploymentBeforeActivationCrash{SelectedContractForkLifecycle: selected, entered: make(chan string, 1)}
					fork = activation
				}
				owner := selectedExternalOwnerWithCut(t, f, cut, fork)
				forkServer := selectedExternalForkServer(t, f, owner, provider.URL)
				key := uuid.NewString()
				params := map[string]any{
					"source_run_id": sourceRunID, "bundle_hash": f.runtime.sourceArtifactFact.BundleHash(),
					"allow_source_freeze": true, "idempotency_key": key,
				}
				forkCtx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
				defer cancel()
				_, rpcErr, transportErr := deploymentForkRPCRequest(forkCtx, forkServer, params)
				if transportErr == nil && len(rpcErr) == 0 {
					t.Fatal("R5 interrupted effect returned fork success")
				}
				var interrupted effects.Attempt
				select {
				case interrupted = <-cut.entered:
				default:
					t.Fatalf("R5 did not reach %s effect-owner cut: rpc=%s transport=%v", stage.name, rpcErr, transportErr)
				}
				if stage.name == "settled" {
					select {
					case <-activation.entered:
					default:
						t.Fatal("R5 settled effect did not reach post-quiescence activation interruption")
					}
				}
				var childID string
				if err := f.db.QueryRowContext(f.ctx, `SELECT fork_run_id FROM run_fork_operations WHERE idempotency_key=$1`, key).Scan(&childID); err != nil {
					t.Fatal(err)
				}
				if stage.name == "settled" {
					var failed, pending int
					if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status='dead_letter'`, childID).Scan(&failed); err != nil {
						t.Fatal(err)
					}
					if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status IN ('pending','in_progress')`, childID).Scan(&pending); err != nil {
						t.Fatal(err)
					}
					if failed != 0 || pending != 0 {
						logSelectedDeploymentForkFailure(t, f.db, childID)
						t.Fatalf("R5 settled cut created incomplete delivery: failed=%d pending=%d", failed, pending)
					}
				}
				attempts := selectedExternalAttempts(t, f, childID)
				if len(attempts) != 1 || attempts[0].AttemptID != interrupted.AttemptID || attempts[0].State != stage.wantState {
					t.Fatalf("R5 %s exact durable cut: attempts=%+v interrupted=%s", stage.name, attempts, interrupted.AttemptID)
				}
				if got := calls.Load(); got != stage.providerCalls {
					t.Fatalf("R5 %s provider calls at cut=%d want %d", stage.name, got, stage.providerCalls)
				}
				if err := owner.RetireSelectedContexts(context.Background()); err != nil {
					t.Fatal(err)
				}
				oldRuntime := f.runtime
				join := beginServingLifetimeJoin(oldRuntime, nil)
				assertServingJoinComplete(t, join, oldRuntime, nil)
				if err := f.topology.capability.Release(context.Background()); err != nil {
					t.Fatal(err)
				}
				f.topology = newNotifyAllChildrenProcessTopology(t, f.ctx, f.selected, f.source)
				f.boot(t)
				f.runtime.fanOutServing.Close()
				restarted, recovered := deploymentForkOwner(t, f)
				t.Logf("R5 %s recovered=%+v attempts=%+v", stage.name, recovered, selectedExternalAttempts(t, f, childID))
				var feedCount, deliveryCount int
				if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM fan_out_intents WHERE run_id=$1 AND origin_kind='deployment'`, childID).Scan(&feedCount); err != nil {
					t.Fatal(err)
				}
				if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1`, childID).Scan(&deliveryCount); err != nil {
					t.Fatal(err)
				}
				t.Logf("R5 %s recovered feed=%d deliveries=%d", stage.name, feedCount, deliveryCount)
				restartedServer := selectedExternalForkServer(t, f, restarted, provider.URL)
				result, retryErr := deploymentForkRPC(t, f.ctx, restartedServer, params)
				t.Logf("R5 %s retry result=%+v error=%s", stage.name, result, retryErr)
				if got := calls.Load(); got != stage.providerCalls {
					t.Fatalf("R5 %s redispatched provider: calls=%d want %d", stage.name, got, stage.providerCalls)
				}
				post := selectedExternalAttempts(t, f, childID)
				if len(post) != 1 || post[0].AttemptID != interrupted.AttemptID {
					t.Fatalf("R5 %s reminted external effect attempt: %+v", stage.name, post)
				}
				switch stage.name {
				case "authorized":
					if post[0].State != effects.StateTerminalFailure || len(retryErr) == 0 {
						t.Fatalf("R5 authorized prelaunch must fail without dispatch: state=%s retry=%+v error=%s", post[0].State, result, retryErr)
					}
				case "response_observed":
					if post[0].State != effects.StateOutcomeUncertain || len(retryErr) == 0 {
						t.Fatalf("R5 observed response must report uncertainty without redispatch: state=%s retry=%+v error=%s", post[0].State, result, retryErr)
					}
				case "settled":
					if post[0].State != effects.StateSettled || len(retryErr) != 0 || result.ForkRunID != childID {
						t.Fatalf("R5 settled response must resume committed turn: state=%s retry=%+v error=%s", post[0].State, result, retryErr)
					}
				}
			})
		}
	}
}
