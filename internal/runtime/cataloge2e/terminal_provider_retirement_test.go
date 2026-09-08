package cataloge2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	flowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

type terminalProviderProbe struct {
	launched    bool
	replacement bool
	fenced      chan struct{}
	ready       chan runtimeeffects.Attempt
	release     chan struct{}
	settled     chan error
	once        sync.Once
}

type terminalManagedProvider struct {
	*scriptedLLMRuntime
	probe      *terminalProviderProbe
	controller *runtimeeffects.Controller
}

func (p *terminalManagedProvider) ContinueManagedSession(ctx context.Context, session *llm.Session, call llm.ManagedCall) (*llm.Response, error) {
	message, err := call.ProviderMessage(ctx, session)
	if err != nil {
		return nil, err
	}
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		return nil, errors.New("terminal provider requires managed surface")
	}
	observed, err := llm.ObserveAPIRequestCapabilitySurface(surface, session.Tools)
	if err != nil {
		return nil, err
	}
	actor, ok := models.ActorFromContext(ctx)
	if !ok {
		return nil, errors.New("terminal provider requires actor")
	}
	ctx = managedcapabilities.WithContext(ctx, observed)
	target := runtimeeffects.UsageTarget{Kind: runtimeeffects.UsageTargetAgentTurn, ID: observed.Authority.ID,
		RunID: observed.Authority.RunID, AgentID: observed.ActorID, AgentIdentity: observed.ActorIdentity,
		SessionID: observed.Authority.SessionID, Memory: session.Memory, FlowInstance: observed.ActorIdentity.FlowInstance(), EntityID: actor.EffectiveEntityID()}
	ctx = runtimeeffects.WithController(runtimeeffects.WithUsageTarget(ctx, target), p.controller)
	handle, err := runtimeeffects.BeginManagedCompletion(ctx, "anthropic_api", []byte(message.Content), call.Frame(), nil)
	if err != nil {
		return nil, err
	}
	if p.probe.launched {
		if err := handle.MarkLaunched(ctx); err != nil {
			return nil, err
		}
	}
	p.probe.ready <- handle.Attempt()
	if p.probe.fenced != nil {
		<-ctx.Done()
		close(p.probe.fenced)
	}
	// This is the accepted provider tail. Cancellation cannot pretend that
	// the provider returned, nor can the lifecycle join run it to completion.
	<-p.probe.release
	if !p.probe.launched {
		err := handle.MarkLaunched(context.WithoutCancel(ctx))
		if err == nil {
			p.probe.settled <- errors.New("retired attempt reached launch")
		} else {
			p.probe.settled <- nil
		}
		return nil, err
	}
	if err := handle.MarkResponseObserved(context.WithoutCancel(ctx), map[string]any{"terminal_proof": true}); err != nil {
		p.probe.settled <- err
		return nil, err
	}
	surfaceJSON, err := json.Marshal(observed)
	if err != nil {
		p.probe.settled <- err
		return nil, err
	}
	frame := call.Frame()
	settlement, err := handle.SettleCompletion(ctx, runtimeeffects.CompletionSettlement{
		Settlement: runtimeeffects.Settlement{State: runtimeeffects.StateSettled, Evidence: map[string]any{"terminal_proof": true}},
		Usage:      runtimeeffects.CompletionUsage{ResolvedModel: "terminal-test", Exactness: runtimeeffects.CompletionUsageUnavailable},
		AgentTurn: &runtimeeffects.CompletionAgentTurn{TurnID: target.ID, RunID: target.RunID, AgentID: target.AgentID,
			Identity: session.MemoryIdentity, Memory: session.Memory, SessionID: session.ID,
			FlowInstance: target.FlowInstance, EntityID: target.EntityID, ParseOK: true,
			TriggerEventID: frame.Turn.Event.ID, TriggerEventType: frame.Turn.Event.Type,
			CapabilitySurfaceID: observed.ID, CapabilitySurface: surfaceJSON},
		Spend: runtimeeffects.CompletionSpend{EntityID: target.EntityID, FlowInstance: target.FlowInstance,
			AgentID: target.AgentID, AgentIdentity: target.AgentIdentity, Model: "regular", ModelAlias: "regular",
			BackendProfile: "test", Provider: "anthropic", Transport: "process", ResolvedModel: "terminal-test", InvocationType: "agent_turn"},
		Now: time.Now().UTC(),
	})
	if err == nil && (!settlement.Committed || settlement.Disposition != runtimeeffects.CompletionSettlementDrained || !settlement.OriginSettled || (settlement.Finalization == nil) != p.probe.replacement) {
		err = fmt.Errorf("terminal provider did not settle exact drain/origin: %+v", settlement)
	}
	p.probe.settled <- err
	if err != nil {
		return nil, err
	}
	return &llm.Response{Message: llm.Message{Role: "assistant"}, CapabilitySurface: &observed}, nil
}

func TestTerminalProviderOriginSettlementBothStores(t *testing.T) {
	proveTerminalProviderOriginSettlement(t, "manager")
}

func TestActivityTerminalResultSettlesProviderSiblingBothStores(t *testing.T) {
	proveTerminalProviderOriginSettlement(t, "activity")
}

func TestTerminalDirectiveProviderOriginSettlementBothStores(t *testing.T) {
	proveTerminalProviderOriginSettlement(t, "directive")
}

func TestReplacementProviderOriginSettlementBothStores(t *testing.T) {
	proveTerminalProviderOriginSettlement(t, "replacement")
}

func proveTerminalProviderOriginSettlement(t *testing.T, consumer string) {
	t.Helper()
	activity, directive := consumer == "activity", consumer == "directive"
	replacement := consumer == "replacement"
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		for _, launched := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/launched=%t", backend, launched), func(t *testing.T) {
				probe := &terminalProviderProbe{launched: launched, ready: make(chan runtimeeffects.Attempt, 1), release: make(chan struct{}), settled: make(chan error, 1)}
				if replacement {
					probe.replacement, probe.fenced = true, make(chan struct{})
				}
				frontier := "agent"
				if activity {
					frontier = "activity"
				}
				root := selectedForkReadinessCatalogFixture(t, 1, frontier)
				var activityCalls atomic.Int32
				if activity {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						activityCalls.Add(1)
						w.Header().Set("Content-Type", "application/json")
						_, _ = w.Write([]byte(`{}`))
					}))
					defer server.Close()
					tools, err := os.ReadFile(filepath.Join("testdata", "terminal-retirement", "activity-tools.yaml"))
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(root, "tools.yaml"), []byte(strings.ReplaceAll(string(tools), "http://terminal-proof.invalid", server.URL)), 0600); err != nil {
						t.Fatal(err)
					}
				}
				h := newRuntimeHarnessWithTerminalProvider(t, root, backend, true, nil, probe)
				defer probe.once.Do(func() { close(probe.release) })
				path := "worker-flow/worker-001"
				entity := materializeCatalogSelectedForkSourceFlow(t, h, catalogRuntimeRunID, path)
				ctx := catalogRunContext(h, catalogRuntimeRunID)
				event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(path+"/worker.ready"), "cataloge2e", "", nil, 0, catalogRuntimeRunID,
					events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entity), path), eventtest.ConcreteTemplateRoutingSource("worker-flow", path, entity), time.Now().UTC())
				published := make(chan error, 1)
				const directiveKey = "terminal-provider-directive"
				if directive {
					configs := h.rt.Manager.ListAgentConfigs()
					var agentID string
					for _, cfg := range configs {
						if cfg.Identity.FlowInstance() == path {
							agentID = cfg.ID
						}
					}
					if agentID == "" {
						t.Fatal("directive target was not materialized")
					}
					go func() {
						_, err := h.rt.Manager.SendDirective(ctx, agentcontrol.SendDirectiveRequest{
							AgentID: agentID, FlowInstance: path, RunID: catalogRuntimeRunID,
							Directive: "terminal provider proof", Source: agentcontrol.DirectiveSourceV1RPC,
							ActorTokenID: "terminal-provider-proof", IdempotencyKey: directiveKey,
						})
						published <- err
					}()
				} else {
					go func() { published <- h.rt.Bus.PublishAndWait(ctx, event) }()
				}
				var attempt runtimeeffects.Attempt
				select {
				case attempt = <-probe.ready:
				case err := <-published:
					t.Fatalf("provider never admitted: %v", err)
				case <-time.After(10 * time.Second):
					t.Fatal("provider admission watchdog")
				}
				type restartResult struct {
					result agentcontrol.RestartResult
					err    error
				}
				restarted := make(chan restartResult, 1)
				if replacement {
					go func() {
						result, err := h.rt.Manager.Restart(ctx, agentcontrol.RestartRequest{RunID: catalogRuntimeRunID, AgentID: attempt.Authority.Target.AgentIdentity.AgentID(), FlowInstance: path})
						restarted <- restartResult{result, err}
					}()
					select {
					case <-probe.fenced:
					case result := <-restarted:
						t.Fatalf("replacement did not wait for accepted provider: %+v", result)
					case <-time.After(10 * time.Second):
						t.Fatal("replacement did not cancel the exact predecessor")
					}
				} else if activity {
					request := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(path+"/worker.inspect"), "cataloge2e", "", nil, 0, catalogRuntimeRunID,
						events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entity), path), eventtest.ConcreteTemplateRoutingSource("worker-flow", path, entity), time.Now().UTC())
					if err := h.rt.Bus.PublishAndWait(ctx, request); err != nil {
						t.Fatal(err)
					}
					if activityCalls.Load() != 1 {
						t.Fatalf("activity calls = %d, want 1", activityCalls.Load())
					}
				} else {
					if err := h.rt.Manager.DeactivateFlowInstanceModel(ctx, runtimepipeline.FlowInstanceDeactivationRequest{Instance: flowidentity.Stored(nil, "worker-flow", path, "worker-001", entity, ""), FinalState: "complete"}); err != nil {
						t.Fatal(err)
					}
				}
				var reader runtimemanager.AgentLifecycleStateReader
				if h.pg != nil {
					reader = h.pg
				} else {
					reader = h.sqlite
				}
				state, found, err := reader.LoadAgentLifecycleState(ctx, attempt.Authority.Target.AgentIdentity)
				want := runtimemanager.AgentLifecycleTerminated
				if launched {
					want = runtimemanager.AgentLifecycleDraining
				}
				if replacement {
					want = runtimemanager.AgentLifecycleRunning
					select {
					case result := <-restarted:
						t.Fatalf("successor published before predecessor provider settlement: %+v", result)
					default:
					}
				}
				if err != nil || !found || state.Phase != want {
					t.Fatalf("terminal provider before settlement: %+v found=%t err=%v", state, found, err)
				}
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				if err := h.rt.Manager.WaitForQuiescence(canceled); !errors.Is(err, context.Canceled) {
					t.Fatalf("join completed ahead of provider settlement: %v", err)
				}
				probe.once.Do(func() { close(probe.release) })
				if err := <-probe.settled; err != nil {
					t.Fatal(err)
				}
				publicationErr := <-published
				if publicationErr != nil && !directive {
					t.Fatalf("settle fenced originating publication: %v", publicationErr)
				}
				if replacement {
					result := <-restarted
					if result.err != nil || result.result.Generation != state.Generation {
						t.Fatalf("replacement after exact origin settled: %+v durable=%+v", result, state)
					}
				}
				if err := h.rt.Manager.WaitForQuiescence(ctx); err != nil {
					t.Fatal(err)
				}
				state, found, err = reader.LoadAgentLifecycleState(ctx, attempt.Authority.Target.AgentIdentity)
				want = runtimemanager.AgentLifecycleTerminated
				if replacement {
					want = runtimemanager.AgentLifecycleRunning
				}
				if err != nil || !found || state.Phase != want {
					t.Fatalf("terminal provider final readback: %+v found=%t err=%v", state, found, err)
				}
				var outcomes runtimeeffects.OutcomeStore
				if h.pg != nil {
					outcomes = h.pg
				} else {
					outcomes = h.sqlite
				}
				outcome, found, err := outcomes.GetExternalEffectOutcome(ctx, attempt.OperationID)
				wantAttempt := runtimeeffects.StateTerminalFailure
				wantDelivery := "dead_letter"
				if launched {
					wantAttempt, wantDelivery = runtimeeffects.StateSettled, "delivered"
				}
				if err != nil || !found || outcome.AttemptState != wantAttempt {
					t.Fatalf("exact attempt readback: %+v found=%t err=%v", outcome, found, err)
				}
				if directive {
					var directives agentcontrol.DirectiveOperationStore
					if h.pg != nil {
						directives = h.pg
					} else {
						directives = h.sqlite
					}
					op, found, err := directives.LoadDirectiveOperationByKey(ctx, agentcontrol.DirectiveOperationMethod, "terminal-provider-proof", directiveKey)
					if err != nil || !found {
						t.Fatalf("directive origin readback: found=%t err=%v", found, err)
					}
					wantState := agentcontrol.DirectiveOperationFailed
					wantCode := "provider_attempt_superseded_before_launch"
					if launched {
						wantState = agentcontrol.DirectiveOperationIndeterminate
						wantCode = "provider_attempt_drained_before_directive_completion"
					}
					if op.State != wantState || op.Failure == nil || op.Failure.Detail.Code != wantCode || publicationErr == nil {
						t.Fatalf("retired directive origin: state=%s result=%v", op.State, publicationErr)
					}
					t.Logf("expected exact retired directive result: %v", publicationErr)
					return
				}
				observed, err := catalogRunScopedOperatorEvents(h, catalogRuntimeRunID)
				if err != nil {
					t.Fatal(err)
				}
				originDeliveries := 0
				for _, item := range observed {
					if item.EventName == path+"/worker.observed" {
						t.Fatal("retired provider projected an agent output")
					}
					if item.EventName != path+"/worker.ready" {
						continue
					}
					for _, delivery := range item.Deliveries {
						if delivery.Route.AgentIdentity.IsZero() {
							continue
						}
						originDeliveries++
						if delivery.Status != wantDelivery {
							t.Fatalf("exact provider origin readback: %+v, want %s", delivery, wantDelivery)
						}
					}
				}
				if originDeliveries != 1 {
					t.Fatalf("provider origin deliveries = %d, want 1", originDeliveries)
				}
			})
		}
	}
}
