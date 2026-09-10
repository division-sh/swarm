package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/registry"
	"github.com/division-sh/swarm/internal/runtime/core/values"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

func compiledAdapterSource(t *testing.T, initialTimer ...bool) *contracts.WorkflowContractBundle {
	t.Helper()
	files := map[string]string{
		"schema.yaml":   "name: adapter-proof\nstages:\n  ready: {initial: true}\n  Ready: {terminal: true}\n  working: {}\n  shared: {}\n  done: {terminal: true}\n  killed: {terminal: true}\n",
		"entities.yaml": "test_entity:\n  marker: text\n",
		"events.yaml":   "direct: {}\ninherited: {}\nrule: {}\ncomplete: {}\nself: {}\nwrite_only: {}\nunmatched: {}\nemit_only: {}\nobserved: {}\nkill: {}\nreject: {}\ndiscard: {}\nguarded: {}\nguard_observed:\n  marker: text\n",
		"nodes.yaml": `router:
  execution_type: system_node
  event_handlers:
    direct: {advances_to: done}
    inherited:
      advances_to: done
      rules:
        - {id: inherited-choice, condition: else}
    rule:
      rules:
        - {id: not-selected, condition: "false", advances_to: done}
        - {id: explicit-choice, condition: "true", advances_to: done}
        - {id: fallback, condition: else, advances_to: done}
    complete:
      data_accumulation:
        writes:
          - {target_field: marker, expression: "'completed'"}
      on_complete:
        - {id: completion-choice, condition: else, advances_to: done}
    self: {advances_to: ready}
    write_only:
      data_accumulation:
        writes:
          - {target_field: marker, expression: "'written'"}
    unmatched:
      rules:
        - {id: never, condition: "false", advances_to: done}
    emit_only: {emit: observed}
    guarded:
      guard:
        checks:
          - {id: first-pass, check: "true"}
          - {check: "true"}
      data_accumulation:
        writes:
          - {target_field: marker, expression: "'guarded'"}
      rules:
        - id: explicit-choice
          condition: "true"
          advances_to: done
          emit:
            event: guard_observed
            fields: {marker: "'guard-output'"}
    kill:
      guard: {id: kill-check, check: "false", on_fail: kill}
      advances_to: done
    reject:
      guard: {id: reject-check, check: "false", on_fail: reject}
      advances_to: done
    discard:
      guard: {id: discard-check, check: "false", on_fail: discard}
      advances_to: done
`,
		"child/schema.yaml":   "name: child\nmode: template\nstages:\n  ready: {initial: true}\n  shared: {terminal: true}\n  foreign_only: {}\n  done: {terminal: true}\n  killed: {terminal: true}\n",
		"child/entities.yaml": "test_entity:\n  marker: text\n",
		"child/events.yaml":   "direct: {}\nkill: {}\nguarded: {}\nguard_observed:\n  marker: text\n",
		"child/nodes.yaml": `router:
  execution_type: system_node
  event_handlers:
    direct: {advances_to: done}
    kill:
      guard: {id: child-kill, check: 'false', on_fail: kill}
      advances_to: done
    guarded:
      guard:
        checks:
          - {id: first-pass, check: "true"}
          - {check: "true"}
      data_accumulation:
        writes:
          - {target_field: marker, expression: "'guarded'"}
      rules:
        - id: explicit-choice
          condition: "true"
          advances_to: done
          emit:
            event: guard_observed
            fields: {marker: "'guard-output'"}
`,
	}
	if len(initialTimer) > 0 && initialTimer[0] {
		files["schema.yaml"] = strings.Replace(files["schema.yaml"], "  ready: {initial: true}", "  ready:\n    initial: true\n    timers:\n      - {id: keep, after: 1h, emit: observed}", 1)
	}
	return loadWorkflowTempBundle(t, files)
}

type compiledAdapterFixture struct {
	t                    *testing.T
	db                   *sql.DB
	store                *workflowInstanceStore
	pc                   *PipelineCoordinator
	bundle               *contracts.WorkflowContractBundle
	ctx                  context.Context
	flow, path, entityID string
	node                 identity.ExecutableNode
	bus                  *recordingPipelineBus
}

func newCompiledAdapterFixture(t *testing.T, backend string, bundle *contracts.WorkflowContractBundle, flow, stage string, seed bool) *compiledAdapterFixture {
	t.Helper()
	db, store := openHandlerEntityRequirementStore(t, backend)
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus: bus, workflowStore: store, expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks: map[string]*sync.Mutex{}, module: &previewWorkflowModule{bundle: bundle},
	}
	configureWorkflowLifecycleForTest(t, pc)
	configurePipelineTestDeliveryOwner(t, pc)
	var ctx context.Context
	if backend == "sqlite" {
		ctx = sqliteExactOnceRunContext(t, db)
	} else {
		ctx = testPipelineRunContext(t, db)
	}
	ctx = withLiveWorkflowInitialEntry(ctx)
	runID := correlation.RunIDFromContext(ctx)
	path := runID
	if flow != "." {
		path = flow + "/" + uuid.NewString()
	}
	f := &compiledAdapterFixture{t: t, db: db, store: store, pc: pc, bundle: bundle, ctx: ctx, flow: flow, path: path, entityID: uuid.NewString(), bus: bus}
	f.node = pipelineSourceNode(t, pc.SemanticSource(), flow, "router")
	if seed {
		instance := materializedWorkflowInstanceForTest(WorkflowInstance{
			InstanceID: testWorkflowInstanceRoute(path).InstanceID, StorageRef: path, EntityID: f.entityID,
			WorkflowName: flow, WorkflowVersion: pc.SemanticSource().WorkflowVersion(), EntityType: "test_entity",
			CurrentState: stage, Fields: map[string]any{"marker": "unchanged"},
			EnteredStageAt: time.Now().UTC().Add(-time.Minute),
		})
		if err := store.upsert(ctx, instance); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func (f *compiledAdapterFixture) load() (WorkflowInstance, bool) {
	f.t.Helper()
	instance, found, err := f.store.Load(f.ctx, testRunScopedWorkflowInstanceFromContext(f.ctx, f.path))
	if err != nil {
		f.t.Fatal(err)
	}
	return instance, found
}

func (f *compiledAdapterFixture) event(event string) events.Event {
	return f.eventPayload(event, []byte("{}"))
}

func (f *compiledAdapterFixture) eventPayload(event string, payload []byte) events.Event {
	f.t.Helper()
	envelope := events.EnvelopeForTargetRoute(testWorkflowSourceEnvelope(f.flow, f.path, f.entityID), events.RouteIdentity{
		FlowID: f.flow, FlowInstance: f.path, EntityID: f.entityID,
	})
	evt := eventtest.RunCreatingRootIngress(uuid.NewString(), events.EventType(event), "", "", payload, 0,
		correlation.RunIDFromContext(f.ctx), "", envelope, time.Now().UTC())
	seedExactOnceEvent(f.t, f.store, f.ctx, evt)
	return evt
}

func (f *compiledAdapterFixture) state() WorkflowState {
	f.t.Helper()
	instance, found := f.load()
	if !found {
		return WorkflowState{EntityID: f.entityID}
	}
	state, err := f.pc.currentWorkflowState(f.ctx, testRunScopedWorkflowInstanceFromContext(f.ctx, f.path), identity.NormalizeEntityID(instance.EntityID))
	if err != nil {
		f.t.Fatal(err)
	}
	return state
}

func (f *compiledAdapterFixture) previewState() engine.StateSnapshot {
	f.t.Helper()
	instance, found := f.load()
	if !found {
		return engine.StateSnapshot{EntityID: identity.NormalizeEntityID(f.entityID)}
	}
	snapshot, _, err := workflowInstanceEngineStateSnapshot(f.pc.SemanticSource(), f.flow, identity.NormalizeEntityID(f.entityID), instance)
	if err != nil {
		f.t.Fatal(err)
	}
	return snapshot
}

func (f *compiledAdapterFixture) execute(event string, evt events.Event) (contractHandlerExecutionResult, error) {
	f.t.Helper()
	handler, ok := f.pc.SemanticSource().ExecutableNodeEventHandlers(f.node)[event]
	if !ok {
		f.t.Fatalf("missing authored handler %s", event)
	}
	return f.pc.executeNodeContractHandler(f.ctx, f.node, handler, workflowTriggerContext{Event: evt, HandlerEventKey: event, State: f.state()}, false)
}

func TestPipelineCompiledOrdinaryCarrierExecutionOnBothStores(t *testing.T) {
	bundle := compiledAdapterSource(t)
	source := semanticview.Wrap(bundle)
	graph, _ := semanticview.WorkflowStageTopology(source, ".")
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, from := range []string{"ready", "working", "shared"} {
			for _, event := range []string{"direct", "inherited", "rule", "complete"} {
				t.Run(backend+"/"+from+"/"+event, func(t *testing.T) {
					f := newCompiledAdapterFixture(t, backend, bundle, ".", from, true)
					evt := f.event(event)
					result, err := f.execute(event, evt)
					if err != nil {
						t.Fatal(err)
					}
					after, found := f.load()
					if !found || after.CurrentState != "done" || len(after.TransitionHistory) != 1 {
						t.Fatalf("transition did not commit: %#v", after)
					}
					record := after.TransitionHistory[0]
					carrier, ok := record.Evidence.Compiled()
					if !ok || carrier.FlowID() != "." || carrier.Edge().From != from || carrier.Edge().To != "done" ||
						!carrier.Edge().Node.Equal(f.node) || carrier.Edge().HandlerEvent != event || record.TriggerEventID != evt.ID() {
						t.Fatalf("wrong selected transition: %#v", record)
					}
					if err := carrier.ValidateAgainst(graph); err != nil {
						t.Fatal(err)
					}
					wantCarrier := contracts.HandlerAdvanceCarrierHandler
					if event == "rule" {
						wantCarrier = contracts.HandlerAdvanceCarrierRules
					}
					if event == "complete" {
						wantCarrier = contracts.HandlerAdvanceCarrierOnComplete
					}
					if carrier.Edge().AdvanceCarrier != wantCarrier {
						t.Fatalf("carrier = %s, want %s", carrier.Edge().AdvanceCarrier, wantCarrier)
					}
					if !record.Evidence.RuleSelection().Equal(result.RuleSelection) {
						t.Fatal("executed rule differs from persisted selection")
					}
					if event != "direct" && result.RuleSelection.Disposition() != handlerselection.DispositionSelected {
						t.Fatal("selected rule lost")
					}
					wantPath := map[string]string{
						"inherited": `nodes["router"].handlers["inherited"].rules[0]`,
						"rule":      `nodes["router"].handlers["rule"].rules[1]`,
						"complete":  `nodes["router"].handlers["complete"].on_complete[0]`,
					}[event]
					if result.RuleSelection.Ref().SemanticPath() != wantPath {
						t.Fatalf("selected rule path = %s, want %s", result.RuleSelection.Ref().SemanticPath(), wantPath)
					}
					if event == "inherited" && carrier.Edge().RuleRef.Valid() {
						t.Fatal("inherited handler target relabeled as a rule-owned advance")
					}
					if event == "complete" && after.Fields["marker"] != "completed" {
						t.Fatal("completion lost accumulated field write")
					}
				})
			}
		}
	}
}

func TestPipelineCompiledTransitionNoOpOnBothStores(t *testing.T) {
	bundle := compiledAdapterSource(t, true)
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, event := range []string{"self", "write_only", "unmatched", "emit_only"} {
			t.Run(backend+"/"+event, func(t *testing.T) {
				f := newCompiledAdapterFixture(t, backend, bundle, ".", "", false)
				if _, err := f.store.MaterializeInitialEntry(f.ctx, testRunScopedWorkflowInstanceFromContext(f.ctx, f.path), f.initialInstance("ready"), time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
				before, _ := f.load()
				timersBefore, err := f.store.listPersistedWorkflowTimerActivations(f.ctx, correlation.RunIDFromContext(f.ctx), f.entityID, true)
				if err != nil || len(timersBefore) != 1 {
					t.Fatalf("initial timer missing: %v %#v", err, timersBefore)
				}
				result, err := f.execute(event, f.event(event))
				if err != nil {
					t.Fatal(err)
				}
				after, _ := f.load()
				timersAfter, err := f.store.listPersistedWorkflowTimerActivations(f.ctx, correlation.RunIDFromContext(f.ctx), f.entityID, true)
				if err != nil || !reflect.DeepEqual(timersBefore, timersAfter) {
					t.Fatalf("no-op changed timer activation: %v %#v", err, timersAfter)
				}
				if after.CurrentState != before.CurrentState || !after.EnteredStageAt.Equal(before.EnteredStageAt) ||
					!reflect.DeepEqual(after.TransitionHistory, before.TransitionHistory) {
					t.Fatalf("no-op changed stage lifecycle: before=%#v after=%#v", before, after)
				}
				if event == "write_only" && after.Fields["marker"] != "written" {
					t.Fatal("legitimate non-transition write was lost")
				}
				if event == "emit_only" && f.bus.publishedCount() != 1 {
					t.Fatal("emit-only effect was lost")
				}
				if event == "unmatched" && result.RuleSelection.Disposition() != handlerselection.DispositionNoMatch {
					t.Fatalf("no-match disposition = %#v", result.RuleSelection)
				}
			})
		}
	}
}

func TestPipelineCompiledTransitionGuardDispositionOnBothStores(t *testing.T) {
	bundle := compiledAdapterSource(t)
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, event := range []string{"kill", "reject", "discard"} {
			t.Run(backend+"/"+event, func(t *testing.T) {
				f := newCompiledAdapterFixture(t, backend, bundle, ".", "ready", true)
				before, _ := f.load()
				evt := f.event(event)
				result, err := f.execute(event, evt)
				if err != nil {
					t.Fatal(err)
				}
				after, _ := f.load()
				want := map[string]HandlerOutcomeStatus{"kill": HandlerOutcomeKilled, "reject": HandlerOutcomeRejected, "discard": HandlerOutcomeDiscarded}[event]
				if result.Outcome == nil || result.Outcome.Status != want {
					t.Fatalf("guard result = %#v, want %s", result.Outcome, want)
				}
				if event != "kill" {
					if after.CurrentState != before.CurrentState || !reflect.DeepEqual(after.TransitionHistory, before.TransitionHistory) {
						t.Fatal("reject/discard transitioned state")
					}
					return
				}
				if after.CurrentState != "killed" || len(after.TransitionHistory) != 1 {
					t.Fatalf("guard kill = %#v", after)
				}
				record := after.TransitionHistory[0]
				if _, ok := record.Evidence.Compiled(); ok {
					t.Fatal("guard termination masquerades as ordinary compiled edge")
				}
				if record.Evidence.FlowID() != "." || record.TriggerEventID != evt.ID() ||
					!slices.Equal(record.GuardsEvaluated, result.GuardsEvaluated) || !slices.Contains(record.GuardsEvaluated, "kill-check") {
					t.Fatalf("guard cause lost evaluated provenance: %#v", record)
				}
			})
		}
		t.Run(backend+"/child_kill", func(t *testing.T) {
			f := newCompiledAdapterFixture(t, backend, bundle, "child", "ready", true)
			evt := f.event("kill")
			result, err := f.execute("kill", evt)
			if err != nil {
				t.Fatal(err)
			}
			after, _ := f.load()
			if result.Outcome == nil || result.Outcome.Status != HandlerOutcomeKilled || after.CurrentState != "killed" || len(after.TransitionHistory) != 1 {
				t.Fatalf("child kill failed: %#v %#v", result, after)
			}
			cause := after.TransitionHistory[0].Evidence
			if cause.FlowID() != "child" || !slices.Contains(cause.GuardsEvaluated(), "child-kill") {
				t.Fatal("child kill borrowed root guard provenance")
			}
			if _, compiled := cause.Compiled(); compiled {
				t.Fatal("child guard kill became an ordinary edge")
			}
		})
	}
}

func TestPipelineCompiledTransitionStageGuardsOnBothStores(t *testing.T) {
	bundle := compiledAdapterSource(t)
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, tc := range []struct {
			stage           string
			reject, failure bool
		}{
			{"ready", false, false}, {"Ready", true, false}, {"shared", false, false},
			{"unknown", false, true}, {"foreign_only", false, true},
		} {
			t.Run(backend+"/"+tc.stage, func(t *testing.T) {
				f := newCompiledAdapterFixture(t, backend, bundle, ".", tc.stage, true)
				before, _ := f.load()
				result, err := f.execute("direct", f.event("direct"))
				if tc.failure {
					if err == nil {
						t.Fatal("foreign/unknown source stage admitted")
					}
				} else if err != nil {
					t.Fatal(err)
				} else if tc.reject {
					if result.Outcome == nil || result.Outcome.Status != HandlerOutcomeTerminalReject {
						t.Fatalf("terminal stage accepted: %#v", result)
					}
				} else if after, _ := f.load(); after.CurrentState != "done" {
					t.Fatal("local nonterminal source rejected")
				}
				if tc.failure || tc.reject {
					after, _ := f.load()
					if !reflect.DeepEqual(before, after) {
						t.Fatal("rejected stage changed persisted state")
					}
				}
			})
		}
		for _, target := range []string{"unknown", "foreign_only", "Done"} {
			t.Run(backend+"/target/"+target, func(t *testing.T) {
				f := newCompiledAdapterFixture(t, backend, bundle, ".", "ready", true)
				before, _ := f.load()
				handler := f.pc.SemanticSource().ExecutableNodeEventHandlers(f.node)["direct"]
				handler.AdvancesTo = target
				_, err := f.pc.executeNodeContractHandler(f.ctx, f.node, handler, workflowTriggerContext{
					Event: f.event("direct"), HandlerEventKey: "direct", State: f.state(),
				}, false)
				if err == nil {
					t.Fatalf("uncompiled target %q admitted", target)
				}
				after, _ := f.load()
				if !reflect.DeepEqual(before, after) || f.bus.publishedCount() != 0 {
					t.Fatal("rejected target committed effects")
				}
			})
		}
		for _, stage := range []string{"unknown", "foreign_only"} {
			t.Run(backend+"/no_advance/"+stage, func(t *testing.T) {
				f := newCompiledAdapterFixture(t, backend, bundle, ".", stage, true)
				before, _ := f.load()
				if _, err := f.execute("write_only", f.event("write_only")); err == nil {
					t.Fatal("no-advance bypassed exact source-stage membership")
				}
				after, _ := f.load()
				if !reflect.DeepEqual(before, after) {
					t.Fatal("invalid no-advance source changed state")
				}
			})
		}
		for _, tc := range []struct {
			flow, stage string
			terminal    bool
		}{
			{".", "ready", false}, {".", "Ready", true}, {".", "shared", false}, {"child", "shared", true},
		} {
			t.Run(backend+"/terminal_guard/"+tc.flow+"/"+tc.stage, func(t *testing.T) {
				f := newCompiledAdapterFixture(t, backend, bundle, tc.flow, tc.stage, true)
				state, err := handlerExecutionStateSnapshot(contracts.SystemNodeEventHandler{}, f.entityID, f.state(), tc.flow, f.pc.SemanticSource().WorkflowVersion())
				if err != nil {
					t.Fatal(err)
				}
				request := engine.ExecutionRequest{Node: f.node, ExecutionFlowID: identity.NormalizeFlowID(tc.flow), State: state}
				for _, builtin := range []string{"not_in_terminal_state", "not_in_terminal_stage"} {
					got, handled, err := (pipelineEngineGuardRunner{coordinator: f.pc}).EvaluateGuard(f.ctx, identity.NormalizeGuardKey(builtin),
						registry.GuardInstruction{Builtin: builtin}, engine.ExecutionContext{Request: request})
					if err != nil || !handled || got != !tc.terminal {
						t.Fatalf("%s: got=%v handled=%v err=%v", builtin, got, handled, err)
					}
				}
			})
		}
		t.Run(backend+"/phase_policy", func(t *testing.T) {
			f := newCompiledAdapterFixture(t, backend, bundle, "child", "ready", true)
			state, err := handlerExecutionStateSnapshot(contracts.SystemNodeEventHandler{}, f.entityID, f.state(), "child", f.pc.SemanticSource().WorkflowVersion())
			if err != nil {
				t.Fatal(err)
			}
			request := engine.ExecutionRequest{Node: f.node, ExecutionFlowID: identity.NormalizeFlowID("child"), State: state}
			for _, required := range []string{"child", ".", "CHILD"} {
				execCtx := engine.ExecutionContext{Request: request, Base: engine.BaseContext{Policy: values.Wrap(map[string]any{"approval": map[string]any{"phase": required}})}}
				got, handled, err := (pipelineEngineGuardRunner{coordinator: f.pc}).EvaluateGuard(f.ctx, identity.NormalizeGuardKey("state_in_phase"),
					registry.GuardInstruction{Builtin: "state_in_phase", PolicyRef: "approval.phase"}, execCtx)
				if err != nil || !handled || got != (required == "child") {
					t.Fatalf("phase policy %q: got=%v handled=%v err=%v", required, got, handled, err)
				}
			}
		})
	}
}

func TestPipelineCompiledTransitionInitialAdmissionOnBothStores(t *testing.T) {
	bundle := compiledAdapterSource(t, true)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend+"/first_event", func(t *testing.T) {
			f := newCompiledAdapterFixture(t, backend, bundle, ".", "", false)
			evt := f.event("direct")
			_, err := f.execute("direct", evt)
			if err != nil {
				t.Fatal(err)
			}
			after, found := f.load()
			if !found || after.CurrentState != "done" || len(after.TransitionHistory) != 1 || after.TransitionHistory[0].From != "ready" {
				t.Fatalf("first event did not use exact initial stage: %#v", after)
			}
		})
		for _, stage := range []string{"working", "foreign_only", "Ready", "pending", ""} {
			t.Run(backend+"/invalid_initial/"+stage, func(t *testing.T) {
				f := newCompiledAdapterFixture(t, backend, bundle, ".", "", false)
				at := time.Now().UTC()
				instance := f.initialInstance(stage)
				if _, err := f.store.MaterializeInitialEntry(f.ctx, testRunScopedWorkflowInstanceFromContext(f.ctx, f.path), instance, at); err == nil {
					t.Fatalf("noninitial stage %q materialized", stage)
				}
				if _, found := f.load(); found {
					t.Fatal("rejected initialization created state")
				}
			})
		}
		t.Run(backend+"/effect_disagrees_with_prepared_state", func(t *testing.T) {
			f := newCompiledAdapterFixture(t, backend, bundle, ".", "", false)
			instance := f.initialInstance("ready")
			effect, err := workflowlifecycle.NewInitialEntry(testWorkflowInstanceRoute(f.path), identity.NormalizeEntityID(f.entityID), "working", executionmode.Live, time.Now().UTC())
			if err != nil {
				t.Fatal(err)
			}
			prepared := PreparedWorkflowLifecycleMutation{}
			if err := f.pc.planWorkflowLifecycleEffect(f.ctx, testRunScopedWorkflowInstanceFromContext(f.ctx, f.path), &instance, effect, &prepared); err == nil {
				t.Fatal("contradictory initial effect accepted")
			}
			if !reflect.DeepEqual(prepared, PreparedWorkflowLifecycleMutation{}) {
				t.Fatal("rejected initial effect planned effects")
			}
		})
		for _, stateless := range []bool{false, true} {
			name, source, stage := "declared", bundle, "ready"
			if stateless {
				name, source, stage = "stateless", statelessCompiledAdapterSource(t), "pending"
			}
			t.Run(backend+"/exact_initial_replay/"+name, func(t *testing.T) {
				f := newCompiledAdapterFixture(t, backend, source, ".", "", false)
				at := time.Now().UTC()
				instance := f.initialInstance(stage)
				result, err := f.store.MaterializeInitialEntry(f.ctx, testRunScopedWorkflowInstanceFromContext(f.ctx, f.path), instance, at)
				if err != nil || result != WorkflowInitialMaterializationCreated {
					t.Fatalf("initial creation: %v %v", result, err)
				}
				before, found := f.load()
				if !found || before.CurrentState != stage || len(before.TransitionHistory) != 0 {
					t.Fatalf("initial state = %#v", before)
				}
				timersBefore, err := f.store.listPersistedWorkflowTimerActivations(f.ctx, correlation.RunIDFromContext(f.ctx), f.entityID, true)
				wantTimers := 1
				if stateless {
					wantTimers = 0
				}
				if err != nil || len(timersBefore) != wantTimers {
					t.Fatalf("initial timer effects: %v %#v", err, timersBefore)
				}
				result, err = f.store.MaterializeInitialEntry(f.ctx, testRunScopedWorkflowInstanceFromContext(f.ctx, f.path), instance, at)
				if err != nil || result != WorkflowInitialMaterializationAlreadyExists {
					t.Fatalf("initial replay: %v %v", result, err)
				}
				after, _ := f.load()
				if !reflect.DeepEqual(before, after) {
					t.Fatal("initial replay changed durable state")
				}
				timersAfter, err := f.store.listPersistedWorkflowTimerActivations(f.ctx, correlation.RunIDFromContext(f.ctx), f.entityID, true)
				if err != nil || !reflect.DeepEqual(timersBefore, timersAfter) {
					t.Fatalf("initial replay repeated timer effects: %v %#v", err, timersAfter)
				}
				if stateless {
					if _, err := f.execute("noop", f.event("noop")); err != nil {
						t.Fatalf("stateless no-advance: %v", err)
					}
					if after, _ := f.load(); after.CurrentState != "pending" || len(after.TransitionHistory) != 0 {
						t.Fatal("stateless handler invented transition")
					}
				}
			})
		}
	}
}

func (f *compiledAdapterFixture) initialInstance(stage string) WorkflowInstance {
	return WorkflowInstance{
		InstanceID: testWorkflowInstanceRoute(f.path).InstanceID, StorageRef: f.path, EntityID: f.entityID,
		WorkflowName: f.flow, WorkflowVersion: f.pc.SemanticSource().WorkflowVersion(), EntityType: "test_entity",
		CurrentState: stage, Fields: map[string]any{"marker": "unchanged"},
	}
}

func statelessCompiledAdapterSource(t *testing.T) *contracts.WorkflowContractBundle {
	t.Helper()
	return loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":   "name: stateless-proof\n",
		"entities.yaml": "test_entity:\n  marker: text\n",
		"events.yaml":   "noop: {}\n",
		"nodes.yaml":    "router:\n  execution_type: system_node\n  event_handlers:\n    noop: {}\n",
	})
}

func requireCompiledPreviewAgreement(t *testing.T, f *compiledAdapterFixture, evt events.Event, before, after WorkflowInstance, preview HandlerPreview, result contractHandlerExecutionResult) {
	t.Helper()
	if result.Outcome == nil || preview.Status != result.Outcome.Status || string(preview.Stage) != after.CurrentState {
		t.Fatalf("preview/execution outcome disagreement: preview=%#v outcome=%#v stage=%s", preview, result.Outcome, after.CurrentState)
	}
	if !preview.RuleSelection.Equal(result.RuleSelection) || !preview.RuleSelection.Equal(result.Outcome.RuleSelection) || preview.RuleID != result.RuleSelection.DisplayLabel() {
		t.Fatal("preview lost exact qualified rule selection")
	}
	if !slices.Equal(preview.GuardsEvaluated, result.GuardsEvaluated) || !slices.Equal(preview.GuardsEvaluated, result.Outcome.GuardsEvaluated) {
		t.Fatal("preview lost ordered evaluated guards")
	}
	if !reflect.DeepEqual(preview.Transition, result.Transition) {
		t.Fatalf("preview/execution cause disagreement: %#v %#v", preview.Transition, result.Transition)
	}
	if result.Transition == nil {
		if !reflect.DeepEqual(before.TransitionHistory, after.TransitionHistory) {
			t.Fatal("absent cause changed durable transition history")
		}
	} else {
		if len(after.TransitionHistory) != len(before.TransitionHistory)+1 {
			t.Fatal("selected cause did not create exactly one history record")
		}
		record := after.TransitionHistory[len(after.TransitionHistory)-1]
		if record.Evidence.ID() != preview.Transition.ID() || !record.Evidence.RuleSelection().Equal(preview.RuleSelection) ||
			!slices.Equal(record.Evidence.GuardsEvaluated(), preview.GuardsEvaluated) || record.TriggerEventID != evt.ID() || !record.FiredAt.Equal(evt.CreatedAt()) {
			t.Fatalf("persisted cause differs from preview evidence: %#v", record)
		}
		graph, found := semanticview.WorkflowStageTopology(f.pc.SemanticSource(), f.flow)
		if !found {
			t.Fatal("fixture lacks exact source graph")
		}
		if err := preview.Transition.ValidateAgainst(graph); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(preview.Metadata, result.PreviewMetadata) || !reflect.DeepEqual(preview.InitialValues, cloneStringAnyMap(result.InitialValuesMaterialized)) ||
		!reflect.DeepEqual(preview.Computed, result.Outcome.Computed) || !slices.Equal(preview.Emits, result.Outcome.Emits) ||
		!slices.Equal(preview.ActionsExecuted, result.Outcome.ActionsExecuted) || preview.SetsGate != result.Outcome.SetsGate ||
		!slices.Equal(preview.ClearGates, result.Outcome.ClearGates) || preview.FanOutCount != result.Outcome.FanOutCount {
		t.Fatalf("preview lost execution outputs: metadata=%#v/%#v initial=%#v/%#v computed=%#v/%#v emits=%#v/%#v actions=%#v/%#v gates=%s/%s clear=%#v/%#v fanout=%d/%d",
			preview.Metadata, result.PreviewMetadata, preview.InitialValues, result.InitialValuesMaterialized, preview.Computed, result.Outcome.Computed,
			preview.Emits, result.Outcome.Emits, preview.ActionsExecuted, result.Outcome.ActionsExecuted, preview.SetsGate, result.Outcome.SetsGate,
			preview.ClearGates, result.Outcome.ClearGates, preview.FanOutCount, result.Outcome.FanOutCount)
	}
	if preview.Metadata["marker"] != after.Fields["marker"] || f.bus.publishedCount() != len(preview.Emits) {
		t.Fatal("preview outputs disagree with actual persistence or publication")
	}
	for i, eventType := range preview.Emits {
		if string(f.bus.publishedEvent(i).Type()) != eventType {
			t.Fatal("published output event differs from preview")
		}
	}
}

func requireGuardedPreviewEvidence(t *testing.T, f *compiledAdapterFixture, preview HandlerPreview) {
	t.Helper()
	handler := f.pc.SemanticSource().ExecutableNodeEventHandlers(f.node)["guarded"]
	wantRef, qualified := handler.Rules[0].DeclarationIdentity()
	if !qualified || preview.RuleSelection.Disposition() != handlerselection.DispositionSelected || !preview.RuleSelection.Ref().Equal(wantRef) || preview.RuleID != "explicit-choice" {
		t.Fatal("preview selected a display-label alias instead of the exact authored rule")
	}
	if !slices.Equal(preview.GuardsEvaluated, []string{"first-pass", "true"}) || preview.Metadata["marker"] != "guarded" {
		t.Fatalf("guarded preview lost declared prefix or field output: %#v", preview)
	}
	var payload map[string]any
	if f.bus.publishedCount() != 1 {
		t.Fatal("guarded execution did not publish its output")
	}
	if err := json.Unmarshal(f.bus.publishedEvent(0).Payload(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["marker"] != "guard-output" {
		t.Fatalf("guarded publication lost payload: %#v", payload)
	}
}

func TestCompiledTransitionPreviewExecutionAgreementOnBothStores(t *testing.T) {
	bundle := compiledAdapterSource(t)
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, event := range []string{"direct", "inherited", "rule", "complete", "self", "unmatched", "write_only", "emit_only", "guarded", "kill", "reject", "discard"} {
			t.Run(backend+"/"+event, func(t *testing.T) {
				f := newCompiledAdapterFixture(t, backend, bundle, ".", "ready", true)
				evt := f.event(event)
				before, _ := f.load()
				preview, previewErr := PreviewContractHandlerExecution(f.ctx, bundle, f.node, evt, f.previewState(), nil)
				if previewErr != nil {
					t.Fatal(previewErr)
				}
				afterPreview, _ := f.load()
				if !reflect.DeepEqual(before, afterPreview) || f.bus.publishedCount() != 0 {
					t.Fatal("preview wrote durable or public effects")
				}
				result, err := f.execute(event, evt)
				if err != nil {
					t.Fatal(err)
				}
				after, _ := f.load()
				advances := slices.Contains([]string{"direct", "inherited", "rule", "complete", "guarded"}, event)
				wantStage, wantHistory := "ready", 0
				if advances {
					wantStage, wantHistory = "done", 1
				}
				if event == "kill" {
					wantStage, wantHistory = "killed", 1
				}
				if after.CurrentState != wantStage || len(after.TransitionHistory) != wantHistory {
					t.Fatalf("live effects = %#v", after)
				}
				if event == "write_only" && after.Fields["marker"] != "written" {
					t.Fatal("live execution lost field write")
				}
				if event == "emit_only" && f.bus.publishedCount() != 1 {
					t.Fatal("live execution lost public emit")
				}
				requireCompiledPreviewAgreement(t, f, evt, before, after, preview, result)
				if event == "guarded" {
					requireGuardedPreviewEvidence(t, f, preview)
				}
				if slices.Contains([]string{"kill", "reject", "discard"}, event) && !slices.Equal(preview.GuardsEvaluated, []string{event + "-check"}) {
					t.Fatal("preview lost failed guard label")
				}
			})
		}
		t.Run(backend+"/template_qualified_selection", func(t *testing.T) {
			f := newCompiledAdapterFixture(t, backend, bundle, "child", "ready", true)
			evt := f.event("guarded")
			before, _ := f.load()
			preview, err := PreviewContractHandlerExecution(f.ctx, bundle, f.node, evt, f.previewState(), nil)
			if err != nil {
				t.Fatal(err)
			}
			afterPreview, _ := f.load()
			if !reflect.DeepEqual(before, afterPreview) || f.bus.publishedCount() != 0 {
				t.Fatal("template preview wrote durable or public effects")
			}
			result, err := f.execute("guarded", evt)
			if err != nil {
				t.Fatal(err)
			}
			after, _ := f.load()
			requireCompiledPreviewAgreement(t, f, evt, before, after, preview, result)
			requireGuardedPreviewEvidence(t, f, preview)
			rootNode := pipelineSourceNode(t, f.pc.SemanticSource(), ".", "router")
			rootHandler := f.pc.SemanticSource().ExecutableNodeEventHandlers(rootNode)["guarded"]
			rootRef, _ := rootHandler.Rules[0].DeclarationIdentity()
			if preview.RuleSelection.Ref().Equal(rootRef) || preview.Transition == nil || preview.Transition.FlowID() != "child" {
				t.Fatal("template preview borrowed root identity for the same rule label")
			}
		})
		t.Run(backend+"/initial", func(t *testing.T) {
			f := newCompiledAdapterFixture(t, backend, bundle, ".", "", false)
			evt := f.event("direct")
			preview, previewErr := PreviewContractHandlerExecution(f.ctx, bundle, f.node, evt, f.previewState(), nil)
			if previewErr != nil {
				t.Fatal(previewErr)
			}
			if _, found := f.load(); found {
				t.Fatal("initial preview created an instance")
			}
			result, err := f.execute("direct", evt)
			if err != nil {
				t.Fatal(err)
			}
			after, _ := f.load()
			if after.CurrentState != "done" || len(after.TransitionHistory) != 1 || after.TransitionHistory[0].From != "ready" || (previewErr == nil && string(preview.Stage) != after.CurrentState) {
				t.Fatalf("initial preview/execution disagreed: %#v %#v", preview, after)
			}
			requireCompiledPreviewAgreement(t, f, evt, WorkflowInstance{}, after, preview, result)
		})
		t.Run(backend+"/foreign_source", func(t *testing.T) {
			f := newCompiledAdapterFixture(t, backend, bundle, ".", "foreign_only", true)
			evt := f.event("direct")
			before, _ := f.load()
			if _, err := PreviewContractHandlerExecution(f.ctx, bundle, f.node, evt, f.previewState(), nil); err == nil {
				t.Fatal("preview accepted foreign source stage")
			}
			if _, err := f.execute("direct", evt); err == nil {
				t.Fatal("execution accepted foreign source stage")
			}
			after, _ := f.load()
			if !reflect.DeepEqual(before, after) {
				t.Fatal("foreign source changed durable state")
			}
		})
		for _, field := range []string{"flow", "entity", "route"} {
			t.Run(backend+"/foreign_snapshot/"+field, func(t *testing.T) {
				f := newCompiledAdapterFixture(t, backend, bundle, ".", "ready", true)
				evt := f.event("direct")
				before, _ := f.load()
				snapshot := f.previewState()
				switch field {
				case "flow":
					snapshot.WorkflowName = "child"
				case "entity":
					snapshot.EntityID = identity.NormalizeEntityID(uuid.NewString())
				case "route":
					snapshot.StateCarrier.Control.FlowPath = "child/" + uuid.NewString()
				}
				if _, err := PreviewContractHandlerExecution(f.ctx, bundle, f.node, evt, snapshot, nil); err == nil {
					t.Fatal("preview accepted foreign snapshot identity")
				}
				after, _ := f.load()
				if !reflect.DeepEqual(before, after) || f.bus.publishedCount() != 0 {
					t.Fatal("foreign snapshot preview committed effects")
				}
			})
		}
		t.Run(backend+"/loop_escape", func(t *testing.T) {
			loopBundle := loadWorkflowTempBundle(t, map[string]string{
				"schema.yaml":   "name: loop-preview\nstages:\n  ready: {initial: true}\n  drafting: {}\n  review: {}\n  escaped: {terminal: true}\nloops:\n  revision:\n    revision_field: revision_id\n    max_attempts: 1\n    escape: {advances_to: escaped}\n",
				"entities.yaml": "test_entity:\n  marker: text\n",
				"events.yaml":   "start: {}\nadmit:\n  revision_id: text\nrepeat:\n  revision_id: text\n",
				"nodes.yaml":    "router:\n  execution_type: system_node\n  event_handlers:\n    start:\n      loop: {start: revision, from: ready}\n      advances_to: drafting\n    admit:\n      loop: {admit: revision, from: drafting}\n      advances_to: review\n    repeat:\n      loop: {repeat: revision, from: review}\n      advances_to: drafting\n",
			})
			f := newCompiledAdapterFixture(t, backend, loopBundle, ".", "ready", true)
			if _, err := f.execute("start", f.event("start")); err != nil {
				t.Fatal(err)
			}
			started, _ := f.load()
			carrier, err := workflowInstanceStateCarrier(started)
			if err != nil {
				t.Fatal(err)
			}
			activation, found, err := loopruntime.Load(carrier.StateBuckets, ".", "revision")
			if err != nil || !found {
				t.Fatalf("missing loop activation: %v", err)
			}
			payload := mustJSON(map[string]any{"revision_id": activation.RevisionID})
			if _, err := f.execute("admit", f.eventPayload("admit", payload)); err != nil {
				t.Fatal(err)
			}
			before, _ := f.load()
			evt := f.eventPayload("repeat", payload)
			snapshot, originalSnapshot := f.previewState(), f.previewState()
			preview, previewErr := PreviewContractHandlerExecution(f.ctx, loopBundle, f.node, evt, snapshot, nil)
			if previewErr != nil {
				t.Error(previewErr)
			}
			if !reflect.DeepEqual(snapshot, originalSnapshot) {
				t.Fatal("loop preview mutated its input snapshot")
			}
			afterPreview, _ := f.load()
			if !reflect.DeepEqual(before, afterPreview) {
				t.Fatal("loop preview changed activation")
			}
			result, err := f.execute("repeat", evt)
			if err != nil {
				t.Fatal(err)
			}
			after, _ := f.load()
			if (previewErr == nil && string(preview.Stage) != "escaped") || after.CurrentState != "escaped" || len(after.TransitionHistory) != len(before.TransitionHistory)+1 {
				t.Fatalf("loop preview/execution disagreed: %#v %#v", preview, after)
			}
			compiled, ok := after.TransitionHistory[len(after.TransitionHistory)-1].Evidence.Compiled()
			if !ok || compiled.Edge().Source != "loop.escape" || compiled.Edge().LoopID != "revision" || compiled.Edge().HandlerEvent != "repeat" {
				t.Fatal("escape lost actual selected loop carrier")
			}
			requireCompiledPreviewAgreement(t, f, evt, before, after, preview, result)
		})
	}
}
