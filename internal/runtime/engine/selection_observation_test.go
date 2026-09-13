package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/computemodule"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

func requireResolvedSelection(t *testing.T, observation handlerselection.Observation) handlerselection.HandlerRuleSelectionFact {
	t.Helper()
	fact, err := observation.ResolvedFact()
	if err != nil {
		t.Fatalf("expected resolved selection observation: %v", err)
	}
	return fact
}

type selectionReplayLoader struct{ calls int }

func (l *selectionReplayLoader) LoadComputeModuleReplayEvidenceForExecution(context.Context, string, string, string) ([]computemodule.ReplayEnvelope, error) {
	l.calls++
	return nil, errors.New("controlled replay evidence read failure")
}

func TestExecutionAdaptersPreserveUnreachedSelection(t *testing.T) {
	for _, boundary := range []string{"nil_node", "wrong_node", "missing_handler", "nil_executor", "nil_loader", "missing_run", "missing_event", "missing_node", "dual_evidence", "evidence_read"} {
		t.Run(boundary, func(t *testing.T) {
			node := testFlowExecutableNode(t, "orders", "worker")
			exec, recorder := transitionTestExecutor(t, sourceWithFixtureStages(stubSource(), "orders", "ready", "ready"))
			req := transitionTestRequest(t, node, "work.requested", contracts.SystemNodeEventHandler{}, "ready")
			loader := &selectionReplayLoader{}
			var input ComputeModuleReplayEvidenceLoader = loader
			runID := "run"
			var result ExecutionResult
			var err error
			switch boundary {
			case "nil_node":
				result, err = (*DeclarativeNode)(nil).Handle(context.Background(), req)
			case "wrong_node":
				req.Node = testFlowExecutableNode(t, "orders", "other")
				result, err = NewDeclarativeNode(node, exec).Handle(context.Background(), req)
			case "missing_handler":
				result, err = NewDeclarativeNode(node, exec).Handle(context.Background(), req)
			default:
				switch boundary {
				case "nil_executor":
					exec = nil
				case "nil_loader":
					input = nil
				case "missing_run":
					runID = ""
				case "missing_event":
					req.Event = events.Event{}
				case "missing_node":
					req.Node = identity.ExecutableNode{}
				case "dual_evidence":
					req.ExpectedComputeModuleTraces = []ComputeModuleTrace{}
				}
				result, err = exec.ExecuteWithPersistedComputeModuleReplayEvidence(context.Background(), input, runID, req)
			}
			if err == nil || !result.HandlerRuleSelection.Equal(handlerselection.NotReached()) || len(recorder.mutations) != 0 || result.Committed || len(result.EmitIntents) != 0 {
				t.Fatalf("adapter fabricated selection/effects: result=%+v err=%v", result, err)
			}
			wantReads := 0
			if boundary == "evidence_read" {
				wantReads = 1
			}
			if loader.calls != wantReads {
				t.Fatalf("unexpected evidence reads: got=%d want=%d", loader.calls, wantReads)
			}
		})
	}
}

type selectionFailureLocker struct{ err error }

func (l selectionFailureLocker) WithEntityLock(context.Context, identity.EntityID, func(context.Context) error) error {
	return l.err
}

type selectionFailureStateRepo struct {
	StateRepository
	err error
}

func (r selectionFailureStateRepo) LoadState(context.Context, StateAddress) (StateSnapshot, bool, error) {
	return StateSnapshot{}, false, r.err
}

func TestExecutorSelectionObservationBeforeEvaluation(t *testing.T) {
	for _, boundary := range []string{"request", "entity_lock", "state_read", "source_stage", "guard_evaluation", "no_fault"} {
		t.Run(boundary, func(t *testing.T) {
			node := testFlowExecutableNode(t, "orders", "worker")
			exec, recorder := transitionTestExecutor(t, sourceWithFixtureStages(stubSource(), "orders", "ready", "ready"))
			req := transitionTestRequest(t, node, "work.requested", contracts.SystemNodeEventHandler{}, "ready")
			fault := errors.New("exact preselection dependency cut")
			switch boundary {
			case "request":
				req.Node = identity.ExecutableNode{}
			case "entity_lock":
				exec.deps.Locker = selectionFailureLocker{err: fault}
			case "state_read":
				exec.deps.StateRepo = selectionFailureStateRepo{StateRepository: exec.deps.StateRepo, err: fault}
			case "source_stage":
				req.State.CurrentState = "missing-stage"
			case "guard_evaluation":
				req.Handler.Guard = &contracts.GuardSpec{Check: "dependency", OnFail: "reject"}
				exec.evaluator = stubEvaluator{errs: map[string]error{"dependency": fault}}
			}
			// Request rejection is deliberately before the fixture's authored-ID
			// completion; all other rows enter the actual executor with valid facts.
			var result ExecutionResult
			var err error
			if boundary == "request" {
				result, err = exec.Execute(context.Background(), req)
			} else {
				result, err = exec.ExecuteSemanticFixture(context.Background(), req)
			}
			if boundary == "no_fault" {
				if err != nil || !requireResolvedSelection(t, result.HandlerRuleSelection).Equal(handlerselection.NotApplicable()) {
					t.Fatalf("definitive direct execution: result=%+v err=%v", result, err)
				}
				return
			}
			if err == nil || !result.HandlerRuleSelection.Equal(handlerselection.NotReached()) || len(recorder.mutations) != 0 || result.Committed || len(result.EmitIntents) != 0 || len(result.ActivityIntents) != 0 {
				t.Fatalf("preselection failure fabricated evidence or effects: result=%+v err=%v mutations=%+v", result, err, recorder.mutations)
			}
		})
	}
}
