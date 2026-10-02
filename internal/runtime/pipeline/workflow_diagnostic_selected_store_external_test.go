package pipeline_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimeruntime "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// This is only the EventBus logger signature adapter; persistence and failure
// projection remain with the production RuntimeLogger and selected store.
type workflowDiagnosticRuntimeLogger struct{ logger *runtimeruntime.RuntimeLogger }

func (l workflowDiagnosticRuntimeLogger) ProjectLifecycleDiagnostic(ctx context.Context, item diaglog.LifecycleDiagnostic) error {
	return l.logger.ProjectLifecycleDiagnostic(ctx, item)
}

func (l workflowDiagnosticRuntimeLogger) Log(ctx context.Context, level diaglog.Level, message, component, action, eventID, eventType, agentID, entityID, sessionID string, correlation map[string]string, detail any, failure *runtimefailures.Envelope, durationUS int) error {
	return l.logger.Log(ctx, runtimeruntime.RuntimeLogEntry{
		Level: level, Message: message, Component: component, Action: action,
		EventID: eventID, EventType: eventType, AgentID: agentID, EntityID: entityID, SessionID: sessionID,
		Correlation: correlation, Detail: detail, Failure: failure, DurationUS: durationUS,
	})
}

func newWorkflowDiagnosticNativeFixture(t *testing.T, backend string, bundle *runtimecontracts.WorkflowContractBundle) runtimepipeline.WorkflowDiagnosticFixtureForTest {
	t.Helper()
	var selected scopedTestDurableStore
	var cards gateRecoveryDecisionStore
	var persistence runtimepipeline.WorkflowPersistence
	switch backend {
	case "sqlite":
		s := storetest.StartSQLiteRuntimeStore(t)
		selected, cards, persistence = s, s, runtimepipeline.NewWorkflowPersistence(s)
	case "postgres":
		dsn, _, _ := testutil.StartPostgres(t)
		s, err := store.NewPostgresStore(dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Errorf("close native diagnostic store: %v", err)
			}
		})
		storetest.BootstrapPostgresRuntimeStore(t, s)
		s.SetEventPayloadAdmitter(func(_ context.Context, event events.Event, flow string) (events.PayloadAdmission, error) {
			return eventtest.PayloadAdmission(event, flow, string(event.Type()))
		})
		selected, cards, persistence = s, s, runtimepipeline.NewWorkflowPersistence(s)
	default:
		t.Fatalf("unknown diagnostic backend %q", backend)
	}
	ctx := testAuthorActivityContext(t, context.Background())
	runID := uuid.NewString()
	runOwner := selected.(interface {
		runtimerunlifecycle.OperationOwner
		runtimerunlifecycle.CandidateStore
	})
	storetest.RequireRunningRun(t, ctx, runOwner, runID, time.Now().UTC())
	ctx = withLiveGateExecution(runtimecorrelation.WithRunID(ctx, runID))
	source := semanticview.Wrap(bundle)
	bus, err := newScopedTestEventBus(t, selected, runtimebus.EventBusOptions{ContractBundle: source}, "platform.stage_timer", "platform.join_timeout", "platform.join_complete")
	if err != nil {
		t.Fatal(err)
	}
	logPersistence := selected.(runtimeruntime.RuntimeLogPersistence)
	bus.SetLoggerHook(workflowDiagnosticRuntimeLogger{logger: runtimeruntime.NewRuntimeLogger(logPersistence, executionposture.Live,
		func(_ context.Context, event events.Event, flow string) (events.PayloadAdmission, error) {
			return eventtest.PayloadAdmission(event, flow, string(event.Type()))
		})})
	nodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		t.Fatal(err)
	}
	pc := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, runtimepipeline.PipelineCoordinatorOptions{
		ExecutionPosture: executionposture.Live, ReceiverExecution: eventreceiver.NormalExecution(),
		Module: proposedEffectProofModule{source: source, nodes: nodes}, WorkOwner: pipelineExternalTestWorkOwner(t),
		SourceArtifactFact: authorActivityTestSourceArtifactFact,
		Persistence:        persistence, DeliveryStore: selected, DeadLetters: selected,
		DecisionCards: cards, ProposedEffects: cards, HumanTasks: cards,
		DecisionCardDraftExpiry: cards, HumanTaskExpiry: cards,
		DeliveryRuntime: bus, RunLifecycle: selected, PipelineObligations: selected.PipelineObligations(),
	})
	if pc == nil {
		t.Fatal("native diagnostic coordinator rejected complete selected ports")
	}
	t.Cleanup(func() {
		joinCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := bus.WaitForQuiescence(joinCtx); err != nil {
			t.Errorf("join diagnostic publications: %v", err)
		}
	})
	reader := selected.(interface {
		ListOperatorRuntimeLogs(context.Context, operatorread.OperatorRuntimeLogListOptions) (operatorread.OperatorRuntimeLogListResult, error)
		ListOperatorEvents(context.Context, operatorread.OperatorEventListOptions) (operatorread.OperatorEventListResult, error)
	})
	return runtimepipeline.WorkflowDiagnosticFixtureForTest{
		Context: ctx, Coordinator: pc,
		CommitConstruction: func(ctx context.Context, owner flowidentity.RunScopedFlowInstance, instance runtimepipeline.WorkflowInstance, at time.Time) {
			commitA2FixtureConstruction(t, pc, selected, ctx, owner, instance, at)
		},
		PublishHandler: func(ctx context.Context, event events.Event) error {
			bus.SetInterceptors(pc)
			return bus.PublishAndWait(ctx, event)
		},
		VerifyHandlerFailure: func(ctx context.Context, event events.Event, node string, failure runtimefailures.Envelope) error {
			logs, err := reader.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{RunID: runID, Limit: 100})
			if err != nil {
				return err
			}
			matches := 0
			for _, log := range logs.Logs {
				if log.Action != "handler_error" || log.EventID != event.ID() {
					continue
				}
				matches++
				if log.RunID != runID || log.Component != "workflow-runtime" || log.Level != "error" || log.EventType != string(event.Type()) ||
					log.EntityID != event.TargetRoute().EntityID || log.CanonicalDetail["node_id"] != node || log.Failure == nil || !reflect.DeepEqual(*log.Failure, failure) {
					return fmt.Errorf("persisted handler failure/context changed: %+v", log)
				}
				for _, key := range []string{"error", "error_code"} {
					if _, retired := log.CanonicalDetail[key]; retired {
						return fmt.Errorf("handler retained details.%s", key)
					}
				}
			}
			if matches != 1 {
				return fmt.Errorf("persisted handler diagnostics = %d, want one", matches)
			}
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(nodes[0].Node), Target: events.MustExistingEntityTarget(event.TargetRoute())}
			id, err := runtimedelivery.DeliveryID(event.ID(), route)
			if err != nil {
				return err
			}
			snapshot, err := selected.Snapshot(ctx, id)
			if err != nil {
				return err
			}
			if snapshot.Status != runtimedelivery.StatusDeadLetter || snapshot.Failure == nil || !reflect.DeepEqual(*snapshot.Failure, failure) {
				return fmt.Errorf("canonical handler settlement changed: %+v", snapshot)
			}
			return nil
		},
		VerifyFailure: func(ctx context.Context, action, diagnosticRunID, activationID, declarationKey string, failure runtimefailures.Envelope) error {
			logs, err := reader.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{RunID: diagnosticRunID, Limit: 100})
			if err != nil {
				return err
			}
			for _, log := range logs.Logs {
				if log.Action != action || log.CanonicalDetail["activation_id"] != activationID {
					continue
				}
				if log.RunID != diagnosticRunID || log.Component != "workflow-runtime" || log.Level != "error" || log.CanonicalDetail["declaration_key"] != declarationKey ||
					log.Failure == nil || !reflect.DeepEqual(*log.Failure, failure) {
					return fmt.Errorf("persisted timer failure/context changed: %+v, want %+v", log, failure)
				}
				if _, retired := log.CanonicalDetail["error"]; retired {
					return fmt.Errorf("persisted timer diagnostic retained details.error")
				}
				if _, retired := log.CanonicalDetail["error_code"]; retired {
					return fmt.Errorf("persisted timer diagnostic retained details.error_code")
				}
				return nil
			}
			return fmt.Errorf("persisted diagnostic %s/%s missing: %+v", action, activationID, logs.Logs)
		},
		Events: func(ctx context.Context) []events.Event {
			rows, err := reader.ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{RunID: runID}, Limit: 100, ExcludeRuntimeLogs: true})
			if err != nil {
				t.Fatal(err)
			}
			result := make([]events.Event, 0, len(rows.Events))
			for _, row := range rows.Events {
				prepared, found, err := selected.LoadPreparedPublishEvent(ctx, row.EventID)
				if err != nil || !found {
					t.Fatalf("load persisted diagnostic sibling event: found=%t err=%v", found, err)
				}
				result = append(result, prepared.Event.Event())
			}
			return result
		},
	}
}

func TestWorkflowTimerFailureDiagnosticsPersistOnBothStores(t *testing.T) {
	runtimepipeline.VerifyWorkflowTimerFailureDiagnosticsPersistOnBothStoresForTest(t, newWorkflowDiagnosticNativeFixture)
}

func TestWorkflowHandlerFailureDiagnosticPersistsOnBothStores(t *testing.T) {
	runtimepipeline.VerifyWorkflowHandlerFailureDiagnosticPersistsOnBothStoresForTest(t, newWorkflowDiagnosticNativeFixture)
}
