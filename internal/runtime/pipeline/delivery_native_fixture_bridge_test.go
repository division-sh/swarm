package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

// This import-cycle bridge carries only original native domain roles and exact
// setup operations. It has no database, adapter or transaction implementation.
type PipelineDeliveryNativeFixtureForTest struct {
	WorkflowHandlerNativeFixtureForTest
	Store                             deliverylifecycle.Store
	Authority                         deliverylifecycle.ExecutionAuthority
	Cards                             decisioncard.Store
	HumanTasks                        decisioncard.HumanTaskStore
	ObserveHumanRequester             func(context.Context, string)
	CreateHumanReply                  func(context.Context, string, string, string) error
	HumanExpiry                       HumanTaskExpiry
	ApplyDecision                     func(context.Context, decisioncard.DecideRequest) (decisioncard.DecisionOutcome, error)
	ApplyDeferral                     func(context.Context, decisioncard.DeferRequest) (decisioncard.DecisionOutcome, error)
	AdmitSchedule                     func(context.Context, runtimegenericschedule.AdmissionCommand) (runtimegenericschedule.AdmissionCommit, error)
	RemoveTimer                       func(context.Context, string, string) error
	SetTimerForeignDeclaration        func(context.Context, string, string) error
	SetTimerMalformedName             func(context.Context, string, string) error
	SetTimerForeignRouteMalformedName func(context.Context, string, string) error
	SetCardInsertFault                func(context.Context, string, bool) error
	HideMutationTable                 func(context.Context, bool) error
	ProbeMutationMissingDomainPath    func(context.Context, string, string) error
	NumericFlowPath                   func(context.Context, string, string) (int64, error)
	AmbiguousFields                   func(context.Context, string, string) (int64, error)
	ActiveTerminatedTimestamp         func(context.Context, string, string, time.Time) (int64, error)
	AdmitAttachment                   func(context.Context, string, string) (DynamicFlowRuntimeActivationAttempt, DynamicFlowRuntimeReadinessPlan, func())
	Continuations                     *deliverycontinuation.Coordinator
	PublishNode                       func(context.Context, events.Event, events.DeliveryRoute) error
	RetryEligible                     func(context.Context, events.Event, events.DeliveryRoute) error
	EventCount                        func(context.Context, string, string) (int, error)
	EventIDCount                      func(context.Context, string) int
	DeliveryCount                     func(context.Context, string, string) (int, error)
	NodeDeliverySnapshot              func(context.Context, string, string, string) (deliverylifecycle.Snapshot, error)
	SelectionFact                     func(context.Context, string, string) (handlerselection.HandlerRuleSelectionFact, int, error)
	Transactions                      func() PipelineDeliveryNativeTransactionCountsForTest
	ConstructInitial                  func(context.Context, WorkflowInstance, WorkflowLifecycleMutationPlan) (CommittedWorkflowLifecycleMutation, error)
	ReopenProjection                  func() WorkflowPersistence
	ReopenExecution                   func() *PipelineDeliveryNativeFixtureForTest
	TransitionWire                    func(context.Context, string, string) (json.RawMessage, error)
	SetTransitionWire                 func(context.Context, string, string, json.RawMessage) (int64, error)
	JoinExecution                     func(context.Context) error
	HoldUnstampedAdmissionTransaction func(context.Context, string) (func() error, error)
	ContainedFootprint                func(context.Context, string, string, string) (PipelineContainedFootprintForTest, error)
	EntityStateOwner                  func(context.Context, string) (string, error)
	MutationHistory                   func(context.Context, string, string) []PipelineNativeMutationRowForTest
	TrackedMutationProjection         func(context.Context, string, string) (mutationlog.EntityStateProjection, []mutationlog.ProjectionMutation, error)
	MutationCount                     func(context.Context, string, string, string, string, string) (int, error)
	AccumulatorHistory                func(context.Context, string, string) ([]mutationlog.ProjectionMutation, error)
	FanOutStorage                     func(context.Context, string, string, identity.ExecutableNode, contracts.FanOutElementRef) ([]PipelineFanOutStorageForTest, error)
	FanOutIntentCount                 func(context.Context, string) (int, error)
}

type PipelineFanOutStorageForTest struct {
	DeliveryID, SemanticDigest, Status string
	Capsule                            json.RawMessage
	Source                             fanoutobligation.SourceRef
	Cursor, Cardinality                int
}

type PipelineContainedFootprintForTest struct {
	Deliveries, Headers, EntitiesByID, EntitiesByPath int
}

type PipelineNativeMutationRowForTest struct {
	Domain, Path, WriterType, WriterID, HandlerStep string
	CausedByEvent                                   string
	OldValue, NewValue                              json.RawMessage
}

type PipelineDeliveryNativeTransactionCountsForTest struct {
	WorkflowCommits, Claims, Renewals, Settlements uint64
	WorkflowRollbacks                              uint64
	OtherCommits                                   uint64
	Active                                         uint64
}

type pipelineDeliveryNativeOpenerForTest func(*testing.T, string, semanticview.Source) *PipelineDeliveryNativeFixtureForTest

func VerifyNativePipelineDeliveryReopenUsesFreshOccurrenceAndOriginalReceiptsForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, pc, ctx, event, route := nativeReceiverPreparationFixtureForTest(t, backend, open)
			id, err := deliverylifecycle.DeliveryID(event.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			before, err := fixture.Store.Snapshot(ctx, id)
			if err != nil || before.Status != deliverylifecycle.StatusPending || before.ClaimVersion != 0 {
				t.Fatalf("predecessor pending receipt: %+v/%v", before, err)
			}
			next := fixture.ReopenExecution()
			if next.Store == fixture.Store || next.Authority != fixture.Authority {
				t.Fatal("reopen reconstructed its store or changed the exact component authority")
			}
			nextPC, nextCtx := nativePipelineDeliveryCoordinatorForTest(t, next, pc.module)
			nextCtx = correlation.WithRunID(nextCtx, event.RunID())
			if nextPC.workOwner == pc.workOwner {
				t.Fatal("reopened execution reused its retired occurrence")
			}
			nextWork, ok := nextPC.workOwner.(*worklifetime.RuntimeOccurrence)
			if !ok {
				t.Fatal("reopened execution did not retain its native runtime occurrence")
			}
			if lease, err := pc.workOwner.Begin(ctx); !errors.Is(err, worklifetime.ErrRetired) || lease != nil {
				t.Fatalf("retired predecessor still admits work: %v/%v", lease, err)
			}
			if err := fixture.PublishNode(ctx, event, route); err == nil {
				t.Fatal("retired predecessor still publishes")
			}
			after, err := next.Store.Snapshot(nextCtx, id)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("reopen mutated the pending exact receipt: %+v/%v", after, err)
			}
			// Durable readback is a receipt, not transfer of live authority. Let
			// the original recovery owner admit and join the successor carrier.
			recoverNativePipelineRetryForTest(t, next, nextCtx)
			beforeReplay := next.Transactions()
			for attempt := 0; attempt < 2; attempt++ {
				if err := next.PublishNode(nextCtx, event, route); err != nil {
					t.Fatalf("native reopened publication replay: %v", err)
				}
			}
			join, cancel := context.WithTimeout(nextCtx, 5*time.Second)
			defer cancel()
			if err := next.JoinExecution(join); err != nil {
				t.Fatal(err)
			}
			if nextWork.ActiveCount() == 0 {
				t.Fatal("reopened publication replay lost its standing recovery owner")
			}
			// Transient quiescence does not stop recovery's selected-store scans.
			if err := next.Continuations.Retire(join); err != nil {
				t.Fatal(err)
			}
			if active := nextWork.ActiveCount(); active != 0 {
				t.Fatalf("reopen counter observation preceded the standing-worker join: active=%d", active)
			}
			settled, err := next.Store.Snapshot(nextCtx, id)
			outcomes, outcomesErr := next.Store.Outcomes(nextCtx, id)
			afterReplay := next.Transactions()
			if err != nil || outcomesErr != nil || settled.Status != deliverylifecycle.StatusDelivered || settled.ClaimVersion != 1 || len(outcomes) != 1 || afterReplay.Active != 0 || afterReplay.Claims != beforeReplay.Claims {
				t.Fatalf("reopen repeated execution or escaped joining: settled=%+v outcomes=%v read_errors=%v/%v counters_before=%+v counters_after=%+v", settled, outcomes, err, outcomesErr, beforeReplay, afterReplay)
			}
		})
	}
}

func VerifyNativePipelineAdmissionTransactionCutHasClosedLifetimeForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, _, ctx, event, _ := nativeReceiverPreparationFixtureForTest(t, backend, open)
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if closeCut, err := fixture.HoldUnstampedAdmissionTransaction(cancelled, event.RunID()); !errors.Is(err, context.Canceled) || closeCut != nil {
				t.Fatalf("cancelled transaction cut admitted: %v", err)
			}
			if closeCut, err := fixture.HoldUnstampedAdmissionTransaction(ctx, uuid.NewString()); err == nil || closeCut != nil {
				t.Fatal("foreign run obtained the native transaction cut")
			}
			if fixture.Transactions().Active != 0 {
				t.Fatal("refused native cut retained a transaction")
			}
			active, cancelActive := context.WithCancel(ctx)
			defer cancelActive()
			closeCut, err := fixture.HoldUnstampedAdmissionTransaction(active, event.RunID())
			if err != nil {
				t.Fatal(err)
			}
			if fixture.Transactions().Active != 1 {
				t.Fatal("native transaction cut did not reach the original coordinator")
			}
			cancelActive()
			if err := closeCut(); err != nil && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if fixture.Transactions().Active != 0 {
				t.Fatal("cancelled native cut did not join before close")
			}
		})
	}
}

func nativePipelineDeliveryCoordinatorForTest(t *testing.T, fixture *PipelineDeliveryNativeFixtureForTest, module WorkflowModule) (*PipelineCoordinator, context.Context) {
	t.Helper()
	pc := fixture.NewCoordinator(PipelineCoordinatorOptions{Module: module})
	ctx, err := pc.receiverExecution.Bind(fixture.Context, executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	return pc, ctx
}

func VerifyPipelineDeliveryNativeRecipePreservesExactStoreAndAuthorityForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			bundle := deliveryAuthoritySourceForTest(t)
			fixture := open(t, backend, semanticview.Wrap(bundle))
			module := handlerTestWorkflowModuleWithBundle(bundle, ".", "node-a")
			nodes, err := LoadWorkflowNodes(semanticview.Wrap(bundle))
			if err != nil {
				t.Fatal(err)
			}
			module.(*previewWorkflowModule).workflowNodes = nodes
			pc, ctx := nativePipelineDeliveryCoordinatorForTest(t, fixture, module)
			run := uuid.NewString()
			ctx = correlation.WithRunID(ctx, run)
			if err := fixture.RequireRun(ctx, run); err != nil {
				t.Fatal(err)
			}
			if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: run, StorageRef: run, EntityID: run, EntityType: "test_entity", WorkflowName: ".",
				WorkflowVersion: bundle.WorkflowVersion(), CurrentState: "queued", Fields: map[string]any{},
			})); err != nil {
				t.Fatal(err)
			}
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "source.evt", "src", "", []byte(`{}`), 0, run, events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: ".", FlowInstance: run, EntityID: run}), time.Now().UTC())
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(pipelineNode(t, ".", "node-a")), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: run, EntityID: run})}
			if missing, err := fixture.Store.ClaimDelivery(ctx, fixture.Authority, event, route); err != nil || missing.Disposition != deliverylifecycle.ClaimAbsent {
				t.Fatalf("unpublished native delivery was claimable: %s/%v", missing.Disposition, err)
			}
			if err := fixture.PublishNode(ctx, event, route); err != nil {
				t.Fatal(err)
			}
			id, err := deliverylifecycle.DeliveryID(event.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			before, err := fixture.Store.Snapshot(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			foreign, err := deliverylifecycle.NewNormalExecutionAuthority(fixture.Authority.SourceArtifact(), "foreign-native-pipeline", 1)
			if err != nil {
				t.Fatal(err)
			}
			if rejected, err := fixture.Store.ClaimDelivery(ctx, foreign, event, route); err != nil || rejected.Disposition != deliverylifecycle.ClaimWrongAuthority {
				t.Fatalf("foreign authority changed native delivery: %s/%v", rejected.Disposition, err)
			}
			after, err := fixture.Store.Snapshot(ctx, id)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("rejected claim mutated exact native obligation: %v", err)
			}
			if handled, err := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, route), event); err != nil || !handled {
				t.Fatalf("original native node execution failed: handled=%t error=%v", handled, err)
			}
			settled, err := fixture.Store.Snapshot(ctx, id)
			outcomes, outcomesErr := fixture.Store.Outcomes(ctx, id)
			if err != nil || outcomesErr != nil || settled.Status != deliverylifecycle.StatusDelivered || settled.ClaimVersion != 1 || len(outcomes) != 1 || outcomes[0].Outcome != "delivered" {
				t.Fatalf("native exact-once settlement failed: %+v/%v/%v/%v", settled, outcomes, err, outcomesErr)
			}
			instance, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, run))
			if err != nil || !found || instance.CurrentState != "done" || len(instance.TransitionHistory) != 1 {
				t.Fatalf("native business mutation did not share the delivery owner: %+v/%v/%v", instance, found, err)
			}
			if handled, err := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, route), event); err != nil || !handled {
				t.Fatalf("terminal native replay failed: %v/%v", handled, err)
			}
			outcomes, err = fixture.Store.Outcomes(ctx, id)
			if err != nil || len(outcomes) != 1 || fixture.Transactions().Active != 0 {
				t.Fatalf("native replay repeated settlement or escaped teardown: %v/%v", outcomes, err)
			}
		})
	}
}

func VerifyNativePipelineTransitionEvidenceFaultPreservesOtherCoordinatesForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, pc, ctx, event, route := nativeReceiverPreparationFixtureForTest(t, backend, open)
			if handled, err := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, route), event); err != nil || !handled {
				t.Fatalf("native transition did not execute: %t/%v", handled, err)
			}
			run, path := event.RunID(), route.Target.Route().FlowInstance
			original, err := fixture.TransitionWire(ctx, run, path)
			if err != nil {
				t.Fatal(err)
			}
			for _, fault := range []string{"other_coordinate", "missing_history", "foreign_run", "cancelled"} {
				t.Run(fault, func(t *testing.T) {
					var config map[string]any
					if err := json.Unmarshal(original, &config); err != nil {
						t.Fatal(err)
					}
					faultRun, faultCtx := run, ctx
					switch fault {
					case "other_coordinate":
						config["workflow_version"] = "foreign"
					case "missing_history":
						config["transition_history"] = []any{}
					case "foreign_run":
						faultRun = uuid.NewString()
					case "cancelled":
						var cancel context.CancelFunc
						faultCtx, cancel = context.WithCancel(ctx)
						cancel()
					}
					wire, err := json.Marshal(config)
					if err != nil {
						t.Fatal(err)
					}
					changed, err := fixture.SetTransitionWire(faultCtx, faultRun, path, wire)
					if changed != 0 || err == nil || (fault == "cancelled" && !errors.Is(err, context.Canceled)) {
						t.Fatalf("unowned projection change was not refused: changed=%d error=%v", changed, err)
					}
					after, err := fixture.TransitionWire(ctx, run, path)
					if err != nil || !reflect.DeepEqual(original, after) {
						t.Fatalf("refused wire fault changed the original projection: %v", err)
					}
				})
			}
			projection := fixture.ReopenProjection()
			instance, found, err := projection.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, path))
			if err != nil || !found {
				t.Fatalf("joined predecessor reopen lost its native projection: %t/%v", found, err)
			}
			if err := fixture.Construct(ctx, instance); err == nil || !strings.Contains(err.Error(), "reopened native projection") {
				t.Fatalf("read-only reopened projection admitted predecessor construction: %v", err)
			}
			if _, err := fixture.ConstructInitial(ctx, instance, WorkflowLifecycleMutationPlan{}); err == nil || !strings.Contains(err.Error(), "reopened native projection") {
				t.Fatalf("read-only reopened projection admitted predecessor lifecycle construction: %v", err)
			}
			before, err := fixture.EventCount(ctx, run, string(event.Type()))
			if err != nil {
				t.Fatal(err)
			}
			if err := fixture.PublishNode(ctx, event, route); err == nil {
				t.Fatal("read-only reopened projection admitted predecessor publication")
			}
			after, err := fixture.EventCount(ctx, run, string(event.Type()))
			if err != nil || before != after {
				t.Fatalf("refused predecessor publication changed durable events: %d/%d/%v", before, after, err)
			}
		})
	}
}
