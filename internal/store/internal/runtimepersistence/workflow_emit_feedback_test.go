package runtimepersistence

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

func TestWorkflowEmitFeedbackBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, cut := range []string{"healthy", "rollback", "lost_ack"} {
				t.Run(cut, func(t *testing.T) {
					f := newReceiverComposedFixture(t, backend)
					mutation := f.prepareEngine(t, []string{"one"}, "emit-feedback")
					command := f.plans[0].(bus.EnginePublicationPlan).PublicationCommand()
					command.StageFeedback = &pipeline.WorkflowPublicationStageRequest{Instance: mutation.State.Identity, EntityID: mutation.State.EntityID}
					publication, err := f.raw.(bus.CommitPublicationOwner).CommitPublication(f.ctx, command)
					if err != nil || !publication.Acknowledged || publication.AcceptedStage == nil {
						t.Fatalf("accept publication: %+v err=%v", publication, err)
					}
					owner, ok := f.raw.(pipeline.WorkflowEmitFeedbackOwner)
					if !ok {
						t.Fatal("canonical publication owner does not retain emit feedback")
					}
					candidate := pipeline.WorkflowEmitFeedback{Receipt: *publication.AcceptedStage, StageOrigin: pipeline.EmitStageAcceptance, Dispatch: pipeline.EmitDispatchQueued}
					invalid := candidate
					invalid.StageOrigin = pipeline.EmitStageHandler
					invalid.Dispatch = pipeline.EmitDispatchReturned
					if result, err := owner.CommitWorkflowEmitFeedback(f.ctx, invalid); err == nil || result.Acknowledged {
						t.Fatalf("acceptance fabricated handler execution: %+v err=%v", result, err)
					}
					injected := errors.New("emit feedback acknowledgment lost")
					if cut != "healthy" {
						f.connector.arm(func(tx driver.Tx) error {
							if cut == "rollback" {
								return errors.Join(injected, tx.Rollback())
							}
							return errors.Join(injected, tx.Commit())
						})
					}
					result, err := owner.CommitWorkflowEmitFeedback(f.ctx, candidate)
					if cut != "healthy" {
						if !errors.Is(err, injected) || result.Acknowledged || result.Feedback != (pipeline.WorkflowEmitFeedback{}) {
							t.Fatalf("unknown feedback commit invented acknowledgment: %+v err=%v", result, err)
						}
						result, err = owner.CommitWorkflowEmitFeedback(f.ctx, candidate)
					}
					if err != nil || !result.Acknowledged || result.Feedback != candidate || result.Replay != (cut == "lost_ack") {
						t.Fatalf("exact feedback resolution: %+v err=%v", result, err)
					}
					mutation.Publications = nil
					advanced, err := f.raw.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(f.ctx, mutation)
					if err != nil || !advanced.Committed {
						t.Fatalf("advance source: %+v err=%v", advanced, err)
					}
					later := candidate
					later.Dispatch = pipeline.EmitDispatchReturned
					repeated, err := owner.CommitWorkflowEmitFeedback(f.ctx, later)
					if err != nil || !repeated.Acknowledged || !repeated.Replay || repeated.Feedback != candidate {
						t.Fatalf("retry changed the original queued result: %+v err=%v", repeated, err)
					}
					readback, found, err := owner.ReadWorkflowEmitFeedback(f.ctx, command.Commit.Event.ID(), mutation.State.Identity)
					if err != nil || !found || readback != candidate {
						t.Fatalf("feedback readback substituted current stage: %+v found=%t err=%v", readback, found, err)
					}
					forged := advanced.Stage
					forgedReceipt, err := pipelineobligation.StageReceiptEvidence(command.Commit.Event.ID(), forged)
					if err != nil {
						t.Fatal(err)
					}
					later.Receipt = forgedReceipt
					if result, err := owner.CommitWorkflowEmitFeedback(f.ctx, later); err == nil || result.Acknowledged {
						t.Fatalf("later header substituted occurrence evidence: %+v err=%v", result, err)
					}
				})
			}
		})
	}
}

func TestWorkflowEmitFeedbackConcurrentBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverComposedFixture(t, backend)
			mutation := f.prepareEngine(t, []string{"one"}, "emit-contended")
			command := f.plans[0].(bus.EnginePublicationPlan).PublicationCommand()
			command.StageFeedback = &pipeline.WorkflowPublicationStageRequest{Instance: mutation.State.Identity, EntityID: mutation.State.EntityID}
			publication, err := f.raw.(bus.CommitPublicationOwner).CommitPublication(f.ctx, command)
			if err != nil || !publication.Acknowledged || publication.AcceptedStage == nil {
				t.Fatalf("accept publication: %+v err=%v", publication, err)
			}
			owner := f.raw.(pipeline.WorkflowEmitFeedbackOwner)
			type response struct {
				result pipeline.WorkflowEmitFeedbackCommit
				err    error
			}
			start := make(chan struct{})
			responses := make(chan response, 2)
			for _, disposition := range []pipeline.EmitDispatchDisposition{pipeline.EmitDispatchQueued, pipeline.EmitDispatchError} {
				go func(disposition pipeline.EmitDispatchDisposition) {
					<-start
					result, err := owner.CommitWorkflowEmitFeedback(f.ctx, pipeline.WorkflowEmitFeedback{
						Receipt: *publication.AcceptedStage, StageOrigin: pipeline.EmitStageAcceptance, Dispatch: disposition,
					})
					responses <- response{result: result, err: err}
				}(disposition)
			}
			close(start)
			first, second := <-responses, <-responses
			if first.err != nil || second.err != nil || !first.result.Acknowledged || !second.result.Acknowledged || first.result.Feedback != second.result.Feedback || first.result.Replay == second.result.Replay {
				t.Fatalf("concurrent replies did not resolve one immutable result: %+v / %+v", first, second)
			}
			readback, found, err := owner.ReadWorkflowEmitFeedback(f.ctx, command.Commit.Event.ID(), mutation.State.Identity)
			if err != nil || !found || readback != first.result.Feedback {
				t.Fatalf("contended durable readback: %+v found=%t err=%v", readback, found, err)
			}
			instance, found, err := f.raw.(pipeline.WorkflowInstancePersistenceReader).LoadWorkflowInstance(f.ctx, mutation.State.Identity)
			if err != nil || !found || instance.CurrentState != mutation.State.ExpectedState || instance.Revision != mutation.State.ExpectedRevision {
				t.Fatalf("presentation writer mutated workflow state: %+v found=%t err=%v", instance, found, err)
			}
		})
	}
}

type emitStageCommitInterceptor struct {
	owner      pipeline.WorkflowEngineMutationOwner
	deliveries deliveryFixtureStore
	command    pipeline.WorkflowEngineMutationCommand
	eventID    string
	calls      int
	fault      error
}

func (*emitStageCommitInterceptor) Intercept(context.Context, events.Event) (bool, []events.Event, pipelineobligation.ExecutionOutcome, error) {
	return true, nil, pipelineobligation.Continue(), nil
}

func (i *emitStageCommitInterceptor) InterceptDeliveryRoute(ctx context.Context, delivery events.DeliveryEvent, route events.DeliveryRoute) (bool, []events.Event, pipelineobligation.ExecutionOutcome, error) {
	if delivery.Event().ID() != i.eventID {
		return true, nil, pipelineobligation.Continue(), nil
	}
	i.calls++
	claimed, err := claimDeliveryFixture(ctx, i.deliveries, delivery.Event(), route)
	if err != nil {
		return false, nil, pipelineobligation.Continue(), err
	}
	command := i.command
	command.Publications = nil
	command.DeliverySuccess = &pipeline.WorkflowEngineDeliverySuccess{Claim: claimed.Claim, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection(), SideEffects: []string{"handler_completed"}}
	committed, err := i.owner.CommitWorkflowEngineMutation(ctx, command)
	if !committed.Committed {
		return false, nil, pipelineobligation.Continue(), err
	}
	outcome, stageErr := (pipelineobligation.ExecutionOutcome{Committed: true}).WithCommittedStage(delivery.Event().ID(), committed.Stage)
	return false, nil, outcome, errors.Join(err, stageErr, i.fault)
}

func TestWorkflowEmitHandlerFeedbackBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, cut := range []string{"healthy", "post_commit_error"} {
				t.Run(cut, func(t *testing.T) {
					f := newReceiverComposedFixture(t, backend)
					mutation := f.prepareEngine(t, []string{"one"}, "emit-handler")
					event := eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), "request", eventtest.Producer(events.EventProducerNode, mustPersistenceRootNode("fan-out-source").Key()), "", []byte(`{}`), 1, events.LineageFromEvent(f.parent), events.EventEnvelope{}, eventtest.RootRoutingSource(f.seed.runID), time.Now().UTC())
					interceptor := &emitStageCommitInterceptor{owner: f.raw.(pipeline.WorkflowEngineMutationOwner), deliveries: f.raw.(deliveryFixtureStore), command: mutation, eventID: event.ID()}
					if cut == "post_commit_error" {
						interceptor.fault = errors.New("acknowledged handler post-commit failure")
					}
					f.pub.SetInterceptors(interceptor)
					request := pipeline.WorkflowPublicationStageRequest{Instance: mutation.State.Identity, EntityID: mutation.State.EntityID}
					result, err := f.pub.PublishEmit(f.ctx, event, request)
					if (err != nil) != (cut == "post_commit_error") || !result.Accepted || !result.FeedbackCommitted || result.Validate() != nil || interceptor.calls != 1 || result.Feedback.StageOrigin != pipeline.EmitStageHandler || result.Feedback.Receipt.Stage().Revision != mutation.State.ExpectedRevision+1 {
						t.Fatalf("handler feedback: %+v calls=%d err=%v", result, interceptor.calls, err)
					}
					if cut == "post_commit_error" && result.Feedback.Dispatch != pipeline.EmitDispatchError {
						t.Fatal("post-commit failure discarded acknowledged stage")
					}
					later := mutation
					later.Publications, later.DeliverySuccess = nil, nil
					later.State.ExpectedRevision = result.Feedback.Receipt.Stage().Revision
					later.State.ExpectedState = result.Feedback.Receipt.Stage().Stage
					later.State.UpdatedAt = later.State.UpdatedAt.Add(time.Second)
					advanced, err := interceptor.owner.CommitWorkflowEngineMutation(f.ctx, later)
					if err != nil || !advanced.Committed {
						t.Fatalf("unrelated subsequent advance: %+v err=%v", advanced, err)
					}
					repeated, err := f.pub.PublishEmit(f.ctx, event, request)
					if (err != nil) != (cut == "post_commit_error") || !repeated.Accepted || !repeated.FeedbackCommitted || !repeated.Replay || repeated.Feedback != result.Feedback || interceptor.calls != 1 {
						t.Fatalf("exact retry re-executed or replaced handler evidence: %+v calls=%d err=%v", repeated, interceptor.calls, err)
					}
				})
			}
		})
	}
}

// Real bus/native-owner composition. This is not public-launcher or real-provider
// qualification, and an acceptance receipt is not credited as handler execution.
func TestWorkflowEmitBusReadbackBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverComposedFixture(t, backend)
			mutation := f.prepareEngine(t, []string{"one"}, "emit-bus")
			event := eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), "request", eventtest.Producer(events.EventProducerNode, mustPersistenceRootNode("fan-out-source").Key()), "", []byte(`{}`), 1, events.LineageFromEvent(f.parent), events.EventEnvelope{}, eventtest.RootRoutingSource(f.seed.runID), time.Now().UTC())
			request := pipeline.WorkflowPublicationStageRequest{Instance: mutation.State.Identity, EntityID: mutation.State.EntityID}
			accepted, err := f.pub.PublishEmit(f.ctx, event, request)
			if err != nil || !accepted.Accepted || !accepted.FeedbackCommitted || accepted.Validate() != nil || accepted.Feedback.StageOrigin != pipeline.EmitStageAcceptance {
				t.Fatalf("native emit feedback: %+v err=%v", accepted, err)
			}
			mutation.Publications = nil
			advanced, err := f.raw.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(f.ctx, mutation)
			if err != nil || !advanced.Committed || advanced.Stage.Revision <= accepted.Feedback.Receipt.Stage().Revision {
				t.Fatalf("advance after returned feedback: %+v err=%v", advanced, err)
			}
			repeated, err := f.pub.PublishEmit(f.ctx, event, request)
			if err != nil || !repeated.Accepted || !repeated.FeedbackCommitted || !repeated.Replay || repeated.Feedback != accepted.Feedback {
				t.Fatalf("exact emit retry changed feedback: %+v err=%v", repeated, err)
			}
			feedback, found, err := f.raw.(pipeline.WorkflowEmitFeedbackOwner).ReadWorkflowEmitFeedback(f.ctx, event.ID(), request.Instance)
			if err != nil || !found || feedback != accepted.Feedback {
				t.Fatalf("durable bus result readback: %+v found=%t err=%v", feedback, found, err)
			}
		})
	}
}

func TestWorkflowEmitEntitylessSourceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverComposedFixture(t, backend)
			mutation := f.prepareEngine(t, []string{"one"}, "entityless-emit")
			source, err := events.NewStaticFlowRoutingSource(events.RouteIdentity{FlowID: ".", FlowInstance: f.seed.runID})
			if err != nil {
				t.Fatal(err)
			}
			event := eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), "request", eventtest.Producer(events.EventProducerNode, mustPersistenceRootNode("fan-out-source").Key()), "", []byte(`{}`), 1, events.LineageFromEvent(f.parent), events.EventEnvelope{}, source, time.Now().UTC())
			request := pipeline.WorkflowPublicationStageRequest{Instance: mutation.State.Identity}
			wrong := request
			wrong.Instance.RunID = uuid.NewString()
			if result, err := f.pub.PublishEmit(f.ctx, event, wrong); err == nil || result.Accepted {
				t.Fatalf("entityless source borrowed another run: %+v err=%v", result, err)
			}
			result, err := f.pub.PublishEmit(f.ctx, event, request)
			if err != nil || !result.Accepted || !result.FeedbackCommitted || result.Feedback.Receipt.Stage().EntityID != mutation.State.EntityID || result.Feedback.StageOrigin != pipeline.EmitStageAcceptance || event.SourceRoute().EntityID != "" {
				t.Fatalf("entityless source did not consume its exact constructed header: %+v err=%v", result, err)
			}
		})
	}
}
