package runtimepersistence

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// Native publication/mutation composition, not public-tool or provider proof.
func TestWorkflowPublicationAcceptanceStageBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			for _, cut := range []string{"healthy", "rollback", "lost_ack"} {
				t.Run(cut, func(t *testing.T) {
					f := newReceiverComposedFixture(t, backend)
					mutation := f.prepareEngine(t, []string{"one"}, "feedback")
					command := f.plans[0].(bus.EnginePublicationPlan).PublicationCommand()
					command.StageFeedback = &pipeline.WorkflowPublicationStageRequest{Instance: mutation.State.Identity, EntityID: mutation.State.EntityID}
					owner := f.raw.(interface {
						CommitPublication(context.Context, bus.PublicationCommand) (bus.CommittedPublication, error)
						ReadWorkflowPublicationStages(context.Context, string, flowidentity.RunScopedFlowInstance) (pipeline.WorkflowPublicationStageEvidence, bool, error)
					})
					wrong := command
					foreign := *command.StageFeedback
					foreign.EntityID = "unrelated"
					wrong.StageFeedback = &foreign
					if result, err := owner.CommitPublication(f.ctx, wrong); err == nil || result.Acknowledged || result.AcceptedStage != nil {
						t.Fatalf("unrelated source acquired acceptance evidence: %+v err=%v", result, err)
					}
					injected := errors.New("publication stage acknowledgment lost")
					if cut != "healthy" {
						f.connector.arm(func(tx driver.Tx) error {
							if cut == "rollback" {
								return errors.Join(injected, tx.Rollback())
							}
							return errors.Join(injected, tx.Commit())
						})
					}
					accepted, err := owner.CommitPublication(f.ctx, command)
					if cut != "healthy" {
						if !errors.Is(err, injected) || accepted.Acknowledged || accepted.AcceptedStage != nil {
							t.Fatalf("unknown acknowledgment invented stage evidence: %+v err=%v", accepted, err)
						}
						accepted, err = owner.CommitPublication(f.ctx, command)
						wantOutcome := bus.EventAppendInserted
						if cut == "lost_ack" {
							wantOutcome = bus.EventAppendExactDuplicate
						}
						if accepted.AppendOutcome != wantOutcome {
							t.Fatalf("exact resolution replayed publication: %+v err=%v", accepted, err)
						}
					}
					if err != nil || !accepted.Acknowledged || accepted.AcceptedStage == nil || accepted.AcceptedStage.Validate() != nil {
						t.Fatalf("publication omitted exact acceptance stage: %+v err=%v", accepted, err)
					}
					stage := accepted.AcceptedStage.Stage()
					if stage.Instance != mutation.State.Identity || stage.EntityID != mutation.State.EntityID || stage.Revision != mutation.State.ExpectedRevision {
						t.Fatalf("receiver stage replaced the author's own stage: %+v", stage)
					}
					mutation.Publications = nil
					advanced, err := f.raw.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(f.ctx, mutation)
					if err != nil || !advanced.Committed || advanced.Stage.Revision != stage.Revision+1 {
						t.Fatalf("advance author after publication: %+v err=%v", advanced, err)
					}
					repeated, err := owner.CommitPublication(f.ctx, command)
					if err != nil || !repeated.Acknowledged || repeated.AppendOutcome != bus.EventAppendExactDuplicate || repeated.AcceptedStage == nil || *repeated.AcceptedStage != *accepted.AcceptedStage {
						t.Fatalf("duplicate reloaded a later author stage: %+v err=%v", repeated, err)
					}
					readback, found, err := owner.ReadWorkflowPublicationStages(f.ctx, command.Commit.Event.ID(), command.StageFeedback.Instance)
					if err != nil || !found || readback.Validate() != nil || readback.Acceptance != *accepted.AcceptedStage || len(readback.Handlers) != 0 {
						t.Fatalf("publication snapshot inferred execution from acceptance: %+v found=%t err=%v", readback, found, err)
					}
				})
			}
		})
	}
}
