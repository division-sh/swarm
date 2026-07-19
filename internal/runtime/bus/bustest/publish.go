package bustest

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

type BeginPublishFunc func(context.Context, events.AdmittedEvent) (runtimebus.EventAppendOutcome, error)
type FinalizePublishFunc func(context.Context, runtimebus.CommitPublishRequest) error

// CommitPublish executes the sealed EventBus plan with a test-only transaction.
// It exists so behavior tests model the selected-store operation without
// acquiring a raw event writer or database transaction.
func CommitPublish(
	ctx context.Context,
	plan runtimebus.CommitPublishPlan,
	begin BeginPublishFunc,
	finalize FinalizePublishFunc,
) (runtimebus.PreparedPublish, error) {
	transaction := &Transaction{Begin: begin, Finalize: finalize}
	postCommit := make([]func(), 0, 4)
	rollback := make([]func(), 0, 4)
	ctx = runtimepipeline.WithPipelinePostCommitActions(ctx, &postCommit)
	ctx = runtimepipeline.WithPipelineRollbackActions(ctx, &rollback)
	prepared, err := plan.PrepareCommitPublish(runtimebus.WithCommitPublishTransaction(ctx, transaction))
	if err != nil {
		runtimepipeline.FlushPipelineRollbackActions(rollback)
		return runtimebus.PreparedPublish{}, err
	}
	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
	return prepared, nil
}

func CommitPublishNoop(ctx context.Context, plan runtimebus.CommitPublishPlan) (runtimebus.PreparedPublish, error) {
	return CommitPublish(ctx, plan, nil, nil)
}

type Transaction struct {
	Begin    BeginPublishFunc
	Finalize FinalizePublishFunc
	active   []string
}

func (t *Transaction) BeginPreparedPublish(ctx context.Context, prepared runtimebus.PreparedPublishEvent) (runtimebus.EventAppendOutcome, error) {
	outcome := runtimebus.EventAppendInserted
	var err error
	if t.Begin != nil {
		outcome, err = t.Begin(ctx, prepared.AdmittedEvent())
	}
	if err == nil && outcome == runtimebus.EventAppendInserted {
		t.active = append(t.active, prepared.AdmittedEvent().ID())
	}
	return outcome, err
}

func (t *Transaction) FinalizePreparedPublish(ctx context.Context, finalization runtimebus.PreparedPublishFinalization) error {
	req := finalization.Request()
	if len(t.active) == 0 || t.active[len(t.active)-1] != req.Event.ID() {
		return errors.New("prepared event finalization does not match the active event")
	}
	if t.Finalize != nil {
		if err := t.Finalize(ctx, req); err != nil {
			return err
		}
	}
	t.active = t.active[:len(t.active)-1]
	return nil
}
