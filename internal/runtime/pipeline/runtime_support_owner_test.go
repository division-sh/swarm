package pipeline

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

func TestQueuedOwnerActionIgnoresCallerCancellationAndFollowsOwnerRetirement(t *testing.T) {
	owner := pipelineTestWorkOwner(t)
	postCommit := make([]OwnerAction, 0, 2)
	rollback := make([]OwnerAction, 0, 2)
	callerCtx, cancelCaller := context.WithCancel(worklifetime.WithOccurrence(context.Background(), owner))
	callerCtx = withPipelinePostCommitActions(callerCtx, &postCommit)
	callerCtx = withPipelineRollbackActions(callerCtx, &rollback)

	callerResult := make(chan error, 1)
	if !queuePipelinePostCommitAction(callerCtx, func(ctx context.Context) {
		callerResult <- ctx.Err()
	}) {
		t.Fatal("queue caller-cancellation proof action")
	}
	cancelCaller()
	flushPipelinePostCommitActions(postCommit[:1])
	if err := <-callerResult; err != nil {
		t.Fatalf("queued action inherited caller cancellation: %v", err)
	}

	ownerResult := make(chan error, 1)
	if !queuePipelinePostCommitAction(callerCtx, func(ctx context.Context) {
		<-ctx.Done()
		ownerResult <- ctx.Err()
	}) {
		t.Fatal("queue owner-retirement proof action")
	}
	owner.Retire()
	flushPipelinePostCommitActions(postCommit[1:])
	if err := <-ownerResult; err != context.Canceled {
		t.Fatalf("queued action retirement error = %v, want context.Canceled", err)
	}
}

func TestQueuedOwnerActionFamiliesFollowStandingRetirementExactlyOnce(t *testing.T) {
	tests := []struct {
		name  string
		queue func(context.Context, OwnerAction) bool
		flush func([]OwnerAction)
		pick  func([]OwnerAction, []OwnerAction, []OwnerAction) []OwnerAction
	}{
		{name: "post-commit", queue: queuePipelinePostCommitAction, flush: flushPipelinePostCommitActions, pick: func(postCommit, _, _ []OwnerAction) []OwnerAction { return postCommit }},
		{name: "rollback", queue: queuePipelineRollbackAction, flush: flushPipelineRollbackActions, pick: func(_, rollback, _ []OwnerAction) []OwnerAction { return rollback }},
		{name: "after-publish", queue: queuePipelineAfterPublishAction, flush: flushPipelineAfterPublishActions, pick: func(_, _, afterPublish []OwnerAction) []OwnerAction { return afterPublish }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			process := worklifetime.NewProcess()
			runtimeOwner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
				RuntimeInstanceID: "owner-action-runtime-" + tc.name,
				BundleHash:        "owner-action-bundle",
			})
			if err != nil {
				t.Fatalf("new runtime occurrence: %v", err)
			}
			standing, err := runtimeOwner.NewStanding(context.Background(), worklifetime.StandingIdentity{
				ServiceID: "owner-action-service-" + tc.name, RunID: "owner-action-run", Generation: 1,
			})
			if err != nil {
				t.Fatalf("new standing occurrence: %v", err)
			}
			postCommit := make([]OwnerAction, 0, 1)
			rollback := make([]OwnerAction, 0, 1)
			afterPublish := make([]OwnerAction, 0, 1)
			callerCtx, cancelCaller := context.WithCancel(worklifetime.WithOccurrence(context.Background(), standing))
			callerCtx = withPipelinePostCommitActions(callerCtx, &postCommit)
			callerCtx = withPipelineRollbackActions(callerCtx, &rollback)
			callerCtx = withPipelineAfterPublishActions(callerCtx, &afterPublish)

			started := make(chan error, 1)
			result := make(chan error, 1)
			var calls atomic.Int32
			if !tc.queue(callerCtx, func(actionCtx context.Context) {
				calls.Add(1)
				started <- actionCtx.Err()
				<-actionCtx.Done()
				result <- actionCtx.Err()
			}) {
				t.Fatalf("queue %s action", tc.name)
			}
			cancelCaller()
			flushed := make(chan struct{})
			go func() {
				tc.flush(tc.pick(postCommit, rollback, afterPublish))
				close(flushed)
			}()
			if err := <-started; err != nil {
				t.Fatalf("%s action inherited caller cancellation: %v", tc.name, err)
			}
			standing.Retire()
			if err := <-result; err != context.Canceled {
				t.Fatalf("%s action retirement error = %v, want context.Canceled", tc.name, err)
			}
			select {
			case <-flushed:
			case <-time.After(time.Second):
				t.Fatalf("%s action did not settle", tc.name)
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("%s action calls = %d, want 1", tc.name, got)
			}
			if err := standing.RetireAndWait(context.Background()); err != nil {
				t.Fatalf("retire standing occurrence: %v", err)
			}
			if _, err := runtimeOwner.RetireAndWait(context.Background()); err != nil {
				t.Fatalf("retire runtime occurrence: %v", err)
			}
			if _, err := process.Join(context.Background()); err != nil {
				t.Fatalf("join process: %v", err)
			}
		})
	}
}

func TestQueuedOwnerActionDropsMutationScopedSQLCapabilities(t *testing.T) {
	owner := pipelineTestWorkOwner(t)
	postCommit := make([]OwnerAction, 0, 1)
	rollback := make([]OwnerAction, 0, 1)
	ctx := worklifetime.WithOccurrence(context.Background(), owner)
	ctx = WithPipelineSQLTxContext(ctx, &sql.Tx{})
	ctx = WithPipelineSQLConnContext(ctx, &sql.Conn{})
	ctx = withPipelinePostCommitActions(ctx, &postCommit)
	ctx = withPipelineRollbackActions(ctx, &rollback)

	result := make(chan error, 1)
	if !queuePipelinePostCommitAction(ctx, func(actionCtx context.Context) {
		if _, ok := PipelineSQLTxFromContext(actionCtx); ok {
			result <- context.Canceled
			return
		}
		if _, ok := PipelineSQLConnFromContext(actionCtx); ok {
			result <- context.DeadlineExceeded
			return
		}
		result <- nil
	}) {
		t.Fatal("queue SQL capability projection proof action")
	}
	flushPipelinePostCommitActions(postCommit)
	if err := <-result; err != nil {
		t.Fatalf("queued action retained mutation-scoped SQL capability: %v", err)
	}
}
