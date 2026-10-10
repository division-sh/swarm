package pipeline

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"
)

type closedWorkflowMutationRefusalForTest struct {
	WorkflowEngineMutationOwner
	calls int
	err   error
}

func (s *closedWorkflowMutationRefusalForTest) CommitWorkflowEngineMutation(context.Context, WorkflowEngineMutationCommand) (CommittedWorkflowEngineMutation, error) {
	s.calls++
	return CommittedWorkflowEngineMutation{}, s.err
}

func VerifyNativeWorkflowMutationUsesOneSelectedCommitForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture, pc, ctx := nativeMutationLoggingFixtureForTest(t, backend, open)
			owner := testRunScopedWorkflowInstanceFromContext(ctx, runtimeRunID(ctx))
			before := fixture.Transactions()
			calls := 0
			if err := pc.workflowStore.mutate(ctx, owner, func(instance *WorkflowInstance) { calls++; instance.Fields["status"] = "ready" }); err != nil {
				t.Fatal(err)
			}
			after := fixture.Transactions()
			if calls != 1 || after.WorkflowCommits != before.WorkflowCommits+1 || after.Active != 0 {
				t.Fatalf("native mutation callbacks=%d counts=%+v -> %+v", calls, before, after)
			}
			loaded, found, err := pc.workflowStore.Load(ctx, owner)
			if err != nil || !found || loaded.Fields["status"] != "ready" {
				t.Fatalf("native state write=%+v found=%t err=%v", loaded, found, err)
			}
			assertNativeTrackedMutationProjectionForTest(t, fixture, ctx, runtimeRunID(ctx), "authored_field:status")
		})
	}
}

func VerifyNativeWorkflowMutationDoesNotRetryClosedOwnerRefusalForTest(t *testing.T, backend string, open pipelineDeliveryNativeOpenerForTest) {
	fixture, pc, ctx := nativeMutationLoggingFixtureForTest(t, backend, open)
	owner := testRunScopedWorkflowInstanceFromContext(ctx, runtimeRunID(ctx))
	before, err := fixture.PhysicalCounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("SQLITE_BUSY: database is locked")
	refusal := &closedWorkflowMutationRefusalForTest{WorkflowEngineMutationOwner: pc.workflowStore.engineMutations, err: sentinel}
	pc.workflowStore.engineMutations = refusal
	calls := 0
	err = pc.workflowStore.mutate(ctx, owner, func(instance *WorkflowInstance) { calls++; instance.Fields["status"] = "must_not_commit" })
	if !errors.Is(err, sentinel) || calls != 1 || refusal.calls != 1 {
		t.Fatalf("native refusal err=%v callbacks=%d commits=%d, want unchanged error and no retry", err, calls, refusal.calls)
	}
	after, err := fixture.PhysicalCounts(ctx)
	if err != nil || after != before {
		t.Fatalf("refused native mutation changed persistence: %+v -> %+v err=%v", before, after, err)
	}
}

func VerifyNativeWorkflowPersistenceHasNoRawTransactionProtocolForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	paths, err := filepath.Glob("*.go")
	if err != nil || len(paths) == 0 {
		t.Fatalf("enumerate pipeline sources: %v", err)
	}
	for _, path := range paths {
		source, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range source.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Recv == nil {
				continue
			}
			switch function.Name.Name {
			case "runPipelineMutation", "runInPipelineTransaction", "runInPipelineTransactionAcknowledged", "RunRuntimeMutationContext", "RunRuntimeMutationContextAcknowledged":
				t.Fatalf("retired raw transaction method %s survives in %s", function.Name.Name, path)
			}
		}
	}
}
