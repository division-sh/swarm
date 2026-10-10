package pipeline

import (
	"context"
	"testing"
)

type WorkflowClosedOwnerNativeFixtureForTest struct {
	Persistence WorkflowPersistence
	Context     context.Context
	Close       func() error
}

func closeWorkflowNativeOwnerForTest(t *testing.T, fixture WorkflowClosedOwnerNativeFixtureForTest, path string) error {
	t.Helper()
	key := testRunScopedWorkflowInstanceFromContext(fixture.Context, path)
	if _, _, err := fixture.Persistence.LoadWorkflowInstance(fixture.Context, key); err != nil {
		t.Fatalf("native owner was not readable before close: %v", err)
	}
	if err := fixture.Close(); err != nil {
		t.Fatalf("close original native owner: %v", err)
	}
	_, _, err := fixture.Persistence.LoadWorkflowInstance(fixture.Context, key)
	if err == nil {
		t.Fatal("closed original native owner remained readable")
	}
	return err
}
