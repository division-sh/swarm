package pipeline

import (
	"context"
	"database/sql"
	"testing"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
)

const testPipelineRunID = "77777777-7777-7777-7777-777777777777"

func testPipelineRunContext(t *testing.T, db *sql.DB) context.Context {
	t.Helper()
	if db == nil {
		t.Fatal("test pipeline run context requires db")
	}
	ctx := runtimecorrelation.WithRunID(context.Background(), testPipelineRunID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
		ON CONFLICT (run_id) DO NOTHING
	`, testPipelineRunID); err != nil {
		t.Fatalf("seed pipeline test run: %v", err)
	}
	return ctx
}

func testPipelineRunContextNoSeed() context.Context {
	return runtimecorrelation.WithRunID(context.Background(), testPipelineRunID)
}

func testWorkflowStoreRunContext(t *testing.T, store *WorkflowInstanceStore) context.Context {
	t.Helper()
	if store == nil {
		t.Fatal("test workflow store run context requires store")
	}
	if store.db == nil {
		return testPipelineRunContextNoSeed()
	}
	return testPipelineRunContext(t, store.db)
}

func testPipelineCoordinatorRunContext(t *testing.T, pc *PipelineCoordinator) context.Context {
	t.Helper()
	if pc == nil {
		t.Fatal("test pipeline coordinator run context requires coordinator")
	}
	if pc.db != nil {
		return testPipelineRunContext(t, pc.db)
	}
	if pc.workflowStore != nil {
		return testWorkflowStoreRunContext(t, pc.workflowStore)
	}
	return testPipelineRunContextNoSeed()
}
