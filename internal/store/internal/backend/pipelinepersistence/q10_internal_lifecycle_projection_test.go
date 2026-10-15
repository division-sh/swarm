package pipelinepersistence

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestQ10NoStageHistoryRetainsInternalLifecycle(t *testing.T) {
	record := pipeline.WorkflowEngineStateRecord{
		CurrentState: "pending", StageDefined: false,
		Fields: []byte(`{}`), Bookkeeping: []byte(`{}`), Gates: []byte(`{}`), Accumulator: []byte(`{}`),
	}
	got, err := workflowEngineStateProjection(record)
	if err != nil {
		t.Fatal(err)
	}
	if got.CurrentState == nil || *got.CurrentState != "pending" {
		t.Fatalf("constructed internal lifecycle lost: %v", got.CurrentState)
	}
}
