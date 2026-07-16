package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestPreviewContractHandlerExecution_DeniesImportBoundaryWildcardRawFallback(t *testing.T) {
	bundle := loadPipelineImportBoundaryWildcardBundle(t, canonicalrouting.ImportBoundaryWildcardDenied)
	_, err := PreviewContractHandlerExecution(
		testAuthorActivityContext(context.Background()),
		bundle,
		"worker-listener",
		eventtest.RootIngress("", "producer/task.done", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		WorkflowState{},
		nil,
	)
	if err == nil {
		t.Fatal("expected preview to deny ungranted sibling event")
	}
	if !strings.Contains(err.Error(), "missing handler worker-listener/producer/task.done") {
		t.Fatalf("preview error = %v, want missing handler denial", err)
	}
}

func TestPreviewContractHandlerExecution_AllowsGrantedImportBoundaryWildcard(t *testing.T) {
	bundle := loadPipelineImportBoundaryWildcardBundle(t, canonicalrouting.ImportBoundaryWildcardObserveGranted)
	preview, err := PreviewContractHandlerExecution(
		testAuthorActivityContext(context.Background()),
		bundle,
		"worker-listener",
		eventtest.RootIngress("", "producer/task.done", "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),
		WorkflowState{},
		nil,
	)
	if err != nil {
		t.Fatalf("PreviewContractHandlerExecution: %v", err)
	}
	if !containsString(preview.ClearGates, "sibling_gate") {
		t.Fatalf("preview ClearGates = %#v, want sibling_gate", preview.ClearGates)
	}
}
