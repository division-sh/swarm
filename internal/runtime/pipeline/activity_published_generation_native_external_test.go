package pipeline_test

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestReadOnlyPublishedAccountingAfterNativeLoopAdvanceAndReopenBothStores(t *testing.T) {
	pipeline.VerifyReadOnlyPublishedAccountingAfterNativeLoopAdvanceAndReopenForTest(t, pipelineDeliveryNativeFixture)
}
