package runtimepersistence

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestDynamicFlowRuntimeReadinessFaultsRespectOriginalSQLiteWriterAdmission(t *testing.T) {
	for _, tc := range []projectionShapeFaultCase{
		{name: "malformed-plan", apply: func(ctx context.Context, selected any, run, path string) (int64, error) {
			return SetDescriptorReadinessMalformedPlanForTest(ctx, selected, run, path, json.RawMessage(`{}`))
		}},
		{name: "creation-clock", apply: func(ctx context.Context, selected any, run, path string) (int64, error) {
			return SetDescriptorReadinessCreationClockForTest(ctx, selected, run, path, time.Now().UTC())
		}},
		{name: "missing-readiness", apply: RemoveDescriptorReadinessForTest},
	} {
		t.Run(tc.name, func(t *testing.T) { verifyWorkflowProjectionFaultSQLiteAdmission(t, tc) })
	}
}
