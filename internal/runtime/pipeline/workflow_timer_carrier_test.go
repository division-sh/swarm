package pipeline

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/timerobligation"
)

func TestWorkflowTimerPrimitiveCarrierRoundTripUsesCanonicalAdmission(t *testing.T) {
	for _, variant := range []string{"active", "recurring", "removed"} {
		t.Run(variant, func(t *testing.T) {
			activation := inheritedWorkflowTimerForTest(t)
			activation.Payload = []byte(`{"integer":7,"double":7.0}`)
			if variant == "recurring" {
				activation.Recurring, activation.RecurrenceInterval = true, time.Hour
			}
			if variant == "removed" {
				activation.Status = workflowTimerStatusCancelled
				activation.CancelCause, activation.CancelledAt = WorkflowTimerCancelCauseRuleRemoved, activation.CreatedAt
			}
			var record timerobligation.WorkflowTimerActivationRecord = activation.PersistenceRecord()
			got, err := DecodeWorkflowTimerActivationPersistenceRecord(record)
			if err != nil || !reflect.DeepEqual(got, activation.Canonical()) {
				t.Fatalf("primitive carrier lost admitted facts: %+v %v", got, err)
			}
			record.Payload[0] = 'x'
			if activation.Payload[0] != '{' {
				t.Fatal("primitive carrier aliases source bytes")
			}
			record = activation.PersistenceRecord()
			record.ForkedFromPointRevision = 0
			if _, err := DecodeWorkflowTimerActivationPersistenceRecord(record); err == nil {
				t.Fatal("lower-layer primitive carrier bypassed semantic lineage admission")
			}
		})
	}
}
