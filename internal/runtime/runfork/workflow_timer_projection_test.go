package runfork

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/timerobligation"
	"github.com/google/uuid"
)

func TestProjectWorkflowTimerRecordRejectsZeroRunUUIDs(t *testing.T) {
	const sourceRunID = "11111111-1111-4111-8111-111111111111"
	const childRunID = "22222222-2222-4222-8222-222222222222"
	ref := timeridentity.WorkflowTimerActivationRef{
		ActivationID: "33333333-3333-4333-8333-333333333333", DeclarationKey: "stage:.:timeout",
		DeclarationRevision: "source-revision", Cause: timeridentity.WorkflowTimerActivationCauseEvent,
	}
	armedAt := time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC)
	point := RunForkPoint{Kind: RunForkPointRunStart, Revision: 4}
	for _, test := range []struct {
		name, sourceRunID, childRunID string
		wantError                     bool
	}{
		{"nonzero_control", sourceRunID, childRunID, false},
		{"zero_source", uuid.Nil.String(), childRunID, true},
		{"zero_child", sourceRunID, uuid.Nil.String(), true},
		{"both_zero", uuid.Nil.String(), uuid.Nil.String(), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			routing, err := events.NewRootRoutingSource(test.sourceRunID)
			if err != nil {
				t.Fatal(err)
			}
			source := timerobligation.WorkflowTimerActivationRecord{
				ActivationID: ref.ActivationID, TaskID: ref.TaskID(), RunID: test.sourceRunID, EntityID: test.sourceRunID,
				Route: flowidentity.Route{ScopeKey: ".", InstanceID: test.sourceRunID, InstancePath: test.sourceRunID}, RoutingSource: routing,
				EventType: "timer.elapsed", ExecutionMode: executionmode.Live, OwnerAgent: "timer-owner", Payload: []byte(`{"retained":true}`),
				TaskType: string(timerobligation.FamilyWorkflowTimer), Status: "active", CreatedAt: armedAt, FireAt: armedAt.Add(time.Hour),
			}
			child, err := ProjectWorkflowTimerRecord(source, ref, test.childRunID, point, nil, armedAt.Add(3*time.Hour))
			if test.wantError {
				if err == nil || !strings.Contains(err.Error(), "exact distinct source and child run UUIDs") || child.ActivationID != "" {
					t.Fatalf("zero UUID did not fail at the run boundary: child=%#v err=%v", child, err)
				}
				return
			}
			if err != nil || child.RunID != childRunID || child.SourceTimerID != source.ActivationID {
				t.Fatalf("otherwise-valid projection fixture failed: child=%#v err=%v", child, err)
			}
		})
	}
}
