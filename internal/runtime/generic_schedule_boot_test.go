package runtime

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestBootWorkflowTimerScheduleProducesGlobalTypedDueBasis(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"boot_delay_seconds": {Value: 45},
		}},
	})

	for _, tc := range []struct {
		name      string
		timer     runtimecontracts.WorkflowTimerContract
		wantKind  runtimegenericschedule.DueBasisKind
		wantDelay time.Duration
	}{
		{
			name: "one shot policy delay",
			timer: runtimecontracts.WorkflowTimerContract{
				ID: "platform.boot_once", Owner: "runtime", Event: "platform.boot_once",
				StartOn: "boot", Delay: "{{boot_delay_seconds}}s",
			},
			wantKind: runtimegenericschedule.DueDelay, wantDelay: 45 * time.Second,
		},
		{
			name: "recurring exact interval",
			timer: runtimecontracts.WorkflowTimerContract{
				ID: "platform.boot_recurring", Owner: "runtime", Event: "platform.boot_recurring",
				StartOn: "boot", Delay: "20s", Recurring: true,
			},
			wantKind: runtimegenericschedule.DueEvery, wantDelay: 20 * time.Second,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			command, ok, err := bootWorkflowTimerSchedule(source, tc.timer, executionposture.Live)
			if err != nil || !ok {
				t.Fatalf("bootWorkflowTimerSchedule = ok:%v err:%v", ok, err)
			}
			if err := command.Validate(); err != nil {
				t.Fatalf("boot command validation: %v", err)
			}
			if command.OwnerKind != runtimegenericschedule.OwnerSystem || command.RoutingSource.Kind() != events.RoutingSourcePlatformControl {
				t.Fatalf("boot authority = owner:%q source:%q", command.OwnerKind, command.RoutingSource.Kind().StorageCode())
			}
			if command.RunID != "" || command.EntityID != "" || command.FlowInstance != "" {
				t.Fatalf("boot scope = run:%q entity:%q flow:%q, want global", command.RunID, command.EntityID, command.FlowInstance)
			}
			if command.ScheduleKey == "" || command.TaskID != command.ScheduleKey {
				t.Fatalf("boot identity = key:%q task:%q", command.ScheduleKey, command.TaskID)
			}
			if command.Due.Kind != tc.wantKind {
				t.Fatalf("due kind = %q, want %q", command.Due.Kind, tc.wantKind)
			}
			gotDelay := command.Due.Delay
			if command.Due.Kind == runtimegenericschedule.DueEvery {
				gotDelay = command.Due.Every
			}
			if gotDelay != tc.wantDelay {
				t.Fatalf("due duration = %s, want %s", gotDelay, tc.wantDelay)
			}
		})
	}
}
