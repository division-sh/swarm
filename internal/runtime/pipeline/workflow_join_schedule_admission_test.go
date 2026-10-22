package pipeline

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/google/uuid"
)

func TestJoinScheduleRejectsInvalidActivationBeforeMutation(t *testing.T) {
	source := workflowJoinLifecycleSource(workflowJoinLifecycleBundle(t))
	runID := uuid.NewString()
	route := testRunScopedWorkflowInstanceForRun(runID, runID).Route
	owner := flowidentity.RunScopedFlowInstance{RunID: runID, Route: route}
	handle := pipelineJoinHandle(t, "", timeridentity.TimerHandleJoinTimeout, runID, runID, runID)
	ref, _ := handle.JoinRef()
	now := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	instance := WorkflowInstance{WorkflowName: ref.FlowPath(), StorageRef: runID, InstanceID: route.InstanceID, EntityID: runID}
	for _, tc := range []struct {
		name string
		edit func(*joinruntime.Activation)
	}{
		{"deadline_before_arm", func(a *joinruntime.Activation) { a.FireAt = now.Add(-time.Second) }},
		{"closed_without_reason", func(a *joinruntime.Activation) { a.Status = joinruntime.StatusClosed }},
		{"corrupt_output_hash", func(a *joinruntime.Activation) {
			a.Outputs["member"] = joinruntime.MemberOutput{Hash: "not-a-canonical-hash", Value: "result"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			activation, err := joinruntime.NewActivation(ref, []string{"member"}, nil, now, now.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			tc.edit(&activation)
			if _, err := joinSchedule(source, owner, instance, activation, executionmode.Live); err == nil {
				t.Fatal("invalid join evidence reached schedule admission")
			}
		})
	}
}
