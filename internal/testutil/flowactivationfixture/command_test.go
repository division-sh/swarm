package flowactivationfixture

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestCommandCarriesPreparedInstanceAndLifecycleWithoutRepair(t *testing.T) {
	fact, err := correlation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	ctx := effects.WithExecutionMode(correlation.WithSourceArtifactFact(correlation.WithRunID(context.Background(), uuid.NewString()), fact), executionmode.Live)
	at := time.Date(2026, time.October, 1, 12, 0, 0, 0, time.UTC)
	instance := pipeline.WorkflowInstance{
		InstanceID: "one", StorageRef: "child/one", WorkflowName: "child", WorkflowVersion: "1",
		EntityID: uuid.NewString(), EntityType: "item", CurrentState: "initial",
		CreatedAt: at, EnteredStageAt: at, Fields: map[string]any{"value": "prepared"},
	}
	before := instance
	before.Fields = map[string]any{"value": "prepared"}
	command, err := Command(ctx, instance, pipeline.WorkflowLifecycleMutationPlan{}, at)
	if err != nil {
		t.Fatal(err)
	}
	if command.Plan.Instance.Fields["value"] != "prepared" || command.Plan.Instance.CurrentState != instance.CurrentState || !command.Plan.OccurredAt.Equal(at) {
		t.Fatalf("command changed prepared state: %+v", command.Plan)
	}
	if !reflect.DeepEqual(before, instance) || len(command.RouteTopology) != 1 || command.RouteTopology[0].Identity.RunID != correlation.RunIDFromContext(ctx) {
		t.Fatal("command changed fixture input or lost exact route owner")
	}
	if _, err := Command(ctx, instance, pipeline.WorkflowLifecycleMutationPlan{RequestCompletionCandidate: true}, at); err == nil {
		t.Fatal("command dropped invalid supplied lifecycle evidence")
	}
	if _, err := Command(context.Background(), instance, pipeline.WorkflowLifecycleMutationPlan{}, at); err == nil {
		t.Fatal("command fabricated missing authority")
	}
	if _, err := Command(correlation.WithSourceArtifactFact(context.Background(), fact), instance, pipeline.WorkflowLifecycleMutationPlan{}, at); err == nil {
		t.Fatal("command fabricated missing execution mode")
	}
}
