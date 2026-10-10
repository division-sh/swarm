package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
)

func initialWorkflowJoinRefForTest(t *testing.T, ctx context.Context, node identity.ExecutableNode, path, entityID, stage, joinID string) timeridentity.JoinRef {
	t.Helper()
	owner := testRunScopedWorkflowInstanceFromContext(ctx, path)
	route := owner.Route
	effect, err := workflowlifecycle.NewInitialEntry(route, identity.NormalizeEntityID(entityID), stage, executionmode.Live, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	entry, found, err := effect.StageEntry(owner)
	if err != nil || !found {
		t.Fatalf("initial lifecycle entry: found=%v err=%v", found, err)
	}
	ref, err := timeridentity.NewJoinRef(node, "item.completed", stage, joinID)
	if err != nil {
		t.Fatal(err)
	}
	ref, err = ref.BindStageEntry(entry, timeridentity.JoinRef{}.Generation())
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func workflowJoinActivationKey(t *testing.T, buckets map[string]map[string]any, node identity.ExecutableNode) string {
	t.Helper()
	activations, err := joinruntime.List(buckets)
	if err != nil {
		t.Fatal(err)
	}
	var key string
	for _, activation := range activations {
		if activation.JoinRef().Node().Equal(node) && activation.Stage() == "awaiting" && activation.JoinID() == "awaiting" {
			if key != "" {
				t.Fatal("fixture requires one exact lifecycle-entry arm; multiple arms must use their retained JoinRef")
			}
			key = joinruntime.ActivationKey(activation.JoinRef())
		}
	}
	if key == "" {
		t.Fatal("fixture is missing its bound lifecycle-entry arm")
	}
	return key
}

func mustJoinRefForTest(t *testing.T, handle timeridentity.TimerHandle) timeridentity.JoinRef {
	t.Helper()
	ref, found := handle.JoinRef()
	if !found {
		t.Fatal("fixture requires a bound join timer handle")
	}
	return ref
}

func assertJoinCompletionCancellationsForTest(t *testing.T, cancellations []runtimegenericschedule.Activation, ref timeridentity.JoinRef) {
	t.Helper()
	deadline, err := timeridentity.JoinTimeoutHandle(ref)
	if err != nil {
		t.Fatal(err)
	}
	complete, err := timeridentity.JoinCompleteHandle(ref)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{deadline.TaskID(): true, complete.TaskID(): true}
	if len(cancellations) != len(want) {
		t.Fatalf("completion cancellations=%d, want exact deadline and completion obligations", len(cancellations))
	}
	for _, cancellation := range cancellations {
		_, actual, found := timeridentity.ParseJoinHandle(parsePayloadMap(genericSchedulePayloadForTest(t, cancellation)))
		if !found || !actual.Equal(ref) || !want[cancellation.Command.TaskID] {
			t.Fatalf("completion cancelled a duplicate or unrelated arm: %s", cancellation.Command.TaskID)
		}
		delete(want, cancellation.Command.TaskID)
	}
}
