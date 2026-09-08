package destructivereset

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestManagedContainerReplayNeverSelectsSameNameSameLabelsSuccessor(t *testing.T) {
	predecessor := managedInspection("reused-name", "agent", true, true)
	predecessor.RuntimeID = "predecessor-object"
	planned := ContainerRefFromIdentity(predecessor.Identity, predecessor.RuntimeID, ContainerActionStop)
	raw, err := json.Marshal(planned)
	if err != nil {
		t.Fatal(err)
	}
	var restored ContainerRef
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	successor := predecessor
	successor.RuntimeID = "successor-object"
	runtime := &recordingManagedContainerRuntime{inspections: map[string]ManagedContainerInspection{
		"reused-name": successor, "successor-object": successor,
	}}
	now := time.Now().UTC()
	result, err := (ManagedContainerStopper{Runtime: runtime}).Apply(context.Background(), ContainerResetRequest{
		ActorTokenID: "operator",
		Result: Result{OperationName: DefaultOperationName, PlannedAt: now,
			Plan: Plan{ManagedContainers: []ContainerRef{restored}}},
		Cleanup: CleanupResult{OperationName: DefaultOperationName, AppliedAt: now},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(runtime.inspected) != 1 || runtime.inspected[0] != predecessor.RuntimeID || len(runtime.stops) != 0 ||
		len(result.Missing) != 1 || result.Missing[0].RuntimeID != predecessor.RuntimeID {
		t.Fatalf("historical container intent selected successor: calls=%v stops=%v result=%+v", runtime.inspected, runtime.stops, result)
	}
}

func TestManagedContainerMissingImmutableIntentFailsBeforeInspection(t *testing.T) {
	identity := managedInspection("named-only", "agent", true, true).Identity
	runtime := &recordingManagedContainerRuntime{}
	now := time.Now().UTC()
	_, err := (ManagedContainerStopper{Runtime: runtime}).Apply(context.Background(), ContainerResetRequest{
		ActorTokenID: "operator",
		Result: Result{OperationName: DefaultOperationName, PlannedAt: now, DryRun: true,
			Plan: Plan{ManagedContainers: []ContainerRef{ContainerRefFromIdentity(identity, "", ContainerActionStop)}}},
	})
	if !errors.Is(err, ErrInvalidRequest) || len(runtime.inspected) != 0 || len(runtime.stops) != 0 {
		t.Fatalf("missing intent used a name fallback: err=%v calls=%v stops=%v", err, runtime.inspected, runtime.stops)
	}
}
