package destructivereset

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestCompositeInventoryReaderCapturesManagedContainersAtPlanTime(t *testing.T) {
	base := &recordingInventoryReader{inventory: Inventory{
		CleanupRuns:        []RunRef{{RunID: "run-1", Status: "running"}},
		CleanupRunSetKnown: true,
	}}
	containers := managedContainerInventoryFunc(func(context.Context) ([]ContainerRef, error) {
		return []ContainerRef{{
			Name:          "swarm-agent-agent-a",
			Kind:          "agent",
			Action:        ContainerActionStop,
			ResetEligible: true,
			RunID:         "run-1",
			AgentID:       "agent-a",
		}}, nil
	})

	inventory, err := (CompositeInventoryReader{Reader: base, Containers: containers}).ReadResetInventory(context.Background())
	if err != nil {
		t.Fatalf("ReadResetInventory: %v", err)
	}
	if len(inventory.EntityContainers) != 1 || inventory.EntityContainers[0].Name != "swarm-agent-agent-a" {
		t.Fatalf("entity containers = %#v, want plan-time managed container refs", inventory.EntityContainers)
	}
	inventory.EntityContainers[0].Name = "tampered"
	again, err := (CompositeInventoryReader{Reader: base, Containers: containers}).ReadResetInventory(context.Background())
	if err != nil {
		t.Fatalf("ReadResetInventory again: %v", err)
	}
	if again.EntityContainers[0].Name != "swarm-agent-agent-a" {
		t.Fatalf("inventory leaked mutable container refs: %#v", again.EntityContainers)
	}
}

func TestManagedContainerStopperDryRunSelectsOnlyResetEligibleLabeledContainers(t *testing.T) {
	now := time.Date(2026, 5, 16, 20, 10, 0, 0, time.UTC)
	runtime := &recordingManagedContainerRuntime{
		inspections: map[string]ManagedContainerInspection{
			"swarm-agent-agent-a": managedInspection("swarm-agent-agent-a", "agent", true, true),
			"swarm-system":        managedInspection("swarm-system", "system", false, true),
			"swarm-unlabeled":     {Exists: true, Running: true},
			"swarm-missing":       {Exists: false},
		},
	}
	result, err := (ManagedContainerStopper{
		Runtime: runtime,
		Now:     func() time.Time { return now },
	}).Apply(context.Background(), ContainerResetRequest{
		ActorTokenID: "operator-token",
		Result: Result{
			OperationName: DefaultOperationName,
			DryRun:        true,
			PlannedAt:     now.Add(-time.Minute),
			Plan: Plan{EntityContainers: []ContainerRef{
				{Name: "swarm-agent-agent-a", Action: ContainerActionStop},
				{Name: "swarm-system", Action: ContainerActionStop},
				{Name: "swarm-unlabeled", Action: ContainerActionStop},
				{Name: "swarm-missing", Action: ContainerActionStop},
			}},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(runtime.stops) != 0 {
		t.Fatalf("dry-run stops = %#v, want none", runtime.stops)
	}
	if len(result.Selected) != 1 || result.Selected[0].Name != "swarm-agent-agent-a" {
		t.Fatalf("selected = %#v, want only reset-eligible agent", result.Selected)
	}
	if len(result.Preserved) != 2 {
		t.Fatalf("preserved = %#v, want system and unlabeled", result.Preserved)
	}
	if len(result.Missing) != 1 || result.Missing[0].Name != "swarm-missing" {
		t.Fatalf("missing = %#v, want missing no-op", result.Missing)
	}
}

func TestManagedContainerStopperApplyReportsStoppedNoopAndPartialFailure(t *testing.T) {
	now := time.Date(2026, 5, 16, 20, 15, 0, 0, time.UTC)
	stopErr := errors.New("docker stop failed")
	runtime := &recordingManagedContainerRuntime{
		inspections: map[string]ManagedContainerInspection{
			"swarm-agent-agent-a": managedInspection("swarm-agent-agent-a", "agent", true, true),
			"swarm-flow-flow-a":   managedInspection("swarm-flow-flow-a", "flow", true, false),
			"swarm-entity-a":      managedInspection("swarm-entity-a", "entity", true, true),
		},
		stopErrors: map[string]error{"swarm-entity-a": stopErr},
	}
	result, err := (ManagedContainerStopper{
		Runtime: runtime,
		Now:     func() time.Time { return now },
	}).Apply(context.Background(), ContainerResetRequest{
		ActorTokenID: "operator-token",
		Result: Result{
			OperationName: DefaultOperationName,
			DryRun:        false,
			PlannedAt:     now.Add(-2 * time.Minute),
			Plan: Plan{EntityContainers: []ContainerRef{
				{Name: "swarm-agent-agent-a", Action: ContainerActionStop},
				{Name: "swarm-flow-flow-a", Action: ContainerActionStop},
				{Name: "swarm-entity-a", Action: ContainerActionStop},
			}},
		},
		Cleanup: CleanupResult{
			OperationName: DefaultOperationName,
			DryRun:        false,
			AppliedAt:     now.Add(-time.Minute),
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(runtime.stops) != 2 || runtime.stops[0] != "swarm-agent-agent-a" || runtime.stops[1] != "swarm-entity-a" {
		t.Fatalf("stops = %#v, want running reset-eligible containers only", runtime.stops)
	}
	if len(result.Stopped) != 1 || result.Stopped[0].Name != "swarm-agent-agent-a" {
		t.Fatalf("stopped = %#v, want agent stopped", result.Stopped)
	}
	if len(result.AlreadyStopped) != 1 || result.AlreadyStopped[0].Name != "swarm-flow-flow-a" {
		t.Fatalf("already stopped = %#v, want flow no-op", result.AlreadyStopped)
	}
	if len(result.Failed) != 1 || result.Failed[0].Container.Name != "swarm-entity-a" || !strings.Contains(result.Failed[0].Error, stopErr.Error()) {
		t.Fatalf("failed = %#v, want entity stop failure", result.Failed)
	}
}

func TestManagedContainerStopperRequiresAppliedCleanupForMutation(t *testing.T) {
	now := time.Date(2026, 5, 16, 20, 20, 0, 0, time.UTC)
	_, err := (ManagedContainerStopper{Runtime: &recordingManagedContainerRuntime{}, Now: func() time.Time { return now }}).Apply(context.Background(), ContainerResetRequest{
		ActorTokenID: "operator-token",
		Result: Result{
			OperationName: DefaultOperationName,
			PlannedAt:     now.Add(-time.Minute),
		},
	})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Apply error = %v, want invalid request for missing applied cleanup", err)
	}
}

type managedContainerInventoryFunc func(context.Context) ([]ContainerRef, error)

func (f managedContainerInventoryFunc) ManagedResetContainerInventory(ctx context.Context) ([]ContainerRef, error) {
	return f(ctx)
}

type recordingManagedContainerRuntime struct {
	inspections map[string]ManagedContainerInspection
	stopErrors  map[string]error
	stops       []string
}

func (r *recordingManagedContainerRuntime) InspectManagedContainer(_ context.Context, name string) (ManagedContainerInspection, error) {
	return r.inspections[strings.TrimSpace(name)], nil
}

func (r *recordingManagedContainerRuntime) StopManagedContainer(_ context.Context, name string) error {
	name = strings.TrimSpace(name)
	r.stops = append(r.stops, name)
	return r.stopErrors[name]
}

func managedInspection(name, kind string, resetEligible, running bool) ManagedContainerInspection {
	return ManagedContainerInspection{
		Exists:      true,
		Running:     running,
		HasIdentity: true,
		Identity: ContainerIdentity{
			Owner:          "runtime",
			Kind:           kind,
			ResetEligible:  resetEligible,
			CreationSource: "test",
			ContainerName:  name,
			WorkspaceScope: kind,
			RunID:          "11111111-1111-1111-1111-111111111111",
			EntityID:       "entity-a",
			AgentID:        "agent-a",
			FlowInstance:   "flow/a",
		},
	}
}
