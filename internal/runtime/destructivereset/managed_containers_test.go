package destructivereset

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/containeridentity"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
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
			RunID:         "11111111-1111-1111-1111-111111111111",
			AgentIdentity: testManagedAgentIdentity(),
		}}, nil
	})

	inventory, err := (CompositeInventoryReader{Reader: base, Containers: containers}).ReadResetInventory(context.Background())
	if err != nil {
		t.Fatalf("ReadResetInventory: %v", err)
	}
	if len(inventory.ManagedContainers) != 1 || inventory.ManagedContainers[0].Name != "swarm-agent-agent-a" {
		t.Fatalf("managed containers = %#v, want plan-time managed container refs", inventory.ManagedContainers)
	}
	inventory.ManagedContainers[0].Name = "tampered"
	again, err := (CompositeInventoryReader{Reader: base, Containers: containers}).ReadResetInventory(context.Background())
	if err != nil {
		t.Fatalf("ReadResetInventory again: %v", err)
	}
	if again.ManagedContainers[0].Name != "swarm-agent-agent-a" {
		t.Fatalf("inventory leaked mutable container refs: %#v", again.ManagedContainers)
	}
}

func TestManagedContainerStopperDryRunSelectsOnlyResetEligibleLabeledContainers(t *testing.T) {
	now := time.Date(2026, 5, 16, 20, 10, 0, 0, time.UTC)
	runtime := &recordingManagedContainerRuntime{
		inspections: map[string]ManagedContainerInspection{
			"swarm-agent-agent-a": managedInspection("swarm-agent-agent-a", "agent", true, true),
			"swarm-system":        managedInspection("swarm-system", "system", false, true),
			"swarm-unlabeled":     {RuntimeID: "swarm-unlabeled", Exists: true, Running: true},
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
			Plan: Plan{ManagedContainers: []ContainerRef{
				ContainerRefFromIdentity(runtime.inspections["swarm-agent-agent-a"].Identity, "swarm-agent-agent-a", ContainerActionStop),
				{Name: "swarm-system", RuntimeID: "swarm-system", Action: ContainerActionStop},
				{Name: "swarm-unlabeled", RuntimeID: "swarm-unlabeled", Action: ContainerActionStop},
				{Name: "swarm-missing", RuntimeID: "swarm-missing", Action: ContainerActionStop},
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
	if len(result.Failed) != 2 {
		t.Fatalf("unproven planned ownership was treated as settled: %#v", result)
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
			"swarm-agent-b":       managedInspection("swarm-agent-b", "agent", true, true),
		},
		stopErrors: map[string]error{"swarm-agent-b": stopErr},
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
			Plan: Plan{ManagedContainers: []ContainerRef{
				ContainerRefFromIdentity(runtime.inspections["swarm-agent-agent-a"].Identity, "swarm-agent-agent-a", ContainerActionStop),
				ContainerRefFromIdentity(runtime.inspections["swarm-flow-flow-a"].Identity, "swarm-flow-flow-a", ContainerActionStop),
				ContainerRefFromIdentity(runtime.inspections["swarm-agent-b"].Identity, "swarm-agent-b", ContainerActionStop),
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
	if len(runtime.stops) != 2 || runtime.stops[0] != "swarm-agent-agent-a" || runtime.stops[1] != "swarm-agent-b" {
		t.Fatalf("stops = %#v, want running reset-eligible containers only", runtime.stops)
	}
	if len(result.Stopped) != 1 || result.Stopped[0].Name != "swarm-agent-agent-a" {
		t.Fatalf("stopped = %#v, want agent stopped", result.Stopped)
	}
	if len(result.AlreadyStopped) != 1 || result.AlreadyStopped[0].Name != "swarm-flow-flow-a" {
		t.Fatalf("already stopped = %#v, want flow no-op", result.AlreadyStopped)
	}
	if len(result.Failed) != 1 || result.Failed[0].Container.Name != "swarm-agent-b" || !strings.Contains(result.Failed[0].Error, stopErr.Error()) {
		t.Fatalf("failed = %#v, want agent stop failure", result.Failed)
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

func TestManagedContainerStopperPreservesSuccessorIdentity(t *testing.T) {
	for _, field := range []string{"run", "artifact", "projection", "owner", "retired_entity", "incomplete_plan", "missing_source"} {
		t.Run(field, func(t *testing.T) {
			now := time.Now().UTC()
			predecessor := managedInspection("swarm-agent-agent-a", "agent", true, true)
			predecessor.Identity.BundleHash = "bundle-v2:sha256:" + strings.Repeat("a", 64)
			predecessor.Identity.SourceProjection = "runtime-projection-v1:" + strings.Repeat("a", 32)
			planned := ContainerRefFromIdentity(predecessor.Identity, predecessor.RuntimeID, ContainerActionStop)
			successor := predecessor
			switch field {
			case "run":
				successor.Identity.RunID = "22222222-2222-2222-2222-222222222222"
			case "artifact":
				successor.Identity.BundleHash = "bundle-v2:sha256:" + strings.Repeat("b", 64)
			case "projection":
				successor.Identity.SourceProjection = "runtime-projection-v1:" + strings.Repeat("b", 32)
			case "owner":
				successor.Identity.Owner = "foreign"
			case "retired_entity":
				successor.Identity.Kind = "entity"
				planned = ContainerRefFromIdentity(successor.Identity, successor.RuntimeID, ContainerActionStop)
			case "incomplete_plan":
				planned.SourceProjection = ""
			case "missing_source":
				successor.Identity.BundleHash = ""
				successor.Identity.SourceProjection = ""
				planned = ContainerRefFromIdentity(successor.Identity, successor.RuntimeID, ContainerActionStop)
			}
			// Exercise the same serialized identity used by durable plan/outcome storage.
			encoded, err := json.Marshal(planned)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(encoded, &planned); err != nil {
				t.Fatal(err)
			}
			runtime := &recordingManagedContainerRuntime{inspections: map[string]ManagedContainerInspection{planned.Name: successor}}
			result, err := (ManagedContainerStopper{Runtime: runtime}).Apply(context.Background(), ContainerResetRequest{
				ActorTokenID: "operator-token",
				Result:       Result{OperationName: DefaultOperationName, PlannedAt: now, Plan: Plan{ManagedContainers: []ContainerRef{planned}}},
				Cleanup:      CleanupResult{OperationName: DefaultOperationName, AppliedAt: now},
			})
			if err != nil {
				t.Fatal(err)
			}
			if len(runtime.stops) != 0 || len(result.Preserved) != 1 || len(result.Failed) != 1 {
				t.Fatalf("reset touched same-name successor: stops=%v result=%+v", runtime.stops, result)
			}
		})
	}
}

type managedContainerInventoryFunc func(context.Context) ([]ContainerRef, error)

func (f managedContainerInventoryFunc) ManagedResetContainerInventory(ctx context.Context) ([]ContainerRef, error) {
	return f(ctx)
}

type recordingManagedContainerRuntime struct {
	inspections map[string]ManagedContainerInspection
	inspected   []string
	stopErrors  map[string]error
	stops       []string
}

func (r *recordingManagedContainerRuntime) InspectManagedContainer(_ context.Context, name string) (ManagedContainerInspection, error) {
	r.inspected = append(r.inspected, name)
	return r.inspections[strings.TrimSpace(name)], nil
}

func (r *recordingManagedContainerRuntime) StopManagedContainer(_ context.Context, target ContainerRef) error {
	name := strings.TrimSpace(target.Name)
	r.stops = append(r.stops, name)
	return r.stopErrors[name]
}

func managedInspection(name, kind string, resetEligible, running bool) ManagedContainerInspection {
	identity := containeridentity.Identity{
		BundleHash:       "bundle-v2:sha256:" + strings.Repeat("a", 64),
		SourceProjection: "runtime-projection-v1:" + strings.Repeat("a", 32),
		Owner:            "runtime",
		Kind:             kind,
		ResetEligible:    resetEligible,
		CreationSource:   "test",
		ContainerName:    name,
		WorkspaceScope:   kind,
		RunID:            "11111111-1111-1111-1111-111111111111",
		FlowInstance:     "flow/a",
	}
	if kind == "agent" || kind == "flow" {
		identity.AgentIdentity = testManagedAgentIdentity()
	}
	return ManagedContainerInspection{
		RuntimeID:   name,
		Exists:      true,
		Running:     running,
		HasIdentity: true,
		Identity:    identity,
	}
}

func testManagedAgentIdentity() runtimeagentidentity.Identity {
	return runtimeagentidentity.Identity{
		RunID: "11111111-1111-1111-1111-111111111111",
		Name: runtimeagentidentity.Name{
			AgentID: "agent-a",
			Owner:   "test/agents.yaml",
			Source:  runtimeagentidentity.NameSourceDeclared,
		},
		Route: runtimeagentidentity.Route{
			Presence:     runtimeagentidentity.RoutePresent,
			ScopeKey:     "flow",
			InstanceID:   "a",
			InstancePath: "flow/a",
		},
	}
}
