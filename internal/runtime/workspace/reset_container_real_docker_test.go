package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/containeridentity"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/google/uuid"
)

// This exercises the real Docker boundary, not a complete served reset. Opt-in
// requires a local image; tests never pull images or clean another run's objects.
func TestResetContainerIntentRealDocker(t *testing.T) {
	if os.Getenv("SWARM_TEST_REAL_DOCKER") != "1" {
		t.Skip("set SWARM_TEST_REAL_DOCKER=1 to exercise the real Docker engine")
	}
	image := os.Getenv("SWARM_TEST_REAL_DOCKER_IMAGE")
	if image == "" {
		image = "golang:1.25-bookworm"
	}
	for _, mode := range []string{"stop original", "lost stop acknowledgment", "same name same labels successor"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			manager := NewDockerManager()
			if _, err := manager.RunDocker(ctx, "image", "inspect", image); err != nil {
				t.Fatalf("real Docker test requires local image %s: %v", image, err)
			}
			projection, _ := testRuntimeSourceProjection(t)
			agent := runtimeagentidentitytest.RootDeclared(t, "reset-proof", "test/agents.yaml")
			identity := containeridentity.Identity{
				Owner: containeridentity.OwnerRuntime, Kind: containeridentity.KindAgent,
				ResetEligible: true, CreationSource: "workspace.ResolveWorkspace",
				ContainerName: "swarm-reset-proof-" + uuid.NewString(),
				BundleHash:    projection.BundleHash(), SourceProjection: projection.Identity(),
				AgentIdentity: agent, RunID: agent.RunID,
			}
			if err := identity.Validate(); err != nil {
				t.Fatal(err)
			}
			create := func() string {
				t.Helper()
				args := []string{"create", "--name", identity.ContainerName, "--network", "none", "--read-only", "--pids-limit", "32", "--memory", "64m"}
				for key, value := range identity.Labels() {
					args = append(args, "--label", key+"="+value)
				}
				args = append(args, "--entrypoint", "/bin/sh", image, "-c", `trap 'exit 0' TERM; while :; do sleep 1; done`)
				id, err := manager.RunDocker(ctx, args...)
				if err != nil {
					t.Fatal(err)
				}
				id = strings.TrimSpace(id)
				t.Cleanup(func() {
					cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()
					if _, err := manager.RunDocker(cleanupCtx, "rm", "--force", id); err != nil && !strings.Contains(strings.ToLower(err.Error()), "no such container") {
						t.Errorf("remove exact real-test container %s: %v", id, err)
					}
				})
				if _, err := manager.RunDocker(ctx, "start", id); err != nil {
					t.Fatal(err)
				}
				return id
			}
			original := create()
			inventory, err := manager.ManagedResetContainerInventory(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var target destructivereset.ContainerRef
			for _, ref := range inventory {
				if ref.RuntimeID == original {
					target = ref
				}
			}
			if target.RuntimeID != original || !target.Identity().Equal(identity) {
				t.Fatalf("inventory lost exact intent: %+v", target)
			}
			raw, err := json.Marshal(target)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(raw, &target); err != nil {
				t.Fatal(err)
			}
			successor := ""
			if mode == "same name same labels successor" {
				if _, err := manager.RunDocker(ctx, "rm", "--force", original); err != nil {
					t.Fatal(err)
				}
				successor = create()
			}
			stopper := manager
			if mode == "lost stop acknowledgment" {
				stopper = NewDockerManager()
				stopper.SetRunDockerFnForTest(func(ctx context.Context, args ...string) (string, error) {
					out, err := manager.RunDocker(ctx, args...)
					if err == nil && args[0] == "stop" {
						return "", errors.New("injected acknowledgment loss after real Docker stop")
					}
					return out, err
				})
			}
			now := time.Now().UTC()
			result, err := (destructivereset.ManagedContainerStopper{Runtime: stopper}).Apply(ctx, destructivereset.ContainerResetRequest{
				ActorTokenID: "real-docker-proof", Result: destructivereset.Result{OperationName: destructivereset.DefaultOperationName, PlannedAt: now,
					Plan: destructivereset.Plan{ManagedContainers: []destructivereset.ContainerRef{target}}},
				Cleanup: destructivereset.CleanupResult{OperationName: destructivereset.DefaultOperationName, AppliedAt: now},
			})
			if err != nil || len(result.Failed) != 0 {
				t.Fatalf("real settlement = %+v, %v", result, err)
			}
			if successor != "" {
				state, err := manager.InspectManagedContainer(ctx, successor)
				if err != nil || !state.Exists || !state.Running || len(result.Missing) != 1 || result.Missing[0].RuntimeID != original {
					t.Fatalf("historical intent altered successor: state=%+v result=%+v err=%v", state, result, err)
				}
			} else {
				state, err := manager.InspectManagedContainer(ctx, original)
				if err != nil || !state.Exists || state.Running || len(result.Stopped) != 1 || result.Stopped[0].RuntimeID != original {
					t.Fatalf("exact original not settled: state=%+v result=%+v err=%v", state, result, err)
				}
			}
		})
	}
}
