package workspace

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	runtimecontaineridentity "github.com/division-sh/swarm/internal/runtime/containeridentity"
)

// The fixture keeps object IDs distinct from reusable names, including after
// a command takes effect but its acknowledgment is lost.
type projectionDockerFixture struct {
	objects map[string]runtimecontaineridentity.Identity
	nextID  int
}

func (f *projectionDockerFixture) run(_ context.Context, args ...string) (string, error) {
	if f.objects == nil {
		f.objects = make(map[string]runtimecontaineridentity.Identity)
	}
	switch args[0] {
	case "create":
		labels := make(map[string]string)
		for i := 0; i+1 < len(args); i++ {
			if args[i] == "--label" {
				key, value, _ := strings.Cut(args[i+1], "=")
				labels[key] = value
			}
		}
		identity, ok, err := runtimecontaineridentity.FromLabels(labels)
		if err != nil || !ok {
			return "", fmt.Errorf("invalid create identity: %v", err)
		}
		f.nextID++
		id := fmt.Sprintf("docker-object-%d", f.nextID)
		f.objects[id] = identity
		return id, nil
	case "inspect":
		name := args[len(args)-1]
		for id, identity := range f.objects {
			if id != name && identity.ContainerName != name {
				continue
			}
			switch args[2] {
			case "{{.State.Running}}":
				return "true", nil
			case "{{json .Config.Labels}}":
				body, err := json.Marshal(identity.Labels())
				return string(body), err
			default:
				return managedContainerInspectJSON(id, identity.Labels(), true), nil
			}
		}
		return "", fmt.Errorf("no such object: %s", name)
	case "rm":
		name := args[len(args)-1]
		for id, identity := range f.objects {
			if id == name || identity.ContainerName == name {
				delete(f.objects, id)
			}
		}
	}
	return "", nil
}

func TestReleaseSourceProjectionPreservesForeignAndSuccessorContainers(t *testing.T) {
	for _, changed := range []string{"projection", "bundle", "owner", "object_after_inspect"} {
		t.Run(changed, func(t *testing.T) {
			projection, _ := testRuntimeSourceProjection(t)
			manager := NewDockerManager()
			cfg := DefaultDockerConfig()
			cfg.WorkspaceNetwork = ""
			manager.SetConfig(cfg)
			bindTestDockerProjection(t, manager, projection)
			fixture := &projectionDockerFixture{}
			manager.SetRunDockerFnForTest(fixture.run)
			if err := manager.EnsureSystemWorkspaces(context.Background()); err != nil {
				t.Fatal(err)
			}
			original := fixture.objects["docker-object-1"]
			successor := original
			switch changed {
			case "projection":
				other, _ := testRuntimeSourceProjection(t)
				successor.SourceProjection = other.Identity()
			case "bundle":
				other, _ := testRuntimeSourceProjectionNamed(t, "foreign")
				successor.BundleHash = other.BundleHash()
			case "owner":
				successor.Owner = "operator"
			}
			if changed != "object_after_inspect" {
				fixture.objects["docker-object-1"] = successor
			}
			var removed []string
			manager.SetRunDockerFnForTest(func(ctx context.Context, args ...string) (string, error) {
				if args[0] == "rm" {
					removed = append(removed, args[len(args)-1])
				}
				out, err := fixture.run(ctx, args...)
				if changed == "object_after_inspect" && args[0] == "inspect" && args[len(args)-1] == original.ContainerName {
					delete(fixture.objects, "docker-object-1")
					fixture.objects["successor-object"] = successor
				}
				return out, err
			})
			err := manager.ReleaseSourceProjection(context.Background())
			if changed == "object_after_inspect" {
				if err != nil {
					t.Fatal(err)
				}
				if _, exists := fixture.objects["successor-object"]; !exists {
					t.Fatalf("removed same-name successor: %v", removed)
				}
			} else {
				if err == nil {
					t.Fatalf("release accepted changed %s ownership: %v", changed, removed)
				}
				if _, exists := fixture.objects["docker-object-1"]; !exists {
					t.Fatalf("removed foreign container: %v", removed)
				}
			}
			// Settle the test's deliberately foreign object without authorizing
			// production teardown to remove it.
			delete(fixture.objects, "docker-object-1")
		})
	}
}
