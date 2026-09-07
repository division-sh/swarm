package workspace

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestBoundWorkspaceOwnsUsableProjectionLease(t *testing.T) {
	for _, backend := range []string{"host", "docker"} {
		for _, binding := range []string{"direct", "rebound"} {
			t.Run(backend+"/"+binding, func(t *testing.T) {
				projection, root := testRuntimeSourceProjection(t)
				hash := projection.BundleHash()
				source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
				var manager Lifecycle
				var dockerCreates []string
				if backend == "host" {
					host := NewHostManager()
					host.SetConfig(HostConfig{WorkspaceRoot: t.TempDir(), SourceMountPoint: LogicalSourceMount})
					host.SetSemanticSource(source)
					manager = host
				} else {
					docker := NewDockerManager()
					cfg := DefaultDockerConfig()
					cfg.WorkspaceNetwork = ""
					docker.SetConfig(cfg)
					docker.SetSemanticSource(source)
					docker.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
						if args[0] == "inspect" {
							return "", fmt.Errorf("no such object")
						}
						if args[0] == "create" {
							dockerCreates = append(dockerCreates, strings.Join(args, " "))
						}
						return "", nil
					})
					manager = docker
				}
				if binding == "rebound" {
					var err error
					manager, err = RebindSourceProjection(manager, projection, source)
					if err != nil {
						t.Fatal(err)
					}
				} else {
					if err := manager.BindSourceProjection(projection); err != nil {
						t.Fatal(err)
					}
				}
				defer manager.ReleaseSourceProjection(context.Background())
				if err := projection.Release(); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(root); err != nil {
					t.Fatalf("retained tree disappeared: %v", err)
				}
				if err := RequireSourceProjectionBinding(manager, hash); err != nil {
					t.Errorf("retained binding invalid: %v", err)
				}
				if err := manager.ValidateSource(context.Background(), source); err != nil {
					t.Fatal(err)
				}
				if err := manager.EnsureSystemWorkspaces(context.Background()); err != nil {
					t.Fatal(err)
				}
				target, err := manager.ResolveWorkspaceForCapabilityAdmission(context.Background(), models.AgentConfig{WorkspaceClass: "system"})
				if err != nil {
					t.Fatalf("retained workspace unusable: %v", err)
				}
				if _, err := manager.ResolveWorkspace(context.Background(), models.AgentConfig{WorkspaceClass: "system"}); err != nil {
					t.Fatal(err)
				}
				if backend == "host" {
					if len(target.Mounts) < 2 || target.Mounts[1].HostPath != root {
						t.Fatalf("wrong retained source mount: %+v", target.Mounts)
					}
				} else {
					if len(dockerCreates) == 0 {
						t.Fatal("no Docker creation proof")
					}
					for _, command := range dockerCreates {
						if !strings.Contains(command, root+":"+LogicalSourceMount+":ro") {
							t.Fatalf("missing retained source mount: %s", command)
						}
					}
				}
				if err := manager.ReleaseSourceProjection(context.Background()); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(root); !os.IsNotExist(err) {
					t.Fatalf("lifecycle final release left source tree: %v", err)
				}
			})
		}
	}
}

func TestMissingProjectionFailsBeforeDockerCreation(t *testing.T) {
	for _, surface := range []string{"eager", "scaffold", "system", "per-agent", "per-flow-instance"} {
		t.Run(surface, func(t *testing.T) {
			projection, root := testRuntimeSourceProjection(t)
			manager := NewDockerManager()
			cfg := DefaultDockerConfig()
			cfg.WorkspaceNetwork = ""
			manager.SetConfig(cfg)
			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
				Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
					"workspace_classes": {Value: map[string]any{"shared": map[string]any{"workspace_scope": "per-flow-instance"}}},
				}},
			})
			manager.SetSemanticSource(source)
			creates := []string{}
			manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
				if args[0] == "inspect" {
					return "", fmt.Errorf("no such object")
				}
				if args[0] == "create" {
					creates = append(creates, strings.Join(args, " "))
				}
				return "", nil
			})
			if err := manager.BindSourceProjection(projection); err != nil {
				t.Fatal(err)
			}
			defer manager.ReleaseSourceProjection(context.Background())
			if err := manager.ValidateSource(context.Background(), source); err != nil {
				t.Fatal(err)
			}
			moved := root + "-unavailable"
			if err := os.Rename(root, moved); err != nil {
				t.Fatal(err)
			}
			restored := false
			defer func() {
				if !restored {
					if err := os.Rename(moved, root); err != nil {
						t.Error(err)
					}
				}
			}()
			resolve := func(capability bool) error {
				if surface == "eager" {
					return manager.EnsureSystemWorkspaces(context.Background())
				} else {
					actor := models.AgentConfig{ExecutionMode: "live", ID: "worker", Identity: runtimeagentidentitytest.RootDeclared(t, "worker", "agents.yaml")}
					switch surface {
					case "scaffold", "system":
						actor.WorkspaceClass = surface
					case "per-flow-instance":
						actor.WorkspaceClass = "shared"
						actor.FlowPath = "child/instance"
						actor.Identity = runtimeagentidentitytest.Declared(t, "worker", "child/agents.yaml", "child", "instance", "child/instance")
					}
					var err error
					if capability {
						_, err = manager.ResolveWorkspaceForCapabilityAdmission(context.Background(), actor)
					} else {
						_, err = manager.ResolveWorkspace(context.Background(), actor)
					}
					return err
				}
			}
			for _, capability := range []bool{false, true} {
				if err := resolve(capability); err == nil || len(creates) > 0 {
					t.Fatalf("unavailable source admitted: capability=%v err=%v creates=%v", capability, err, creates)
				}
			}
			if err := os.Rename(moved, root); err != nil {
				t.Fatal(err)
			}
			restored = true
			for _, capability := range []bool{false, true} {
				if err := resolve(capability); err != nil {
					t.Fatalf("healthy source rejected: %v", err)
				}
			}
			if len(creates) == 0 {
				t.Fatal("healthy control did not create a workspace")
			}
			for _, command := range creates {
				if !strings.Contains(command, root+":"+LogicalSourceMount+":ro") {
					t.Fatalf("healthy source not mounted: %s", command)
				}
			}
		})
	}
}

func TestMissingProjectionHostControl(t *testing.T) {
	projection, root := testRuntimeSourceProjection(t)
	manager := NewHostManager()
	manager.SetConfig(HostConfig{WorkspaceRoot: t.TempDir(), SourceMountPoint: LogicalSourceMount})
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	manager.SetSemanticSource(source)
	if err := manager.BindSourceProjection(projection); err != nil {
		t.Fatal(err)
	}
	defer manager.ReleaseSourceProjection(context.Background())
	if err := manager.ValidateSource(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	moved := root + "-unavailable"
	if err := os.Rename(root, moved); err != nil {
		t.Fatal(err)
	}
	defer os.Rename(moved, root)
	if mounts, err := manager.hostExecutionMounts(t.TempDir(), ""); err == nil || mounts != nil {
		t.Fatalf("missing source advertised by Host mount descriptor: mounts=%v err=%v", mounts, err)
	}
	if err := manager.EnsureSystemWorkspaces(context.Background()); err == nil {
		t.Fatal("missing source accepted by eager Host")
	}
	if _, err := manager.ResolveWorkspaceForCapabilityAdmission(context.Background(), models.AgentConfig{WorkspaceClass: "system"}); err == nil {
		t.Fatal("missing source accepted by lazy Host")
	}
}
