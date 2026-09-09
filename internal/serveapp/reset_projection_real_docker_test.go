package serveapp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/containeridentity"
	agentidentitytest "github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestResetStartupProjectionDisposalRealDocker(t *testing.T) {
	if os.Getenv("SWARM_TEST_REAL_DOCKER") != "1" {
		t.Skip("set SWARM_TEST_REAL_DOCKER=1 to exercise the real Docker engine")
	}
	image := os.Getenv("SWARM_TEST_REAL_DOCKER_IMAGE")
	if image == "" {
		image = "golang:1.25-bookworm"
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"ordinary", "lost-ack", "remove-rejected"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()
				docker := workspace.NewDockerManager()
				if _, err := docker.RunDocker(ctx, "image", "inspect", image); err != nil {
					t.Fatal(err)
				}
				root := t.TempDir()
				if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: reset-projection\n"), 0600); err != nil {
					t.Fatal(err)
				}
				artifact, err := sourceartifact.AdmitDirectory(root)
				if err != nil {
					t.Fatal(err)
				}
				projection, err := sourceartifact.MaterializeRuntimeProjection(artifact)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := projection.Release(); err != nil {
						t.Error(err)
					}
				})
				intent, err := projection.CleanupIntent()
				if err != nil {
					t.Fatal(err)
				}
				successor, err := sourceartifact.MaterializeRuntimeProjection(artifact)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := successor.Release(); err != nil {
						t.Error(err)
					}
				})
				create := func(kind string, owned bool, running bool) string {
					t.Helper()
					identity := containeridentity.Identity{Owner: containeridentity.OwnerRuntime, Kind: kind, CreationSource: "reset-projection-proof", ContainerName: "swarm-reset-projection-" + uuid.NewString(), BundleHash: intent.BundleHash, SourceProjection: intent.Identity}
					if !owned {
						identity.SourceProjection = successor.Identity()
					}
					if kind == containeridentity.KindAgent || kind == containeridentity.KindFlow {
						identity.AgentIdentity = agentidentitytest.RootDeclared(t, "reset-proof", "proof/agents.yaml")
						identity.RunID = identity.AgentIdentity.RunID
						identity.ResetEligible = true
						if kind == containeridentity.KindFlow {
							identity.FlowInstance = "proof/instance"
						}
					}
					if err := identity.Validate(); err != nil {
						t.Fatal(err)
					}
					mount := projection.PrivateRoot()
					if !owned {
						mount = successor.PrivateRoot()
					}
					args := []string{"create", "--name", identity.ContainerName, "--network", "none", "--mount", "type=bind,src=" + mount + ",dst=/opt/swarm/source,readonly"}
					for key, value := range identity.Labels() {
						args = append(args, "--label", key+"="+value)
					}
					args = append(args, "--entrypoint", "/bin/sh", image, "-c", "sleep 60")
					id, err := docker.RunDocker(ctx, args...)
					if err != nil {
						t.Fatal(err)
					}
					id = strings.TrimSpace(id)
					t.Cleanup(func() {
						cleanupCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
						defer cancel()
						if _, err := docker.RunDocker(cleanupCtx, "rm", "--force", id); err != nil && !strings.Contains(strings.ToLower(err.Error()), "no such container") {
							t.Error(err)
						}
					})
					if running {
						if _, err := docker.RunDocker(ctx, "start", id); err != nil {
							t.Fatal(err)
						}
					}
					return id
				}
				owned := []string{create(containeridentity.KindScaffold, true, true), create(containeridentity.KindSystem, true, true), create(containeridentity.KindAgent, true, false), create(containeridentity.KindFlow, true, false)}
				foreign := create(containeridentity.KindScaffold, false, true)
				settler := workspace.NewDockerManager()
				settler.SetRunDockerFnForTest(func(ctx context.Context, args ...string) (string, error) {
					if args[0] == "rm" {
						if _, err := os.Stat(intent.Root); err != nil {
							t.Errorf("source removed before Docker disposal: %v", err)
						}
						if mode == "remove-rejected" {
							return "", errors.New("injected real resource removal rejection")
						}
					}
					out, err := docker.RunDocker(ctx, args...)
					if args[0] == "rm" && err == nil && mode == "lost-ack" {
						return "", errors.New("injected acknowledgment loss after real removal")
					}
					return out, err
				})
				opID := uuid.NewString()
				var selected startupownership.Store
				if backend == "sqlite" {
					selected = storetest.StartSQLiteRuntimeStore(t)
				} else {
					_, db, cleanup := testutil.StartPostgres(t)
					t.Cleanup(cleanup)
					selected = storetest.AdmitPostgresRuntimeStore(t, db)
				}
				capability, err := selected.AcquireProcessCapability(ctx, startupownership.AcquireRequest{OwnerID: "reset-resource-proof", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString()})
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := capability.Release(context.Background()); err != nil {
						t.Error(err)
					}
				}()
				_, err = capability.AdmitResetOperation(ctx, destructivereset.Request{OperationID: opID, ActorTokenID: "operator", IdempotencyKey: opID, RequestHash: opID, RequestedAt: time.Now().UTC(), IncludeSourceArtifacts: true, IncludeSourceArtifactsSet: true, SourceProjections: []destructivereset.SourceProjection{{Cleanup: intent, ManagedContainers: true}}})
				if err != nil {
					t.Fatal(err)
				}
				contexts, err := runtime.NewRuntimeContextManager(nil)
				if err != nil {
					t.Fatal(err)
				}
				supervisor := &processLifecycleSupervisor{resetStartup: true, processCapability: capability, runtimeContexts: contexts, resetContainerRuntime: settler}
				// Recover the exact disposal identity from the real pending journal,
				// not from a test-selected Docker inventory or a copied process handle.
				reset, err := supervisor.BeginDestructiveReset(ctx, opID)
				if err != nil {
					t.Fatal(err)
				}
				defer reset.Release()
				err = reset.SettleResources(ctx)
				if mode == "remove-rejected" {
					if err == nil {
						t.Fatal("unsettled real resources accepted")
					}
					if _, err := os.Stat(projection.PrivateRoot()); err != nil {
						t.Fatalf("failed disposal removed source: %v", err)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					if _, err := os.Stat(intent.Root); !os.IsNotExist(err) {
						t.Fatalf("projection remains: %v", err)
					}
				}
				for _, id := range owned {
					state, err := docker.InspectManagedContainer(ctx, id)
					if err != nil || state.Exists != (mode == "remove-rejected") {
						t.Fatalf("exact projection resource %s: %+v, %v", id, state, err)
					}
				}
				state, err := docker.InspectManagedContainer(ctx, foreign)
				if err != nil || !state.Exists || !state.Running {
					t.Fatalf("successor affected: %+v, %v", state, err)
				}
				if _, err := os.Stat(successor.PrivateRoot()); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
