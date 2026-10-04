package workspace

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/containeridentity"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/dataaccess"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestForkChatWorkspaceAdmissionCannotRetireSourceExecution(t *testing.T) {
	projection, _ := testRuntimeSourceProjection(t)
	manager := NewDockerManager()
	cfg := DefaultDockerConfig()
	cfg.SourceProjection, cfg.WorkspaceNetwork = projection, ""
	manager.SetConfig(cfg)
	bindTestDockerProjection(t, manager, projection)
	manager.SetSemanticSource(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}))
	manager.SetDataProjectionProvider(workspaceProjectionProviderFunc(func(context.Context, models.AgentConfig) (dataaccess.Projection, error) {
		return dataaccess.Projection{ID: dataaccess.ProjectionID("data-projection-v1:sha256:" + strings.Repeat("b", 64)), Root: t.TempDir()}, nil
	}))
	containers := map[string]map[string]string{}
	var removals int
	var failStart bool
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		switch args[0] {
		case "inspect":
			name := args[len(args)-1]
			labels := containers[name]
			if labels == nil {
				return "", fmt.Errorf("no such object")
			}
			if args[2] == "{{.State.Running}}" {
				return "true", nil
			}
			if args[2] == "{{json .}}" {
				raw, err := json.Marshal(map[string]any{"Id": name, "State": map[string]any{"Running": true}, "Config": map[string]any{"Labels": labels}})
				return string(raw), err
			}
			if strings.Contains(args[2], "ExtraHosts") {
				return `{"hosts":["host.docker.internal:host-gateway"],"mounts":[{"Type":"bind","Source":"` + currentWorkerTestPath(t) + `","Destination":"` + WorkerContainerPath + `","RW":false}]}`, nil
			}
			raw, err := json.Marshal(labels)
			return string(raw), err
		case "create":
			labels := map[string]string{}
			for i := 0; i+1 < len(args); i++ {
				if args[i] == "--label" {
					key, value, _ := strings.Cut(args[i+1], "=")
					labels[key] = value
				}
			}
			containers[args[2]] = labels
		case "start":
			if failStart {
				return "", fmt.Errorf("exact sandbox start failed")
			}
		case "rm":
			removals++
			delete(containers, args[len(args)-1])
		}
		return "", nil
	})
	runID := uuid.NewString()
	actor := models.AgentConfig{ID: "source-agent", ExecutionMode: effects.ExecutionModeMock, Identity: agentidentitytest.RootDeclaredForRun(t, runID, "source-agent", "test/agents.yaml")}
	source, err := manager.ResolveWorkspace(context.Background(), actor)
	if err != nil {
		t.Fatal(err)
	}
	labels := containers[source.Container]
	if labels["dev.swarm.data_projection_id"] == "" {
		t.Fatal("control did not create a source execution projection")
	}
	turnID := uuid.NewString()
	authority := effects.Authority{
		Kind: effects.AuthorityConversationForkChat, ID: turnID, ExecutionOwner: "exact-sandbox-owner",
		LeaseExpiresAt: time.Now().Add(time.Minute), FenceGeneration: 1, ExecutionMode: effects.ExecutionModeMock,
		ForkChat: effects.ConversationForkChatAuthority{ForkTurnID: turnID, ForkID: uuid.NewString(), SourceRunID: runID,
			BundleHash: projection.BundleHash(), ActorTokenID: "operator", RequestOccurrenceID: uuid.NewString(), RequestHash: "sandbox-request"},
	}
	if !authority.Valid() {
		t.Fatal("control has invalid sandbox authority")
	}
	actor.Type, actor.Role = "forkchat", "forkchat"
	store := &forkWorkspaceAuthorityStore{Harness: effecttest.New(), authority: authority}
	ctx := effects.WithController(effects.WithAuthority(context.Background(), authority), effects.NewController(store))
	target, err := manager.ResolveForkChatWorkspace(ctx, actor)
	if err != nil {
		t.Fatal(err)
	}
	if target.Container == source.Container || containers[target.Container]["dev.swarm.data_projection_id"] != "" {
		t.Fatalf("sandbox target aliases source or inherits live data: %+v", target)
	}
	if removals != 0 {
		t.Fatalf("fork-chat admission retired %d source execution containers; err=%v", removals, err)
	}
	request, err := ClaudeForkState(actor.Identity, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	claude, err := manager.ResolveClaudeWorkspace(ctx, actor, request, "")
	if err != nil || claude.Container != target.Container || claude.ClaudeState == nil {
		t.Fatalf("Claude and mock do not share the exact sandbox projection: %+v %v", claude, err)
	}
	if err := claude.ClaudeState.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if removals != 1 || !reflect.DeepEqual(containers[source.Container], labels) || len(containers) != 1 {
		t.Fatalf("cleanup did not exclusively retire sandbox: removals=%d containers=%+v", removals, containers)
	}
	t.Run("foreign-target-cannot-be-replaced", func(t *testing.T) {
		foreign := make(map[string]string, len(labels))
		for key, value := range labels {
			foreign[key] = value
		}
		foreign[containeridentity.LabelContainerName] = target.Container
		containers[target.Container] = foreign
		_, err := manager.ResolveForkChatWorkspace(ctx, actor)
		if err == nil || failures.FromError(err, "test", "target").Failure.Detail.Code != "forkchat_workspace_unavailable" || removals != 1 || !reflect.DeepEqual(containers[target.Container], foreign) {
			t.Fatalf("foreign target was adopted/replaced: err=%v removals=%d containers=%+v", err, removals, containers)
		}
		delete(containers, target.Container)
	})
	t.Run("partial-start-cleanup-is-exact", func(t *testing.T) {
		failStart = true
		defer func() { failStart = false }()
		_, err := manager.ResolveForkChatWorkspace(ctx, actor)
		if err == nil || failures.FromError(err, "test", "target").Failure.Detail.Code != "forkchat_workspace_unavailable" || removals != 2 || len(containers) != 1 || !reflect.DeepEqual(containers[source.Container], labels) {
			t.Fatalf("failed sandbox did not join exact cleanup: err=%v removals=%d containers=%+v", err, removals, containers)
		}
	})
}

func currentWorkerTestPath(t *testing.T) string {
	t.Helper()
	artifact, err := currentWorkerArtifact()
	if err != nil {
		t.Fatal(err)
	}
	return artifact.path
}

type forkWorkspaceAuthorityStore struct {
	*effecttest.Harness
	authority effects.Authority
}

func (s *forkWorkspaceAuthorityStore) IsForkChatWorkspaceAuthorityCurrent(ctx context.Context, a effects.Authority) (bool, error) {
	return s.IsExternalEffectAuthorityCurrent(ctx, a)
}

func (s *forkWorkspaceAuthorityStore) IsExternalEffectAuthorityCurrent(_ context.Context, a effects.Authority) (bool, error) {
	return a.Kind == effects.AuthorityConversationForkChat && a.ForkChat == s.authority.ForkChat &&
		a.ID == s.authority.ID && a.ExecutionOwner == s.authority.ExecutionOwner &&
		a.FenceGeneration == s.authority.FenceGeneration && a.LeaseExpiresAt.Equal(s.authority.LeaseExpiresAt), nil
}

func TestForkChatWorkspaceRejectsBadAuthorityBeforeMutation(t *testing.T) {
	projection, _ := testRuntimeSourceProjection(t)
	actor := models.AgentConfig{ID: "source-agent", ExecutionMode: effects.ExecutionModeMock,
		Identity: agentidentitytest.RootDeclaredForRun(t, uuid.NewString(), "source-agent", "agents.yaml")}
	turn := uuid.NewString()
	authority := effects.Authority{Kind: effects.AuthorityConversationForkChat, ID: turn,
		ExecutionOwner: "owner", LeaseExpiresAt: time.Now().Add(time.Minute), FenceGeneration: 1, ExecutionMode: actor.ExecutionMode,
		ForkChat: effects.ConversationForkChatAuthority{ForkTurnID: turn, ForkID: uuid.NewString(), SourceRunID: actor.Identity.RunID,
			BundleHash: projection.BundleHash(), ActorTokenID: "operator", RequestOccurrenceID: uuid.NewString(), RequestHash: "request"}}
	store := &forkWorkspaceAuthorityStore{Harness: effecttest.New(), authority: authority}
	for _, backend := range []string{BackendDocker, BackendHost} {
		t.Run(backend, func(t *testing.T) {
			docker := NewDockerManager()
			cfg := DefaultDockerConfig()
			cfg.SourceProjection = projection
			docker.SetConfig(cfg)
			bindTestDockerProjection(t, docker, projection)
			mutations := 0
			docker.SetRunDockerFnForTest(func(context.Context, ...string) (string, error) { mutations++; return "", nil })
			host := NewHostManager()
			hostCfg := DefaultHostConfig()
			hostCfg.WorkspaceRoot = t.TempDir() + "/not-created"
			host.SetConfig(hostCfg)
			if err := host.BindSourceProjection(projection); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := host.ReleaseSourceProjection(context.Background()); err != nil {
					t.Error(err)
				}
			})
			var owner ForkChatWorkspaceResolver = docker
			if backend == BackendHost {
				owner = host
			}
			for _, test := range []struct {
				name   string
				change func(*effects.Authority)
			}{
				{"missing", func(a *effects.Authority) { *a = effects.Authority{} }},
				{"malformed", func(a *effects.Authority) { a.ForkChat.RequestHash = "" }},
				{"expired", func(a *effects.Authority) { a.LeaseExpiresAt = time.Now().Add(-time.Second) }},
				{"source", func(a *effects.Authority) { a.ForkChat.SourceRunID = uuid.NewString() }},
				{"bundle", func(a *effects.Authority) { a.ForkChat.BundleHash = "bundle-v2:sha256:" + strings.Repeat("f", 64) }},
				{"mode", func(a *effects.Authority) { a.ExecutionMode = effects.ExecutionModeLive }},
				{"foreign-turn", func(a *effects.Authority) { a.ID = uuid.NewString(); a.ForkChat.ForkTurnID = a.ID }},
				{"foreign-fork", func(a *effects.Authority) { a.ForkChat.ForkID = uuid.NewString() }},
				{"foreign-occurrence", func(a *effects.Authority) { a.ForkChat.RequestOccurrenceID = uuid.NewString() }},
				{"foreign-owner", func(a *effects.Authority) { a.ExecutionOwner = "other" }},
				{"stale-fence", func(a *effects.Authority) { a.FenceGeneration++ }},
			} {
				t.Run(test.name, func(t *testing.T) {
					a := authority
					test.change(&a)
					ctx := effects.WithController(effects.WithAuthority(context.Background(), a), effects.NewController(store))
					_, err := owner.ResolveForkChatWorkspace(ctx, actor)
					if err == nil || failures.FromError(err, "test", "target").Failure.Detail.Code != "forkchat_workspace_authority_invalid" {
						t.Fatalf("not a typed pre-target refusal: %v", err)
					}
					if mutations != 0 {
						t.Fatalf("refused authority reached Docker: %d", mutations)
					}
					if _, err := os.Stat(hostCfg.WorkspaceRoot); !os.IsNotExist(err) {
						t.Fatalf("refused authority mutated host: %v", err)
					}
				})
			}
		})
	}
}
