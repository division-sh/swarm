package workspace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/division-sh/swarm/internal/runtime/containeridentity"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

type ForkChatWorkspaceResolver interface {
	ResolveForkChatWorkspace(context.Context, actors.AgentConfig) (*Target, error)
}

func ResolveForForkChat(ctx context.Context, resolver Resolver, actor actors.AgentConfig) (*Target, error) {
	owner, ok := resolver.(ForkChatWorkspaceResolver)
	if !ok {
		return nil, forkChatWorkspaceRefusal(fmt.Errorf("workspace does not own fork-chat execution"))
	}
	return owner.ResolveForkChatWorkspace(ctx, actor)
}

func forkChatWorkspaceRefusal(err error) error {
	return failures.Wrap(failures.ClassLifecycleConflict, "forkchat_workspace_authority_invalid", "workspace", "prepare_forkchat_target", nil, err)
}

func forkChatTargetIdentity(ctx context.Context, actor actors.AgentConfig, bundleHash, projectionID string) (string, effects.Authority, error) {
	authority, ok := effects.AuthorityFromContext(ctx)
	identity, err := actor.ConcreteIdentity()
	if err != nil || !ok || authority.Kind != effects.AuthorityConversationForkChat || !authority.Valid() ||
		!authority.LeaseExpiresAt.After(time.Now()) || authority.ForkChat.SourceRunID != identity.RunID ||
		authority.ForkChat.BundleHash != bundleHash || authority.ExecutionMode != actor.ExecutionMode ||
		authority.Normal != (effects.LifecycleToken{}) || authority.SelectedFork != (effects.SelectedContractForkAuthority{}) {
		return "", effects.Authority{}, forkChatWorkspaceRefusal(fmt.Errorf("fork-chat authority does not identify the snapshot actor and selected source"))
	}
	if source, ok := correlation.SourceArtifactFactFromContext(ctx); ok && source.BundleHash() != bundleHash {
		return "", effects.Authority{}, forkChatWorkspaceRefusal(fmt.Errorf("fork-chat source provenance differs from the workspace"))
	}
	if err := ctx.Err(); err != nil {
		return "", effects.Authority{}, forkChatWorkspaceRefusal(err)
	}
	current, err := effects.ForkChatWorkspaceCurrent(ctx)
	if err != nil || !current {
		return "", effects.Authority{}, forkChatWorkspaceRefusal(errors.Join(err, fmt.Errorf("fork-chat authority is not current in the selected store")))
	}
	if err := ctx.Err(); err != nil || !authority.LeaseExpiresAt.After(time.Now()) {
		return "", effects.Authority{}, forkChatWorkspaceRefusal(errors.Join(err, fmt.Errorf("fork-chat authority expired during target observation")))
	}
	fingerprint, err := identity.Fingerprint()
	if err != nil {
		return "", effects.Authority{}, forkChatWorkspaceRefusal(err)
	}
	digest := sha256.New()
	for _, field := range []string{"swarm-forkchat-target-v1", bundleHash, projectionID, fingerprint,
		authority.ForkChat.ForkID, authority.ForkChat.ForkTurnID, authority.ForkChat.SourceRunID,
		authority.ForkChat.ActorTokenID, authority.ForkChat.RequestOccurrenceID, authority.ForkChat.RequestHash,
		authority.ExecutionOwner, strconv.FormatUint(authority.FenceGeneration, 10), string(authority.ExecutionMode)} {
		writeDurableIdentityField(digest, field)
	}
	return "swarm-forkchat-" + hex.EncodeToString(digest.Sum(nil)), authority, nil
}

func (m *DockerManager) ResolveForkChatWorkspace(ctx context.Context, actor actors.AgentConfig) (_ *Target, retErr error) {
	name, authority, err := forkChatTargetIdentity(ctx, actor, m.cfg.BundleHash, m.cfg.SourceProjectionID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithDeadline(ctx, authority.LeaseExpiresAt)
	defer cancel()
	mounts, err := m.standardMountArgs()
	if err != nil {
		return nil, err
	}
	identity := containeridentity.Identity{
		Owner: containeridentity.OwnerRuntime, Kind: containeridentity.KindSystem,
		CreationSource: "workspace.ResolveForkChatWorkspace", ContainerName: name, WorkspaceScope: "forkchat",
		BundleHash: m.cfg.BundleHash, SourceProjection: m.cfg.SourceProjectionID,
	}
	// Snapshot provenance selects immutable authored bytes, never source-run data
	// or its writable workspace/provider backing. All sandbox writes are disposable.
	args := append(mounts, "--tmpfs", m.cfg.WorkspaceWorkdir+":rw,mode=0700,uid=10001,gid=10001",
		"--tmpfs", ClaudeStateDirectory+":rw,mode=0700,uid=10001,gid=10001",
		"-w", m.cfg.WorkspaceWorkdir, m.cfg.WorkspaceImage, "sleep", "infinity")
	target := &Target{Container: name, Workdir: m.cfg.WorkspaceWorkdir, Backend: BackendDocker, Mounts: dockerExecutionMounts(m.cfg, false)}
	target.release = func(ctx context.Context) error {
		if err := m.removeProjectionContainer(ctx, identity); err != nil {
			return err
		}
		m.projectionMu.Lock()
		delete(m.projectionContainers, name)
		m.projectionMu.Unlock()
		return nil
	}
	if err := m.ensureContainerRunningWithIdentity(ctx, name, identity, args, false); err != nil {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		return nil, failures.Wrap(failures.ClassDependencyUnavailable, "forkchat_workspace_unavailable", "workspace", "prepare_forkchat_target", nil, errors.Join(err, target.Release(cleanup)))
	}
	return target, nil
}

func (m *HostManager) ResolveForkChatWorkspace(ctx context.Context, actor actors.AgentConfig) (*Target, error) {
	name, _, err := forkChatTargetIdentity(ctx, actor, m.cfg.BundleHash, m.cfg.SourceProjectionID)
	if err != nil {
		return nil, err
	}
	if err := m.beginProjectionOperation(); err != nil {
		return nil, err
	}
	defer m.projectionOps.Done()
	if err := m.ensurePrereqs(ctx); err != nil {
		return nil, err
	}
	root, err := m.hostRoot()
	if err != nil {
		return nil, err
	}
	workdir, err := os.MkdirTemp(root, name+"-")
	if err != nil {
		return nil, err
	}
	mounts, err := m.hostExecutionMounts(workdir, "")
	if err != nil {
		return nil, errors.Join(err, os.RemoveAll(workdir))
	}
	return &Target{Workdir: workdir, Backend: BackendHost, Mounts: mounts, release: func(context.Context) error { return os.RemoveAll(workdir) }}, nil
}
