package workspace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/containeridentity"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

const ClaudeStateDirectory = "/opt/swarm/provider/claude"

// ClaudeStateRequest carries storage authority, never a provider resume ID.
type ClaudeStateRequest struct {
	identity agentidentity.Identity
	session  string
	kind     string
}

func ClaudeSessionState(memory agentmemory.Plan, identity agentidentity.Identity, session string) (ClaudeStateRequest, error) {
	if _, err := memory.Normalize(); err != nil {
		return ClaudeStateRequest{}, err
	}
	if err := agentmemory.ValidateIdentity(identity, memory.Enabled); err != nil {
		return ClaudeStateRequest{}, err
	}
	kind := "invocation"
	if memory.Enabled {
		kind = "session"
	}
	return newClaudeStateRequest(identity, session, kind)
}

func ClaudeForkState(identity agentidentity.Identity, session string) (ClaudeStateRequest, error) {
	if err := identity.Validate(); err != nil {
		return ClaudeStateRequest{}, err
	}
	return newClaudeStateRequest(identity, session, "fork")
}

// A reclaimed delivery retains its invocation, even when its process-local
// stateless session is rebound. The claim still fences every provider dispatch.
func ClaudeDeliveryState(identity agentidentity.Identity, claim deliverylifecycle.Claim) (ClaudeStateRequest, error) {
	if err := identity.Validate(); err != nil {
		return ClaudeStateRequest{}, err
	}
	if err := claim.Validate(); err != nil {
		return ClaudeStateRequest{}, err
	}
	if claim.RunID() != identity.RunID || claim.SubscriberClass() != deliverylifecycle.SubscriberAgent || claim.SubscriberID() != identity.AgentID() {
		return ClaudeStateRequest{}, fmt.Errorf("Claude invocation claim does not identify its actor")
	}
	return ClaudeStateRequest{identity: identity.Normalize(), session: claim.DeliveryID(), kind: "delivery"}, nil
}

func ClaudeProbeState(attempt string) (ClaudeStateRequest, error) {
	return newClaudeStateRequest(agentidentity.Identity{}, attempt, "probe")
}

func newClaudeStateRequest(identity agentidentity.Identity, session, kind string) (ClaudeStateRequest, error) {
	id, err := uuid.Parse(session)
	if err != nil || id == uuid.Nil || id.String() != session {
		return ClaudeStateRequest{}, fmt.Errorf("Claude state requires an exact session/attempt UUID")
	}
	return ClaudeStateRequest{identity: identity.Normalize(), session: session, kind: kind}, nil
}

func (r ClaudeStateRequest) Reusable() bool { return r.kind == "session" }

func (r ClaudeStateRequest) persistent() bool { return r.Reusable() || r.kind == "delivery" }

func (r ClaudeStateRequest) key(bundleHash string) (string, error) {
	if err := sourceartifact.ValidateHash(bundleHash); err != nil {
		return "", err
	}
	if r.kind != "session" && r.kind != "invocation" && r.kind != "probe" && r.kind != "fork" && r.kind != "delivery" {
		return "", fmt.Errorf("Claude state authority missing")
	}
	actor := "probe"
	if r.kind != "probe" {
		var err error
		actor, err = r.identity.Fingerprint()
		if err != nil {
			return "", err
		}
	}
	digest := sha256.New()
	for _, field := range []string{"swarm-claude-state-v1", bundleHash, actor, r.session, r.kind} {
		writeDurableIdentityField(digest, field)
	}
	return "swarm-claude-state-v1-" + hex.EncodeToString(digest.Sum(nil)), nil
}

// ClaudeState is a checked, invocation-bound view of workspace-owned private backing.
// Releasing it always removes the disposable execution container, never reusable files.
type ClaudeState interface {
	Directory() string
	CheckHead(context.Context, string) error
	Release(context.Context) error
}

type ClaudeWorkspaceResolver interface {
	ResolveClaudeWorkspace(context.Context, actors.AgentConfig, ClaudeStateRequest, string) (*Target, error)
}

type dockerClaudeState struct {
	manager   *DockerManager
	container string
	key       string
	workdir   string
	identity  containeridentity.Identity
}

func (*dockerClaudeState) Directory() string { return ClaudeStateDirectory }

func (s *dockerClaudeState) CheckHead(ctx context.Context, head string) error {
	id, err := uuid.Parse(head)
	if err != nil || id == uuid.Nil || id.String() != head {
		return fmt.Errorf("Claude provider head must be an exact UUID")
	}
	_, err = s.manager.RunDocker(ctx, "exec", s.container, "node", "-e", claudeCheckHeadScript, ClaudeStateDirectory, s.key, s.workdir, head)
	if err != nil {
		return fmt.Errorf("Claude provider head backing unavailable or corrupt: %w", err)
	}
	return nil
}

func (s *dockerClaudeState) Release(ctx context.Context) error {
	if err := s.manager.removeProjectionContainer(ctx, s.identity); err != nil {
		return err
	}
	s.manager.projectionMu.Lock()
	delete(s.manager.projectionContainers, s.container)
	s.manager.projectionMu.Unlock()
	return nil
}

func (m *DockerManager) ResolveClaudeWorkspace(ctx context.Context, actor actors.AgentConfig, request ClaudeStateRequest, confirmedHead string) (_ *Target, retErr error) {
	key, err := request.key(m.cfg.BundleHash)
	if err != nil {
		return nil, err
	}
	if request.kind != "probe" {
		identity, err := actor.ConcreteIdentity()
		if err != nil || identity.Normalize() != request.identity.Normalize() {
			return nil, fmt.Errorf("Claude state actor does not match its session authority")
		}
	}
	if source, ok := correlation.SourceArtifactFactFromContext(ctx); ok && source.BundleHash() != m.cfg.BundleHash {
		return nil, fmt.Errorf("Claude state source does not match the admitted workspace")
	}
	var base *Target
	if request.kind == "probe" {
		base, err = m.ResolveWorkspaceForCapabilityAdmission(ctx, actor)
	} else {
		base, err = m.ResolveWorkspace(ctx, actor)
	}
	if err != nil {
		return nil, err
	}
	if request.persistent() && confirmedHead != "" {
		if _, err := m.RunDocker(ctx, "volume", "inspect", key); err != nil {
			return nil, fmt.Errorf("confirmed Claude session backing is missing: %w", err)
		}
	}
	// A private per-session container shares only the existing logical workspace
	// mounts, not another session's provider home or the operator's credentials.
	container := base.Container + "-claude-" + strings.TrimPrefix(key, "swarm-claude-state-v1-")[:24]
	identity, found, err := m.inspectRuntimeContainerIdentity(ctx, base.Container)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("Claude base workspace has no runtime identity")
	}
	identity.ContainerName = container
	args := []string{"--volumes-from", base.Container}
	if request.persistent() {
		args = append(args, "--mount", "type=volume,source="+key+",target="+ClaudeStateDirectory)
	} else {
		args = append(args, "--tmpfs", ClaudeStateDirectory+":rw,mode=0700,uid=10001,gid=10001")
	}
	args = append(args, "-w", base.Workdir, m.cfg.WorkspaceImage, "sleep", "infinity")
	state := &dockerClaudeState{manager: m, container: container, key: key, workdir: base.Workdir, identity: identity}
	bound := false
	defer func() {
		// Successful bindings are released by the invocation or projection owner.
		// A failed bind has never authorized a provider and must not keep a container.
		if !bound {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			retErr = errors.Join(retErr, state.Release(cleanup))
		}
	}()
	if err := m.EnsureContainerRunningWithIdentity(ctx, container, identity, args); err != nil {
		return nil, err
	}
	if _, err := m.RunDocker(ctx, "exec", "--user", "0", container, "node", "-e", claudePrepareStateScript, ClaudeStateDirectory, key, fmt.Sprint(confirmedHead != "")); err != nil {
		return nil, fmt.Errorf("prepare Claude private backing: %w", err)
	}
	if confirmedHead != "" {
		if err := state.CheckHead(ctx, confirmedHead); err != nil {
			return nil, err
		}
	}
	target := *base
	target.Container = container
	target.ClaudeState = state
	bound = true
	return &target, nil
}

const claudePrepareStateScript = `
const fs=require('fs'); const [root,key,existing]=process.argv.slice(1);
const marker=root+'/.swarm-authority';
const stat=fs.lstatSync(root); if(!stat.isDirectory()||stat.isSymbolicLink()) throw Error('invalid provider backing');
if(!fs.existsSync(marker)) {
  if(existing==='true'||fs.readdirSync(root).length!==0) throw Error('provider backing authority missing');
  fs.chownSync(root,10001,10001); fs.chmodSync(root,0o700);
  fs.writeFileSync(marker,key,{flag:'wx',mode:0o400}); fs.chownSync(marker,10001,10001);
}
if(fs.lstatSync(marker).isSymbolicLink()||fs.readFileSync(marker,'utf8')!==key) throw Error('provider backing authority mismatch');
const current=fs.statSync(root); if(current.uid!==10001||(current.mode&0o777)!==0o700) throw Error('provider backing permissions invalid');
`

const claudeCheckHeadScript = `
const fs=require('fs'),rl=require('readline'); const [root,key,cwd,head]=process.argv.slice(1);
if(fs.lstatSync(root+'/.swarm-authority').isSymbolicLink()||fs.readFileSync(root+'/.swarm-authority','utf8')!==key) throw Error('provider backing authority mismatch');
fs.accessSync(root,fs.constants.R_OK|fs.constants.W_OK);
const project=root+'/projects/'+cwd.replace(/[^a-zA-Z0-9]/g,'-');
for(const dir of [root,root+'/projects',project]) {const st=fs.lstatSync(dir);if(!st.isDirectory()||st.isSymbolicLink()) throw Error('provider transcript path invalid');}
const path=project+'/'+head+'.jsonl';
const stat=fs.lstatSync(path); if(!stat.isFile()||stat.isSymbolicLink()||stat.size===0) throw Error('provider transcript missing');
(async()=>{let found=false; for await(const line of rl.createInterface({input:fs.createReadStream(path),crlfDelay:Infinity})) {
  if(!line.trim()) continue; const row=JSON.parse(line);
  if(row.type==='user'||row.type==='assistant') {if(row.sessionId!==head) throw Error('provider transcript identity mismatch'); found=true;}
} if(!found) throw Error('provider transcript has no conversation');})().catch(()=>{console.error('provider transcript unavailable or corrupt');process.exitCode=1;});
`
