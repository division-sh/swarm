package workspace

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestClaudeStateNamespace(t *testing.T) {
	actor := agentidentitytest.DeclaredForRun(t, uuid.NewString(), "actor", "agents.yaml", "flow", "instance", "flow/instance")
	session := uuid.NewString()
	memory := agentmemory.Authored(true)
	request, err := ClaudeSessionState(memory, actor, session)
	if err != nil {
		t.Fatal(err)
	}
	hash := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	key, err := request.key(hash)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutation := range map[string]func(ClaudeStateRequest) ClaudeStateRequest{
		"session": func(r ClaudeStateRequest) ClaudeStateRequest { r.session = uuid.NewString(); return r },
		"run":     func(r ClaudeStateRequest) ClaudeStateRequest { r.identity.RunID = uuid.NewString(); return r },
		"kind":    func(r ClaudeStateRequest) ClaudeStateRequest { r.kind = "invocation"; return r },
	} {
		t.Run(name, func(t *testing.T) {
			other, err := mutation(request).key(hash)
			if err != nil || other == key {
				t.Fatalf("key=%s err=%v", other, err)
			}
		})
	}
	other, err := request.key("bundle-v2:sha256:" + strings.Repeat("a", 63) + "b")
	if err != nil || key == other {
		t.Fatal("full source hash not isolated")
	}
	if _, err := (ClaudeStateRequest{}).key(hash); err == nil {
		t.Fatal("accepted absent authority")
	}
	if _, err := ClaudeProbeState("not-a-uuid"); err == nil {
		t.Fatal("accepted invalid probe")
	}
	claim := func(id string, version int64) deliverylifecycle.Claim {
		value, err := deliverylifecycle.AdmitPersistedClaim(id, actor.RunID, "route", uuid.NewString(), version, deliverylifecycle.SubscriberAgent, actor.AgentID())
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	id := uuid.NewString()
	one, err := ClaudeDeliveryState(actor, claim(id, 1))
	if err != nil {
		t.Fatal(err)
	}
	two, err := ClaudeDeliveryState(actor, claim(id, 2))
	if err != nil {
		t.Fatal(err)
	}
	a, _ := one.key(hash)
	b, _ := two.key(hash)
	if a != b || one.Reusable() || !one.persistent() {
		t.Fatal("claim replacement changed invocation or acquired memory")
	}
	otherDelivery, _ := ClaudeDeliveryState(actor, claim(uuid.NewString(), 1))
	c, _ := otherDelivery.key(hash)
	if a == c {
		t.Fatal("distinct deliveries share invocation backing")
	}
	foreign := actor
	foreign.RunID = uuid.NewString()
	if _, err := ClaudeDeliveryState(foreign, claim(id, 3)); err == nil {
		t.Fatal("accepted foreign delivery")
	}
}

// Opt-in real Docker proof: synthetic JSONL, network none, no credentials.
func TestClaudeStateDockerRetentionAndRefusal(t *testing.T) {
	image := os.Getenv("SWARM_CLAUDE_STATE_DOCKER_IMAGE")
	if image == "" {
		t.Skip("set SWARM_CLAUDE_STATE_DOCKER_IMAGE for offline Docker proof")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	sourceName := "claude-private-" + uuid.NewString()
	first, _ := testRuntimeSourceProjectionNamed(t, sourceName)
	second, _ := testRuntimeSourceProjectionNamed(t, sourceName)
	newManager := func() *DockerManager {
		m := NewDockerManager()
		cfg := DefaultDockerConfig()
		cfg.WorkspaceImage = image
		cfg.WorkspaceNetwork = "none"
		m.SetConfig(cfg)
		return m
	}
	m1, m2 := newManager(), newManager()
	bindTestDockerProjection(t, m1, first)
	bindTestDockerProjection(t, m2, second)
	actor := actors.AgentConfig{ExecutionMode: "live", ID: "state-agent", FlowPath: "flow/instance", Identity: agentidentitytest.DeclaredForRun(t, uuid.NewString(), "state-agent", "agents.yaml", "flow", "instance", "flow/instance")}
	ctx = correlation.WithRunID(ctx, actor.Identity.RunID)
	request, err := ClaudeSessionState(agentmemory.Authored(true), actor.Identity, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	key, err := request.key(first.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := actor.Identity.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	workKey := durableWorkspaceKeyForTest(t, first.BundleHash(), durableWorkspaceAgent, fingerprint)
	volumes := []string{key, workKey}
	t.Cleanup(func() {
		for _, manager := range []*DockerManager{m1, m2} {
			if err := manager.ReleaseSourceProjection(context.Background()); err != nil {
				t.Error(err)
			}
		}
		for _, volume := range volumes {
			if _, err := m1.RunDocker(context.Background(), "volume", "rm", volume); err != nil {
				t.Error(err)
			}
		}
	})
	a, err := m1.ResolveClaudeWorkspace(ctx, actor, request, "")
	if err != nil {
		t.Fatal(err)
	}
	head := uuid.NewString()
	seed := `const fs=require('fs');const [root,cwd,head]=process.argv.slice(1);const p=root+'/projects/'+cwd.replace(/[^a-zA-Z0-9]/g,'-');fs.mkdirSync(p,{recursive:true});fs.writeFileSync(p+'/'+head+'.jsonl',JSON.stringify({type:'user',sessionId:head,uuid:'44444444-4444-4444-8444-444444444444',parentUuid:null,isSidechain:false,cwd,version:'2.1.87',timestamp:'2026-09-08T00:00:00Z',message:{role:'user',content:'offline synthetic retained context'}})+'\n');`
	if _, err := m1.RunDocker(ctx, "exec", a.Container, "node", "-e", seed, ClaudeStateDirectory, a.Workdir, head); err != nil {
		t.Fatal(err)
	}
	if err := a.ClaudeState.CheckHead(ctx, head); err != nil {
		t.Fatal(err)
	}
	resume := func(m *DockerManager, target *Target, head string) {
		out, err := m.RunDocker(ctx, "exec", "-e", "CLAUDE_CONFIG_DIR="+ClaudeStateDirectory, "-w", target.Workdir, target.Container, "claude", "-p", "--resume", head, "--fork-session", "--session-id", uuid.NewString(), "--output-format", "stream-json", "--verbose", "offline probe")
		result := strings.ToLower(fmt.Sprintf("%s %v", out, err))
		if strings.Contains(result, "no conversation found") || (!strings.Contains(result, "login") && !strings.Contains(result, "authentication") && !strings.Contains(result, "api key")) {
			t.Fatalf("expected recognized transcript then offline auth refusal: %s", result)
		}
	}
	resume(m1, a, head)
	if err := m1.ReleaseSourceProjection(ctx); err != nil {
		t.Fatal(err)
	}
	b, err := m2.ResolveClaudeWorkspace(ctx, actor, request, head)
	if err != nil {
		t.Fatal(err)
	}
	if a.Container == b.Container {
		t.Fatal("projection container was not replaced")
	}
	resume(m2, b, head)
	// Stopped-container replacement retains the same private backing.
	if _, err := m2.RunDocker(ctx, "stop", b.Container); err != nil {
		t.Fatal(err)
	}
	b, err = m2.ResolveClaudeWorkspace(ctx, actor, request, head)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.ClaudeState.CheckHead(ctx, head); err != nil {
		t.Fatal(err)
	}
	// Forced container removal is also disposable; no reconstruction of files.
	if _, err := m2.RunDocker(ctx, "rm", "--force", b.Container); err != nil {
		t.Fatal(err)
	}
	b, err = m2.ResolveClaudeWorkspace(ctx, actor, request, head)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"invocation", "fork"} {
		t.Run(kind, func(t *testing.T) {
			var ephemeral ClaudeStateRequest
			var err error
			if kind == "fork" {
				ephemeral, err = ClaudeForkState(actor.Identity, uuid.NewString())
			} else {
				ephemeral, err = ClaudeSessionState(agentmemory.Authored(false), actor.Identity, uuid.NewString())
			}
			if err != nil {
				t.Fatal(err)
			}
			e, err := m2.ResolveClaudeWorkspace(ctx, actor, ephemeral, "")
			if err != nil {
				t.Fatal(err)
			}
			if err := e.ClaudeState.CheckHead(ctx, head); err == nil {
				t.Fatal("temporary invocation inherited conversation files")
			}
			ephemeralHead := uuid.NewString()
			if _, err := m2.RunDocker(ctx, "exec", e.Container, "node", "-e", seed, ClaudeStateDirectory, e.Workdir, ephemeralHead); err != nil {
				t.Fatal(err)
			}
			e, err = m2.ResolveClaudeWorkspace(ctx, actor, ephemeral, ephemeralHead)
			if err != nil {
				t.Fatal(err)
			}
			resume(m2, e, ephemeralHead)
			if err := b.ClaudeState.CheckHead(ctx, ephemeralHead); err == nil {
				t.Fatal("conversation inherited temporary invocation files")
			}
			if err := e.ClaudeState.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := m2.ResolveClaudeWorkspace(ctx, actor, ephemeral, ephemeralHead); err == nil {
				t.Fatal("terminated tmpfs survived")
			}
			key, _ := ephemeral.key(first.BundleHash())
			if _, err := m2.RunDocker(ctx, "volume", "inspect", key); err == nil {
				t.Fatal("ephemeral invocation created reusable backing")
			}
		})
	}
	if err := m2.EnsureSystemWorkspaces(ctx); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []durableWorkspaceKind{durableWorkspaceScaffold, durableWorkspaceSystemEntity, durableWorkspaceSystemNginx, durableWorkspaceSystemSystemd} {
		volumes = append(volumes, durableWorkspaceKeyForTest(t, first.BundleHash(), kind, ""))
	}
	probeRequest, err := ClaudeProbeState(uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	probe, err := m2.ResolveClaudeWorkspace(ctx, actor, probeRequest, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := probe.ClaudeState.CheckHead(ctx, head); err == nil {
		t.Fatal("startup probe inherited conversation files")
	}
	probeHead := uuid.NewString()
	if _, err := m2.RunDocker(ctx, "exec", probe.Container, "node", "-e", seed, ClaudeStateDirectory, probe.Workdir, probeHead); err != nil {
		t.Fatal(err)
	}
	if err := probe.ClaudeState.CheckHead(ctx, probeHead); err != nil {
		t.Fatal(err)
	}
	if err := b.ClaudeState.CheckHead(ctx, probeHead); err == nil {
		t.Fatal("conversation inherited startup-probe files")
	}
	if err := probe.ClaudeState.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := m2.ResolveClaudeWorkspace(ctx, actor, probeRequest, probeHead); err == nil {
		t.Fatal("released probe artifacts survived")
	}
	// Corruption and absence are admission failures, not new conversations.
	if _, err := m2.RunDocker(ctx, "exec", b.Container, "node", "-e", `const fs=require('fs');const [root,cwd,head]=process.argv.slice(1);fs.writeFileSync(root+'/projects/'+cwd.replace(/[^a-zA-Z0-9]/g,'-')+'/'+head+'.jsonl','not JSON\n');`, ClaudeStateDirectory, b.Workdir, head); err != nil {
		t.Fatal(err)
	}
	if _, err := m2.ResolveClaudeWorkspace(ctx, actor, request, head); err == nil {
		t.Fatal("accepted corrupt transcript")
	}
	missing := request
	missing.session = uuid.NewString()
	if _, err := m2.ResolveClaudeWorkspace(ctx, actor, missing, head); err == nil {
		t.Fatal("recreated missing confirmed backing")
	}
	missingKey, _ := missing.key(first.BundleHash())
	if _, err := m2.RunDocker(ctx, "volume", "inspect", missingKey); err == nil {
		t.Fatal("missing backing was created")
	}

	// A stateless delivery reclamation changes its claim/session attempt, not
	// the exact invocation backing. No reusable conversation memory is granted.
	deliveryID := uuid.NewString()
	deliveryRequest := func(version int64) ClaudeStateRequest {
		claim, err := deliverylifecycle.AdmitPersistedClaim(deliveryID, actor.Identity.RunID, "route", uuid.NewString(), version, deliverylifecycle.SubscriberAgent, actor.ID)
		if err != nil {
			t.Fatal(err)
		}
		request, err := ClaudeDeliveryState(actor.Identity, claim)
		if err != nil {
			t.Fatal(err)
		}
		return request
	}
	invocation := deliveryRequest(1)
	invocationKey, err := invocation.key(first.BundleHash())
	if err != nil {
		t.Fatal(err)
	}
	volumes = append(volumes, invocationKey)
	initial, err := m2.ResolveClaudeWorkspace(ctx, actor, invocation, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m2.RunDocker(ctx, "exec", initial.Container, "node", "-e", seed, ClaudeStateDirectory, initial.Workdir, head); err != nil {
		t.Fatal(err)
	}
	if err := initial.ClaudeState.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if exists, _, err := m2.InspectContainer(ctx, initial.Container); err != nil || exists {
		t.Fatalf("invocation container not disposed: exists=%v err=%v", exists, err)
	}
	reclaimed := deliveryRequest(2)
	if reclaimed.Reusable() {
		t.Fatal("stateless reclamation acquired conversation memory")
	}
	next, err := m2.ResolveClaudeWorkspace(ctx, actor, reclaimed, head)
	if err != nil {
		t.Fatalf("reclaimed invocation lost confirmed backing: %v", err)
	}
	if err := next.ClaudeState.CheckHead(ctx, head); err != nil {
		t.Fatal(err)
	}
	resume(m2, next, head)
	foreign := actor
	foreign.Identity.RunID = uuid.NewString()
	if _, err := m2.ResolveClaudeWorkspace(ctx, foreign, reclaimed, head); err == nil {
		t.Fatal("foreign actor adopted invocation backing")
	}
	if _, err := m2.RunDocker(ctx, "exec", "--user", "0", next.Container, "node", "-e", `const fs=require('fs');const p=process.argv[1]+'/.swarm-authority';fs.chmodSync(p,0o600);fs.writeFileSync(p,'foreign');`, ClaudeStateDirectory); err != nil {
		t.Fatal(err)
	}
	if _, err := m2.ResolveClaudeWorkspace(ctx, actor, reclaimed, head); err == nil {
		t.Fatal("foreign durable marker authorized resume")
	}
}

func TestClaudeStateDockerSharedWorkspaceIsolation(t *testing.T) {
	image := os.Getenv("SWARM_CLAUDE_STATE_DOCKER_IMAGE")
	if image == "" {
		t.Skip("set SWARM_CLAUDE_STATE_DOCKER_IMAGE for offline Docker proof")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	projection, _ := testRuntimeSourceProjectionNamed(t, "claude-shared-"+uuid.NewString())
	m := NewDockerManager()
	cfg := DefaultDockerConfig()
	cfg.WorkspaceImage = image
	cfg.WorkspaceNetwork = "none"
	m.SetConfig(cfg)
	bindTestDockerProjection(t, m, projection)
	m.SetSemanticSource(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{"workspace_classes": {Value: map[string]any{"shared": map[string]any{"workspace_scope": "per-flow-instance"}}}}}}))
	run := uuid.NewString()
	session := uuid.NewString()
	var targets []*Target
	var volumes []string
	workKey := durableWorkspaceKeyForTest(t, projection.BundleHash(), durableWorkspaceFlow, "flow/instance")
	volumes = append(volumes, workKey)
	t.Cleanup(func() {
		if err := m.ReleaseSourceProjection(context.Background()); err != nil {
			t.Error(err)
		}
		for _, v := range volumes {
			if _, err := m.RunDocker(context.Background(), "volume", "rm", v); err != nil {
				t.Error(err)
			}
		}
	})
	for _, name := range []string{"first", "second"} {
		actor := actors.AgentConfig{ExecutionMode: "live", ID: name, FlowPath: "flow/instance", WorkspaceClass: "shared", Identity: agentidentitytest.DeclaredForRun(t, run, name, "agents.yaml", "flow", "instance", "flow/instance")}
		request, err := ClaudeSessionState(agentmemory.Authored(true), actor.Identity, session)
		if err != nil {
			t.Fatal(err)
		}
		key, err := request.key(projection.BundleHash())
		if err != nil {
			t.Fatal(err)
		}
		volumes = append(volumes, key)
		target, err := m.ResolveClaudeWorkspace(ctx, actor, request, "")
		if err != nil {
			t.Fatal(err)
		}
		targets = append(targets, target)
	}
	// Seed a workfile as fixture data; workspace writability is not this test's
	// provider-private authority claim.
	if _, err := m.RunDocker(ctx, "exec", "--user", "0", targets[0].Container, "node", "-e", `require('fs').writeFileSync('/workspace/shared','shared');`); err != nil {
		t.Fatal(err)
	}
	if _, err := m.RunDocker(ctx, "exec", targets[0].Container, "node", "-e", `require('fs').writeFileSync(process.argv[1]+'/private','first');`, ClaudeStateDirectory); err != nil {
		t.Fatal(err)
	}
	if _, err := m.RunDocker(ctx, "exec", targets[1].Container, "node", "-e", `const fs=require('fs');if(fs.readFileSync('/workspace/shared','utf8')!=='shared'||fs.existsSync(process.argv[1]+'/private'))process.exit(1);`, ClaudeStateDirectory); err != nil {
		t.Fatal("shared workfiles leaked provider-private files: ", err)
	}
}
