package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/storetest"
)

const completion2270InvocationFile = "swarm-completion-2270-business.jsonl"

const completion2270CLIProgram = `#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const args = process.argv.slice(2);
if (args.length === 1 && args[0] === '--version') { console.log('completion protocol fixture 1'); process.exit(0); }
const booleans = new Set(['-p', '--verbose', '--include-partial-messages', '--strict-mcp-config', '--fork-session', '--dangerously-skip-permissions']);
const values = new Set(['--session-id', '--resume', '--output-format', '--permission-mode', '--system-prompt', '--tools', '--allowedTools', '--mcp-config', '--model']);
const flags = new Map();
for (let i = 0; i < args.length; i++) {
  const flag = args[i];
  if (flags.has(flag) || (!booleans.has(flag) && !values.has(flag))) throw Error('invalid invocation shape');
  if (booleans.has(flag)) flags.set(flag, true);
  else {
    if (++i === args.length) throw Error('missing invocation argument');
    flags.set(flag, args[i]);
  }
}
const session = flags.get('--session-id');
const input = fs.readFileSync(0, 'utf8');
const shape = ['-p', '--verbose', '--include-partial-messages', '--session-id', '--tools', '--allowedTools', '--mcp-config', '--strict-mcp-config'].every(flag => flags.has(flag));
if (!shape || !session || flags.get('--output-format') !== 'stream-json') throw Error('missing exact request/session/surface');
const startup = !flags.has('--model') && !flags.has('--resume') && !flags.has('--fork-session') &&
  input === 'Startup validation probe. Do not call any tools. Reply with the exact text ok.';
if (!startup && flags.get('--model') !== 'request-A') throw Error('missing exact business model');
if (!startup) {
  const evidence = {attempt_id: session, invocation_id: crypto.randomUUID(), model: flags.get('--model')};
  fs.appendFileSync(path.join(process.cwd(), 'swarm-completion-2270-business.jsonl'), JSON.stringify(evidence) + '\n');
}
(async () => {
  const configuration = JSON.parse(flags.get('--mcp-config'));
  const tools = flags.get('--tools').split(',').filter(Boolean);
  const servers = [];
  if (!configuration.mcpServers || Object.keys(configuration.mcpServers).length === 0) throw Error('missing MCP server');
  for (const [name, server] of Object.entries(configuration.mcpServers)) {
    const headers = {'Content-Type': 'application/json', 'Accept': 'application/json', ...server.headers};
    const rpc = async body => {
      const response = await fetch(server.url, {method: 'POST', headers, body: JSON.stringify(body)});
      const raw = await response.text();
      if (!response.ok) throw Error('MCP request refused');
      if (body.id === undefined) return;
      const result = JSON.parse(raw);
      if (result.error || result.id !== body.id) throw Error('MCP discovery refused');
      return result.result;
    };
    await rpc({jsonrpc: '2.0', id: 1, method: 'initialize', params: {protocolVersion: '2025-03-26', capabilities: {}, clientInfo: {name: 'completion-proof', version: '1'}}});
    await rpc({jsonrpc: '2.0', method: 'notifications/initialized', params: {}});
    const listed = await rpc({jsonrpc: '2.0', id: 2, method: 'tools/list', params: {}});
    if (!Array.isArray(listed.tools)) throw Error('invalid discovered tools');
    tools.push(...listed.tools.map(tool => 'mcp__' + name + '__' + tool.name));
    servers.push({name, status: 'connected'});
  }
  console.log(JSON.stringify({type: 'system', subtype: 'init', tools, mcp_servers: servers, model: 'initialization-not-execution', session_id: session}));
  if (startup) return;
  const directory = process.env.CLAUDE_CONFIG_DIR + '/projects/' + process.cwd().replace(/[^a-zA-Z0-9]/g, '-');
  fs.mkdirSync(directory, {recursive: true});
  fs.writeFileSync(directory + '/' + session + '.jsonl', JSON.stringify({type: 'assistant', sessionId: session}) + '\n');
  console.log(JSON.stringify({type: 'result', subtype: 'success', result: 'ok', model: 'reported-B', session_id: session, total_cost_usd: 0,
    usage: {input_tokens: 12, output_tokens: 3}}));
})().catch(error => { console.error(error.message); process.exitCode = 1; });
`

type completion2270Invocation struct {
	AttemptID    string `json:"attempt_id"`
	InvocationID string `json:"invocation_id"`
	Model        string `json:"model"`
}

type completion2270InvocationProof struct {
	mu       sync.Mutex
	contexts *runtimepkg.RuntimeContextManager
	retained []completion2270Invocation
	readErr  error
}

// Selected workspaces are not published contexts. Capture their physical
// witness at the existing joined release boundary, without retaining authority.
type completion2270ObservedWorkspace struct {
	*workspace.DockerManager
	proof       *completion2270InvocationProof
	selected    bool
	captureOnce sync.Once
	captureErr  error
}

func completion2270ObserveSelectedWorkspace(req selectedAPICapabilityRequest, proof *completion2270InvocationProof) (selectedAPICapabilityRequest, error) {
	manager, ok := req.Workspaces.(*workspace.DockerManager)
	if !ok || manager == nil || proof == nil {
		return req, errors.New("selected CLI observation requires its concrete Docker workspace owner")
	}
	req.Workspaces = &completion2270ObservedWorkspace{DockerManager: manager, proof: proof}
	return req, nil
}

func (w *completion2270ObservedWorkspace) RebindSourceProjection(projection *sourceartifact.RuntimeProjection, source semanticview.Source) (workspace.Lifecycle, error) {
	lifecycle, err := w.DockerManager.RebindSourceProjection(projection, source)
	if err != nil {
		return nil, err
	}
	return &completion2270ObservedWorkspace{DockerManager: lifecycle.(*workspace.DockerManager), proof: w.proof, selected: true}, nil
}

func (w *completion2270ObservedWorkspace) ReleaseSourceProjection(ctx context.Context) error {
	if w.selected {
		w.captureOnce.Do(func() {
			rows, err := readCompletion2270WorkspaceInvocations(ctx, w.DockerManager, map[string]bool{})
			w.captureErr = err
			w.proof.mu.Lock()
			defer w.proof.mu.Unlock()
			w.proof.retained = append(w.proof.retained, rows...)
			w.proof.readErr = errors.Join(w.proof.readErr, err)
		})
	}
	return errors.Join(w.captureErr, w.DockerManager.ReleaseSourceProjection(ctx))
}

func (p *completion2270InvocationProof) observe(contexts *runtimepkg.RuntimeContextManager) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.contexts = contexts
}

func (p *completion2270InvocationProof) read(ctx context.Context) ([]completion2270Invocation, error) {
	p.mu.Lock()
	contexts := p.contexts
	retained := append([]completion2270Invocation(nil), p.retained...)
	retainedErr := p.readErr
	p.mu.Unlock()
	if retainedErr != nil {
		return nil, retainedErr
	}
	if contexts == nil {
		return nil, fmt.Errorf("CLI invocation proof has no published runtime context owner")
	}
	loaded := contexts.LoadedContexts()
	if len(loaded) == 0 {
		return nil, fmt.Errorf("CLI invocation proof has no loaded runtime context")
	}
	seen := map[string]bool{}
	var evidence []completion2270Invocation
	for _, contextDef := range loaded {
		use, lookup, err := contexts.AcquireBundleHash(ctx, contextDef.BundleHash())
		if err != nil || use == nil || !lookup.Loaded() || use.Runtime() == nil || use.Context.PublicationGeneration != contextDef.PublicationGeneration {
			return nil, fmt.Errorf("acquire published CLI workspace owner: %w", errors.Join(err, use.Done(), errors.New("runtime unavailable or replaced")))
		}
		rows, readErr := readCompletion2270WorkspaceInvocations(ctx, use.Runtime().Workspace, seen)
		if err := errors.Join(readErr, use.Done()); err != nil {
			return nil, err
		}
		evidence = append(evidence, rows...)
	}
	return append(evidence, retained...), nil
}

func readCompletion2270WorkspaceInvocations(ctx context.Context, lifecycle workspace.Lifecycle, seen map[string]bool) ([]completion2270Invocation, error) {
	manager, ok := lifecycle.(*workspace.DockerManager)
	if observed, wrapped := lifecycle.(*completion2270ObservedWorkspace); wrapped && observed != nil {
		manager, ok = observed.DockerManager, true
	}
	if !ok || manager == nil {
		return nil, fmt.Errorf("CLI invocation proof requires the acquired Docker workspace owner")
	}
	containers, err := manager.RuntimeWorkspaceContainers(ctx)
	if err != nil {
		return nil, err
	}
	var evidence []completion2270Invocation
	for _, container := range containers {
		if seen[container] {
			continue
		}
		seen[container] = true
		output, err := manager.RunDocker(ctx, "exec", container, "sh", "-c", `if [ -f "$1" ]; then cat "$1"; fi`, "completion-proof-read", completion2270InvocationFile)
		if err != nil {
			return nil, fmt.Errorf("read original CLI invocation evidence: %w", err)
		}
		rows, err := decodeCompletion2270Invocations(string(output))
		if err != nil {
			return nil, err
		}
		evidence = append(evidence, rows...)
	}
	return evidence, nil
}

func decodeCompletion2270Invocations(raw string) ([]completion2270Invocation, error) {
	decoder := json.NewDecoder(strings.NewReader(raw))
	var evidence []completion2270Invocation
	for {
		var row completion2270Invocation
		if err := decoder.Decode(&row); err != nil {
			if err == io.EOF {
				return evidence, nil
			}
			return nil, err
		}
		evidence = append(evidence, row)
	}
}

func validateCompletion2270SingleInvocation(rows []completion2270Invocation, attemptID string) error {
	if len(rows) != 1 || rows[0].AttemptID != attemptID || rows[0].InvocationID == "" || rows[0].Model != "request-A" {
		return fmt.Errorf("CLI business invocations=%+v, want one exact admitted attempt %s", rows, attemptID)
	}
	return nil
}

func requireCompletion2270NoBusinessStartup(t *testing.T, selected effects.Store, proof *completion2270InvocationProof) {
	t.Helper()
	evidence, err := proof.read(context.Background())
	attempts, readErr := storetest.ReadCompletionAttemptStorage(context.Background(), selected, "claude_cli", "")
	if err != nil || readErr != nil || len(evidence) != 0 || len(attempts) != 0 {
		t.Fatalf("startup probe gained business completion evidence: invocations=%+v attempts=%+v err=%v read_err=%v", evidence, attempts, err, readErr)
	}
}

func requireCompletion2270BusinessInvocation(t *testing.T, selected effects.Store, profile string, calls *atomic.Int32, proof *completion2270InvocationProof, expectedAttempt string) string {
	t.Helper()
	if profile != llmselection.BackendClaudeCLI {
		if calls.Load() != 1 {
			t.Fatalf("API business invocations=%d, want one", calls.Load())
		}
		return ""
	}
	attempts, err := storetest.ReadCompletionAttemptStorage(context.Background(), selected, "claude_cli", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 || attempts[0].State != string(effects.StateSettled) || attempts[0].Kind != string(effects.KindProviderTurn) || (expectedAttempt != "" && attempts[0].AttemptID != expectedAttempt) {
		t.Fatalf("CLI native business attempts=%+v, expected=%s", attempts, expectedAttempt)
	}
	evidence, err := proof.read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := validateCompletion2270SingleInvocation(evidence, attempts[0].AttemptID); err != nil {
		t.Fatal(err)
	}
	row := attempts[0]
	if row.OperationID == "" || row.TargetKind != string(effects.UsageTargetAgentTurn) || len(row.Targets) != 1 || row.Targets[0].TargetID != row.TargetID || row.Targets[0].AttemptID != row.AttemptID || len(row.Spend) != 1 || row.Spend[0].LedgerID == "" || row.Spend[0].AttemptID != row.AttemptID || row.Spend[0].Evidence.Request.RequestedModel != "request-A" {
		t.Fatalf("CLI attempt lost exact operation/target/spend evidence: %+v", row)
	}
	return row.AttemptID
}
