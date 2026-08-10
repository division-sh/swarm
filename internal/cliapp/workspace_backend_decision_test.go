package cliapp

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

func TestWorkspaceBackendDecisionCapabilityMatrix(t *testing.T) {
	tests := []struct {
		name        string
		cfg         *config.Config
		agents      map[string]runtimecontracts.AgentRegistryEntry
		preference  WorkspaceBackendSelection
		wantBackend string
		wantClass   workspaceCapabilityClass
		wantNo      bool
		wantUnsafe  bool
		wantErr     string
		wantReason  WorkspaceCapabilityReasonKind
	}{
		{
			name:        "no agents defaults to no workspace lifecycle",
			cfg:         testWorkspaceBackendConfig(llmselection.BackendOpenAIResponses),
			wantBackend: WorkspaceBackendNone,
			wantClass:   workspaceCapabilityNone,
			wantNo:      true,
		},
		{
			name: "api backed agents default to host workspace",
			cfg:  testWorkspaceBackendConfig(llmselection.BackendOpenAIResponses),
			agents: map[string]runtimecontracts.AgentRegistryEntry{
				"worker": {ID: "worker"},
			},
			wantBackend: workspace.BackendHost,
			wantClass:   workspaceCapabilityWorkspaceWrite,
		},
		{
			name: "file io stays host workspace write",
			cfg:  testWorkspaceBackendConfig(llmselection.BackendOpenAIResponses),
			agents: map[string]runtimecontracts.AgentRegistryEntry{
				"worker": {ID: "worker", NativeTools: map[string]any{"file_io": true}},
			},
			wantBackend: workspace.BackendHost,
			wantClass:   workspaceCapabilityWorkspaceWrite,
		},
		{
			name: "native bash defaults to docker",
			cfg:  testWorkspaceBackendConfig(llmselection.BackendOpenAIResponses),
			agents: map[string]runtimecontracts.AgentRegistryEntry{
				"worker": {ID: "worker", NativeTools: map[string]any{"bash": true}},
			},
			wantBackend: workspace.BackendDocker,
			wantClass:   workspaceCapabilityExec,
		},
		{
			name: "claude cli defaults to docker and marks host unsupported",
			cfg:  testWorkspaceBackendConfig(llmselection.BackendClaudeCLI),
			agents: map[string]runtimecontracts.AgentRegistryEntry{
				"worker": {ID: "worker"},
			},
			wantBackend: workspace.BackendDocker,
			wantClass:   workspaceCapabilityExec,
			wantReason:  WorkspaceReasonClaudeCLI,
		},
		{
			name: "flag host is loud unsafe opt out for host supported exec",
			cfg:  testWorkspaceBackendConfig(llmselection.BackendOpenAIResponses),
			agents: map[string]runtimecontracts.AgentRegistryEntry{
				"worker": {ID: "worker", NativeTools: map[string]any{"bash": true}},
			},
			preference:  WorkspaceBackendSelection{Backend: workspace.BackendHost, Source: "--workspace-backend", PreferenceExplicit: true, AllowExecOnHost: true},
			wantBackend: workspace.BackendHost,
			wantClass:   workspaceCapabilityExec,
			wantUnsafe:  true,
		},
		{
			name: "config host needs explicit unsafe allow for exec",
			cfg:  testWorkspaceBackendConfig(llmselection.BackendOpenAIResponses),
			agents: map[string]runtimecontracts.AgentRegistryEntry{
				"worker": {ID: "worker", NativeTools: map[string]any{"bash": true}},
			},
			preference: WorkspaceBackendSelection{Backend: workspace.BackendHost, Source: "workspace.backend", PreferenceExplicit: true},
			wantErr:    "workspace.allow_exec_on_host",
		},
		{
			name: "config host with unsafe allow is accepted for host supported exec",
			cfg:  testWorkspaceBackendConfig(llmselection.BackendOpenAIResponses),
			agents: map[string]runtimecontracts.AgentRegistryEntry{
				"worker": {ID: "worker", NativeTools: map[string]any{"bash": true}},
			},
			preference:  WorkspaceBackendSelection{Backend: workspace.BackendHost, Source: "workspace.backend", PreferenceExplicit: true, AllowExecOnHost: true},
			wantBackend: workspace.BackendHost,
			wantClass:   workspaceCapabilityExec,
			wantUnsafe:  true,
		},
		{
			name: "legacy env host cannot authorize unsafe exec",
			cfg:  testWorkspaceBackendConfig(llmselection.BackendOpenAIResponses),
			agents: map[string]runtimecontracts.AgentRegistryEntry{
				"worker": {ID: "worker", NativeTools: map[string]any{"bash": true}},
			},
			preference: WorkspaceBackendSelection{Backend: workspace.BackendHost, Source: "SWARM_WORKSPACE_BACKEND", PreferenceExplicit: true},
			wantErr:    "cannot authorize unsafe host execution",
		},
		{
			name: "claude cli host remains split even with unsafe allow",
			cfg:  testWorkspaceBackendConfig(llmselection.BackendClaudeCLI),
			agents: map[string]runtimecontracts.AgentRegistryEntry{
				"worker": {ID: "worker"},
			},
			preference: WorkspaceBackendSelection{Backend: workspace.BackendHost, Source: "--workspace-backend", PreferenceExplicit: true, AllowExecOnHost: true},
			wantErr:    "claude_cli",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision, err := DecideWorkspaceBackend(tt.preference, tt.cfg, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Agents: tt.agents}))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("DecideWorkspaceBackend error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("DecideWorkspaceBackend: %v", err)
			}
			if decision.Backend != tt.wantBackend || decision.CapabilityClass != tt.wantClass || decision.NoWorkspace != tt.wantNo || decision.UnsafeHost != tt.wantUnsafe {
				t.Fatalf("decision = %#v, want backend=%s class=%s no=%v unsafe=%v", decision, tt.wantBackend, tt.wantClass, tt.wantNo, tt.wantUnsafe)
			}
			if tt.wantReason != "" && !workspaceBackendHasReason(decision.Reasons, tt.wantReason) {
				t.Fatalf("reasons = %#v, want kind %q", decision.Reasons, tt.wantReason)
			}
		})
	}
}

func TestWorkspaceBackendHostRemediationUsesTypedExecReasons(t *testing.T) {
	tests := []struct {
		name              string
		agent             runtimecontracts.AgentRegistryEntry
		wantProblem       []string
		wantRemediation   []string
		forbidRemediation string
		wantClaudeOnly    bool
	}{
		{
			name:            "claude only offers API backend as complete alternative",
			agent:           runtimecontracts.AgentRegistryEntry{ID: "worker"},
			wantProblem:     []string{"unmocked agent worker uses claude_cli backend"},
			wantRemediation: []string{"Use Docker", "llm.backend: anthropic", "Docker-free local run"},
			wantClaudeOnly:  true,
		},
		{
			name:              "mixed native bash names every blocker and requires host authorization",
			agent:             runtimecontracts.AgentRegistryEntry{ID: "worker", NativeTools: map[string]any{"bash": true}},
			wantProblem:       []string{"unmocked agent worker uses claude_cli backend", "agent worker has native_tools.bash"},
			wantRemediation:   []string{"Use Docker", "llm.backend: anthropic", "workspace.allow_exec_on_host: true"},
			forbidRemediation: "or switch to an API backend",
		},
		{
			name:              "mixed exec tool names every blocker and requires host authorization",
			agent:             runtimecontracts.AgentRegistryEntry{ID: "worker", Tools: []string{"shell"}},
			wantProblem:       []string{"unmocked agent worker uses claude_cli backend", "agent worker has exec-class tool shell"},
			wantRemediation:   []string{"Use Docker", "llm.backend: anthropic", "workspace.allow_exec_on_host: true"},
			forbidRemediation: "or switch to an API backend",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			preference := WorkspaceBackendSelection{Backend: workspace.BackendHost, Source: "--workspace-backend", PreferenceExplicit: true, AllowExecOnHost: true}
			decision, err := DecideWorkspaceBackend(preference, testWorkspaceBackendConfig(llmselection.BackendClaudeCLI), semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
				Agents: map[string]runtimecontracts.AgentRegistryEntry{"worker": tt.agent},
			}))
			if err == nil {
				t.Fatal("DecideWorkspaceBackend unexpectedly accepted claude_cli host execution")
			}
			var decisionErr *workspaceBackendDecisionError
			if !errors.As(err, &decisionErr) {
				t.Fatalf("error type = %T, want *workspaceBackendDecisionError", err)
			}
			for _, want := range tt.wantProblem {
				if !strings.Contains(decisionErr.Problem, want) {
					t.Fatalf("problem = %q, want %q", decisionErr.Problem, want)
				}
			}
			for _, want := range tt.wantRemediation {
				if !strings.Contains(decisionErr.Remediation, want) {
					t.Fatalf("remediation = %q, want %q", decisionErr.Remediation, want)
				}
			}
			if tt.forbidRemediation != "" && strings.Contains(decisionErr.Remediation, tt.forbidRemediation) {
				t.Fatalf("remediation = %q, must not present API switch as a complete exit", decisionErr.Remediation)
			}
			if got := workspaceBackendExecReasonsAreClaudeOnly(decision.Reasons); got != tt.wantClaudeOnly {
				t.Fatalf("typed Claude-only discrimination = %v, want %v; reasons=%#v", got, tt.wantClaudeOnly, decision.Reasons)
			}
		})
	}
}

func TestWorkspaceBackendClaudeCLIUsesEffectivePerAgentMockSelection(t *testing.T) {
	mocked := testWorkspaceBackendMockPerformance()
	t.Run("fully mocked bundle keeps ordinary host workspace lifecycle", func(t *testing.T) {
		decision, err := DecideWorkspaceBackend(WorkspaceBackendSelection{}, testWorkspaceBackendConfig(llmselection.BackendClaudeCLI), semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
			Agents: map[string]runtimecontracts.AgentRegistryEntry{
				"alpha": {ID: "alpha", Mock: mocked},
				"beta":  {ID: "beta", Mock: mocked},
			},
		}))
		if err != nil {
			t.Fatalf("DecideWorkspaceBackend: %v", err)
		}
		if decision.Backend != workspace.BackendHost || decision.CapabilityClass != workspaceCapabilityWorkspaceWrite {
			t.Fatalf("decision = %#v, want host workspace-write lifecycle", decision)
		}
		if workspaceBackendHasReason(decision.Reasons, WorkspaceReasonClaudeCLI) {
			t.Fatalf("fully mocked agents contributed claude_cli execution reason: %#v", decision.Reasons)
		}
		if !workspaceBackendHasReason(decision.Reasons, WorkspaceReasonLifecycle) {
			t.Fatalf("fully mocked agents lost ordinary workspace lifecycle reason: %#v", decision.Reasons)
		}
	})

	t.Run("mixed bundle refuses host and names only unmocked Claude agent", func(t *testing.T) {
		preference := WorkspaceBackendSelection{Backend: workspace.BackendHost, Source: "workspace.backend", PreferenceExplicit: true}
		decision, err := DecideWorkspaceBackend(preference, testWorkspaceBackendConfig(llmselection.BackendClaudeCLI), semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
			Agents: map[string]runtimecontracts.AgentRegistryEntry{
				"mocked-worker": {ID: "mocked-worker", Mock: mocked},
				"live-worker":   {ID: "live-worker"},
			},
		}))
		if err == nil {
			t.Fatal("DecideWorkspaceBackend unexpectedly admitted mixed claude_cli bundle on host")
		}
		if !strings.Contains(err.Error(), "unmocked agent live-worker uses claude_cli backend") {
			t.Fatalf("error = %q, want named unmocked agent", err)
		}
		if strings.Contains(err.Error(), "unmocked agent mocked-worker") {
			t.Fatalf("error misclassified mocked agent as live: %q", err)
		}
		var claudeAgents []string
		for _, reason := range decision.Reasons {
			if reason.Kind == WorkspaceReasonClaudeCLI {
				claudeAgents = append(claudeAgents, reason.AgentID)
			}
		}
		if len(claudeAgents) != 1 || claudeAgents[0] != "live-worker" {
			t.Fatalf("claude_cli reasons = %v, want only live-worker; all reasons=%#v", claudeAgents, decision.Reasons)
		}
	})

	t.Run("mock does not waive independently declared native execution", func(t *testing.T) {
		decision, err := DecideWorkspaceBackend(WorkspaceBackendSelection{}, testWorkspaceBackendConfig(llmselection.BackendClaudeCLI), semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
			Agents: map[string]runtimecontracts.AgentRegistryEntry{
				"worker": {ID: "worker", Mock: mocked, NativeTools: map[string]any{"bash": true}},
			},
		}))
		if err != nil {
			t.Fatalf("DecideWorkspaceBackend: %v", err)
		}
		if decision.Backend != workspace.BackendDocker || decision.CapabilityClass != workspaceCapabilityExec {
			t.Fatalf("decision = %#v, want Docker for independent native bash execution", decision)
		}
		if workspaceBackendHasReason(decision.Reasons, WorkspaceReasonClaudeCLI) || !workspaceBackendHasReason(decision.Reasons, WorkspaceReasonNativeBash) {
			t.Fatalf("reasons = %#v, want native bash without claude_cli", decision.Reasons)
		}
	})
}

func TestWorkspaceBackendCensusesScopedLiveAgentsHiddenByAmbiguousAliases(t *testing.T) {
	source := scopedWorkspaceBackendAgentFixture(t)
	if _, exists := source.AgentEntries()["shared-worker"]; exists {
		t.Fatal("ambiguous shared-worker unexpectedly survived in flattened agent aliases")
	}
	decision, err := DecideWorkspaceBackend(WorkspaceBackendSelection{}, testWorkspaceBackendConfig(llmselection.BackendClaudeCLI), source)
	if err != nil {
		t.Fatalf("DecideWorkspaceBackend: %v", err)
	}
	if decision.Backend != workspace.BackendDocker || decision.CapabilityClass != workspaceCapabilityExec {
		t.Fatalf("decision = %#v, want scoped live Claude agents to require Docker", decision)
	}
	labels := []string{}
	for _, reason := range decision.Reasons {
		if reason.Kind == WorkspaceReasonClaudeCLI {
			labels = append(labels, reason.AgentID)
		}
	}
	joined := strings.Join(labels, "\n")
	for _, want := range []string{
		"project packages/project-a agent shared-worker",
		"project packages/project-b agent shared-worker",
		"flow flow-a agent shared-worker",
		"flow flow-b agent shared-worker",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("Claude workspace reasons = %q, want %q", joined, want)
		}
	}
}

func scopedWorkspaceBackendAgentFixture(t *testing.T) semanticview.Source {
	return scopedWorkspaceBackendAgentFixtureOptions(t, true)
}

func TestLocalPreflightAgentCensusIncludesOnlyAmbiguousScopedDeclarations(t *testing.T) {
	source := scopedWorkspaceBackendAgentFixtureOptions(t, false)
	if len(source.AgentEntries()) != 0 {
		t.Fatalf("flattened agent aliases = %#v, want none", source.AgentEntries())
	}
	if !sourceDeclaresAgents(source) {
		t.Fatal("local preflight classified scoped agent declarations as agent-free")
	}
}

func scopedWorkspaceBackendAgentFixtureOptions(t *testing.T, includeRootAgent bool) semanticview.Source {
	t.Helper()
	root := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: scoped-workspace-backend
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - path: packages/project-a
  - path: packages/project-b
flows:
  - id: flow-a
    flow: flow-a
    mode: static
  - id: flow-b
    flow: flow-b
    mode: static
`)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: scoped-workspace-backend\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "entities.yaml"), "item:\n  item_id: string\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	rootAgents := "{}\n"
	if includeRootAgent {
		rootAgents = "root-mock:\n  id: root-mock\n  model: regular\n  memory: false\n  intent:\n    inline: Exercise root workspace backend selection.\n  mock:\n    kind: python\n    module: mocks/root-mock.py\n"
	}
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "agents.yaml"), rootAgents)
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "mocks", "root-mock.py"), "def handle(input):\n    return {'text': 'mock'}\n")
	writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")

	for _, project := range []string{"project-a", "project-b"} {
		dir := filepath.Join(root, "packages", project)
		writeWorkflowValidationFixtureFile(t, filepath.Join(dir, "package.yaml"), "name: "+project+"\nversion: \"1.0.0\"\nflows: []\n")
		writeWorkflowValidationFixtureFile(t, filepath.Join(dir, "agents.yaml"), "shared-worker:\n  id: shared-worker\n  model: regular\n  memory: false\n  intent:\n    inline: Exercise project-scoped workspace backend selection.\n")
		writeWorkflowValidationFixtureFile(t, filepath.Join(dir, "nodes.yaml"), "{}\n")
	}
	for _, flowID := range []string{"flow-a", "flow-b"} {
		dir := filepath.Join(root, "flows", flowID)
		writeWorkflowValidationFixtureFile(t, filepath.Join(dir, "schema.yaml"), "name: "+flowID+"\nmode: static\ninitial_state: active\nstates: [active]\n")
		writeWorkflowValidationFixtureFile(t, filepath.Join(dir, "events.yaml"), "{}\n")
		writeWorkflowValidationFixtureFile(t, filepath.Join(dir, "policy.yaml"), "{}\n")
		writeWorkflowValidationFixtureFile(t, filepath.Join(dir, "agents.yaml"), "shared-worker:\n  id: shared-worker\n  model: regular\n  memory: false\n  intent:\n    inline: Exercise flow-scoped workspace backend selection.\n")
		writeWorkflowValidationFixtureFile(t, filepath.Join(dir, "nodes.yaml"), "{}\n")
	}

	repoRoot := RepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func TestWorkspaceBackendReasonsPreserveEveryAgentCapabilityAcrossAggregateClass(t *testing.T) {
	tests := []struct {
		name   string
		agents map[string]runtimecontracts.AgentRegistryEntry
	}{
		{
			name: "exec agent sorts first",
			agents: map[string]runtimecontracts.AgentRegistryEntry{
				"a-exec":  {ID: "a-exec", NativeTools: map[string]any{"bash": true}},
				"z-files": {ID: "z-files", NativeTools: map[string]any{"file_io": true}},
			},
		},
		{
			name: "file agent sorts first",
			agents: map[string]runtimecontracts.AgentRegistryEntry{
				"a-files": {ID: "a-files", NativeTools: map[string]any{"file_io": true}},
				"z-exec":  {ID: "z-exec", NativeTools: map[string]any{"bash": true}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision, err := DecideWorkspaceBackend(WorkspaceBackendSelection{}, testWorkspaceBackendConfig(llmselection.BackendOpenAIResponses), semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Agents: tt.agents}))
			if err != nil {
				t.Fatalf("DecideWorkspaceBackend: %v", err)
			}
			if decision.CapabilityClass != workspaceCapabilityExec || decision.Backend != workspace.BackendDocker {
				t.Fatalf("decision = %#v, want aggregate exec/Docker", decision)
			}
			var bashAgents, fileAgents []string
			for _, reason := range decision.Reasons {
				switch reason.Kind {
				case WorkspaceReasonNativeBash:
					bashAgents = append(bashAgents, reason.AgentID)
				case WorkspaceReasonNativeFileIO:
					fileAgents = append(fileAgents, reason.AgentID)
				}
			}
			if len(bashAgents) != 1 || len(fileAgents) != 1 {
				t.Fatalf("typed reasons = %#v, want one bash and one file_io fact independent of aggregate class", decision.Reasons)
			}
		})
	}
}

func TestConfiguredWorkspaceLifecycleForBackendNoWorkspace(t *testing.T) {
	lifecycle, err := ConfiguredWorkspaceLifecycleForBackend(nil, nil, "", semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), WorkspaceMountSources{}, WorkspaceBackendSelection{Backend: WorkspaceBackendNone, Source: "capability-derived", NoWorkspace: true})
	if err != nil {
		t.Fatalf("ConfiguredWorkspaceLifecycleForBackend: %v", err)
	}
	if lifecycle != nil {
		t.Fatalf("lifecycle = %#v, want nil for no-workspace decision", lifecycle)
	}
}

func TestWorkspaceAdmittedForkChatExecutorRejectsClaudeCLIWithoutDocker(t *testing.T) {
	executor := NewWorkspaceAdmittedForkChatExecutor(recordingForkChatExecutor{}, staticWorkspaceAgentRuntimeResolver{runtime: &runtimellm.ClaudeCLIRuntime{}}, WorkspaceBackendSelection{Backend: WorkspaceBackendNone, NoWorkspace: true})
	_, err := executor.ExecuteForkChat(context.Background(), runfork.ConversationForkChatPrepared{}, "inspect")
	if err == nil || !strings.Contains(err.Error(), "conversation.fork_chat") || !strings.Contains(err.Error(), "claude_cli") {
		t.Fatalf("ExecuteForkChat error = %v, want forkchat claude_cli admission failure", err)
	}
}

func TestWorkspaceAdmittedForkChatExecutorAllowsAPIBackend(t *testing.T) {
	inner := recordingForkChatExecutor{result: runfork.ConversationForkChatExecution{AssistantMessage: "ok"}}
	executor := NewWorkspaceAdmittedForkChatExecutor(inner, staticWorkspaceAgentRuntimeResolver{runtime: &runtimellm.OpenAIResponsesRuntime{}}, WorkspaceBackendSelection{Backend: WorkspaceBackendNone, NoWorkspace: true})
	got, err := executor.ExecuteForkChat(context.Background(), runfork.ConversationForkChatPrepared{}, "inspect")
	if err != nil {
		t.Fatalf("ExecuteForkChat: %v", err)
	}
	if got.AssistantMessage != "ok" {
		t.Fatalf("ExecuteForkChat result = %#v, want inner executor result", got)
	}
}

type recordingForkChatExecutor struct {
	result runfork.ConversationForkChatExecution
}

type staticWorkspaceAgentRuntimeResolver struct {
	runtime runtimellm.Runtime
}

func (r staticWorkspaceAgentRuntimeResolver) ResolveAgentRuntime(actor models.AgentConfig) (runtimellm.AgentRuntimeResolution, error) {
	return runtimellm.AgentRuntimeResolution{Actor: actor, Runtime: r.runtime}, nil
}

func (r recordingForkChatExecutor) ExecuteForkChat(context.Context, runfork.ConversationForkChatPrepared, string) (runfork.ConversationForkChatExecution, error) {
	return r.result, nil
}

func testWorkspaceBackendConfig(backend string) *config.Config {
	return &config.Config{LLM: config.LLMConfig{Backend: backend}}
}

func testWorkspaceBackendMockPerformance() mockperformance.Performance {
	return mockperformance.Performance{
		Kind:   mockperformance.KindPython,
		Module: "mocks/worker.py",
		Source: []byte("def handle(input):\n    return {'text': 'ok'}\n"),
		Digest: "sha256:test",
	}
}
