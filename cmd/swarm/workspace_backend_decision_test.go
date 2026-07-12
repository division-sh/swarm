package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/store"
)

func TestWorkspaceBackendDecisionCapabilityMatrix(t *testing.T) {
	tests := []struct {
		name        string
		cfg         *config.Config
		agents      map[string]runtimecontracts.AgentRegistryEntry
		preference  workspaceBackendSelection
		wantBackend string
		wantClass   workspaceCapabilityClass
		wantNo      bool
		wantUnsafe  bool
		wantErr     string
		wantReason  workspaceCapabilityReasonKind
	}{
		{
			name:        "no agents defaults to no workspace lifecycle",
			cfg:         testWorkspaceBackendConfig(llmselection.BackendOpenAIResponses),
			wantBackend: workspaceBackendNone,
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
			wantReason:  workspaceReasonClaudeCLI,
		},
		{
			name: "flag host is loud unsafe opt out for host supported exec",
			cfg:  testWorkspaceBackendConfig(llmselection.BackendOpenAIResponses),
			agents: map[string]runtimecontracts.AgentRegistryEntry{
				"worker": {ID: "worker", NativeTools: map[string]any{"bash": true}},
			},
			preference:  workspaceBackendSelection{Backend: workspace.BackendHost, Source: "--workspace-backend", PreferenceExplicit: true, AllowExecOnHost: true},
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
			preference: workspaceBackendSelection{Backend: workspace.BackendHost, Source: "workspace.backend", PreferenceExplicit: true},
			wantErr:    "workspace.allow_exec_on_host",
		},
		{
			name: "config host with unsafe allow is accepted for host supported exec",
			cfg:  testWorkspaceBackendConfig(llmselection.BackendOpenAIResponses),
			agents: map[string]runtimecontracts.AgentRegistryEntry{
				"worker": {ID: "worker", NativeTools: map[string]any{"bash": true}},
			},
			preference:  workspaceBackendSelection{Backend: workspace.BackendHost, Source: "workspace.backend", PreferenceExplicit: true, AllowExecOnHost: true},
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
			preference: workspaceBackendSelection{Backend: workspace.BackendHost, Source: "SWARM_WORKSPACE_BACKEND", PreferenceExplicit: true},
			wantErr:    "cannot authorize unsafe host execution",
		},
		{
			name: "claude cli host remains split even with unsafe allow",
			cfg:  testWorkspaceBackendConfig(llmselection.BackendClaudeCLI),
			agents: map[string]runtimecontracts.AgentRegistryEntry{
				"worker": {ID: "worker"},
			},
			preference: workspaceBackendSelection{Backend: workspace.BackendHost, Source: "--workspace-backend", PreferenceExplicit: true, AllowExecOnHost: true},
			wantErr:    "claude_cli",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision, err := decideWorkspaceBackend(tt.preference, tt.cfg, semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Agents: tt.agents}))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("decideWorkspaceBackend error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("decideWorkspaceBackend: %v", err)
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
			wantProblem:     []string{"agent worker uses claude_cli backend"},
			wantRemediation: []string{"Use Docker", "llm.backend: anthropic", "Docker-free local run"},
			wantClaudeOnly:  true,
		},
		{
			name:              "mixed native bash names every blocker and requires host authorization",
			agent:             runtimecontracts.AgentRegistryEntry{ID: "worker", NativeTools: map[string]any{"bash": true}},
			wantProblem:       []string{"agent worker uses claude_cli backend", "agent worker has native_tools.bash"},
			wantRemediation:   []string{"Use Docker", "llm.backend: anthropic", "workspace.allow_exec_on_host: true"},
			forbidRemediation: "or switch to an API backend",
		},
		{
			name:              "mixed exec tool names every blocker and requires host authorization",
			agent:             runtimecontracts.AgentRegistryEntry{ID: "worker", Tools: []string{"shell"}},
			wantProblem:       []string{"agent worker uses claude_cli backend", "agent worker has exec-class tool shell"},
			wantRemediation:   []string{"Use Docker", "llm.backend: anthropic", "workspace.allow_exec_on_host: true"},
			forbidRemediation: "or switch to an API backend",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			preference := workspaceBackendSelection{Backend: workspace.BackendHost, Source: "--workspace-backend", PreferenceExplicit: true, AllowExecOnHost: true}
			decision, err := decideWorkspaceBackend(preference, testWorkspaceBackendConfig(llmselection.BackendClaudeCLI), semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
				Agents: map[string]runtimecontracts.AgentRegistryEntry{"worker": tt.agent},
			}))
			if err == nil {
				t.Fatal("decideWorkspaceBackend unexpectedly accepted claude_cli host execution")
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

func TestConfiguredWorkspaceLifecycleForBackendNoWorkspace(t *testing.T) {
	lifecycle, err := configuredWorkspaceLifecycleForBackend(nil, nil, "", semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), workspaceMountSources{}, workspaceBackendSelection{Backend: workspaceBackendNone, Source: "capability-derived", NoWorkspace: true})
	if err != nil {
		t.Fatalf("configuredWorkspaceLifecycleForBackend: %v", err)
	}
	if lifecycle != nil {
		t.Fatalf("lifecycle = %#v, want nil for no-workspace decision", lifecycle)
	}
}

func TestWorkspaceAdmittedForkChatExecutorRejectsClaudeCLIWithoutDocker(t *testing.T) {
	executor := newWorkspaceAdmittedForkChatExecutor(recordingForkChatExecutor{}, testWorkspaceBackendConfig(llmselection.BackendClaudeCLI), workspaceBackendSelection{Backend: workspaceBackendNone, NoWorkspace: true})
	_, err := executor.ExecuteForkChat(context.Background(), store.ConversationForkChatPrepared{}, "inspect")
	if err == nil || !strings.Contains(err.Error(), "conversation.fork_chat") || !strings.Contains(err.Error(), "claude_cli") {
		t.Fatalf("ExecuteForkChat error = %v, want forkchat claude_cli admission failure", err)
	}
}

func TestWorkspaceAdmittedForkChatExecutorAllowsAPIBackend(t *testing.T) {
	inner := recordingForkChatExecutor{result: store.ConversationForkChatExecution{AssistantMessage: "ok"}}
	executor := newWorkspaceAdmittedForkChatExecutor(inner, testWorkspaceBackendConfig(llmselection.BackendOpenAIResponses), workspaceBackendSelection{Backend: workspaceBackendNone, NoWorkspace: true})
	got, err := executor.ExecuteForkChat(context.Background(), store.ConversationForkChatPrepared{}, "inspect")
	if err != nil {
		t.Fatalf("ExecuteForkChat: %v", err)
	}
	if got.AssistantMessage != "ok" {
		t.Fatalf("ExecuteForkChat result = %#v, want inner executor result", got)
	}
}

type recordingForkChatExecutor struct {
	result store.ConversationForkChatExecution
}

func (r recordingForkChatExecutor) ExecuteForkChat(context.Context, store.ConversationForkChatPrepared, string) (store.ConversationForkChatExecution, error) {
	return r.result, nil
}

func testWorkspaceBackendConfig(backend string) *config.Config {
	return &config.Config{LLM: config.LLMConfig{Backend: backend}}
}
