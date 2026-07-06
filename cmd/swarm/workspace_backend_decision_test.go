package main

import (
	"context"
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
		name          string
		cfg           *config.Config
		agents        map[string]runtimecontracts.AgentRegistryEntry
		preference    workspaceBackendSelection
		wantBackend   string
		wantClass     workspaceCapabilityClass
		wantNo        bool
		wantUnsafe    bool
		wantErr       string
		wantHostBlock string
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
			wantBackend:   workspace.BackendDocker,
			wantClass:     workspaceCapabilityExec,
			wantHostBlock: "claude_cli",
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
			preference: workspaceBackendSelection{Backend: workspace.BackendHost, Source: envWorkspaceBackend, PreferenceExplicit: true},
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
			if tt.wantHostBlock != "" && !joinedContains(decision.HostUnsupported, tt.wantHostBlock) {
				t.Fatalf("host unsupported = %#v, want %q", decision.HostUnsupported, tt.wantHostBlock)
			}
		})
	}
}

func TestConfiguredWorkspaceLifecycleForBackendNoWorkspace(t *testing.T) {
	lifecycle, err := configuredWorkspaceLifecycleForBackend(nil, "", semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{}), workspaceMountSources{}, workspaceBackendSelection{Backend: workspaceBackendNone, Source: "capability-derived", NoWorkspace: true})
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
