package cliapp

import (
	"context"
	"fmt"
	"strings"

	apiv1 "github.com/division-sh/swarm/internal/apiv1"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

type workspaceAdmittedForkChatExecutor struct {
	inner    apiv1.ForkChatExecutor
	decision WorkspaceBackendSelection
	runtimes runtimellm.AgentRuntimeResolver
}

func NewWorkspaceAdmittedForkChatExecutor(inner apiv1.ForkChatExecutor, runtimes runtimellm.AgentRuntimeResolver, decision WorkspaceBackendSelection) apiv1.ForkChatExecutor {
	return workspaceAdmittedForkChatExecutor{
		inner:    inner,
		decision: decision,
		runtimes: runtimes,
	}
}

func (e workspaceAdmittedForkChatExecutor) ExecuteForkChat(ctx context.Context, prepared runfork.ConversationForkChatPrepared, message string) (runfork.ConversationForkChatExecution, error) {
	if e.runtimes == nil {
		return runfork.ConversationForkChatExecution{}, fmt.Errorf("conversation.fork_chat llm runtime resolver is required")
	}
	resolved, err := e.runtimes.ResolveAgentRuntime(prepared.Snapshot.SourceAgent)
	if err != nil {
		return runfork.ConversationForkChatExecution{}, fmt.Errorf("conversation.fork_chat resolve llm runtime: %w", err)
	}
	providerContract, hasProviderContract := runtimellm.ProviderContractForRuntime(resolved.Runtime)
	if hasProviderContract && providerContract.Transport == runtimellm.ProviderTransportCLI {
		switch {
		case e.decision.Backend == workspace.BackendDocker:
			return e.inner.ExecuteForkChat(ctx, prepared, message)
		case e.decision.Backend == workspace.BackendHost || e.decision.UnsafeHost:
			return runfork.ConversationForkChatExecution{}, fmt.Errorf("conversation.fork_chat cannot use host workspace backend with claude_cli; use docker because claude_cli process execution on host remains split")
		case e.decision.NoWorkspace || e.decision.Backend == WorkspaceBackendNone:
			return runfork.ConversationForkChatExecution{}, fmt.Errorf("conversation.fork_chat requires container isolation because forkchat uses claude_cli; start Docker or configure an API-backed LLM backend")
		default:
			return runfork.ConversationForkChatExecution{}, fmt.Errorf("conversation.fork_chat workspace backend decision %q cannot run claude_cli; use docker", strings.TrimSpace(e.decision.Backend))
		}
	}
	return e.inner.ExecuteForkChat(ctx, prepared, message)
}
