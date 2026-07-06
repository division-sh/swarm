package main

import (
	"context"
	"fmt"
	"strings"

	apiv1 "github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/config"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/store"
)

type workspaceAdmittedForkChatExecutor struct {
	inner     apiv1.ForkChatExecutor
	decision  workspaceBackendSelection
	profileID string
}

func newWorkspaceAdmittedForkChatExecutor(inner apiv1.ForkChatExecutor, cfg *config.Config, decision workspaceBackendSelection) apiv1.ForkChatExecutor {
	profileID := ""
	if cfg != nil {
		if profile, err := cfg.LLMBackendProfile(); err == nil {
			profileID = strings.TrimSpace(profile.ID)
		}
	}
	return workspaceAdmittedForkChatExecutor{
		inner:     inner,
		decision:  decision,
		profileID: profileID,
	}
}

func (e workspaceAdmittedForkChatExecutor) ExecuteForkChat(ctx context.Context, prepared store.ConversationForkChatPrepared, message string) (store.ConversationForkChatExecution, error) {
	if e.profileID == llmselection.BackendClaudeCLI {
		switch {
		case e.decision.Backend == workspace.BackendDocker:
			return e.inner.ExecuteForkChat(ctx, prepared, message)
		case e.decision.Backend == workspace.BackendHost || e.decision.UnsafeHost:
			return store.ConversationForkChatExecution{}, fmt.Errorf("conversation.fork_chat cannot use host workspace backend with claude_cli; use docker because claude_cli process execution on host remains split")
		case e.decision.NoWorkspace || e.decision.Backend == workspaceBackendNone:
			return store.ConversationForkChatExecution{}, fmt.Errorf("conversation.fork_chat requires container isolation because forkchat uses claude_cli; start Docker or configure an API-backed LLM backend")
		default:
			return store.ConversationForkChatExecution{}, fmt.Errorf("conversation.fork_chat workspace backend decision %q cannot run claude_cli; use docker", strings.TrimSpace(e.decision.Backend))
		}
	}
	return e.inner.ExecuteForkChat(ctx, prepared, message)
}
