package tools

import (
	"context"
	"fmt"
	"sort"
	"strings"

	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

func ValidateNativeToolBootConfig(ctx context.Context, source semanticview.Source, store runtimecredentials.Store, runtimes *llm.AgentRuntimeSet, workspaces workspace.Resolver) ([]error, error) {
	if source == nil {
		return nil, nil
	}
	var failures []string
	declarations := semanticview.AgentDeclarations(source)
	localIDCounts := map[string]int{}
	for _, declaration := range declarations {
		localIDCounts[declaration.LocalID]++
	}
	for _, declaration := range declarations {
		agentID := declaration.Label(localIDCounts[declaration.LocalID] > 1)
		entry := declaration.Entry
		actor := nativeToolAgentConfig(declaration.LocalID, entry)
		if !actor.NativeTools.Any() {
			continue
		}
		if runtimes == nil {
			failures = append(failures, fmt.Sprintf("agent %s llm runtime resolver is required", strings.TrimSpace(agentID)))
			continue
		}
		resolved, err := runtimes.ResolveAgentRuntime(actor)
		if err != nil {
			failures = append(failures, fmt.Sprintf("agent %s execution selection: %v", strings.TrimSpace(agentID), err))
			continue
		}
		if resolved.Selection.Profile.ID == llmselection.BackendMock {
			continue
		}
		actor = resolved.Actor
		if owner, ok := semanticview.AgentDeclarationOwner(source, declaration.OwnerFlowID, declaration.LocalID); ok {
			name, err := runtimeagentidentity.DeclaredName(actor.ID, owner)
			if err != nil {
				failures = append(failures, err.Error())
				continue
			}
			actor.Identity, err = runtimeagentidentity.New(name, runtimeagentidentity.RootRoute())
			if err != nil {
				failures = append(failures, err.Error())
				continue
			}
		}
		if err := ValidateNativeToolAgentAdmission(ctx, actor, NativeToolAdmissionOptions{
			Runtime:     resolved.Runtime,
			Credentials: store,
			Source:      source,
			Workspaces:  workspaces,
		}); err != nil {
			failures = append(failures, err.Error())
		}
	}
	if len(failures) == 0 {
		return nil, nil
	}
	sort.Strings(failures)
	return nil, fmt.Errorf("native tool admission failed: %s", strings.Join(failures, "; "))
}
