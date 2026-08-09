package tools

import (
	"context"
	"fmt"
	"sort"
	"strings"

	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

func ValidateNativeToolBootConfig(ctx context.Context, source semanticview.Source, store runtimecredentials.Store, runtimes *llm.AgentRuntimeSet, workspaces workspace.Resolver) ([]error, error) {
	if source == nil {
		return nil, nil
	}
	var failures []string
	for _, agentID := range sortedAgentIDs(source.AgentEntries()) {
		entry := source.AgentEntries()[agentID]
		actor := nativeToolAgentConfig(agentID, entry)
		if !actor.NativeTools.Any() {
			continue
		}
		if runtimes == nil {
			failures = append(failures, fmt.Sprintf("agent %s llm runtime resolver is required", strings.TrimSpace(agentID)))
			continue
		}
		selection, err := runtimes.SelectionForArtifact(entry.Mock.Configured())
		if err != nil {
			failures = append(failures, fmt.Sprintf("agent %s execution selection: %v", strings.TrimSpace(agentID), err))
			continue
		}
		modelRuntime, err := runtimes.RuntimeForSelection(selection)
		if err != nil {
			failures = append(failures, fmt.Sprintf("agent %s llm runtime: %v", strings.TrimSpace(agentID), err))
			continue
		}
		if owner, ok := semanticview.AgentDeclarationOwner(source, "", agentID); ok {
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
			Runtime:     modelRuntime,
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
