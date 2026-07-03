package tools

import (
	"context"
	"fmt"
	"sort"
	"strings"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

func ValidateNativeToolBootConfig(ctx context.Context, source semanticview.Source, store runtimecredentials.Store, runtime llm.Runtime, workspaces workspace.Resolver) ([]error, error) {
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
		if err := ValidateNativeToolAgentAdmission(ctx, actor, NativeToolAdmissionOptions{
			Runtime:     runtime,
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
