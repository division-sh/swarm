package tools

import (
	"context"
	"fmt"
	"sort"
	"strings"

	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
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
		namePlan, err := semanticview.ScopedAgentNamePlan(source, declaration)
		if err != nil {
			failures = append(failures, err.Error())
			continue
		}
		agentID := namePlan.AgentID
		if localIDCounts[declaration.LocalID] > 1 {
			agentID += " (" + declaration.Label(true) + ")"
		}
		entry := declaration.Entry
		actor := nativeToolAgentConfig(namePlan.AgentID, namePlan.EffectiveRole(entry), entry)
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
		name, err := namePlan.Materialize()
		if err == nil {
			route := runtimeagentidentity.RootRoute()
			flowID := strings.TrimSpace(declaration.OwnerFlowID)
			flowPath := ""
			switch flowID {
			case "":
			case ".":
			default:
				flowPath = strings.Trim(strings.TrimSpace(source.FlowPath(flowID)), "/")
				if flowPath == "" {
					failures = append(failures, fmt.Sprintf("agent %s scoped declaration owner %s has no canonical flow path", strings.TrimSpace(agentID), flowID))
					continue
				}
				route, err = runtimeflowidentity.StoredRoute("", "", flowPath).AgentIdentityRoute()
				if err != nil {
					failures = append(failures, fmt.Sprintf("agent %s scoped declaration route: %v", strings.TrimSpace(agentID), err))
					continue
				}
			}
			actor.Identity, err = runtimeagentidentity.New(name, route)
			if err != nil {
				failures = append(failures, err.Error())
				continue
			}
			actor.FlowID = flowID
			actor.FlowPath = flowPath
			actor.NormalizeRuntimeDescriptor()
		} else {
			failures = append(failures, err.Error())
			continue
		}
		if err := validateNativeToolAgentCapabilityAdmission(ctx, actor, NativeToolAdmissionOptions{
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
