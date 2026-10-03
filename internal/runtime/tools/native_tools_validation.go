package tools

import (
	"context"
	"fmt"
	"sort"
	"strings"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

func ValidateNativeToolBootConfig(ctx context.Context, posture executionposture.Posture, aliases llmselection.ModelAliases, source semanticview.Source, store runtimecredentials.Store, providers llm.AgentProviderContractResolver, workspaces workspace.Resolver) ([]error, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !posture.Valid() {
		return nil, fmt.Errorf("native tool boot validation requires command execution purpose")
	}
	if source == nil {
		return nil, fmt.Errorf("native tool admission requires semantic source")
	}
	var failures []string
	declarations := semanticview.AgentDeclarations(source)
	localIDCounts := map[string]int{}
	for _, declaration := range declarations {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		localIDCounts[declaration.LocalID]++
	}
	for _, declaration := range declarations {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
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
		if providers == nil {
			failures = append(failures, fmt.Sprintf("agent %s llm runtime resolver is required", strings.TrimSpace(agentID)))
			continue
		}
		projected, err := llm.ResolveAgentExecution(posture, providers.ConfiguredDefault(), aliases, actor)
		if err != nil {
			failures = append(failures, fmt.Sprintf("agent %s execution selection: %v", strings.TrimSpace(agentID), err))
			continue
		}
		contract, err := providers.ResolveAgentProviderContract(projected.Actor)
		if err != nil {
			failures = append(failures, fmt.Sprintf("agent %s execution selection: %v", strings.TrimSpace(agentID), err))
			continue
		}
		if projected.Selection.Profile.ID == llmselection.BackendMock {
			continue
		}
		actor = projected.Actor
		_, err = namePlan.Materialize()
		if err == nil {
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
			}
			actor.FlowID = flowID
			actor.FlowPath = flowPath
			actor.NormalizeRuntimeDescriptor()
		} else {
			failures = append(failures, err.Error())
			continue
		}
		if err := validateNativeToolAgentCapabilityAdmission(ctx, actor, NativeToolAdmissionOptions{
			ProviderContract: contract,
			Credentials:      store,
			Source:           source,
			Workspaces:       workspaces,
		}); err != nil {
			failures = append(failures, err.Error())
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(failures) == 0 {
		return nil, nil
	}
	sort.Strings(failures)
	return nil, fmt.Errorf("native tool admission failed: %s", strings.Join(failures, "; "))
}
