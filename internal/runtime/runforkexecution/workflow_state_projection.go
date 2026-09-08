package runforkexecution

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
)

func prepareSelectedContractWorkflowReadiness(
	ctx context.Context,
	replay SelectedContractReplayPersistence,
	loaded LoadedSelectedContractSource,
	planning runfork.RunForkSelectedContractRecipientPlanning,
	plan runfork.RunForkPlan,
	frontier runfork.RunForkContractFrontierAdmission,
	sourceEventIDs []string,
	options SelectedContractAgentRuntimeOptions,
) (selectedContractAgentRuntimePlan, runforkreadiness.Admission, error) {
	var agentRuntime selectedContractAgentRuntimePlan
	if replay == nil {
		return agentRuntime, runforkreadiness.Admission{}, fmt.Errorf("selected-contract workflow readiness requires replay persistence")
	}
	if !options.ExecutionPosture.Valid() {
		return agentRuntime, runforkreadiness.Admission{}, fmt.Errorf("selected-contract execution posture is invalid")
	}
	sourceModes, err := replay.LoadRunForkSelectedContractSourceEventModes(ctx, plan.SourceRunID, sourceEventIDs)
	if err != nil {
		return agentRuntime, runforkreadiness.Admission{}, err
	}
	if len(sourceModes) != len(sourceEventIDs) {
		return agentRuntime, runforkreadiness.Admission{}, fmt.Errorf("selected-contract source event mode projection is incomplete")
	}
	sourceModeByEvent := make(map[string]executionmode.Mode, len(sourceEventIDs))
	for i, eventID := range sourceEventIDs {
		mode := sourceModes[i]
		if err := options.ExecutionPosture.Admit(mode, "selected-contract source event admission"); err != nil {
			return agentRuntime, runforkreadiness.Admission{}, err
		}
		sourceModeByEvent[strings.TrimSpace(eventID)] = mode
	}
	modelOptions, _, err := selectedContractAgentModelOptions(options)
	if err != nil {
		return agentRuntime, runforkreadiness.Admission{}, err
	}
	admitted, err := runforkreadiness.Admit(runforkreadiness.AdmissionRequest{
		Binding: runforkreadiness.Binding{Plan: plan, ContractSelection: frontier.ContractSelection,
			SourceArtifactFact: loaded.SourceArtifactFact, EffectiveSourceIdentity: loaded.EffectiveSourceIdentity, FrontierAdmission: frontier, RecipientPlanning: planning, SourceModes: sourceModeByEvent},
		Source: loaded.Source, ModelOptions: modelOptions,
	})
	if err != nil {
		return agentRuntime, runforkreadiness.Admission{}, err
	}
	prepared, err := admitted.Projection()
	if err != nil {
		return agentRuntime, runforkreadiness.Admission{}, err
	}
	agentRuntime, err = prepareSelectedContractAgentRuntimeMaterialization(ctx, loaded, planning, prepared.Blueprints, options)
	agentRuntime.Flows = prepared.Flows
	return agentRuntime, admitted, err
}

func bindRecoveredSelectedContractAgentRuntime(
	ctx context.Context,
	workflow runtimepipeline.WorkflowPersistence,
	forkRunID string,
	loaded LoadedSelectedContractSource,
	states []runfork.RunForkSelectedContractWorkflowState,
	agentRuntime selectedContractAgentRuntimePlan,
) (selectedContractAgentRuntimePlan, error) {
	forkRunID = strings.TrimSpace(forkRunID)
	bundleHash := loaded.SourceArtifactFact.BundleHash()
	topologies := make([]runfork.RunForkSelectedContractAgentTopology, 0)
	seen := make(map[runtimeagentidentity.Identity]struct{})
	for _, state := range states {
		if state.Mode != "template" {
			continue
		}
		owner, err := runtimeflowidentity.NewRunScopedFlowInstance(forkRunID, state.Route)
		if err != nil {
			return selectedContractAgentRuntimePlan{}, err
		}
		instance, found, err := workflow.LoadWorkflowInstance(ctx, owner)
		if err != nil {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("load selected-contract recovered workflow %s: %w", state.Route.InstancePath, err)
		}
		if !found {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract recovered workflow %s has no durable flow owner", state.Route.InstancePath)
		}
		if instance.EntityID != state.EntityID || instance.EntityType != state.EntityType ||
			instance.WorkflowName != state.FlowID || instance.WorkflowVersion != state.WorkflowVersion ||
			instance.Mode != state.Mode || instance.StorageRef != state.Route.InstancePath || instance.InstanceID != state.Route.InstanceID {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract recovered workflow %s disagrees with exact entity/type/workflow descriptor", state.Route.InstancePath)
		}
		expectedJSON, err := json.Marshal(state.Config)
		if err != nil {
			return selectedContractAgentRuntimePlan{}, err
		}
		actualJSON, err := json.Marshal(instance.Config)
		if err != nil || string(expectedJSON) != string(actualJSON) {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract recovered workflow %s disagrees with declaration configuration", state.Route.InstancePath)
		}
		readiness, found, err := workflow.LoadDynamicFlowRuntimeReadiness(ctx, forkRunID, state.Route)
		if err != nil {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("load selected-contract recovered workflow readiness %s: %w", state.Route.InstancePath, err)
		}
		if !found {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract recovered workflow %s has no durable readiness owner", state.Route.InstancePath)
		}
		if !readiness.Eligible() || readiness.InstancePath != state.Route.InstancePath ||
			!readiness.OwningRunSource.Matches(loaded.SourceArtifactFact) ||
			instance.Status != readiness.InstanceStatus || !instance.TerminatedAt.Equal(readiness.InstanceTerminatedAt) {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract recovered workflow %s has ineligible or contradictory lifecycle/readiness ownership", state.Route.InstancePath)
		}
		plan, err := readiness.Plan.Normalized()
		if err != nil {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("normalize selected-contract recovered workflow readiness %s: %w", state.Route.InstancePath, err)
		}
		if plan.RunID != forkRunID || plan.Identity.Route() != state.Route || plan.Identity.TemplateID != state.FlowID ||
			plan.Identity.EntityID != state.EntityID || plan.WorkflowVersion != state.WorkflowVersion ||
			plan.ExecutionMode != state.ExecutionMode || plan.BundleHash != bundleHash {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract recovered workflow readiness %s disagrees with regenerated fork state", state.Route.InstancePath)
		}
		expected := make(map[runtimeagentidentity.Identity]string, len(state.Agents))
		for _, agent := range state.Agents {
			identity, err := agent.Plan.Live(forkRunID)
			if err != nil {
				return selectedContractAgentRuntimePlan{}, fmt.Errorf("bind selected-contract recovered agent plan: %w", err)
			}
			expected[identity.Normalize()] = strings.TrimSpace(agent.ConfigRevision)
		}
		if len(expected) != len(plan.Agents) {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract recovered workflow readiness %s has %d agents; regenerated state requires %d", state.Route.InstancePath, len(plan.Agents), len(expected))
		}
		admission, err := runtimemanager.DynamicFlowAgentTopologyAdmission(plan)
		if err != nil {
			return selectedContractAgentRuntimePlan{}, fmt.Errorf("derive selected-contract recovered topology %s: %w", state.Route.InstancePath, err)
		}
		for _, persisted := range plan.Agents {
			identity := persisted.Identity.Normalize()
			revision, ok := expected[identity]
			if !ok || revision != persisted.ConfigRevision {
				return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract recovered agent %s disagrees with durable readiness", identity.Description())
			}
			if _, duplicate := seen[identity]; duplicate {
				return selectedContractAgentRuntimePlan{}, fmt.Errorf("selected-contract recovered agent %s has multiple readiness owners", identity.Description())
			}
			seen[identity] = struct{}{}
			topologies = append(topologies, runfork.RunForkSelectedContractAgentTopology{
				Identity: identity, Admission: admission,
			})
		}
	}
	bound, err := agentRuntime.bindRun(forkRunID, topologies)
	if err != nil {
		return selectedContractAgentRuntimePlan{}, fmt.Errorf("bind selected-contract recovered agent runtime: %w", err)
	}
	return bound, nil
}
