package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"empireai/internal/events"
	"empireai/internal/models"
	runtimecontracts "empireai/internal/runtime/contracts"
	runtimepipeline "empireai/internal/runtime/pipeline"
	"empireai/internal/runtime/semanticview"
	workspace "empireai/internal/runtime/workspace"
)

var errLegacyOpCoLifecycleDisabled = errors.New("legacy OpCo lifecycle is not part of the MAS runtime; flow instance activation replaces OpCo spawning (see manager/flow_activation.go)")
var errMutableRoutingDisabled = errors.New("mutable routing is not part of the MAS runtime; routes derive from contracts")

func (am *AgentManager) SpawnOpCo(verticalID string, mandate models.MandateDocument) error {
	if am == nil {
		return errLegacyOpCoLifecycleDisabled
	}
	verticalID = FirstNonEmptyString(verticalID, mandate.VerticalID)
	if verticalID == "" {
		return fmt.Errorf("vertical id is required")
	}
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		filepath.Join(repoRoot, "docs", "specs", "mas-platform", "empire", "contracts"),
		filepath.Join(repoRoot, "docs", "specs", "mas-platform", "platform", "contracts", "platform-spec.yaml"),
	)
	if err != nil {
		return err
	}
	if !bundle.HasFlow("operating") {
		return errLegacyOpCoLifecycleDisabled
	}
	config := map[string]any{
		"geography":          strings.TrimSpace(mandate.Geography),
		"founder_notes":      strings.TrimSpace(mandate.FounderNotes),
		"founder_directives": strings.TrimSpace(mandate.FounderDirectives),
	}
	return am.ActivateFlowInstance(context.Background(), runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: semanticview.Wrap(bundle),
		TemplateID:     "operating",
		InstanceID:     verticalID,
		VerticalID:     verticalID,
		FlowPath:       "operating/" + verticalID,
		InitialState:   "approved",
		Config:         config,
	})
}

func (am *AgentManager) TeardownOpCo(verticalID string) error {
	_ = verticalID
	// Flow instance activation replaces OpCo teardown.
	// See: manager/flow_activation.go
	return errLegacyOpCoLifecycleDisabled
}

func (am *AgentManager) SetWorkspaceLifecycle(workspaces workspace.Lifecycle) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.workspaces = workspaces
}

func (am *AgentManager) ConfigureRouting(rule PersistedRoutingRule) error {
	_ = rule
	return errMutableRoutingDisabled
}

// ConfigureRoutingTemplateMigration applies routing changes as part of a template migration.
// Routing now derives from contracts, so legacy route ops are ignored during migration.
func (am *AgentManager) ConfigureRoutingTemplateMigration(rule PersistedRoutingRule) error {
	_ = rule
	return nil
}

type genericAgent struct {
	id            string
	agentType     string
	subscriptions []events.EventType
}

func newGenericAgent(cfg models.AgentConfig) Agent {
	if cfg.Type == "" {
		cfg.Type = "generic"
	}
	merged := make([]string, 0, len(cfg.Subscriptions))
	merged = append(merged, cfg.Subscriptions...)
	if len(cfg.Config) > 0 {
		var aux struct {
			Subscriptions []string `json:"subscriptions"`
		}
		if err := json.Unmarshal(cfg.Config, &aux); err == nil {
			merged = append(merged, aux.Subscriptions...)
		}
	}

	uniq := make(map[string]struct{})
	subs := make([]events.EventType, 0, len(merged))
	for _, s := range merged {
		if s == "" {
			continue
		}
		if _, ok := uniq[s]; ok {
			continue
		}
		uniq[s] = struct{}{}
		subs = append(subs, events.EventType(s))
	}

	return &genericAgent{
		id:            cfg.ID,
		agentType:     cfg.Type,
		subscriptions: subs,
	}
}

func (a *genericAgent) ID() string                        { return a.id }
func (a *genericAgent) Type() string                      { return a.agentType }
func (a *genericAgent) Subscriptions() []events.EventType { return a.subscriptions }
func (a *genericAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func opCoAgentID(role, verticalID string) string { return OpCoAgentID(role, verticalID) }

func mergeAgentConfig(base, patch models.AgentConfig) models.AgentConfig {
	return MergeAgentConfig(base, patch)
}

func extractSystemPromptFromConfig(raw json.RawMessage) string {
	return ExtractSystemPromptFromConfig(raw)
}

func withSystemPrompt(raw json.RawMessage, prompt string) json.RawMessage {
	return WithSystemPrompt(raw, prompt)
}
