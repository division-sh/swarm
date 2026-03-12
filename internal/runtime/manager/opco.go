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
	workspace "empireai/internal/runtime/workspace"
)

var errLegacyOpCoLifecycleDisabled = errors.New("legacy OpCo lifecycle is not part of the MAS runtime; flow instance activation replaces OpCo spawning (see manager/flow_activation.go)")

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
	if _, ok := bundle.FlowContracts["operating"]; !ok {
		return errLegacyOpCoLifecycleDisabled
	}
	config := map[string]any{
		"geography":          strings.TrimSpace(mandate.Geography),
		"founder_notes":      strings.TrimSpace(mandate.FounderNotes),
		"founder_directives": strings.TrimSpace(mandate.FounderDirectives),
	}
	return am.ActivateFlowInstance(context.Background(), runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: bundle,
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
	return am.configureRouting(rule, false)
}

// ConfigureRoutingTemplateMigration applies routing changes as part of a template migration.
// It is the only path allowed to mutate routes whose existing source is "bootstrap".
func (am *AgentManager) ConfigureRoutingTemplateMigration(rule PersistedRoutingRule) error {
	return am.configureRouting(rule, true)
}

func (am *AgentManager) configureRouting(rule PersistedRoutingRule, allowBootstrapMutation bool) error {
	if rule.VerticalID == "" || rule.EventPattern == "" || rule.SubscriberID == "" {
		return errors.New("vertical_id, event_pattern, and subscriber_id are required")
	}
	if rule.Status == "" {
		rule.Status = "active"
	}
	if rule.InstalledBy == "" {
		rule.InstalledBy = "runtime"
	}
	if rule.Source == "" {
		rule.Source = "discovered"
	}

	key := routeRuleKey(rule.VerticalID, rule.EventPattern, rule.SubscriberID)
	if existing, ok := am.getRouteMeta(key); ok {
		if existing.Source == "bootstrap" && !allowBootstrapMutation {
			return errors.New("bootstrap routes are immutable")
		}
		if rule.Source == "" {
			rule.Source = existing.Source
		}
		if rule.BootstrapVersion == 0 {
			rule.BootstrapVersion = existing.BootstrapVersion
		}
	}

	if am.store != nil {
		if err := am.store.UpsertRoutingRule(am.runtimeContext(), rule); err != nil {
			return err
		}
	}
	am.setRouteMeta(key, rule)
	return nil
}

func (am *AgentManager) hydrateRoutingTables(rules []PersistedRoutingRule) error {
	for _, r := range rules {
		if r.VerticalID == "" {
			continue
		}
		am.setRouteMeta(routeRuleKey(r.VerticalID, r.EventPattern, r.SubscriberID), r)
	}
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

func routeRuleKey(verticalID, eventPattern, subscriberID string) string {
	return RouteRuleKey(verticalID, eventPattern, subscriberID)
}

func (am *AgentManager) getRouteMeta(key string) (PersistedRoutingRule, bool) {
	am.mu.RLock()
	defer am.mu.RUnlock()
	r, ok := am.routeMeta[key]
	return r, ok
}

func (am *AgentManager) setRouteMeta(key string, rule PersistedRoutingRule) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.routeMeta[key] = rule
}
