package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	runtimebus "empireai/internal/runtime/bus"
	runtimepipeline "empireai/internal/runtime/pipeline"
	runtimesharedjson "empireai/internal/runtime/sharedjson"
	workspace "empireai/internal/runtime/workspace"
	"github.com/google/uuid"
)

func (am *AgentManager) SpawnOpCo(verticalID string, mandate models.MandateDocument) error {
	if verticalID == "" {
		return errors.New("verticalID is required")
	}

	if am.workspaces != nil {
		if err := am.workspaces.EnsureVerticalWorkspace(am.runtimeContext(), verticalID); err != nil {
			return fmt.Errorf("ensure vertical workspace: %w", err)
		}
	}

	// In-memory/test mode fallback: keep legacy roster/routes so unit tests and
	// inmemory runs still work without a Postgres-backed template store.
	if am.store == nil {
		agents := defaultOpCoRoster(verticalID)
		for _, rec := range agents {
			if err := am.spawnAgentInternal(am.runtimeContext(), rec, true); err != nil {
				return err
			}
		}
		rules := defaultOpCoRoutes(verticalID)
		for _, rule := range rules {
			am.setRouteMeta(routeRuleKey(rule.VerticalID, rule.EventPattern, rule.SubscriberID), rule)
		}
		rt := &runtimebus.RoutingTable{VerticalID: verticalID}
		for _, r := range rules {
			if r.Status != "active" {
				continue
			}
			rt.Routes = append(rt.Routes, runtimebus.Route{
				EventPattern: r.EventPattern,
				SubscriberID: r.SubscriberID,
				Status:       r.Status,
			})
		}
		if err := am.bus.SetRoutingTable(verticalID, rt); err != nil {
			return err
		}
		payload, _ := json.Marshal(map[string]any{
			"vertical_id":      verticalID,
			"ceo_agent_id":     opCoAgentID("opco-ceo", verticalID),
			"agent_count":      len(agents),
			"template_version": "inmemory",
			"priority":         "normal",
			"mandate":          mandate,
		})
		_ = am.bus.Publish(am.runtimeContext(), events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("opco.ceo_ready"),
			SourceAgent: "agent-manager",
			VerticalID:  verticalID,
			Payload:     payload,
			CreatedAt:   time.Now(),
		})
		return nil
	}

	if am.store != nil {
		if err := am.store.EnsureVerticalSchema(am.runtimeContext(), verticalID); err != nil {
			return fmt.Errorf("ensure vertical schema: %w", err)
		}
	}
	template, err := am.loadLatestTemplate(am.runtimeContext())
	if err != nil {
		return err
	}
	bootstrapVersion := am.resolveBootstrapVersion(am.runtimeContext(), template.Version)

	// Template placeholder context (spec v2.0 uses {vertical_id}, {vertical_name}, etc.).
	verticalName := ""
	verticalGeography := ""
	verticalSlug := ""
	if reader, ok := am.store.(VerticalInfoReader); ok && reader != nil {
		if info, found, err := reader.GetVerticalInfo(am.runtimeContext(), verticalID); err == nil && found {
			verticalName = strings.TrimSpace(info.Name)
			verticalGeography = strings.TrimSpace(info.Geography)
			verticalSlug = strings.TrimSpace(info.Slug)
		}
	}

	ceoID := opCoAgentID("opco-ceo", verticalID)
	if ceoID == "" {
		return errors.New("failed to derive opco ceo id")
	}

	orgRoster := renderOpCoRoster(template.Agents, verticalID)
	mandateText := renderMandateText(mandate)

	agents := make([]PersistedAgent, 0, len(template.Agents))
	for _, at := range template.Agents {
		role := strings.TrimSpace(at.Role)
		if role == "" {
			continue
		}
		systemPrompt := expandTemplateText(strings.TrimSpace(at.SystemPrompt), map[string]string{
			"{vertical_id}":        verticalID,
			"{vertical_name}":      verticalName,
			"{vertical_slug}":      verticalSlug,
			"{geography}":          verticalGeography,
			"{org_roster}":         orgRoster,
			"{mandate_document}":   mandateText,
			"{founder_directives}": strings.TrimSpace(FirstNonEmptyString(mandate.FounderDirectives, mandate.FounderNotes)),
		})
		cfg := models.AgentConfig{
			ID:         opCoAgentID(role, verticalID),
			Type:       strings.TrimSpace(at.Type),
			Role:       role,
			Mode:       "operating",
			VerticalID: verticalID,
			ParentAgent: func() string {
				parent := strings.TrimSpace(at.ParentRole)
				if parent == "" {
					return ""
				}
				return opCoAgentID(parent, verticalID)
			}(),
			Subscriptions: append([]string(nil), at.Subscriptions...),
		}
		if cfg.ID == "" {
			return fmt.Errorf("failed to derive agent id for role=%s", role)
		}
		// Persist runtime-only config (prompt/tools/constraints) in AgentConfig.Config.
		cfg.Config = mustJSON(map[string]any{
			"system_prompt": systemPrompt,
			"tools":         normalizeStringList(at.Tools),
			"constraints":   at.Constraints,
		})

		agents = append(agents, PersistedAgent{
			Config:          cfg,
			ParentAgentID:   cfg.ParentAgent,
			CoordinatorID:   ceoID,
			Status:          "active",
			HiredBy:         "agent-manager",
			TemplateVersion: template.Version,
		})
	}
	if len(agents) == 0 {
		return fmt.Errorf("org template %s produced no opco agents", template.Version)
	}

	// Persisting agents with parent_agent_id requires parent rows to exist first.
	// Templates are author-friendly, not guaranteed to be topologically ordered.
	agents, err = orderAgentsByParent(agents)
	if err != nil {
		return fmt.Errorf("order opco agents by parent: %w", err)
	}
	for _, rec := range agents {
		if err := am.spawnAgentInternal(am.runtimeContext(), rec, true); err != nil {
			return err
		}
	}

	installedBy := ceoID
	rules := make([]PersistedRoutingRule, 0, len(template.BootstrapRoutes)+len(template.SeededRoutes))
	for _, rt := range template.BootstrapRoutes {
		rules = append(rules, PersistedRoutingRule{
			VerticalID:       verticalID,
			EventPattern:     strings.TrimSpace(rt.EventPattern),
			SubscriberID:     resolveTemplateSubscriber(verticalID, rt),
			InstalledBy:      installedBy,
			Reason:           strings.TrimSpace(rt.Reason),
			Status:           "active",
			Source:           "bootstrap",
			BootstrapVersion: bootstrapVersion,
		})
	}
	for _, rt := range template.SeededRoutes {
		rules = append(rules, PersistedRoutingRule{
			VerticalID:       verticalID,
			EventPattern:     strings.TrimSpace(rt.EventPattern),
			SubscriberID:     resolveTemplateSubscriber(verticalID, rt),
			InstalledBy:      installedBy,
			Reason:           strings.TrimSpace(rt.Reason),
			Status:           "active",
			Source:           "seeded",
			BootstrapVersion: bootstrapVersion,
		})
	}

	for _, rule := range rules {
		if rule.EventPattern == "" || rule.SubscriberID == "" {
			continue
		}
		am.setRouteMeta(routeRuleKey(rule.VerticalID, rule.EventPattern, rule.SubscriberID), rule)
		if am.store != nil {
			if err := am.store.UpsertRoutingRule(am.runtimeContext(), rule); err != nil {
				return fmt.Errorf("persist routing rule %s -> %s: %w", rule.EventPattern, rule.SubscriberID, err)
			}
		}
	}

	rt := &runtimebus.RoutingTable{VerticalID: verticalID}
	for _, r := range rules {
		if r.Status != "active" {
			continue
		}
		rt.Routes = append(rt.Routes, runtimebus.Route{
			EventPattern: r.EventPattern,
			SubscriberID: r.SubscriberID,
			Status:       r.Status,
		})
	}
	if err := am.bus.SetRoutingTable(verticalID, rt); err != nil {
		return err
	}

	if err := am.installDefaultOpCoHeartbeats(am.runtimeContext(), verticalID); err != nil {
		log.Printf("install default opco heartbeats failed vertical=%s err=%v", verticalID, err)
	}

	if am.store != nil {
		if err := am.store.SetVerticalTemplateVersion(am.runtimeContext(), verticalID, template.Version); err != nil {
			return err
		}
	}

	payload, _ := json.Marshal(map[string]any{
		"vertical_id":      verticalID,
		"ceo_agent_id":     ceoID,
		"agent_count":      len(agents),
		"template_version": template.Version,
		"priority":         "normal",
		"mandate":          mandate,
	})
	_ = am.bus.Publish(am.runtimeContext(), events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("opco.ceo_ready"),
		SourceAgent: "agent-manager",
		VerticalID:  verticalID,
		Payload:     payload,
		CreatedAt:   time.Now(),
	})

	return nil
}

func (am *AgentManager) installDefaultOpCoHeartbeats(ctx context.Context, verticalID string) error {
	if strings.TrimSpace(verticalID) == "" {
		return nil
	}
	store, ok := am.store.(runtimepipeline.SchedulePersistence)
	if !ok || store == nil {
		return nil
	}
	specs := []struct {
		role      string
		eventType string
		interval  string
	}{
		{role: "vp-product", eventType: "heartbeat.vp_product", interval: "@every 2h"},
		{role: "chief-of-staff", eventType: "heartbeat.chief_of_staff", interval: "@every 4h"},
		{role: "opco-ceo", eventType: "heartbeat.opco_ceo", interval: "@every 8h"},
	}
	for _, spec := range specs {
		if err := store.UpsertSchedule(ctx, runtimepipeline.Schedule{
			AgentID:    opCoAgentID(spec.role, verticalID),
			EventType:  spec.eventType,
			Mode:       "cron",
			Cron:       spec.interval,
			VerticalID: verticalID,
			Payload:    []byte("{}"),
		}); err != nil {
			return fmt.Errorf("upsert schedule %s/%s: %w", spec.role, spec.eventType, err)
		}
	}
	return nil
}

// orderAgentsByParent returns a stable ordering where parents appear before children.
// This matters when persisting agent rows with parent_agent_id foreign keys.
func orderAgentsByParent(in []PersistedAgent) ([]PersistedAgent, error) {
	return OrderAgentsByParent(in)
}

type orgTemplateSnapshot struct {
	Version         string
	Agents          []orgTemplateAgent
	BootstrapRoutes []orgTemplateRoute
	SeededRoutes    []orgTemplateRoute
}

func expandTemplateText(raw string, vars map[string]string) string {
	out := raw
	if strings.TrimSpace(out) == "" || len(vars) == 0 {
		return out
	}
	for k, v := range vars {
		if strings.TrimSpace(k) == "" {
			continue
		}
		out = strings.ReplaceAll(out, k, v)
	}
	return out
}

func renderOpCoRoster(agents []orgTemplateAgent, verticalID string) string {
	parts := make([]string, 0, len(agents))
	for _, a := range agents {
		role := strings.TrimSpace(a.Role)
		if role == "" {
			continue
		}
		parts = append(parts, fmt.Sprintf("- %s (%s)", role, opCoAgentID(role, verticalID)))
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, "\n")
}

func renderMandateText(m models.MandateDocument) string { return RenderMandateText(m) }

type orgTemplateAgent struct {
	Role          string         `json:"role"`
	ParentRole    string         `json:"parent_role"`
	Type          string         `json:"type"`
	SystemPrompt  string         `json:"system_prompt"`
	Tools         []string       `json:"tools"`
	Subscriptions []string       `json:"subscriptions"`
	Constraints   map[string]any `json:"constraints,omitempty"`
}

type orgTemplateRoute struct {
	EventPattern   string `json:"event_pattern"`
	SubscriberRole string `json:"subscriber_role"`
	SubscriberID   string `json:"subscriber_id"`
	Reason         string `json:"reason"`
}

func (am *AgentManager) loadLatestTemplate(ctx context.Context) (orgTemplateSnapshot, error) {
	if am.store == nil {
		return orgTemplateSnapshot{}, errors.New("org template requires persistent store")
	}
	rec, err := am.store.LoadLatestOrgTemplate(ctx)
	if err != nil {
		return orgTemplateSnapshot{}, fmt.Errorf("load latest org template: %w", err)
	}
	snap := orgTemplateSnapshot{Version: strings.TrimSpace(rec.Version)}
	if snap.Version == "" {
		return orgTemplateSnapshot{}, errors.New("latest org template has empty version")
	}
	_ = json.Unmarshal(defaultJSON(rec.Agents, []byte("[]")), &snap.Agents)
	_ = json.Unmarshal(defaultJSON(rec.BootstrapRoutes, []byte("[]")), &snap.BootstrapRoutes)
	_ = json.Unmarshal(defaultJSON(rec.SeededRoutes, []byte("[]")), &snap.SeededRoutes)
	if len(snap.BootstrapRoutes) == 0 {
		return orgTemplateSnapshot{}, fmt.Errorf("org template %s has no bootstrap routes", snap.Version)
	}
	return snap, nil
}

func (am *AgentManager) resolveBootstrapVersion(ctx context.Context, templateVersion string) int {
	if am == nil || am.store == nil {
		return 1
	}
	resolver, ok := am.store.(BootstrapVersionResolver)
	if !ok || resolver == nil {
		return 1
	}
	version, err := resolver.ResolveBootstrapVersion(ctx, templateVersion)
	if err != nil {
		log.Printf("resolve bootstrap version failed template=%s err=%v", strings.TrimSpace(templateVersion), err)
		return 1
	}
	if version <= 0 {
		return 1
	}
	return version
}

func resolveTemplateSubscriber(verticalID string, rt orgTemplateRoute) string {
	if strings.TrimSpace(rt.SubscriberID) != "" {
		return strings.TrimSpace(rt.SubscriberID)
	}
	role := strings.TrimSpace(rt.SubscriberRole)
	if role == "" {
		return ""
	}
	return opCoAgentID(role, verticalID)
}

func defaultJSON(raw, fallback []byte) []byte { return DefaultJSON(raw, fallback) }

func mustJSON(v any) json.RawMessage {
	return json.RawMessage(runtimesharedjson.MustJSON(v))
}

func normalizeStringList(in []string) []string { return NormalizeStringList(in) }

func (am *AgentManager) TeardownOpCo(verticalID string) error {
	if strings.TrimSpace(verticalID) == "" {
		return errors.New("verticalID is required")
	}
	am.mu.RLock()
	toRemove := make([]string, 0)
	for id, cfg := range am.agentCfg {
		if cfg.VerticalID == verticalID {
			toRemove = append(toRemove, id)
		}
	}
	am.mu.RUnlock()

	errs := make([]string, 0)
	for _, agentID := range toRemove {
		if err := am.TeardownAgent(agentID); err != nil {
			errs = append(errs, fmt.Sprintf("teardown agent %s: %v", agentID, err))
		}
	}

	if err := am.bus.SetRoutingTable(verticalID, &runtimebus.RoutingTable{VerticalID: verticalID}); err != nil {
		errs = append(errs, fmt.Sprintf("reset routing table: %v", err))
	}
	if am.store != nil {
		if err := am.store.DeactivateRoutingRulesByVertical(am.runtimeContext(), verticalID); err != nil {
			errs = append(errs, fmt.Sprintf("deactivate routing: %v", err))
		}
	}
	if am.workspaces != nil {
		if err := am.workspaces.StopVerticalWorkspace(am.runtimeContext(), verticalID); err != nil {
			errs = append(errs, fmt.Sprintf("stop workspace: %v", err))
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	if am.bus != nil {
		payload := OpCOTeardownCompletePayload{
			VerticalID:       strings.TrimSpace(verticalID),
			AgentsRemoved:    len(toRemove),
			RoutingCleared:   true,
			WorkspaceStopped: am.workspaces != nil,
			Priority:         "normal",
		}
		_ = am.bus.Publish(am.runtimeContext(), events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("opco.teardown_complete"),
			SourceAgent: "agent-manager",
			VerticalID:  strings.TrimSpace(verticalID),
			Payload:     mustJSON(payload),
			CreatedAt:   time.Now(),
		})
	}
	return nil
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

	table := am.bus.GetRoutingTable(rule.VerticalID)
	if table == nil {
		table = &runtimebus.RoutingTable{VerticalID: rule.VerticalID}
	}
	updated := false
	for i := range table.Routes {
		r := &table.Routes[i]
		if r.EventPattern == rule.EventPattern && r.SubscriberID == rule.SubscriberID {
			r.Status = rule.Status
			updated = true
			break
		}
	}
	if !updated {
		table.Routes = append(table.Routes, runtimebus.Route{
			EventPattern: rule.EventPattern,
			SubscriberID: rule.SubscriberID,
			Status:       rule.Status,
		})
	}
	if err := am.bus.SetRoutingTable(rule.VerticalID, table); err != nil {
		return err
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
	perVertical := make(map[string]*runtimebus.RoutingTable)
	for _, r := range rules {
		if r.VerticalID == "" {
			continue
		}
		rt := perVertical[r.VerticalID]
		if rt == nil {
			rt = &runtimebus.RoutingTable{VerticalID: r.VerticalID}
			perVertical[r.VerticalID] = rt
		}
		rt.Routes = append(rt.Routes, runtimebus.Route{
			EventPattern: r.EventPattern,
			SubscriberID: r.SubscriberID,
			Status:       r.Status,
		})
		am.setRouteMeta(routeRuleKey(r.VerticalID, r.EventPattern, r.SubscriberID), r)
	}
	for verticalID, rt := range perVertical {
		if err := am.bus.SetRoutingTable(verticalID, rt); err != nil {
			return fmt.Errorf("set routing table for %s: %w", verticalID, err)
		}
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

func defaultOpCoRoster(verticalID string) []PersistedAgent {
	return DefaultOpCoRoster(verticalID)
}

func defaultOpCoRoutes(verticalID string) []PersistedRoutingRule {
	return DefaultOpCoRoutes(verticalID)
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
