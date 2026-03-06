package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	goruntime "runtime"
	"sort"
	"strings"

	"empireai/internal/commgraph"
	"empireai/internal/config"
	models "empireai/internal/models"
	runtimetools "empireai/internal/runtime/tools"
	"empireai/internal/templateops"
)

func (s *Server) buildPipelineDesignGraphFromSources(_ context.Context, vertical string) ([]graphNode, []graphEdge, map[string]any, error) {
	nodes := make([]graphNode, 0, 192)
	edges := make([]graphEdge, 0, 320)
	seenNodes := map[string]struct{}{}
	seenEdges := map[string]struct{}{}

	addNode := func(n graphNode) {
		n.ID = strings.TrimSpace(n.ID)
		if n.ID == "" {
			return
		}
		if strings.TrimSpace(n.Label) == "" {
			n.Label = n.ID
		}
		if _, ok := seenNodes[n.ID]; ok {
			return
		}
		nodes = append(nodes, n)
		seenNodes[n.ID] = struct{}{}
	}
	addEdge := func(e graphEdge) {
		e.From = strings.TrimSpace(e.From)
		e.To = strings.TrimSpace(e.To)
		e.Label = strings.TrimSpace(e.Label)
		e.EventType = strings.TrimSpace(e.EventType)
		if e.From == "" || e.To == "" {
			return
		}
		if e.Status == "" {
			e.Status = "active"
		}
		key := fmt.Sprintf("%s|%s|%s|%s|%s|%s", e.From, e.To, e.Kind, e.Label, e.EventType, e.Source)
		if _, ok := seenEdges[key]; ok {
			return
		}
		edges = append(edges, e)
		seenEdges[key] = struct{}{}
	}

	addNode(graphNode{ID: "sys:human-board", Kind: "human", Label: "Human Board", Group: "human"})
	addNode(graphNode{ID: "sys:mailbox", Kind: "mailbox", Label: "Mailbox", Group: "human"})
	addNode(graphNode{ID: commgraph.RuntimeProducerID, Kind: "system", Label: "Runtime", Group: "factory"})
	addNode(graphNode{ID: "runtime:pipeline-coordinator", Kind: "runtime_process", Label: "PipelineCoordinator", Group: "factory"})
	addNode(graphNode{ID: "runtime:scan-accumulator", Kind: "state_machine", Label: "ScanAccumulator", Group: "factory"})
	addNode(graphNode{ID: "runtime:scoring-accumulator", Kind: "state_machine", Label: "ScoringAccumulator", Group: "factory"})
	addNode(graphNode{ID: "runtime:validation-pipeline", Kind: "state_machine", Label: "ValidationPipeline", Group: "factory"})
	addNode(graphNode{ID: "runtime:compute-composite", Kind: "runtime_process", Label: "computeComposite()", Group: "factory"})
	addNode(graphNode{ID: "gate:viability-floor", Kind: "gate", Label: "Viability Floor", Group: "factory"})
	addNode(graphNode{ID: "gate:hard-gates", Kind: "gate", Label: "Hard Gates", Group: "factory"})
	addNode(graphNode{ID: "pipeline:vertical-stages", Kind: "state_machine", Label: "Vertical Stage Machine", Group: "factory"})

	agents := s.loadPipelineDesignAgents()
	for _, a := range agents {
		addNode(a)
	}

	ensureEventNode := func(eventType string) string {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			return ""
		}
		id := "evt:" + eventType
		addNode(graphNode{
			ID:    id,
			Kind:  "event",
			Label: eventType,
			Group: "factory",
		})
		return id
	}

	eventTypes := map[string]struct{}{}
	addEventType := func(eventType string) {
		eventType = strings.TrimSpace(eventType)
		if eventType == "" {
			return
		}
		eventTypes[eventType] = struct{}{}
	}

	for _, evt := range commgraph.RuntimeEvents() {
		evtID := ensureEventNode(evt)
		addEventType(evt)
		if evtID == "" {
			continue
		}
		addEdge(graphEdge{
			From:      commgraph.RuntimeProducerID,
			To:        evtID,
			Kind:      "producer",
			Label:     evt,
			EventType: evt,
			Source:    "producer_registry",
		})
	}
	for _, evt := range commgraph.HumanEvents() {
		evtID := ensureEventNode(evt)
		addEventType(evt)
		if evtID == "" {
			continue
		}
		addEdge(graphEdge{
			From:      commgraph.HumanProducerID,
			To:        evtID,
			Kind:      "producer",
			Label:     evt,
			EventType: evt,
			Source:    "producer_registry",
		})
	}

	for _, n := range agents {
		role := strings.TrimSpace(n.Role)
		for _, evt := range commgraph.ProducerEventsForRole(role) {
			evtID := ensureEventNode(evt)
			addEventType(evt)
			if evtID == "" {
				continue
			}
			addEdge(graphEdge{
				From:      n.ID,
				To:        evtID,
				Kind:      "producer",
				Label:     evt,
				EventType: evt,
				Source:    "producer_registry",
			})
		}
		for _, sub := range n.Subscriptions {
			sub = strings.TrimSpace(sub)
			if sub == "" {
				continue
			}
			evtID := ensureEventNode(sub)
			addEventType(sub)
			if evtID == "" {
				continue
			}
			addEdge(graphEdge{
				From:      evtID,
				To:        n.ID,
				Kind:      "subscription",
				Label:     sub,
				EventType: sub,
				Source:    "subscription",
			})
		}
	}

	for evt := range commgraph.KnownProducedEvents() {
		addEventType(evt)
	}
	eventSchemas := runtimetools.EventSchemaSnapshot()
	for evt := range eventSchemas {
		addEventType(evt)
	}

	for evt := range eventTypes {
		evt = strings.TrimSpace(evt)
		if evt == "" {
			continue
		}
		evtID := ensureEventNode(evt)
		if evtID == "" {
			continue
		}
		intercepted, passthrough := flowInterceptPolicyForDesign(evt)
		handler := pipelineHandlerRef(evt)
		if !intercepted && !passthrough && handler == "" {
			continue
		}
		if handler != "" {
			handlerID := "int:" + sanitizeFlowNodeID(handler)
			addNode(graphNode{
				ID:    handlerID,
				Kind:  "runtime_process",
				Label: strings.TrimPrefix(handler, "pipeline_coordinator.go:"),
				Group: "factory",
			})
			addEdge(graphEdge{
				From:      evtID,
				To:        handlerID,
				Kind:      "routing",
				Label:     evt,
				EventType: evt,
				Source:    "pipeline",
			})
		} else {
			addEdge(graphEdge{
				From:      evtID,
				To:        "runtime:pipeline-coordinator",
				Kind:      "routing",
				Label:     evt,
				EventType: evt,
				Source:    "pipeline",
			})
		}
	}

	if strings.TrimSpace(vertical) != "" {
		addNode(graphNode{ID: "focus:vertical", Kind: "system", Label: "Vertical Focus: " + strings.TrimSpace(vertical), Group: "factory"})
		addEdge(graphEdge{From: "focus:vertical", To: "pipeline:vertical-stages", Kind: "routing", Label: "filter", Source: "ui"})
	}

	producersByEvent := map[string]map[string]struct{}{}
	consumersByEvent := map[string]map[string]struct{}{}
	for _, e := range edges {
		if strings.TrimSpace(e.EventType) == "" {
			continue
		}
		switch e.Kind {
		case "producer":
			if _, ok := producersByEvent[e.EventType]; !ok {
				producersByEvent[e.EventType] = map[string]struct{}{}
			}
			producersByEvent[e.EventType][strings.TrimSpace(e.From)] = struct{}{}
		case "subscription":
			if _, ok := consumersByEvent[e.EventType]; !ok {
				consumersByEvent[e.EventType] = map[string]struct{}{}
			}
			consumersByEvent[e.EventType][strings.TrimSpace(e.To)] = struct{}{}
		}
	}

	stageSet := map[string]struct{}{}
	rubricSet := map[string]struct{}{}
	for i := range edges {
		eventType := strings.TrimSpace(edges[i].EventType)
		if eventType == "" {
			continue
		}
		if producers := producersByEvent[eventType]; len(producers) > 0 {
			edges[i].Producers = mapKeys(producers)
		}
		if consumers := consumersByEvent[eventType]; len(consumers) > 0 {
			edges[i].Consumers = mapKeys(consumers)
		}
		if schema, ok := eventSchemas[eventType]; ok {
			edges[i].SchemaRequired = eventSchemaRequired(schema.Schema)
			edges[i].SchemaProperties = eventSchemaProperties(schema.Schema)
		}
		intercepted, passthrough := flowInterceptPolicyForDesign(eventType)
		edges[i].Intercepted = intercepted
		edges[i].Passthrough = passthrough
		edges[i].InterceptorHandle = pipelineHandlerRef(eventType)
		edges[i].Stages = pipelineStagesForEvent(eventType)
		edges[i].Rubrics = pipelineRubricsForEvent(eventType)
		for _, stage := range edges[i].Stages {
			stageSet[stage] = struct{}{}
		}
		for _, rubric := range edges[i].Rubrics {
			rubricSet[rubric] = struct{}{}
		}
	}

	sort.SliceStable(nodes, func(i, j int) bool {
		if nodes[i].Group == nodes[j].Group {
			return nodes[i].ID < nodes[j].ID
		}
		return nodes[i].Group < nodes[j].Group
	})
	sort.SliceStable(edges, func(i, j int) bool {
		if edges[i].From == edges[j].From {
			if edges[i].To == edges[j].To {
				return edges[i].Label < edges[j].Label
			}
			return edges[i].To < edges[j].To
		}
		return edges[i].From < edges[j].From
	})

	meta := map[string]any{
		"design_version": "2.0.26",
		"lanes":          []string{"human", "factory", "opco"},
		"node_count":     len(nodes),
		"edge_count":     len(edges),
		"rubrics":        sortedStringKeys(rubricSet),
		"stages":         sortedStringKeys(stageSet),
		"sources": []string{
			"configs/agents/roster.yaml",
			"configs/agents/templates/*.yaml",
			"internal/commgraph/registry.go",
			"internal/runtime/event_emit_tools.go",
			"internal/dashboard/server.go:flowInterceptPolicy",
		},
	}
	return nodes, edges, meta, nil
}

func (s *Server) loadPipelineDesignAgents() []graphNode {
	agentsDir := resolvePipelineAgentsDir(s.cfg)
	if agentsDir == "" {
		return fallbackPipelineAgentNodes()
	}

	nodes := map[string]graphNode{}
	addNode := func(n graphNode) {
		n.ID = strings.TrimSpace(n.ID)
		if n.ID == "" {
			return
		}
		if prev, ok := nodes[n.ID]; ok {
			nodes[n.ID] = mergeGraphNode(prev, n)
			return
		}
		nodes[n.ID] = n
	}

	globalSpecs, gErr := templateops.LoadGlobalAgentsFromYAML(agentsDir)
	if gErr == nil {
		for _, spec := range globalSpecs {
			prompt, tools, _, constraints := parseAgentRuntimeConfig(spec.Config)
			group := "holding"
			if strings.EqualFold(strings.TrimSpace(spec.Mode), "factory") {
				group = "factory"
			}
			role := strings.TrimSpace(spec.Role)
			if role == "" {
				role = strings.TrimSpace(spec.ID)
			}
			id := role
			addNode(graphNode{
				ID:            id,
				Kind:          "agent",
				Label:         id,
				Group:         group,
				Role:          role,
				Mode:          spec.Mode,
				Status:        "template",
				SystemPrompt:  prompt,
				Tools:         tools,
				Subscriptions: normalizeStrings(spec.Subscriptions),
				Constraints:   constraints,
			})
		}
	}

	templateDir := filepath.Join(agentsDir, "templates")
	routesPath := filepath.Join(templateDir, "routes.yaml")
	agentsJSON, _, _, tErr := templateops.CompileTemplateFromYAML(templateDir, routesPath)
	if tErr == nil {
		var templateAgents []map[string]any
		if json.Unmarshal(agentsJSON, &templateAgents) == nil {
			for _, a := range templateAgents {
				role := strings.TrimSpace(asString(a["role"]))
				if role == "" {
					continue
				}
				systemPrompt := strings.TrimSpace(asString(a["system_prompt"]))
				tools := parseStringList(a["tools"])
				subs := parseStringList(a["subscriptions"])
				constraints := map[string]any{}
				if c, ok := a["constraints"].(map[string]any); ok {
					constraints = c
				}
				addNode(graphNode{
					ID:            role,
					Kind:          "agent",
					Label:         role,
					Group:         "opco",
					Role:          role,
					Mode:          "operating",
					Status:        "template",
					SystemPrompt:  systemPrompt,
					Tools:         normalizeStrings(tools),
					Subscriptions: normalizeStrings(subs),
					Constraints:   constraints,
				})
			}
		}
	}

	if len(nodes) == 0 {
		return fallbackPipelineAgentNodes()
	}
	out := make([]graphNode, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Group == out[j].Group {
			return out[i].ID < out[j].ID
		}
		return out[i].Group < out[j].Group
	})
	return out
}

func fallbackPipelineAgentNodes() []graphNode {
	roles := []graphNode{
		{ID: "empire-coordinator", Kind: "agent", Label: "empire-coordinator", Role: "empire-coordinator", Group: "factory", Mode: "holding"},
		{ID: "factory-cto", Kind: "agent", Label: "factory-cto", Role: "factory-cto", Group: "factory", Mode: "factory"},
		{ID: "spec-auditor", Kind: "agent", Label: "spec-auditor", Role: "spec-auditor", Group: "holding", Mode: "holding"},
		{ID: "market-research-agent", Kind: "agent", Label: "market-research-agent", Role: "market-research-agent", Group: "factory", Mode: "factory"},
		{ID: "trend-research-agent", Kind: "agent", Label: "trend-research-agent", Role: "trend-research-agent", Group: "factory", Mode: "factory"},
		{ID: "analysis-agent", Kind: "agent", Label: "analysis-agent", Role: "analysis-agent", Group: "factory", Mode: "factory"},
		{ID: "business-research-agent", Kind: "agent", Label: "business-research-agent", Role: "business-research-agent", Group: "factory", Mode: "factory"},
		{ID: "lightweight-spec-agent", Kind: "agent", Label: "lightweight-spec-agent", Role: "lightweight-spec-agent", Group: "factory", Mode: "factory"},
		{ID: "spec-reviewer", Kind: "agent", Label: "spec-reviewer", Role: "spec-reviewer", Group: "factory", Mode: "factory"},
		{ID: "pre-brand-agent", Kind: "agent", Label: "pre-brand-agent", Role: "pre-brand-agent", Group: "factory", Mode: "factory"},
		{ID: "validation-coordinator", Kind: "agent", Label: "validation-coordinator", Role: "validation-coordinator", Group: "factory", Mode: "factory"},
		{ID: "opco-ceo", Kind: "agent", Label: "opco-ceo", Role: "opco-ceo", Group: "opco", Mode: "operating"},
	}
	return roles
}

func resolvePipelineAgentsDir(_ *config.Config) string {
	candidates := make([]string, 0, 8)

	if wd, err := os.Getwd(); err == nil {
		candidates = append(candidates,
			filepath.Join(wd, "configs", "agents"),
			filepath.Join(wd, "..", "configs", "agents"),
			filepath.Join(wd, "..", "..", "configs", "agents"),
		)
	}
	if _, file, _, ok := goruntime.Caller(0); ok {
		repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
		candidates = append(candidates, filepath.Join(repoRoot, "configs", "agents"))
	}
	seen := map[string]struct{}{}
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		abs, err := filepath.Abs(candidate)
		if err != nil {
			continue
		}
		if _, ok := seen[abs]; ok {
			continue
		}
		seen[abs] = struct{}{}
		if _, err := os.Stat(filepath.Join(abs, "roster.yaml")); err == nil {
			return abs
		}
	}
	return ""
}

func flowInterceptPolicyForDesign(eventType string) (intercepted bool, passthrough bool) {
	eventType = strings.TrimSpace(eventType)
	switch eventType {
	case "vertical.scored":
		return flowInterceptPolicy(eventType, []byte(`{"result":"marginal"}`))
	default:
		return flowInterceptPolicy(eventType, nil)
	}
}

func sanitizeFlowNodeID(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	replacer := strings.NewReplacer(":", "-", ".", "-", "/", "-", " ", "-", "(", "", ")", "", ",", "", "*", "")
	v = replacer.Replace(v)
	v = strings.Trim(v, "-")
	if v == "" {
		return "handler"
	}
	return v
}

func pipelineStagesForEvent(eventType string) []string {
	eventType = strings.ToLower(strings.TrimSpace(eventType))
	if eventType == "" {
		return nil
	}
	switch {
	case strings.HasPrefix(eventType, "scan."),
		strings.HasPrefix(eventType, "market_research."),
		strings.HasPrefix(eventType, "trend_research."),
		strings.HasPrefix(eventType, "scanner."),
		strings.HasPrefix(eventType, "category."),
		strings.HasPrefix(eventType, "trend."),
		strings.HasPrefix(eventType, "source."),
		eventType == "campaign.completed":
		return []string{"discovery"}
	case strings.HasPrefix(eventType, "scoring."),
		strings.HasPrefix(eventType, "score."),
		eventType == "vertical.discovered",
		eventType == "vertical.scored",
		eventType == "vertical.shortlisted",
		eventType == "vertical.marginal",
		eventType == "vertical.rejected",
		eventType == "timer.portfolio_digest":
		return []string{"scoring"}
	case strings.HasPrefix(eventType, "validation."),
		strings.HasPrefix(eventType, "research."),
		strings.HasPrefix(eventType, "spec."),
		strings.HasPrefix(eventType, "cto."),
		strings.HasPrefix(eventType, "brand."),
		eventType == "vertical.ready_for_review",
		eventType == "vertical.resumed":
		return []string{"validation"}
	case eventType == "vertical.approved",
		eventType == "vertical.killed",
		eventType == "vertical.needs_more_data",
		strings.HasPrefix(eventType, "human_task."),
		eventType == "mailbox.item_decided":
		return []string{"mailbox"}
	case strings.HasPrefix(eventType, "opco."),
		strings.HasPrefix(eventType, "build."),
		strings.HasPrefix(eventType, "deploy."),
		strings.HasPrefix(eventType, "devops."),
		strings.HasPrefix(eventType, "qa."),
		strings.HasPrefix(eventType, "product."),
		strings.HasPrefix(eventType, "growth."),
		strings.HasPrefix(eventType, "support."),
		strings.HasPrefix(eventType, "launch."):
		return []string{"opco"}
	default:
		return []string{"system"}
	}
}

func pipelineRubricsForEvent(eventType string) []string {
	eventType = strings.ToLower(strings.TrimSpace(eventType))
	switch {
	case strings.HasPrefix(eventType, "score."),
		strings.HasPrefix(eventType, "scoring."),
		eventType == "vertical.discovered",
		eventType == "vertical.scored",
		eventType == "vertical.shortlisted",
		eventType == "vertical.marginal",
		eventType == "vertical.rejected":
		return []string{"automation_micro", "local_services", "saas"}
	default:
		return nil
	}
}

func sortedStringKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		k = strings.TrimSpace(k)
		if k == "" {
			continue
		}
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func modelGroupForDesign(spec models.AgentConfig) string {
	mode := strings.ToLower(strings.TrimSpace(spec.Mode))
	switch mode {
	case "factory":
		return "factory"
	case "holding":
		return "holding"
	default:
		return "holding"
	}
}
