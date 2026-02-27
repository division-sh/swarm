package specaudit

import (
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"strings"

	"empireai/internal/commgraph"
)

type Issue struct {
	Severity string `json:"severity"`
	Code     string `json:"code"`
	Location string `json:"location"`
	Message  string `json:"message"`
}

type Result struct {
	SpecType string  `json:"spec_type"`
	Passed   bool    `json:"passed"`
	Issues   []Issue `json:"issues,omitempty"`
}

type specTier int

const (
	tierMVP specTier = iota + 1
	tierTemplate
	tierTechnical
)

func Validate(specType string, raw []byte) Result {
	specType = normalizeSpecType(specType)
	if specType == "" {
		specType = "vertical_spec"
	}
	res := Result{SpecType: specType, Passed: true}

	if len(raw) == 0 {
		addIssue(&res, "blocker", "empty_spec", "$", "spec content is empty")
		return res
	}

	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		addIssue(&res, "blocker", "invalid_json", "$", fmt.Sprintf("invalid JSON: %v", err))
		return res
	}

	switch detectSpecTier(specType, obj) {
	case tierTemplate:
		validateTemplate(obj, &res)
		validateTemplateEnvelope(obj, &res)
	case tierTechnical:
		validateTechnicalSpec(obj, &res)
	default:
		validateMVPSpec(obj, &res)
	}
	return res
}

func normalizeSpecType(v string) string {
	t := strings.ToLower(strings.TrimSpace(v))
	t = strings.ReplaceAll(t, "-", "_")
	return t
}

func detectSpecTier(specType string, obj map[string]any) specTier {
	switch normalizeSpecType(specType) {
	case "template":
		return tierTemplate
	case "technical_spec":
		return tierTechnical
	case "vertical_spec":
		if hasTemplateSignals(obj) {
			if hasTechnicalSignals(obj) {
				return tierTechnical
			}
			return tierTemplate
		}
		return tierMVP
	default:
		if hasTemplateSignals(obj) {
			if hasTechnicalSignals(obj) {
				return tierTechnical
			}
			return tierTemplate
		}
		return tierMVP
	}
}

func hasTemplateSignals(obj map[string]any) bool {
	for _, k := range []string{
		"agents",
		"bootstrap_routes",
		"seeded_routes",
		"event_topology",
		"routing",
		"subscriptions",
		"tool_allowlists",
		"tools",
	} {
		if v, ok := obj[k]; ok && !isEmpty(v) {
			return true
		}
	}
	return false
}

func hasTechnicalSignals(obj map[string]any) bool {
	for _, k := range []string{
		"data_model",
		"schema",
		"api_endpoints",
		"handlers",
		"error_handling",
		"integration_points",
		"flow",
		"flows",
		"state_transitions",
		"implementation",
	} {
		if v, ok := obj[k]; ok && !isEmpty(v) {
			return true
		}
	}
	return false
}

func validateMVPSpec(obj map[string]any, res *Result) {
	type requirement struct {
		name string
		keys []string
	}

	required := []requirement{
		{name: "problem_statement", keys: []string{"problem", "problem_statement"}},
		{name: "core_workflow", keys: []string{"core_workflow", "workflow"}},
		{name: "features", keys: []string{"features"}},
		{name: "data_sketch", keys: []string{"data_sketch", "data_model"}},
		{name: "user_story", keys: []string{"user_story"}},
		{name: "out_of_scope", keys: []string{"out_of_scope", "exclusions", "scope_exclusions"}},
	}
	for _, req := range required {
		if _, loc, ok := firstNonEmptyField(obj, req.keys...); !ok {
			addIssue(res, "blocker", "missing_field", loc, fmt.Sprintf("required %s field is missing or empty", req.name))
		}
	}

	features, loc, ok := firstNonEmptyField(obj, "features")
	if ok {
		list, ok := features.([]any)
		if !ok {
			addIssue(res, "blocker", "invalid_features_shape", loc, "features must be an array")
		} else {
			if len(list) > 5 {
				addIssue(res, "high", "scope_creep_too_many_features", loc, "MVP spec must contain at most 5 features")
			}
			if len(list) > 0 && len(list) < 3 {
				addIssue(res, "high", "mvp_feature_count_low", loc, "MVP spec should contain at least 3 features")
			}
		}
	}

	for _, field := range []string{"pricing", "metrics", "risks"} {
		if _, _, ok := firstNonEmptyField(obj, field); !ok {
			addIssue(res, "medium", "recommended_field_missing", "$."+field, "recommended field is missing")
		}
	}

	if hasTechnologyChoices(obj) {
		addIssue(res, "medium", "premature_technology_choice", "$", "technology choices should be omitted from Tier 1 MVP specs")
	}
	if hasEdgeCaseSections(obj) {
		addIssue(res, "medium", "edge_case_scope_drift", "$", "edge-case/error-handling branches should be omitted from Tier 1 MVP specs")
	}
}

func validateTemplateEnvelope(obj map[string]any, res *Result) {
	for _, f := range []string{"version", "agents", "bootstrap_routes"} {
		v, ok := obj[f]
		if !ok || isEmpty(v) {
			addIssue(res, "blocker", "missing_field", "$."+f, "required field is missing or empty")
		}
	}
	for _, f := range []string{"seeded_routes", "notes"} {
		if v, ok := obj[f]; !ok || isEmpty(v) {
			addIssue(res, "medium", "recommended_field_missing", "$."+f, "recommended field is missing")
		}
	}
}

func validateTechnicalSpec(obj map[string]any, res *Result) {
	validateTemplateEnvelope(obj, res)
	validateTemplate(obj, res)

	type requirement struct {
		name string
		keys []string
	}
	required := []requirement{
		{name: "data_model", keys: []string{"data_model", "schema", "database_schema", "tables"}},
		{name: "flow", keys: []string{"flows", "flow", "state_transitions", "workflow"}},
		{name: "implementation", keys: []string{"api_endpoints", "handlers", "implementation", "components"}},
	}
	for _, req := range required {
		if _, loc, ok := firstNonEmptyField(obj, req.keys...); !ok {
			addIssue(res, "blocker", "missing_field", loc, fmt.Sprintf("required %s field is missing or empty for technical_spec", req.name))
		}
	}

	for _, f := range []string{"edge_cases", "error_handling", "integration_points"} {
		if _, _, ok := firstNonEmptyField(obj, f); !ok {
			addIssue(res, "medium", "recommended_field_missing", "$."+f, "recommended implementation detail is missing")
		}
	}
}

func firstNonEmptyField(obj map[string]any, keys ...string) (any, string, bool) {
	for _, key := range keys {
		if key == "" {
			continue
		}
		v, ok := obj[key]
		if !ok || isEmpty(v) {
			continue
		}
		return v, "$." + key, true
	}
	if len(keys) > 0 {
		return nil, "$." + keys[0], false
	}
	return nil, "$", false
}

func hasTechnologyChoices(obj map[string]any) bool {
	needles := []string{
		"postgres",
		"postgresql",
		"mysql",
		"redis",
		"kafka",
		"kubernetes",
		"docker",
		"aws",
		"gcp",
		"azure",
		"react",
		"nextjs",
		"golang",
		"nodejs",
		"typescript",
	}
	payload, err := json.Marshal(obj)
	if err != nil {
		return false
	}
	blob := strings.ToLower(string(payload))
	for _, needle := range needles {
		if strings.Contains(blob, needle) {
			return true
		}
	}
	return false
}

func hasEdgeCaseSections(obj map[string]any) bool {
	for _, key := range []string{"edge_cases", "error_handling", "failure_modes", "exceptions"} {
		if v, ok := obj[key]; ok && !isEmpty(v) {
			return true
		}
	}
	return false
}

func isEmpty(v any) bool {
	switch x := v.(type) {
	case nil:
		return true
	case string:
		return strings.TrimSpace(x) == ""
	case []any:
		return len(x) == 0
	case map[string]any:
		return len(x) == 0
	default:
		return false
	}
}

var eventPatternRe = regexp.MustCompile(`^[a-z0-9*]+([._][a-z0-9*]+)*$`)

func validateTemplate(obj map[string]any, res *Result) {
	agentsRaw, _ := obj["agents"].([]any)
	bootstrapRaw, _ := obj["bootstrap_routes"].([]any)
	seededRaw, _ := obj["seeded_routes"].([]any)

	roleSet := make(map[string]struct{}, len(agentsRaw))
	parentByRole := make(map[string]string, len(agentsRaw))
	roleSubscriptions := make(map[string][]string, len(agentsRaw))

	for i, raw := range agentsRaw {
		agent, ok := raw.(map[string]any)
		if !ok {
			addIssue(res, "blocker", "invalid_agent_shape", fmt.Sprintf("$.agents[%d]", i), "agent entry must be an object")
			continue
		}
		role, _ := agent["role"].(string)
		role = normalizeRole(role)
		if role == "" {
			addIssue(res, "blocker", "missing_role", fmt.Sprintf("$.agents[%d].role", i), "agent role is required")
			continue
		}
		if _, exists := roleSet[role]; exists {
			addIssue(res, "blocker", "duplicate_role", fmt.Sprintf("$.agents[%d].role", i), "agent role must be unique")
			continue
		}
		roleSet[role] = struct{}{}

		parent, _ := agent["parent_role"].(string)
		parentByRole[role] = normalizeRole(parent)

		if tools, ok := agent["tools"].([]any); ok {
			for j, t := range tools {
				tv, _ := t.(string)
				if strings.TrimSpace(tv) == "" {
					addIssue(res, "medium", "empty_tool_entry", fmt.Sprintf("$.agents[%d].tools[%d]", i, j), "tool names must be non-empty")
				}
			}
		}
		if subs, ok := agent["subscriptions"].([]any); ok {
			for j, sub := range subs {
				pat, _ := sub.(string)
				pat = strings.TrimSpace(pat)
				if pat == "" {
					continue
				}
				roleSubscriptions[role] = append(roleSubscriptions[role], pat)
				if !commgraph.HasProducerForPattern(pat) {
					res.Issues = append(res.Issues, Issue{
						Severity: "blocker",
						Code:     "unknown_subscription_producer",
						Location: fmt.Sprintf("$.agents[%d].subscriptions[%d]", i, j),
						Message:  "subscription has no known producer in communication graph registry",
					})
					res.Passed = false
				}
			}
		}
	}

	for role, parent := range parentByRole {
		if parent == "" {
			continue
		}
		if _, ok := roleSet[parent]; !ok {
			addIssue(res, "blocker", "missing_parent_role", "$.agents", fmt.Sprintf("parent_role %q for role %q is not present in agents", parent, role))
		}
	}
	if hasParentCycle(parentByRole) {
		addIssue(res, "blocker", "parent_cycle", "$.agents", "parent_role graph contains a cycle")
	}

	bootstrapRoutes := validateTemplateRoutes(bootstrapRaw, "$.bootstrap_routes", roleSet, res)
	seededRoutes := validateTemplateRoutes(seededRaw, "$.seeded_routes", roleSet, res)
	allRoutes := append(bootstrapRoutes, seededRoutes...)
	validateTemplateRouteConsumers(allRoutes, roleSubscriptions, res)
	validateTemplateProducerCoverage(roleSet, allRoutes, res)
	validateTemplateCommunication(roleSet, res)
}

type templateRoute struct {
	EventPattern   string
	SubscriberRole string
	Location       string
}

func validateTemplateRoutes(routes []any, root string, roleSet map[string]struct{}, res *Result) []templateRoute {
	out := make([]templateRoute, 0, len(routes))
	for i, raw := range routes {
		route, ok := raw.(map[string]any)
		if !ok {
			addIssue(res, "blocker", "invalid_route_shape", fmt.Sprintf("%s[%d]", root, i), "route entry must be an object")
			continue
		}
		pattern, _ := route["event_pattern"].(string)
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			addIssue(res, "blocker", "missing_event_pattern", fmt.Sprintf("%s[%d].event_pattern", root, i), "event_pattern is required")
		} else if !eventPatternRe.MatchString(pattern) {
			addIssue(res, "medium", "invalid_event_pattern", fmt.Sprintf("%s[%d].event_pattern", root, i), "event pattern format is invalid")
		} else if !commgraph.HasProducerForPattern(pattern) {
			res.Issues = append(res.Issues, Issue{
				Severity: "blocker",
				Code:     "unknown_event_producer",
				Location: fmt.Sprintf("%s[%d].event_pattern", root, i),
				Message:  "route event_pattern has no known producer in communication graph registry",
			})
			res.Passed = false
		}

		subscriberRole, _ := route["subscriber_role"].(string)
		subscriberRole = normalizeRole(subscriberRole)
		subscriberID, _ := route["subscriber_id"].(string)
		subscriberID = strings.TrimSpace(subscriberID)
		if subscriberRole == "" && subscriberID == "" {
			addIssue(res, "blocker", "missing_subscriber", fmt.Sprintf("%s[%d]", root, i), "route must define subscriber_role or subscriber_id")
			continue
		}
		if subscriberRole != "" {
			if _, ok := roleSet[subscriberRole]; !ok {
				addIssue(res, "blocker", "unknown_subscriber_role", fmt.Sprintf("%s[%d].subscriber_role", root, i), "subscriber_role is not defined in agents")
			}
			out = append(out, templateRoute{
				EventPattern:   pattern,
				SubscriberRole: subscriberRole,
				Location:       fmt.Sprintf("%s[%d]", root, i),
			})
		}
	}
	return out
}

func validateTemplateRouteConsumers(routes []templateRoute, roleSubscriptions map[string][]string, res *Result) {
	for _, route := range routes {
		if route.EventPattern == "" || route.SubscriberRole == "" {
			continue
		}
		subs := roleSubscriptions[route.SubscriberRole]
		if len(subs) == 0 {
			if routeManagedWorkerRole(route.SubscriberRole) {
				continue
			}
			addIssue(
				res,
				"blocker",
				"route_subscriber_has_no_subscriptions",
				route.Location+".subscriber_role",
				fmt.Sprintf("route subscriber role %q has no subscriptions for event %q", route.SubscriberRole, route.EventPattern),
			)
			continue
		}
		matched := false
		for _, sub := range subs {
			if patternsIntersect(route.EventPattern, sub) {
				matched = true
				break
			}
		}
		if !matched {
			addIssue(
				res,
				"blocker",
				"unconsumed_route_pattern",
				route.Location+".event_pattern",
				fmt.Sprintf("route pattern %q targets %q but none of its subscriptions match", route.EventPattern, route.SubscriberRole),
			)
		}
	}
}

func routeManagedWorkerRole(role string) bool {
	switch normalizeRole(role) {
	case "pm-agent", "cto-agent", "tech-writer", "backend-agent", "frontend-agent", "qa-agent", "devops-agent", "marketing-agent", "support-agent":
		return true
	default:
		return false
	}
}

func validateTemplateProducerCoverage(roleSet map[string]struct{}, routes []templateRoute, res *Result) {
	for role := range roleSet {
		events := commgraph.ProducerEventsForRole(role)
		for _, evt := range events {
			if evt == "" {
				continue
			}
			if routeSetMatchesEvent(routes, evt) {
				continue
			}
			res.Issues = append(res.Issues, Issue{
				Severity: "medium",
				Code:     "produced_event_unrouted",
				Location: "$.bootstrap_routes",
				Message:  fmt.Sprintf("produced event %q from role %q has no matching route pattern", evt, role),
			})
		}
	}
}

func routeSetMatchesEvent(routes []templateRoute, eventType string) bool {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return false
	}
	for _, route := range routes {
		if routePatternMatches(route.EventPattern, eventType) {
			return true
		}
	}
	return false
}

func patternsIntersect(a, b string) bool {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	if a == "" || b == "" {
		return false
	}
	if routePatternMatches(a, b) || routePatternMatches(b, a) {
		return true
	}
	for evt := range commgraph.KnownProducedEvents() {
		if routePatternMatches(a, evt) && routePatternMatches(b, evt) {
			return true
		}
	}
	return false
}

func routePatternMatches(pattern, eventType string) bool {
	switch {
	case pattern == "", pattern == "*":
		return true
	default:
		if strings.Contains(pattern, "*") {
			if ok, err := path.Match(pattern, eventType); err == nil && ok {
				return true
			}
		}
		if strings.HasSuffix(pattern, "*") {
			return strings.HasPrefix(eventType, strings.TrimSuffix(pattern, "*"))
		}
		return pattern == eventType
	}
}

func normalizeRole(role string) string {
	return commgraph.CanonicalRole(role)
}

func validateTemplateCommunication(roleSet map[string]struct{}, res *Result) {
	for _, rule := range commgraph.MessageAuthorities() {
		scope := strings.TrimSpace(strings.ToLower(rule.Scope))
		if scope == "holding" {
			continue
		}
		sender := strings.TrimSpace(strings.ToLower(rule.SenderRole))
		if sender == "" {
			continue
		}
		if _, ok := roleSet[sender]; !ok {
			continue
		}
		for _, recipient := range rule.RecipientRoles {
			recipient = strings.TrimSpace(strings.ToLower(recipient))
			if recipient == "" {
				continue
			}
			if _, ok := roleSet[recipient]; !ok {
				res.Issues = append(res.Issues, Issue{
					Severity: "medium",
					Code:     "unknown_message_recipient",
					Location: "$.agents",
					Message:  fmt.Sprintf("message authority sender %q references unknown recipient role %q", sender, recipient),
				})
			}
		}
	}

	for _, loop := range commgraph.MailboxRoundTrips() {
		sender := strings.TrimSpace(strings.ToLower(loop.SenderRole))
		if sender == "" {
			continue
		}
		if _, ok := roleSet[sender]; !ok {
			continue
		}
		ret := strings.TrimSpace(strings.ToLower(loop.ReturnToRole))
		switch ret {
		case "", "requesting-agent":
			continue
		}
		if _, ok := roleSet[ret]; !ok {
			res.Issues = append(res.Issues, Issue{
				Severity: "medium",
				Code:     "unknown_mailbox_return_role",
				Location: "$.agents",
				Message:  fmt.Sprintf("mailbox loop sender %q returns to unknown role %q", sender, ret),
			})
		}
	}
}

func hasParentCycle(parentByRole map[string]string) bool {
	visited := make(map[string]bool, len(parentByRole))
	onStack := make(map[string]bool, len(parentByRole))
	var walk func(string) bool
	walk = func(role string) bool {
		if onStack[role] {
			return true
		}
		if visited[role] {
			return false
		}
		visited[role] = true
		onStack[role] = true
		parent := strings.TrimSpace(parentByRole[role])
		if parent != "" {
			if _, ok := parentByRole[parent]; ok && walk(parent) {
				return true
			}
		}
		onStack[role] = false
		return false
	}
	for role := range parentByRole {
		if walk(role) {
			return true
		}
	}
	return false
}

func addIssue(res *Result, severity, code, location, message string) {
	if strings.EqualFold(strings.TrimSpace(severity), "blocker") {
		res.Passed = false
	}
	res.Issues = append(res.Issues, Issue{
		Severity: severity,
		Code:     code,
		Location: location,
		Message:  message,
	})
}
