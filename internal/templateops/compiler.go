package templateops

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type AgentYAML struct {
	ID            string         `yaml:"id"`
	Role          string         `yaml:"role"`
	Mode          string         `yaml:"mode"`
	ModelTier     string         `yaml:"model_tier"`
	Type          string         `yaml:"type"` // optional alias for model_tier
	ParentRole    string         `yaml:"parent_role"`
	Parent        string         `yaml:"parent"` // optional alias for parent_role
	SystemPrompt  string         `yaml:"system_prompt"`
	Tools         []string       `yaml:"tools"`
	Subscriptions []string       `yaml:"subscriptions"`
	Constraints   map[string]any `yaml:"constraints"`
}

type RoutesYAML struct {
	BootstrapRoutes []RouteYAML `yaml:"bootstrap_routes"`
	SeededRoutes    []RouteYAML `yaml:"seeded_routes"`
}

type RouteYAML struct {
	EventPattern   string `yaml:"event_pattern"`
	SubscriberRole string `yaml:"subscriber_role"`
	SubscriberID   string `yaml:"subscriber_id"`
	Reason         string `yaml:"reason"`
}

// CompileTemplateFromYAML reads agent YAML templates + routing YAML and emits
// JSON arrays suitable for org_templates columns: agents/bootstrap_routes/seeded_routes.
func CompileTemplateFromYAML(agentsDir, routesPath string) (agentsJSON, bootstrapJSON, seededJSON []byte, _ error) {
	agentsDir = strings.TrimSpace(agentsDir)
	if agentsDir == "" {
		return nil, nil, nil, fmt.Errorf("agentsDir is required")
	}
	routesPath = strings.TrimSpace(routesPath)
	if routesPath == "" {
		return nil, nil, nil, fmt.Errorf("routesPath is required")
	}

	files, err := listYAMLFiles(agentsDir)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(files) == 0 {
		return nil, nil, nil, fmt.Errorf("no agent yaml files found in %s", agentsDir)
	}

	agents := make([]map[string]any, 0, len(files))
	seenRoles := make(map[string]struct{}, len(files))
	for _, f := range files {
		// It's common (and convenient) to colocate the routing YAML (routes.yaml)
		// alongside the agent template YAMLs. If we see a routes-shaped document,
		// skip it here; it will be loaded separately via routesPath.
		var probe map[string]any
		if err := readYAMLFile(f, &probe); err != nil {
			return nil, nil, nil, err
		}
		if _, ok := probe["bootstrap_routes"]; ok {
			continue
		}
		if _, ok := probe["seeded_routes"]; ok {
			continue
		}

		var a AgentYAML
		if err := readYAMLFile(f, &a); err != nil {
			return nil, nil, nil, err
		}
		role := coalesce(strings.TrimSpace(a.Role), strings.TrimSpace(a.ID))
		role = strings.TrimSpace(role)
		if role == "" {
			return nil, nil, nil, fmt.Errorf("template agent missing role (file=%s)", f)
		}
		if _, ok := seenRoles[role]; ok {
			return nil, nil, nil, fmt.Errorf("duplicate template agent role %q (file=%s)", role, f)
		}
		seenRoles[role] = struct{}{}

		parentRole := coalesce(strings.TrimSpace(a.ParentRole), strings.TrimSpace(a.Parent))
		if parentRole == "" {
			parentRole = defaultParentRole(role)
		}

		agent := map[string]any{
			"role":          role,
			"parent_role":   strings.TrimSpace(parentRole),
			"type":          strings.TrimSpace(coalesce(a.ModelTier, a.Type)),
			"system_prompt": strings.TrimSpace(a.SystemPrompt),
			"tools":         normalizeStringList(a.Tools),
			"subscriptions": normalizeStringList(a.Subscriptions),
		}
		if a.Constraints != nil && len(a.Constraints) > 0 {
			agent["constraints"] = a.Constraints
		}
		agents = append(agents, agent)
	}

	var routes RoutesYAML
	if err := readYAMLFile(routesPath, &routes); err != nil {
		return nil, nil, nil, err
	}
	bootstrap := compileRoutes(routes.BootstrapRoutes)
	seeded := compileRoutes(routes.SeededRoutes)

	agentsJSON, err = json.Marshal(agents)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal agents json: %w", err)
	}
	bootstrapJSON, err = json.Marshal(bootstrap)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal bootstrap routes json: %w", err)
	}
	seededJSON, err = json.Marshal(seeded)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal seeded routes json: %w", err)
	}
	return agentsJSON, bootstrapJSON, seededJSON, nil
}

func compileRoutes(in []RouteYAML) []map[string]any {
	out := make([]map[string]any, 0, len(in))
	for _, r := range in {
		pattern := strings.TrimSpace(r.EventPattern)
		if pattern == "" {
			continue
		}
		role := strings.TrimSpace(r.SubscriberRole)
		id := strings.TrimSpace(r.SubscriberID)
		reason := strings.TrimSpace(r.Reason)
		obj := map[string]any{
			"event_pattern": pattern,
			"reason":        reason,
		}
		if role != "" {
			obj["subscriber_role"] = role
		}
		if id != "" {
			obj["subscriber_id"] = id
		}
		out = append(out, obj)
	}
	return out
}

func listYAMLFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read dir %s: %w", dir, err)
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		ext := strings.ToLower(filepath.Ext(name))
		if ext != ".yaml" && ext != ".yml" {
			continue
		}
		out = append(out, filepath.Join(dir, name))
	}
	sort.Strings(out)
	return out, nil
}

func readYAMLFile(path string, out any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read yaml file %s: %w", path, err)
	}
	if err := yaml.Unmarshal(b, out); err != nil {
		return fmt.Errorf("parse yaml file %s: %w", path, err)
	}
	return nil
}

func defaultParentRole(role string) string {
	switch strings.TrimSpace(role) {
	case "opco-ceo":
		return ""
	case "chief-of-staff":
		return "opco-ceo"
	case "vp-product":
		return "opco-ceo"
	case "vp-growth":
		return "opco-ceo"
	case "cto-agent":
		return "vp-product"
	case "pm-agent":
		return "vp-product"
	case "support-agent":
		return "vp-product"
	case "marketing-agent":
		return "vp-growth"
	case "tech-writer":
		return "cto-agent"
	case "backend-agent":
		return "cto-agent"
	case "frontend-agent":
		return "cto-agent"
	case "qa-agent":
		return "cto-agent"
	case "devops-agent":
		return "cto-agent"
	default:
		return ""
	}
}
