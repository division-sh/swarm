package templateops

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"empireai/internal/promptcontracts"
	models "empireai/internal/runtime/actors"
	runtimecontracts "empireai/internal/runtime/contracts"
)

type GlobalAgentRosterYAML struct {
	Agents map[string]GlobalAgentRosterEntry `yaml:"agents"`
}

type GlobalAgentRosterEntry struct {
	ConfigPath string `yaml:"config_path"`
}

// LoadGlobalAgentsFromYAML reads holding/factory agent configs from YAML authoring
// surface under configs/agents/*.yaml (not templates). It returns AgentConfig rows
// ready for persistence/spawn.
func LoadGlobalAgentsFromYAML(agentsDir string) ([]models.AgentConfig, error) {
	agentsDir = strings.TrimSpace(agentsDir)
	if agentsDir == "" {
		return nil, fmt.Errorf("agentsDir is required")
	}
	files, err := resolveGlobalAgentFiles(agentsDir)
	if err != nil {
		return nil, err
	}

	out := make([]models.AgentConfig, 0, len(files))
	seen := make(map[string]struct{}, len(files))
	for _, f := range files {
		var a AgentYAML
		if err := readYAMLFile(f, &a); err != nil {
			return nil, err
		}
		id := strings.TrimSpace(coalesce(a.ID, strings.TrimSuffix(filepath.Base(f), filepath.Ext(f))))
		if id == "" {
			return nil, fmt.Errorf("agent missing id (file=%s)", f)
		}
		if strings.TrimSpace(a.SystemPrompt) != "" {
			return nil, fmt.Errorf("agent %s uses legacy system_prompt in YAML (file=%s); use MAS contract prompts for %s instead", id, f, id)
		}
		contractPrompt, foundContractPrompt, err := promptcontracts.Load(id, "")
		if err != nil || !foundContractPrompt || strings.TrimSpace(contractPrompt) == "" {
			contractPrompt, foundContractPrompt, err = runtimecontracts.LoadPromptForAgent(models.AgentConfig{
				ID:   id,
				Role: strings.TrimSpace(coalesce(a.Role, id)),
				Mode: strings.TrimSpace(strings.ToLower(a.Mode)),
			}, "")
		}
		if err != nil {
			return nil, fmt.Errorf("load contract prompt for %s: %w", id, err)
		}
		systemPrompt := strings.TrimSpace(contractPrompt)
		if !foundContractPrompt || systemPrompt == "" {
			return nil, fmt.Errorf("agent %s missing required contract prompt in MAS spec bundle", id)
		}
		if _, ok := seen[id]; ok {
			return nil, fmt.Errorf("duplicate agent id %q (file=%s)", id, f)
		}
		seen[id] = struct{}{}

		role := strings.TrimSpace(coalesce(a.Role, id))
		mode := strings.TrimSpace(strings.ToLower(a.Mode))
		if mode == "" {
			mode = inferGlobalAgentMode(id, role)
		}
		tier := strings.TrimSpace(coalesce(a.ModelTier, a.Type))
		if tier == "" {
			tier = "sonnet"
		}

		cfgObj := map[string]any{}
		if systemPrompt != "" {
			cfgObj["system_prompt"] = systemPrompt
		}
		if len(a.Tools) > 0 {
			cfgObj["tools"] = normalizeStringList(a.Tools)
		}
		if a.Constraints != nil && len(a.Constraints) > 0 {
			cfgObj["constraints"] = a.Constraints
		}
		cfgJSON, _ := json.Marshal(cfgObj)

		out = append(out, models.AgentConfig{
			ID:            id,
			Type:          tier,
			Role:          role,
			Mode:          mode,
			Subscriptions: normalizeStringList(a.Subscriptions),
			Config:        cfgJSON,
		})
	}

	sort.SliceStable(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func resolveGlobalAgentFiles(agentsDir string) ([]string, error) {
	absBase, err := filepath.Abs(agentsDir)
	if err != nil {
		return nil, fmt.Errorf("resolve agents dir %s: %w", agentsDir, err)
	}
	rosterPath := filepath.Join(agentsDir, "roster.yaml")
	var roster GlobalAgentRosterYAML
	if err := readYAMLFile(rosterPath, &roster); err != nil {
		return nil, fmt.Errorf("load global agent roster %s: %w", rosterPath, err)
	}
	if len(roster.Agents) == 0 {
		return nil, fmt.Errorf("global agent roster is empty: %s", rosterPath)
	}

	keys := make([]string, 0, len(roster.Agents))
	for k := range roster.Agents {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	files := make([]string, 0, len(keys))
	seenPath := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		entry := roster.Agents[key]
		cfgPath := strings.TrimSpace(entry.ConfigPath)
		if cfgPath == "" {
			return nil, fmt.Errorf("roster agent %q missing config_path in %s", key, rosterPath)
		}
		if !filepath.IsAbs(cfgPath) {
			cfgPath = filepath.Join(agentsDir, cfgPath)
		}
		cfgPath = filepath.Clean(cfgPath)
		absCfgPath, err := filepath.Abs(cfgPath)
		if err != nil {
			return nil, fmt.Errorf("resolve roster agent %q config path %s: %w", key, cfgPath, err)
		}
		relPath, err := filepath.Rel(absBase, absCfgPath)
		if err != nil {
			return nil, fmt.Errorf("resolve roster agent %q path relation: %w", key, err)
		}
		if relPath == ".." || strings.HasPrefix(relPath, ".."+string(filepath.Separator)) {
			return nil, fmt.Errorf("roster agent %q config_path escapes agents dir: %s", key, cfgPath)
		}
		ext := strings.ToLower(filepath.Ext(cfgPath))
		if ext != ".yaml" && ext != ".yml" {
			return nil, fmt.Errorf("roster agent %q config_path must be .yaml/.yml: %s", key, cfgPath)
		}
		if _, ok := seenPath[cfgPath]; ok {
			return nil, fmt.Errorf("duplicate roster config_path %s in %s", cfgPath, rosterPath)
		}
		seenPath[cfgPath] = struct{}{}
		files = append(files, cfgPath)
	}
	return files, nil
}

func inferGlobalAgentMode(id, role string) string {
	key := strings.ToLower(strings.TrimSpace(coalesce(role, id)))
	switch key {
	case "discovery-coordinator",
		"scanner-agent",
		"analysis-agent",
		"validation-coordinator",
		"pre-brand-agent",
		"business-research-agent",
		"lightweight-spec-agent",
		"spec-reviewer",
		"market-research-agent",
		"trend-research-agent",
		"factory-cto":
		return "factory"
	default:
		return "holding"
	}
}
