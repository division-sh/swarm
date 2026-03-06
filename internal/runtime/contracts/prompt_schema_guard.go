package contracts

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

func ValidatePromptSchemaGuards(repoRoot string) error {
	promptsDir := filepath.Join(repoRoot, "contracts", "prompts")
	schemas := EventSchemaRegistry()

	type guardCase struct {
		promptFile       string
		emitTool         string
		requiredTopLevel []string
		forbiddenTokens  []string
	}

	cases := []guardCase{
		{
			promptFile:       "market-research-agent.md",
			emitTool:         "emit_category_assessed",
			requiredTopLevel: []string{"opportunity_name", "preliminary_icp", "build_sketch", "evidence", "opportunity_hypothesis", "opportunity_pattern", "signal_sources", "required_capabilities"},
			forbiddenTokens:  []string{"automation_micro", "market_intersection", "urgency"},
		},
		{
			promptFile:       "market-research-agent.corpus.md",
			emitTool:         "emit_category_assessed",
			requiredTopLevel: []string{"opportunity_name", "preliminary_icp", "build_sketch", "evidence", "opportunity_hypothesis", "opportunity_pattern", "signal_sources", "required_capabilities"},
			forbiddenTokens:  []string{"automation_micro", "market_intersection", "urgency"},
		},
		{
			promptFile:       "trend-research-agent.md",
			emitTool:         "emit_trend_identified",
			requiredTopLevel: []string{"opportunity_name", "preliminary_icp", "build_sketch", "evidence", "trend_description", "opportunity_hypothesis", "geographic_scope"},
			forbiddenTokens:  []string{"market_intersection", "urgency"},
		},
	}

	for _, tc := range cases {
		path := filepath.Join(promptsDir, tc.promptFile)
		raw, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		text := string(raw)

		eventType := strings.ReplaceAll(strings.TrimPrefix(tc.emitTool, "emit_"), "_", ".")
		schema, ok := schemas[eventType]
		if !ok {
			return fmt.Errorf("unknown emit tool %s", tc.emitTool)
		}
		props := schemaProperties(schema.Schema["properties"])
		if len(props) == 0 {
			return fmt.Errorf("schema for %s has no properties", eventType)
		}

		fields := extractPromptEmitTopLevelFields(text, tc.emitTool)
		if len(fields) > 0 {
			invalid := make([]string, 0)
			for _, f := range fields {
				if _, ok := props[f]; !ok {
					invalid = append(invalid, f)
				}
			}
			if len(invalid) > 0 {
				sort.Strings(invalid)
				return fmt.Errorf("prompt %s: fields not in %s schema: %v", tc.promptFile, eventType, invalid)
			}
		}

		for _, required := range tc.requiredTopLevel {
			if !promptMentionsField(text, fields, required) {
				return fmt.Errorf("prompt %s: missing required top-level field %q for %s", tc.promptFile, required, tc.emitTool)
			}
		}

		for _, forbidden := range tc.forbiddenTokens {
			if promptContainsToken(text, forbidden) {
				return fmt.Errorf("prompt %s: contains forbidden legacy token %q", tc.promptFile, forbidden)
			}
		}
	}
	return nil
}

var promptEmitFieldBulletPattern = regexp.MustCompile(`^\s*-\s*([a-zA-Z0-9_]+)\s*:`)

func extractPromptEmitTopLevelFields(promptText, emitTool string) []string {
	lines := strings.Split(promptText, "\n")
	tool := strings.ToLower(strings.TrimSpace(emitTool))
	if tool == "" {
		return nil
	}

	collecting := false
	fields := make([]string, 0, 16)
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		lower := strings.ToLower(line)

		if !collecting && strings.Contains(lower, tool) && strings.Contains(lower, "with") {
			collecting = true
			continue
		}
		if !collecting {
			continue
		}

		if strings.HasPrefix(line, "===") {
			break
		}
		if strings.HasPrefix(lower, "when you ") || strings.HasPrefix(lower, "for subcategories") || strings.HasPrefix(lower, "this tells the runtime") {
			break
		}

		m := promptEmitFieldBulletPattern.FindStringSubmatch(raw)
		if len(m) >= 2 {
			field := strings.TrimSpace(m[1])
			if field != "" {
				fields = append(fields, field)
			}
		}
	}
	return uniquePromptStrings(fields)
}

func promptMentionsField(promptText string, parsedFields []string, field string) bool {
	if containsExactPrompt(parsedFields, field) {
		return true
	}
	return promptContainsToken(promptText, field)
}

func containsExactPrompt(in []string, want string) bool {
	want = strings.TrimSpace(want)
	for _, item := range in {
		if strings.TrimSpace(item) == want {
			return true
		}
	}
	return false
}

func promptContainsToken(text, token string) bool {
	token = strings.TrimSpace(token)
	if token == "" {
		return false
	}
	pattern := regexp.MustCompile(`\b` + regexp.QuoteMeta(token) + `\b`)
	return pattern.FindStringIndex(text) != nil
}

func uniquePromptStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func schemaProperties(raw any) map[string]any {
	props, _ := raw.(map[string]any)
	return props
}
