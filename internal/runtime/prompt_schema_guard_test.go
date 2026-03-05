package runtime

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

func TestPromptSchemaGuard_EmitFieldListsMatchEventSchemas(t *testing.T) {
	t.Helper()
	ensureEventSchemaRegistry()

	repoRoot := contractComplianceRepoRoot(t)
	promptsDir := filepath.Join(repoRoot, "contracts", "prompts")

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
		tc := tc
		t.Run(tc.promptFile, func(t *testing.T) {
			path := filepath.Join(promptsDir, tc.promptFile)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read %s: %v", path, err)
			}
			text := string(raw)

			eventType, ok := eventTypeFromEmitToolName(tc.emitTool)
			if !ok {
				t.Fatalf("unknown emit tool %s", tc.emitTool)
			}
			schema := schemaForEventType(eventType)
			props := schemaProperties(schema.Schema["properties"])
			if len(props) == 0 {
				t.Fatalf("schema for %s has no properties", eventType)
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
					t.Fatalf("prompt %s: fields not in %s schema: %v", tc.promptFile, eventType, invalid)
				}
			}

			for _, required := range tc.requiredTopLevel {
				if !promptMentionsField(text, fields, required) {
					t.Fatalf("prompt %s: missing required top-level field %q for %s", tc.promptFile, required, tc.emitTool)
				}
			}

			for _, forbidden := range tc.forbiddenTokens {
				if promptContainsToken(text, forbidden) {
					t.Fatalf("prompt %s: contains forbidden legacy token %q", tc.promptFile, forbidden)
				}
			}
		})
	}
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
	return uniqueNonEmpty(fields)
}

func guardContainsString(in []string, want string) bool {
	want = strings.TrimSpace(want)
	for _, item := range in {
		if strings.TrimSpace(item) == want {
			return true
		}
	}
	return false
}

func promptMentionsField(promptText string, parsedFields []string, field string) bool {
	if guardContainsString(parsedFields, field) {
		return true
	}
	return promptContainsToken(promptText, field)
}

func promptContainsToken(text, token string) bool {
	token = strings.TrimSpace(token)
	if token == "" {
		return false
	}
	pattern := regexp.MustCompile(`\b` + regexp.QuoteMeta(token) + `\b`)
	return pattern.FindStringIndex(text) != nil
}
