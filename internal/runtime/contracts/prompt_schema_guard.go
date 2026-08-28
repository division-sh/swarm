package contracts

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/stringsutil"
)

type PromptSchemaGuardFinding struct {
	CheckID  string
	Severity string
	Message  string
	Location string
}

func ValidatePromptSchemaGuards(repoRoot string) error {
	bundle, err := LoadWorkflowContractBundle(repoRoot)
	if err != nil {
		return err
	}
	return ValidatePromptSchemaGuardsForBundle(bundle)
}

func ValidatePromptSchemaGuardsForBundle(bundle *WorkflowContractBundle) error {
	findings, err := PromptSchemaGuardFindingsForBundle(bundle)
	if err != nil {
		return err
	}
	if len(findings) == 0 {
		return nil
	}
	return fmt.Errorf("%s [%s] %s", strings.TrimSpace(findings[0].CheckID), strings.TrimSpace(findings[0].Location), strings.TrimSpace(findings[0].Message))
}

func PromptSchemaGuardFindingsForBundle(bundle *WorkflowContractBundle) ([]PromptSchemaGuardFinding, error) {
	if bundle == nil {
		return nil, fmt.Errorf("workflow contract bundle is required")
	}
	cases := DerivePromptSchemaGuards(bundle)
	findings := make([]PromptSchemaGuardFinding, 0)

	for _, tc := range cases {
		path := tc.IntentSource
		text := tc.IntentContent

		eventType := strings.ReplaceAll(strings.TrimPrefix(tc.EmitTool, "emit_"), "_", ".")
		schema, _, ok := EventSchemaForFlowEvent(bundle, tc.FlowID, eventType)
		if !ok {
			return nil, fmt.Errorf("unknown emit tool %s", tc.EmitTool)
		}
		props := schemaProperties(schema.Schema["properties"])
		if len(props) == 0 {
			return nil, fmt.Errorf("schema for %s has no properties", eventType)
		}

		fields := extractPromptEmitTopLevelFields(text, tc.EmitTool)
		if len(fields) > 0 {
			invalid := make([]string, 0)
			for _, f := range fields {
				if _, ok := props[f]; !ok {
					invalid = append(invalid, f)
				}
			}
			if len(invalid) > 0 {
				sort.Strings(invalid)
				findings = append(findings, PromptSchemaGuardFinding{
					CheckID:  "agent_prompt_lint_structural",
					Severity: "hard_invalidity",
					Message:  fmt.Sprintf("intent %s: fields not in %s schema: %v", tc.IntentSource, eventType, invalid),
					Location: path,
				})
			}
		}

		for _, required := range tc.RequiredTopLevel {
			if !promptMentionsField(text, fields, required) {
				findings = append(findings, PromptSchemaGuardFinding{
					CheckID:  "agent_prompt_lint_structural",
					Severity: "hard_invalidity",
					Message:  fmt.Sprintf("intent %s: missing required top-level field %q for %s", tc.IntentSource, required, tc.EmitTool),
					Location: path,
				})
			}
		}

		for _, forbidden := range tc.ForbiddenTokens {
			if promptContainsToken(text, forbidden) {
				findings = append(findings, PromptSchemaGuardFinding{
					CheckID:  "agent_prompt_lint_structural",
					Severity: "hard_invalidity",
					Message:  fmt.Sprintf("intent %s: contains forbidden legacy token %q", tc.IntentSource, forbidden),
					Location: path,
				})
			}
		}
	}
	return findings, nil
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
	return stringsutil.Unique(fields)
}

func promptMentionsField(promptText string, parsedFields []string, field string) bool {
	if stringsutil.ContainsTrimmed(parsedFields, field) {
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

func schemaProperties(raw any) map[string]any {
	props, _ := raw.(map[string]any)
	return props
}
