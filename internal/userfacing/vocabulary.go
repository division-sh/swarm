package userfacing

import (
	"regexp"
	"strings"
)

type VocabularyProfile string

const (
	ProfileOperatorOutput       VocabularyProfile = "operator_output"
	ProfileStatusDetail         VocabularyProfile = "status_detail"
	ProfileGeneratedConfig      VocabularyProfile = "generated_config"
	ProfilePublicConfigGuidance VocabularyProfile = "public_config_guidance"
)

type ForbiddenTerm struct {
	Text      string
	WholeWord bool
}

var globalPublicTerms = []ForbiddenTerm{
	{Text: "Wave 1"},
	{Text: "unified", WholeWord: true},
}

var profileTerms = map[VocabularyProfile][]ForbiddenTerm{
	ProfileOperatorOutput: {},
	ProfileStatusDetail: {
		{Text: "test_"},
		{Text: "heuristic"},
		{Text: "quiescence"},
		{Text: "runtime_state"},
		{Text: "session_scope"},
		{Text: "blocking_layer"},
		{Text: "blocking_reason"},
		{Text: "current_session_ref"},
		{Text: "last_turn_ref"},
		{Text: "last_tool_outcome"},
		{Text: "next_cursor"},
	},
	ProfileGeneratedConfig: {
		{Text: "config.example.yaml"},
		{Text: "runtime-config.example.yaml"},
		{Text: "llm.claude_cli.retries"},
		{Text: "\n#   password:"},
		{Text: "Generated from cmd/swarm unified config metadata"},
		{Text: "Elevated/local-only keys"},
		{Text: "claude-3-5-sonnet"},
		{Text: ".swarm/dev.db"},
	},
	ProfilePublicConfigGuidance: {
		{Text: "config.example.yaml"},
		{Text: "runtime-config.example.yaml"},
		{Text: "Optional path to Swarm runtime config"},
		{Text: "Path to Swarm runtime config"},
		{Text: "unified swarm.yaml/runtime config"},
		{Text: "the runtime config"},
		{Text: "runtime config is required"},
		{Text: "load runtime config through"},
		{Text: "workspace.docker_bin in runtime config"},
		{Text: "CLI config file"},
		{Text: "Path to unified"},
		{Text: "Optional path to unified"},
		{Text: "unified Swarm config"},
		{Text: "unified non-secret"},
	},
}

func ForbiddenTerms(profile VocabularyProfile) []ForbiddenTerm {
	terms := make([]ForbiddenTerm, 0, len(globalPublicTerms)+len(profileTerms[profile]))
	terms = append(terms, globalPublicTerms...)
	terms = append(terms, profileTerms[profile]...)
	return terms
}

func FindForbidden(profile VocabularyProfile, text string) []string {
	found := []string{}
	for _, term := range ForbiddenTerms(profile) {
		if matchesForbiddenTerm(text, term) {
			found = append(found, term.Text)
		}
	}
	return found
}

func matchesForbiddenTerm(text string, term ForbiddenTerm) bool {
	if !term.WholeWord {
		return strings.Contains(strings.ToLower(text), strings.ToLower(term.Text))
	}
	pattern := `(?i)(^|[^[:alnum:]_])` + regexp.QuoteMeta(term.Text) + `([^[:alnum:]_]|$)`
	return regexp.MustCompile(pattern).MatchString(text)
}
