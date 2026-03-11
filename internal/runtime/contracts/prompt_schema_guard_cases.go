package contracts

type PromptSchemaGuardCase struct {
	PromptFile       string
	EmitTool         string
	RequiredTopLevel []string
	ForbiddenTokens  []string
}

func PromptSchemaGuards() []PromptSchemaGuardCase {
	return []PromptSchemaGuardCase{
		{
			PromptFile:       "market-research-agent.md",
			EmitTool:         "emit_category_assessed",
			RequiredTopLevel: []string{"opportunity_name", "preliminary_icp", "build_sketch", "evidence", "opportunity_hypothesis", "opportunity_pattern", "signal_sources", "required_capabilities"},
			ForbiddenTokens:  []string{"automation_micro", "market_intersection", "urgency"},
		},
		{
			PromptFile:       "market-research-agent.corpus.md",
			EmitTool:         "emit_category_assessed",
			RequiredTopLevel: []string{"opportunity_name", "preliminary_icp", "build_sketch", "evidence", "opportunity_hypothesis", "opportunity_pattern", "signal_sources", "required_capabilities"},
			ForbiddenTokens:  []string{"automation_micro", "market_intersection", "urgency"},
		},
		{
			PromptFile:       "trend-research-agent.md",
			EmitTool:         "emit_trend_identified",
			RequiredTopLevel: []string{"opportunity_name", "preliminary_icp", "build_sketch", "evidence", "trend_description", "opportunity_hypothesis", "geographic_scope"},
			ForbiddenTokens:  []string{"market_intersection", "urgency"},
		},
	}
}
