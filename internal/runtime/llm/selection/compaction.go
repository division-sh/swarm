package selection

// NativeCompactionMethod binds documented compaction to the actual route. A
// compatible protocol or a model-name prefix is not a capability declaration.
func NativeCompactionMethod(backend, destination, model string) string {
	if backend == BackendClaudeCLI {
		return "provider_owned"
	}
	if model == "" {
		return ""
	}
	switch backend {
	case BackendAnthropic:
		if destination != "https://api.anthropic.com/v1/messages" {
			return ""
		}
		// https://platform.claude.com/docs/en/build-with-claude/compaction-on-demand
		switch model {
		case "claude-fable-5-1", "claude-mythos-5-1", "claude-fable-5", "claude-mythos-5", "claude-mythos-preview",
			"claude-opus-5-5", "claude-opus-5", "claude-sonnet-5-5", "claude-sonnet-5", "claude-haiku-5-5",
			"claude-opus-4-8", "claude-opus-4-7", "claude-opus-4-6", "claude-sonnet-4-6":
			return "anthropic_native"
		}
	case BackendOpenAIResponses:
		// Bind the documented concrete models, not arbitrary models on a
		// compatible endpoint or a model-name family inferred from a prefix.
		// https://developers.openai.com/api/reference/resources/responses/methods/compact
		if destination != "https://api.openai.com/v1/responses" {
			return ""
		}
		switch model {
		case "gpt-6-astra", "gpt-5.5", "gpt-5.5-2026-04-23", "gpt-5.4", "gpt-5.4-2026-03-05",
			"gpt-5.4-mini", "gpt-5.4-mini-2026-03-17", "gpt-5.4-nano", "gpt-5.4-nano-2026-03-17":
			return "openai_native"
		}
	}
	return ""
}
