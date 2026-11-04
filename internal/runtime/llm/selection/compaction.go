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

// ContextCompactionBudget returns only documented concrete-route capacities.
// The Responses reservation is made explicit in its ordinary request encoder;
// DeepSeek's unmodified request uses the documented default thinking budget.
func ContextCompactionBudget(backend, destination, model string) (window, output int) {
	if backend == BackendAnthropic && destination == "https://api.anthropic.com/v1/messages" {
		switch model {
		case "claude-sonnet-4-5", "claude-sonnet-4-5-20250929", "claude-haiku-4-5", "claude-haiku-4-5-20251001":
			// https://platform.claude.com/docs/en/models/sonnet-4-5/overview
			// https://platform.claude.com/docs/en/models/haiku-4-5/overview
			return 200000, 1024
		case "claude-3-haiku-20240307", "claude-3-5-haiku", "claude-3-5-haiku-20241022", "claude-3-5-sonnet", "claude-3-5-sonnet-20240620", "claude-3-5-sonnet-20241022", "claude-3-opus", "claude-3-opus-20240229":
			return 200000, 1024
		case "claude-opus-4-6", "claude-sonnet-4-6", "claude-opus-4-7", "claude-opus-4-8", "claude-fable-5-1", "claude-opus-5-5", "claude-sonnet-5-5", "claude-haiku-5-5",
			"claude-mythos-5-1", "claude-fable-5", "claude-mythos-5", "claude-mythos-preview", "claude-opus-5", "claude-sonnet-5":
			// https://platform.claude.com/docs/en/build-with-claude/context-windows
			return 1000000, 1024
		}
	}
	if backend == BackendOpenAIResponses && destination == "https://api.openai.com/v1/responses" {
		switch model {
		case "gpt-5.4", "gpt-5.4-2026-03-05", "gpt-5.5", "gpt-5.5-2026-04-23":
			// https://developers.openai.com/api/docs/models/gpt-5.4
			// https://developers.openai.com/api/docs/models/gpt-5.5
			return 1050000, 128000
		case "gpt-5.4-mini", "gpt-5.4-mini-2026-03-17", "gpt-5.4-nano", "gpt-5.4-nano-2026-03-17":
			// https://developers.openai.com/api/docs/models/gpt-5.4-mini
			// https://developers.openai.com/api/docs/models/gpt-5.4-nano
			return 400000, 128000
		}
	}
	if backend == BackendOpenAICompatible && (destination == "https://api.deepseek.com/chat/completions" || destination == "https://api.deepseek.com/v1/chat/completions") {
		switch model {
		case "deepseek-flash", "deepseek-v4-pro":
			// https://api-docs.deepseek.com/quick_start/pricing/
			// https://api-docs.deepseek.com/api/create-chat-completion/
			return 1000000, 65536
		}
	}
	return 0, 0
}
