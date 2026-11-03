package effects

// StartupRecoveryCounts describes existing recovery decisions, not possession
// or permission to execute them.
type StartupRecoveryCounts struct {
	ProviderTurnsToResume     int `json:"provider_turns_to_resume"`
	ProviderTurnsToDeadLetter int `json:"provider_turns_to_dead_letter"`
	HTTPToolCallsUnknown      int `json:"http_tool_calls_outcome_unknown"`
}
