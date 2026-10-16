package failures

const agentToolOutcomeUnknown = "result unknown: the call may or may not have reached the system; it will not be retried automatically"

// AgentToolFailureValue is a presentation, not a platform.failure/v1 envelope.
// Machine transports and durable evidence continue to use EnvelopeValue.
func AgentToolFailureValue(envelope Envelope) (map[string]any, error) {
	value, err := EnvelopeValue(envelope)
	if err != nil {
		return nil, err
	}
	if envelope.Class == ClassOutcomeUncertain {
		delete(value, "schema_version")
		delete(value, "remediation")
		value["presentation"] = "agent_tool_failure"
		value["message"] = agentToolOutcomeUnknown
	}
	return value, nil
}

// AgentToolFailureText renders tool-result content without changing its typed
// runtimeError carrier or operator-facing envelope.
func AgentToolFailureText(envelope Envelope) (string, error) {
	if err := ValidateEnvelope(envelope); err != nil {
		return "", err
	}
	if envelope.Class == ClassOutcomeUncertain {
		return agentToolOutcomeUnknown, nil
	}
	return envelope.Message, nil
}
