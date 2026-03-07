package llm

import "strings"

func isSessionInUseError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "session id") && strings.Contains(msg, "already in use")
}

func isSessionNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "no conversation found with session id") {
		return true
	}
	if strings.Contains(msg, "conversation not found") && strings.Contains(msg, "session") {
		return true
	}
	if strings.Contains(msg, "session") && strings.Contains(msg, "not found") {
		return true
	}
	return false
}

func shouldRotateSessionOnCLIError(err error) bool {
	return isSessionInUseError(err) || isSessionNotFoundError(err)
}

func rotateSessionRetryReason(err error) string {
	switch {
	case isSessionInUseError(err):
		return "session in use"
	case isSessionNotFoundError(err):
		return "session not found"
	default:
		return "runtime recovery"
	}
}

func isUnsupportedCLIFlagError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if !(strings.Contains(msg, "unknown option") || strings.Contains(msg, "unknown flag") || strings.Contains(msg, "unrecognized option")) {
		return false
	}
	return strings.Contains(msg, "--system-prompt") || strings.Contains(msg, "--tools")
}

func isPromptArgRequiredError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "input must be provided either through stdin or as a prompt argument when using --print")
}
