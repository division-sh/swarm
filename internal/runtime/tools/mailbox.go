package tools

import (
	"fmt"
	"strings"

	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
)

func NormalizeMailboxType(raw string) (string, error) {
	t := strings.ToLower(strings.TrimSpace(raw))
	t = strings.ReplaceAll(t, "-", "_")
	t = strings.ReplaceAll(t, ".", "_")
	if t == "" {
		return "", fmt.Errorf("invalid mailbox type %q", raw)
	}
	if err := decisioncard.ValidateNoticeShape(t, nil); err != nil {
		return "", err
	}
	return t, nil
}

func NormalizeMailboxPriority(raw string) (string, error) {
	p := strings.ToLower(strings.TrimSpace(raw))
	switch p {
	case "", "normal":
		return "normal", nil
	case "medium":
		return "normal", nil
	case "urgent":
		return "high", nil
	case "low", "high", "critical":
		return p, nil
	default:
		return "", fmt.Errorf("invalid mailbox priority %q", raw)
	}
}
