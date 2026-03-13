package tools

import (
	"fmt"
	"strings"
)

func NormalizeMailboxType(raw string) (string, error) {
	t := strings.ToLower(strings.TrimSpace(raw))
	t = strings.ReplaceAll(t, "-", "_")
	t = strings.ReplaceAll(t, ".", "_")
	if t == "" {
		return "", fmt.Errorf("invalid mailbox type %q", raw)
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
