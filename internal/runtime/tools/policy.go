package tools

import "strings"

func IsUniversal(toolName string) bool {
	toolName = strings.TrimSpace(toolName)
	if toolName == "" {
		return false
	}
	descriptor, ok := hitlToolDescriptorForName(toolName)
	return ok && descriptor.grant == hitlGrantUniversal
}
