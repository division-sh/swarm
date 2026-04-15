package toolidentity

import "strings"

const RuntimeToolsMCPPrefix = "mcp__runtime-tools__"

func CanonicalName(name string) string {
	name = strings.TrimSpace(name)
	if strings.HasPrefix(name, RuntimeToolsMCPPrefix) {
		name = strings.TrimPrefix(name, RuntimeToolsMCPPrefix)
	}
	switch name {
	case "Bash":
		return "bash"
	case "WebSearch", "WebFetch":
		return "web_search"
	case "Read":
		return "read_file"
	case "Write", "Edit":
		return "write_file"
	default:
		return name
	}
}

func IsEmitToolName(name string) bool {
	return strings.HasPrefix(CanonicalName(name), "emit_")
}
