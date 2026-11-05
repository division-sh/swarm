package identity

import "strings"

const RuntimeToolsMCPPrefix = "mcp__runtime-tools__"

func CanonicalToolName(name string) string {
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

// ToolDeclarationNames preserves each identity traversed before runtime routing.
func ToolDeclarationNames(name string) []string {
	requested := strings.TrimSpace(name)
	bare := strings.TrimPrefix(requested, RuntimeToolsMCPPrefix)
	canonical := CanonicalToolName(requested)
	result := []string{}
	for _, candidate := range []string{requested, bare, canonical} {
		if candidate == "" {
			continue
		}
		duplicate := false
		for _, previous := range result {
			duplicate = duplicate || previous == candidate
		}
		if !duplicate {
			result = append(result, candidate)
		}
	}
	return result
}

func IsEmitToolName(name string) bool {
	return strings.HasPrefix(CanonicalToolName(name), "emit_")
}
