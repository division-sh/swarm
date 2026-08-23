package tools

import (
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
)

func canonicalRuntimeToolInput(name string, input any) any {
	name = normalizeNativeToolName(name)
	if name == "" || toolKindPolicy(name) == toolcapabilities.KindEmit {
		return input
	}
	var payload map[string]any
	if err := decodeToolInput(input, &payload); err != nil || payload == nil {
		return input
	}

	switch name {
	case "read_file":
		if strings.TrimSpace(asString(payload["path"])) == "" {
			if filePath := strings.TrimSpace(asString(payload["file_path"])); filePath != "" {
				payload["path"] = filePath
			}
		}
	case "write_file":
		if strings.TrimSpace(asString(payload["path"])) == "" {
			if filePath := strings.TrimSpace(asString(payload["file_path"])); filePath != "" {
				payload["path"] = filePath
			}
		}
	}
	return payload
}
