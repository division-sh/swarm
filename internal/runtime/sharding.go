package runtime

import (
	"encoding/json"
	"fmt"
	"strings"
)

func toStringList(raw any) []string {
	switch t := raw.(type) {
	case nil:
		return nil
	case []string:
		out := make([]string, 0, len(t))
		for _, item := range t {
			item = strings.TrimSpace(item)
			if item != "" {
				out = append(out, item)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			s := strings.TrimSpace(fmt.Sprintf("%v", item))
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	case string:
		text := strings.TrimSpace(t)
		if text == "" {
			return nil
		}
		if strings.HasPrefix(text, "[") {
			var items []string
			if err := json.Unmarshal([]byte(text), &items); err == nil {
				return toStringList(items)
			}
		}
		parts := strings.Split(text, ",")
		out := make([]string, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part != "" {
				out = append(out, part)
			}
		}
		return out
	default:
		return []string{strings.TrimSpace(fmt.Sprintf("%v", raw))}
	}
}

func absInt(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
