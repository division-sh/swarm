package llm

import (
	"encoding/json"
	"fmt"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/runtime/core/toolresultpolicy"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

const providerResultContentKey = "provider_content"

func renderToolOutput(value any, emit bool) (string, error) {
	if emit {
		return "ok", nil
	}
	if text, ok := value.(string); ok {
		return truncateProviderToolOutput(text), nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return truncateProviderToolOutput(string(raw)), nil
}

func renderToolFailure(failure runtimefailures.Envelope) (string, error) {
	detail, err := runtimefailures.AgentToolFailureText(failure)
	if err != nil {
		return "", err
	}
	return truncateProviderToolOutput(fmt.Sprintf("error: %s: %s", failure.Detail.Code, detail)), nil
}

func truncateProviderToolOutput(text string) string {
	return truncateProviderOutput(text, toolresultpolicy.MaxInlineToolResultBytes)
}

func truncateProviderOutput(text string, limit int) string {
	const marker = "\n[... tool output truncated ...]\n"
	if len(text) <= limit {
		return text
	}
	if limit < len(marker) {
		return marker
	}
	head := (limit - len(marker)) / 2
	tail := len(text) - (limit - len(marker) - head)
	for head > 0 && !utf8.RuneStart(text[head]) {
		head--
	}
	for tail < len(text) && !utf8.RuneStart(text[tail]) {
		tail++
	}
	return text[:head] + marker + text[tail:]
}

// The provider copy shares the existing batch budget with the typed receipts;
// it must never force a complete receipt to become a preview or raise a limit.
func attachProviderToolContents(entries []map[string]any, contents []string, limit int) ([]byte, error) {
	raw, err := json.Marshal(entries)
	if err != nil {
		return nil, err
	}
	remaining := limit - len(raw)
	for i, content := range contents {
		const fieldOverhead = len(`,"provider_content":`)
		budget := remaining/(len(contents)-i) - fieldOverhead
		low, high := 0, len(content)
		var rendered string
		for low <= high {
			mid := low + (high-low)/2
			candidate := truncateProviderOutput(content, mid)
			encoded, err := json.Marshal(candidate)
			if err != nil {
				return nil, err
			}
			if len(encoded) > budget {
				high = mid - 1
			} else {
				rendered = candidate
				low = mid + 1
			}
		}
		encoded, _ := json.Marshal(rendered)
		if len(encoded) > budget || (rendered == "" && content != "") {
			return nil, runtimefailures.New(runtimefailures.ClassDataLimitExceeded, "aggregate_tool_result_limit_exceeded", "llm-conversation", "deliver_tool_results", map[string]any{"limit": limit})
		}
		entries[i][providerResultContentKey] = rendered
		remaining -= fieldOverhead + len(encoded)
	}
	return json.Marshal(entries)
}
