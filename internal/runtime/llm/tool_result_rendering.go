package llm

import (
	"encoding/json"
	"fmt"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/runtime/core/toolresultpolicy"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

const providerResultContentKey = "provider_content"

const providerTruncationMarker = "\n[... tool output truncated ...]\n"

type providerToolContent struct {
	text     string
	followUp string
}

func renderToolOutput(value, projected any, emit bool) (providerToolContent, error) {
	if emit {
		return providerToolContent{text: "ok"}, nil
	}
	content := providerToolContent{}
	if receipt, ok := projected.(map[string]any); ok && receipt["truncated"] == true && receipt["follow_up"] != nil {
		raw, err := json.Marshal(receipt["follow_up"])
		if err != nil {
			return content, err
		}
		content.followUp = string(raw)
	}
	if text, ok := value.(string); ok {
		content.text = text
	} else {
		raw, err := json.Marshal(value)
		if err != nil {
			return content, err
		}
		content.text = string(raw)
	}
	return content, nil
}

func (content providerToolContent) render(limit int) string {
	if content.followUp == "" {
		return truncateProviderOutput(content.text, limit)
	}
	// Retrieval instructions are indivisible; only the original output is cut.
	marker := providerTruncationMarker + "follow_up: " + content.followUp + "\n"
	if len(content.text)+len(marker) <= limit {
		return content.text + marker
	}
	return truncateProviderOutputWithMarker(content.text, limit, marker)
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
	if len(text) <= limit {
		return text
	}
	return truncateProviderOutputWithMarker(text, limit, providerTruncationMarker)
}

func truncateProviderOutputWithMarker(text string, limit int, marker string) string {
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
func attachProviderToolContents(entries []map[string]any, contents []providerToolContent, limit int) ([]byte, error) {
	raw, err := json.Marshal(entries)
	if err != nil {
		return nil, err
	}
	remaining := limit - len(raw)
	for i, content := range contents {
		const fieldOverhead = len(`,"provider_content":`)
		budget := remaining/(len(contents)-i) - fieldOverhead
		low, high := 0, toolresultpolicy.MaxInlineToolResultBytes
		var rendered string
		for low <= high {
			mid := low + (high-low)/2
			candidate := content.render(mid)
			if len(candidate) > mid {
				low = mid + 1
				continue
			}
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
		if len(encoded) > budget || (rendered == "" && content.text != "") {
			return nil, runtimefailures.New(runtimefailures.ClassDataLimitExceeded, "aggregate_tool_result_limit_exceeded", "llm-conversation", "deliver_tool_results", map[string]any{
				"limit_kind": "provider_tool_content_bytes",
				"limit":      min(budget, toolresultpolicy.MaxInlineToolResultBytes),
				"actual":     len(content.text) + len(content.followUp),
			})
		}
		entries[i][providerResultContentKey] = rendered
		remaining -= fieldOverhead + len(encoded)
	}
	return json.Marshal(entries)
}
