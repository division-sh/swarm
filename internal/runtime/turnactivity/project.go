package turnactivity

import (
	"errors"
	"fmt"
	"strings"

	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
)

type Activity struct {
	Kind         string
	EventID      string
	EventType    string
	ToolName     string
	ToolUseID    string
	Text         string
	OK           *bool
	BlockOrdinal int
}

func Project(blocks []runtimellm.TurnBlock, parseOK bool) ([]Activity, string, string, error) {
	activity := make([]Activity, 0, len(blocks))
	assistantOutput := ""
	outcome := ""
	for ordinal, block := range blocks {
		switch strings.TrimSpace(block.Kind) {
		case "dispatch":
			data, ok, err := block.DispatchData()
			if err != nil || !ok {
				return nil, "", "", fmt.Errorf("decode public dispatch activity: %w", firstError(err, errors.New("dispatch data is required")))
			}
			activity = append(activity, Activity{Kind: "dispatch", EventID: strings.TrimSpace(data.TriggerEventID), EventType: strings.TrimSpace(data.TriggerEventType), BlockOrdinal: ordinal})
		case "tool_use":
			name := strings.TrimSpace(block.ToolName)
			if name == "" {
				return nil, "", "", errors.New("decode public tool activity: tool_name is required")
			}
			link, _, err := block.ToolLinkData()
			if err != nil {
				return nil, "", "", fmt.Errorf("decode public tool activity: %w", err)
			}
			activity = append(activity, Activity{Kind: "tool", ToolName: name, ToolUseID: strings.TrimSpace(link.ToolUseID), BlockOrdinal: ordinal})
		case "tool_result":
			name := strings.TrimSpace(block.ToolName)
			if name == "" {
				return nil, "", "", errors.New("decode public tool result activity: tool_name is required")
			}
			link, _, err := block.ToolLinkData()
			if err != nil {
				return nil, "", "", fmt.Errorf("decode public tool result activity: %w", err)
			}
			ok := parseOK
			activity = append(activity, Activity{Kind: "tool_result", ToolName: name, ToolUseID: strings.TrimSpace(link.ToolUseID), OK: &ok, BlockOrdinal: ordinal})
		case "publish":
			data, ok, err := block.PublishData()
			if err != nil || !ok {
				return nil, "", "", fmt.Errorf("decode public publish activity: %w", firstError(err, errors.New("publish data is required")))
			}
			activity = append(activity, Activity{Kind: "publish", EventID: strings.TrimSpace(data.EventID), EventType: strings.TrimSpace(block.Title), BlockOrdinal: ordinal})
		case "assistant_text":
			if text := strings.TrimSpace(block.Text); text != "" {
				assistantOutput = text
				activity = append(activity, Activity{Kind: "output", Text: text, BlockOrdinal: ordinal})
			}
		case "outcome":
			if text := strings.TrimSpace(block.Text); text != "" {
				outcome = text
			}
		case "turn_summary":
			summary, ok, err := block.TurnSummaryData()
			if err != nil || !ok {
				return nil, "", "", fmt.Errorf("decode public turn summary: %w", firstError(err, errors.New("turn summary data is required")))
			}
			if assistantOutput == "" {
				assistantOutput = strings.TrimSpace(summary.AssistantVisibleOutput)
			}
			if outcome == "" {
				outcome = strings.TrimSpace(summary.Outcome)
			}
		case "reasoning", "progress", "runtime_log":
		default:
		}
	}
	return activity, assistantOutput, outcome, nil
}

func firstError(err, fallback error) error {
	if err != nil {
		return err
	}
	return fallback
}
