package llm

import (
	"encoding/json"
	"strings"

	runtimebus "swarm/internal/runtime/bus"
)

const turnSummaryBlockKind = "turn_summary"

func BuildTurnBlocks(rec AgentTurnRecord) []TurnBlock {
	if len(rec.TurnBlocks) > 0 {
		return normalizeTurnBlocks(rec.TurnBlocks)
	}
	blocks := make([]TurnBlock, 0, 8)
	if dispatch := buildDispatchBlock(rec); dispatch.Kind != "" {
		blocks = append(blocks, dispatch)
	}
	blocks = append(blocks, buildFlightRecorderBlocks(rec)...)
	if len(rec.ResponseRaw) == 0 {
		return normalizeTurnBlocks(blocks)
	}
	return normalizeTurnBlocks(append(blocks, parseTurnBlocksFromRaw(rec.ResponseRaw)...))
}

func buildDispatchBlock(rec AgentTurnRecord) TurnBlock {
	if strings.TrimSpace(rec.TriggerEventType) == "" &&
		strings.TrimSpace(rec.TriggerEventID) == "" &&
		strings.TrimSpace(rec.EntityID) == "" &&
		strings.TrimSpace(rec.TaskID) == "" {
		return TurnBlock{}
	}
	return newDispatchTurnBlock(strings.TrimSpace(rec.TriggerEventType), TurnBlockDispatchData{
		TriggerEventID:   strings.TrimSpace(rec.TriggerEventID),
		TriggerEventType: strings.TrimSpace(rec.TriggerEventType),
		EntityID:         strings.TrimSpace(rec.EntityID),
		TaskID:           strings.TrimSpace(rec.TaskID),
	})
}

func buildFlightRecorderBlocks(rec AgentTurnRecord) []TurnBlock {
	if len(rec.FlightRecorder) == 0 {
		return nil
	}
	blocks := make([]TurnBlock, 0, len(rec.FlightRecorder))
	for _, entry := range rec.FlightRecorder {
		switch strings.TrimSpace(entry.Kind) {
		case "publish":
			if block, ok := buildPublishBlock(entry); ok {
				blocks = append(blocks, block)
			}
		case "runtime_log":
			if block, ok := buildRuntimeLogBlock(entry); ok {
				blocks = append(blocks, block)
			}
		}
	}
	return blocks
}

func buildPublishBlock(entry runtimebus.FlightRecorderEntry) (TurnBlock, bool) {
	eventType := strings.TrimSpace(entry.EventType)
	if eventType == "" {
		return TurnBlock{}, false
	}
	return newPublishTurnBlock(eventType, TurnBlockPublishData{
		EventID:                     strings.TrimSpace(entry.EventID),
		EntityID:                    strings.TrimSpace(entry.EntityID),
		ParentEventID:               strings.TrimSpace(entry.ParentEventID),
		RoutedRecipients:            append([]runtimebus.PublishDiagnosticRecipient(nil), entry.RoutedRecipients...),
		RoutedRecipientsCount:       len(entry.RoutedRecipients),
		SubscriptionRecipients:      append([]string(nil), entry.SubscriptionRecipients...),
		SubscriptionRecipientsCount: len(entry.SubscriptionRecipients),
	}), true
}

func buildRuntimeLogBlock(entry runtimebus.FlightRecorderEntry) (TurnBlock, bool) {
	message := strings.TrimSpace(entry.Message)
	detailsRaw := rawTurnBlockValue(entry.Details)
	if message == "" && len(detailsRaw) == 0 {
		return TurnBlock{}, false
	}
	title := message
	if title == "" {
		title = "runtime log"
	}
	return newRuntimeLogTurnBlock(title, TurnBlockRuntimeLogData{
		LogLevel:   strings.TrimSpace(entry.LogLevel),
		Message:    message,
		Details:    detailsRaw,
		StackTrace: strings.TrimSpace(entry.StackTrace),
	}), true
}

func parseTurnBlocksFromRaw(raw []byte) []TurnBlock {
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) == 1 {
		if blocks := parseTurnBlocksFromObjectLine(lines[0]); len(blocks) > 0 {
			return blocks
		}
	}
	out := make([]TurnBlock, 0, 8)
	pending := map[int]*cliPendingToolCall{}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(asString(obj["type"]))) {
		case "assistant":
			out = append(out, blocksFromAssistantObject(obj)...)
		case "user":
			out = append(out, blocksFromUserObject(obj)...)
		case "result":
			if text := strings.TrimSpace(asString(obj["result"])); text != "" {
				out = append(out, TurnBlock{Kind: "outcome", Text: text})
			}
		case "stream_event":
			out = append(out, blocksFromStreamEvent(obj, pending)...)
		default:
			out = append(out, parseGenericProviderObject(obj)...)
		}
	}
	return normalizeTurnBlocks(out)
}

func parseTurnBlocksFromObjectLine(line string) []TurnBlock {
	line = strings.TrimSpace(line)
	if line == "" {
		return nil
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(line), &obj); err != nil {
		return nil
	}
	if len(obj) == 0 {
		return nil
	}
	if _, ok := obj["type"]; ok {
		return nil
	}
	blocks := parseGenericProviderObject(obj)
	if len(blocks) > 0 {
		return normalizeTurnBlocks(blocks)
	}
	if text := strings.TrimSpace(firstReadableString(
		asString(obj["result"]),
		asString(obj["assistant_text"]),
	)); text != "" {
		return []TurnBlock{{Kind: "assistant_text", Text: text}, {Kind: "outcome", Text: text}}
	}
	return nil
}

func parseGenericProviderObject(obj map[string]any) []TurnBlock {
	blocks := []TurnBlock{}
	if content, ok := obj["content"].([]any); ok {
		blocks = append(blocks, parseBlocksFromContent(content)...)
	}
	if msg, ok := obj["message"].(map[string]any); ok {
		if content, ok := msg["content"].([]any); ok {
			blocks = append(blocks, parseBlocksFromContent(content)...)
		}
	}
	if text := strings.TrimSpace(firstReadableString(
		asString(obj["result"]),
		asString(obj["assistant_text"]),
		asString(obj["output"]),
	)); text != "" {
		blocks = append(blocks, TurnBlock{Kind: "assistant_text", Text: text})
		blocks = append(blocks, TurnBlock{Kind: "outcome", Text: text})
	}
	return blocks
}

func blocksFromAssistantObject(obj map[string]any) []TurnBlock {
	payload := obj
	if msg, ok := obj["message"].(map[string]any); ok && len(msg) > 0 {
		payload = msg
	}
	blocks := []TurnBlock{}
	if content, ok := payload["content"].([]any); ok {
		blocks = append(blocks, parseBlocksFromContent(content)...)
	}
	return blocks
}

func parseBlocksFromContent(content []any) []TurnBlock {
	blocks := make([]TurnBlock, 0, len(content))
	for _, item := range content {
		entry, _ := item.(map[string]any)
		if len(entry) == 0 {
			continue
		}
		switch strings.TrimSpace(strings.ToLower(asString(entry["type"]))) {
		case "text":
			if text := strings.TrimSpace(asString(entry["text"])); text != "" {
				blocks = append(blocks, TurnBlock{Kind: "assistant_text", Text: text})
			}
		case "thinking":
			if thought := strings.TrimSpace(firstReadableString(
				asString(entry["thinking"]),
				asString(entry["text"]),
			)); thought != "" {
				blocks = append(blocks, TurnBlock{Kind: "reasoning", Text: thought})
			}
		case "tool_use":
			name := strings.TrimSpace(asString(entry["name"]))
			if name == "" {
				continue
			}
			input := entry["input"]
			if input == nil {
				input = entry["arguments"]
			}
			blocks = append(blocks, newToolUseTurnBlock(name, input, strings.TrimSpace(asString(entry["id"]))))
		}
	}
	return blocks
}

func blocksFromUserObject(obj map[string]any) []TurnBlock {
	content, ok := cliMessageContent(obj)
	if !ok {
		return nil
	}
	blocks := []TurnBlock{}
	for _, item := range content {
		entry, _ := item.(map[string]any)
		if strings.TrimSpace(strings.ToLower(asString(entry["type"]))) != "tool_result" {
			continue
		}
		block := newToolResultTurnBlock("", nil, strings.TrimSpace(asString(entry["tool_use_id"])))
		if content, ok := entry["content"].([]any); ok && len(content) > 0 {
			if first, ok := content[0].(map[string]any); ok {
				if text := strings.TrimSpace(asString(first["text"])); text != "" {
					var decoded any
					if json.Unmarshal([]byte(text), &decoded) == nil {
						block.Output = rawTurnBlockValue(decoded)
					} else {
						block.Output = rawTurnBlockValue(text)
					}
				}
			}
		}
		blocks = append(blocks, block)
	}
	return blocks
}

func blocksFromStreamEvent(obj map[string]any, pending map[int]*cliPendingToolCall) []TurnBlock {
	event, _ := obj["event"].(map[string]any)
	if len(event) == 0 {
		return nil
	}
	switch strings.ToLower(strings.TrimSpace(asString(event["type"]))) {
	case "content_block_start":
		index := asInt(event["index"])
		block, _ := event["content_block"].(map[string]any)
		if strings.ToLower(strings.TrimSpace(asString(block["type"]))) != "tool_use" {
			return nil
		}
		call := &cliPendingToolCall{
			ID:   strings.TrimSpace(asString(block["id"])),
			Name: strings.TrimSpace(asString(block["name"])),
		}
		if input, ok := block["input"]; ok {
			call.Input = input
		}
		if call.Name != "" {
			pending[index] = call
		}
	case "content_block_delta":
		index := asInt(event["index"])
		call, ok := pending[index]
		if !ok || call == nil {
			return nil
		}
		delta, _ := event["delta"].(map[string]any)
		if strings.ToLower(strings.TrimSpace(asString(delta["type"]))) != "input_json_delta" {
			return nil
		}
		call.InputJSON.WriteString(asString(delta["partial_json"]))
	case "content_block_stop":
		index := asInt(event["index"])
		call, ok := pending[index]
		if !ok || call == nil {
			return nil
		}
		delete(pending, index)
		args := call.Input
		if raw := strings.TrimSpace(call.InputJSON.String()); raw != "" {
			var decoded any
			if err := json.Unmarshal([]byte(raw), &decoded); err == nil {
				args = decoded
			}
		}
		if strings.TrimSpace(call.Name) != "" {
			return []TurnBlock{newToolUseTurnBlock(call.Name, args, strings.TrimSpace(call.ID))}
		}
	}
	return nil
}

func dedupeTurnBlocks(blocks []TurnBlock) []TurnBlock {
	if len(blocks) <= 1 {
		return blocks
	}
	out := make([]TurnBlock, 0, len(blocks))
	seen := map[string]struct{}{}
	for _, block := range blocks {
		raw, _ := json.Marshal(block)
		key := string(raw)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, block)
	}
	return out
}

func normalizeTurnBlocks(blocks []TurnBlock) []TurnBlock {
	blocks = stripTurnSummaryBlocks(blocks)
	blocks = dedupeTurnBlocks(blocks)
	if len(blocks) == 0 {
		return blocks
	}
	toolNamesByUseID := map[string]string{}
	for _, block := range blocks {
		if strings.TrimSpace(block.Kind) != "tool_use" {
			continue
		}
		toolUseID := strings.TrimSpace(blockToolUseID(block))
		toolName := strings.TrimSpace(block.ToolName)
		if toolUseID == "" || toolName == "" {
			continue
		}
		toolNamesByUseID[toolUseID] = toolName
	}
	out := make([]TurnBlock, 0, len(blocks))
	for _, block := range blocks {
		if len(toolNamesByUseID) > 0 && strings.TrimSpace(block.Kind) == "tool_result" && strings.TrimSpace(block.ToolName) == "" {
			if toolUseID := strings.TrimSpace(blockToolUseID(block)); toolUseID != "" {
				if toolName := strings.TrimSpace(toolNamesByUseID[toolUseID]); toolName != "" {
					block.ToolName = toolName
				}
			}
		}
		out = append(out, block)
	}
	if summary, ok := buildTurnSummaryBlock(out); ok {
		out = append(out, summary)
	}
	return out
}

func stripTurnSummaryBlocks(blocks []TurnBlock) []TurnBlock {
	if len(blocks) == 0 {
		return nil
	}
	out := make([]TurnBlock, 0, len(blocks))
	for _, block := range blocks {
		if strings.TrimSpace(block.Kind) == turnSummaryBlockKind {
			continue
		}
		out = append(out, block)
	}
	return out
}

func buildTurnSummaryBlock(blocks []TurnBlock) (TurnBlock, bool) {
	assistantVisibleOutput := ""
	outcome := ""
	reasoning := []string{}
	progress := []string{}
	toolResults := []TurnSummaryToolResult{}
	reasoningSeen := map[string]struct{}{}
	progressSeen := map[string]struct{}{}
	for _, block := range blocks {
		switch strings.TrimSpace(block.Kind) {
		case "assistant_text":
			if text := strings.TrimSpace(block.Text); text != "" {
				assistantVisibleOutput = text
			}
		case "outcome":
			if text := strings.TrimSpace(block.Text); text != "" {
				outcome = text
				if assistantVisibleOutput == "" {
					assistantVisibleOutput = text
				}
			}
		case "reasoning":
			if text := strings.TrimSpace(block.Text); text != "" {
				if _, ok := reasoningSeen[text]; ok {
					continue
				}
				reasoningSeen[text] = struct{}{}
				reasoning = append(reasoning, text)
			}
		case "progress":
			if text := strings.TrimSpace(block.Text); text != "" {
				if _, ok := progressSeen[text]; ok {
					continue
				}
				progressSeen[text] = struct{}{}
				progress = append(progress, text)
			}
		case "tool_result":
			if result, ok := buildTurnSummaryToolResult(block); ok {
				toolResults = append(toolResults, result)
			}
		}
	}
	if outcome == "" {
		outcome = assistantVisibleOutput
	}
	data := TurnSummaryTurnBlockData{
		AssistantVisibleOutput: assistantVisibleOutput,
		Outcome:                outcome,
		ReasoningBlocks:        reasoning,
		ProgressUpdates:        progress,
		ToolResults:            toolResults,
	}
	if data.AssistantVisibleOutput == "" &&
		data.Outcome == "" &&
		len(data.ReasoningBlocks) == 0 &&
		len(data.ProgressUpdates) == 0 &&
		len(data.ToolResults) == 0 {
		return TurnBlock{}, false
	}
	return newTurnSummaryTurnBlock(data), true
}

func buildTurnSummaryToolResult(block TurnBlock) (TurnSummaryToolResult, bool) {
	result := TurnSummaryToolResult{
		ToolName: strings.TrimSpace(block.ToolName),
	}
	if toolUseID := strings.TrimSpace(blockToolUseID(block)); toolUseID != "" {
		result.ToolUseID = toolUseID
	}
	if len(block.Output) > 0 {
		result.Output = append(json.RawMessage(nil), block.Output...)
	}
	if result.ToolName == "" && result.ToolUseID == "" && len(result.Output) == 0 {
		return TurnSummaryToolResult{}, false
	}
	return result, true
}

func blockToolUseID(block TurnBlock) string {
	link, ok, err := block.ToolLinkData()
	if err != nil || !ok {
		return ""
	}
	return strings.TrimSpace(link.ToolUseID)
}

func firstReadableString(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
