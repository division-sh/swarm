package channelnative_test

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/registration"
	"github.com/division-sh/swarm/tests/support/testutil/packfixture"
)

func TestRegisteredTelegramConfirmationUsesExactExistingSerializer(t *testing.T) {
	installed := packfixture.ConnectorTool(t, "telegram", "telegram.send_interactive")
	toolID, tool := installed.ToolID, installed.Tool
	input := map[string]any{"chat_id": "12345", "text": effects.RegisteredOperatorAcknowledgementText,
		"reply_markup": map[string]any{"inline_keyboard": []any{}}}
	request, err := registration.ChannelWriteFingerprint(toolID, tool, input)
	if err != nil || channelnative.ValidateTelegramConfirmation(string(request), "12345", effects.RegisteredOperatorAcknowledgementText) != nil {
		t.Fatal("existing serializer's exact request was refused", err)
	}
	for name, mutate := range map[string]func(map[string]any){
		"other tool":        func(r map[string]any) { r["tool_id"] = "telegram.send_message" },
		"other destination": func(r map[string]any) { r["input"].(map[string]any)["chat_id"] = "67890" },
		"other text":        func(r map[string]any) { r["input"].(map[string]any)["text"] = "another message" },
		"extra input":       func(r map[string]any) { r["input"].(map[string]any)["account"] = "67890" },
		"other credentials": func(r map[string]any) {
			r["contract"].(map[string]any)["credentials"] = []any{"telegram.webhook_signing_secret"}
		},
		"other endpoint": func(r map[string]any) {
			r["contract"].(map[string]any)["http"].(map[string]any)["url"] = "https://another.invalid/sendMessage"
		},
		"extra header": func(r map[string]any) {
			r["contract"].(map[string]any)["http"].(map[string]any)["headers"] = map[string]any{"X-Extra": "yes"}
		},
		"other body": func(r map[string]any) {
			r["contract"].(map[string]any)["http"].(map[string]any)["body"] = map[string]any{"text": "{{input.text}}"}
		},
		"other transport": func(r map[string]any) { r["contract"].(map[string]any)["mcp"] = map[string]any{"server": "foreign"} },
	} {
		t.Run(name, func(t *testing.T) {
			value, err := canonicaljson.Decode(request)
			if err != nil {
				t.Fatal(err)
			}
			object := value.Interface().(map[string]any)
			mutate(object)
			raw, err := canonicaljson.Bytes(object)
			if err != nil {
				t.Fatal(err)
			}
			if channelnative.ValidateTelegramConfirmation(string(raw), "12345", effects.RegisteredOperatorAcknowledgementText) == nil {
				t.Fatal("closed acknowledgement admitted another request")
			}
		})
	}
}
