package channelnative

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

// ValidateTelegramConfirmation checks the existing HTTP serializer's semantic
// request. No credential value or prepared token-bearing URL enters this proof.
func ValidateTelegramConfirmation(request, destination, text string) error {
	if _, err := canonicaljson.Decode([]byte(request)); err != nil {
		return err
	}
	var value map[string]any
	if err := json.Unmarshal([]byte(request), &value); err != nil {
		return err
	}
	contract, ok := value["contract"].(map[string]any)
	if len(value) != 3 || value["tool_id"] != "telegram.send_interactive" || !ok ||
		contract["category"] != "provider_connector" || contract["handler_type"] != "http" {
		return fmt.Errorf("registered Telegram acknowledgement requires its exact connector")
	}
	for key := range contract {
		switch key {
		case "category", "description", "handler_type", "permission", "rate_limit", "rate_limit_max_wait", "input_schema", "output_schema", "credentials", "generated_schema", "http", "response_mapping", "response_success", "compiled_result":
		default:
			return fmt.Errorf("registered Telegram acknowledgement contains another execution contract")
		}
	}
	expectedHTTP := map[string]any{"method": "POST", "url": "https://api.telegram.org/bot{{credentials.telegram.telegram_bot_token}}/sendMessage",
		"body": map[string]any{"chat_id": "{{input.chat_id}}", "text": "{{input.text}}", "reply_markup": "{{input.reply_markup}}"}}
	expectedInput := map[string]any{"chat_id": destination, "text": text,
		"reply_markup": map[string]any{"inline_keyboard": []any{}}}
	expectedSuccess := map[string]any{"kind": "json_field_equals", "path": "response.body.ok", "equals": true}
	expectedMapping := map[string]any{"message_id": "{{response.body.result.message_id}}"}
	if destination == "" || text == "" || !reflect.DeepEqual(contract["http"], expectedHTTP) ||
		!reflect.DeepEqual(contract["response_success"], expectedSuccess) || !reflect.DeepEqual(contract["response_mapping"], expectedMapping) ||
		!reflect.DeepEqual(contract["credentials"], []any{"telegram.telegram_bot_token"}) ||
		!reflect.DeepEqual(value["input"], expectedInput) {
		return fmt.Errorf("registered Telegram acknowledgement requires its fixed request and destination")
	}
	return nil
}
