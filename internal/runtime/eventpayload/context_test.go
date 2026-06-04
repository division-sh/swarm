package eventpayload

import "testing"

func TestActivationContextNamesAreNotGloballyRuntimeOwnedPayloadFields(t *testing.T) {
	for _, key := range []string{"instance_id", "template_id", "flow_path", "parent_entity_id"} {
		if IsRuntimeOwnedCanonicalContextField(key) {
			t.Fatalf("%s must not be globally runtime-owned; it can be authored business payload on non-auto-emit surfaces", key)
		}
	}
	payload := StripUndeclaredRuntimeOwnedCanonicalContext(map[string]any{
		"template_id": "application-basic-v1",
	}, map[string]struct{}{})
	if got := payload["template_id"]; got != "application-basic-v1" {
		t.Fatalf("template_id payload = %#v, want retained business field", got)
	}
}
