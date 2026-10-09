package packmodel

import (
	"strings"
	"testing"
)

func TestTriggerEnvelopeSessionTransportPresenceAndExclusivity(t *testing.T) {
	session := strings.Replace(admittedEnvelopeFixture, "receive_https_route: demo", "receive_session_events: demo", 1)
	for _, state := range []string{"missing", "null", "empty_text", "empty_list", "empty_map", "wrong_kind", "valid", "merge"} {
		t.Run(state, func(t *testing.T) {
			_, err := ParseEnvelopeAt(envelopePresenceBody(t, []byte(session), "capabilities.can.receive_session_events", state), "packs/demo/pack.yaml")
			if (err == nil) != (state == "valid" || state == "merge") {
				t.Fatalf("session field admission = %v", err)
			}
		})
	}
	for _, edit := range []string{
		"receive_session_events: demo, receive_https_route: route",
		"receive_session_events: demo, receive_https_route: ''",
		"receive_session_events: demo, verify_secret: secret",
		"receive_session_events: demo, verify_secret: ''",
	} {
		body := strings.Replace(session, "receive_session_events: demo", edit, 1)
		if _, err := ParseEnvelopeAt([]byte(body), "packs/demo/pack.yaml"); err == nil || !strings.Contains(err.Error(), "packs/demo/pack.yaml:") {
			t.Fatalf("inactive or competing HTTP capability admitted: %s, %v", edit, err)
		}
	}
	for _, kind := range []string{TypeConnector, TypeChannel} {
		body := strings.Replace(session, "type: trigger", "type: "+kind, 1)
		if _, err := ParseEnvelopeAt([]byte(body), "packs/demo/pack.yaml"); err == nil {
			t.Fatalf("session trigger capability crossed to %s", kind)
		}
		capabilities := Capabilities{Can: CanCapabilities{ReceiveSessionEvents: "demo"}, Cannot: []string{"example"}}
		if err := capabilities.ValidateForType("provider.demo", kind); err == nil {
			t.Fatalf("structural session capability crossed to %s", kind)
		}
	}
}
