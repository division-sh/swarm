package effectpersistence

import (
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
)

func TestUnstartedOriginCodecPreservesOnlyExactWorkAuthority(t *testing.T) {
	run := uuid.NewString()
	claim, err := deliverylifecycle.AdmitPersistedClaim(uuid.NewString(), run, "canonical-route", uuid.NewString(), 3, deliverylifecycle.SubscriberAgent, "worker")
	if err != nil {
		t.Fatal(err)
	}
	origin, err := effects.DeliveryCompletionOrigin(claim)
	if err != nil {
		t.Fatal(err)
	}
	owner := flowidentity.RunScopedFlowInstance{RunID: run, Route: flowidentity.StoredRoute("orders/child", "child", "orders/one/child")}
	raw, err := encodeUnstartedOrigin(origin, owner)
	if err != nil {
		t.Fatal(err)
	}
	got, restored, err := decodeUnstartedOrigin(raw)
	if err != nil || !got.Same(origin) || restored != owner {
		t.Fatalf("exact origin/constructor owner changed: %+v %+v err=%v", got, restored, err)
	}
	for _, field := range []string{"version", "owner", "kind", "delivery_id", "run_id", "route", "token", "claim_version", "agent_id"} {
		t.Run("missing_"+field, func(t *testing.T) {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			delete(fields, field)
			bad, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := decodeUnstartedOrigin(bad); err == nil {
				t.Fatal("incomplete origin evidence was admitted")
			}
		})
	}
	for _, bad := range []string{string(raw) + `{}`, `{"version":2}`, `{"version":1,"unknown":true}`, `null`} {
		if _, _, err := decodeUnstartedOrigin([]byte(bad)); err == nil {
			t.Fatal("ambiguous version/trailing evidence was admitted")
		}
	}
	var mixed map[string]json.RawMessage
	if err := json.Unmarshal(raw, &mixed); err != nil {
		t.Fatal(err)
	}
	mixed["directive_id"] = json.RawMessage(`"` + uuid.NewString() + `"`)
	bad, err := json.Marshal(mixed)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := decodeUnstartedOrigin(bad); err == nil {
		t.Fatal("mixed origin authorities were admitted")
	}
}
