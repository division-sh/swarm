package engine

import "testing"

func TestStateCarrierFromPersistedRoundTrip(t *testing.T) {
	carrier, err := StateCarrierFromPersisted(
		map[string]any{
			"activation": "authored",
			"score":      91,
		},
		map[string]any{"activation": "platform"},
		map[string]bool{"ready": true},
		map[string]any{
			"evidence": map[string]any{
				"count": 2,
			},
		},
	)
	if err != nil {
		t.Fatalf("StateCarrierFromPersisted error = %v", err)
	}
	if got := carrier.Fields["score"]; got != 91 {
		t.Fatalf("metadata score = %#v, want 91", got)
	}
	if carrier.Fields["activation"] != "authored" || carrier.Bookkeeping["activation"] != "platform" {
		t.Fatalf("carrier owner collision = fields:%#v bookkeeping:%#v", carrier.Fields, carrier.Bookkeeping)
	}
	if !carrier.Gates["ready"] {
		t.Fatalf("carrier gates = %#v, want ready=true", carrier.Gates)
	}
	if got := carrier.StateBuckets["evidence"]["count"]; got != 2 {
		t.Fatalf("carrier state bucket evidence.count = %#v, want 2", got)
	}

	if carrier.PersistedFields()["activation"] != "authored" || carrier.PersistedBookkeeping()["activation"] != "platform" {
		t.Fatalf("persisted owners = fields:%#v bookkeeping:%#v", carrier.PersistedFields(), carrier.PersistedBookkeeping())
	}
	persistedBuckets := carrier.PersistedStateBuckets()
	evidence, ok := persistedBuckets["evidence"].(map[string]any)
	if !ok || evidence["count"] != 2 {
		t.Fatalf("persisted state buckets evidence = %#v", persistedBuckets["evidence"])
	}
}

func TestStateCarrierFromPersistedRejectsMalformedShapes(t *testing.T) {
	t.Run("state_buckets", func(t *testing.T) {
		_, err := StateCarrierFromPersisted(nil, nil, nil, map[string]any{
			"evidence": "bad",
		})
		if err == nil {
			t.Fatal("expected malformed state bucket error")
		}
	})
}
