package engine

import "testing"

func TestStateCarrierFromPersistedRoundTrip(t *testing.T) {
	carrier, err := StateCarrierFromPersisted(
		map[string]any{
			"subject_id": "ent-1",
			"score":      91,
			"gates": map[string]any{
				"ready": true,
			},
		},
		map[string]any{
			"evidence": map[string]any{
				"count": 2,
			},
		},
	)
	if err != nil {
		t.Fatalf("StateCarrierFromPersisted error = %v", err)
	}
	if got := carrier.Metadata["score"]; got != 91 {
		t.Fatalf("metadata score = %#v, want 91", got)
	}
	if _, exists := carrier.Metadata["gates"]; exists {
		t.Fatalf("carrier metadata should exclude persisted gates: %#v", carrier.Metadata)
	}
	if !carrier.Gates["ready"] {
		t.Fatalf("carrier gates = %#v, want ready=true", carrier.Gates)
	}
	if got := carrier.StateBuckets["evidence"]["count"]; got != 2 {
		t.Fatalf("carrier state bucket evidence.count = %#v, want 2", got)
	}

	persistedMetadata := carrier.PersistedMetadata()
	if _, exists := persistedMetadata["subject_id"]; exists {
		t.Fatalf("persisted metadata should exclude deprecated subject_id: %#v", persistedMetadata)
	}
	if gates, ok := persistedMetadata["gates"].(map[string]any); !ok || gates["ready"] != true {
		t.Fatalf("persisted metadata gates = %#v", persistedMetadata["gates"])
	}
	persistedBuckets := carrier.PersistedStateBuckets()
	evidence, ok := persistedBuckets["evidence"].(map[string]any)
	if !ok || evidence["count"] != 2 {
		t.Fatalf("persisted state buckets evidence = %#v", persistedBuckets["evidence"])
	}
}

func TestStateCarrierFromPersistedRejectsMalformedShapes(t *testing.T) {
	t.Run("gates", func(t *testing.T) {
		_, err := StateCarrierFromPersisted(map[string]any{
			"gates": map[string]any{
				"ready": "true",
			},
		}, nil)
		if err == nil {
			t.Fatal("expected malformed gates error")
		}
	})

	t.Run("state_buckets", func(t *testing.T) {
		_, err := StateCarrierFromPersisted(nil, map[string]any{
			"evidence": "bad",
		})
		if err == nil {
			t.Fatal("expected malformed state bucket error")
		}
	})
}
