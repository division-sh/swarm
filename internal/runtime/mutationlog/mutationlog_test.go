package mutationlog

import (
	"strings"
	"testing"
)

func TestReconstructEntityStateProjection_FailsOnMalformedGateKey(t *testing.T) {
	_, err := ReconstructEntityStateProjection([]ProjectionMutation{{
		Field:    "gates.",
		NewValue: true,
	}})
	if err == nil || !strings.Contains(err.Error(), "gates mutation key is required") {
		t.Fatalf("ReconstructEntityStateProjection error = %v", err)
	}
}

func TestReconstructEntityStateProjection_FailsOnMalformedAccumulatorKey(t *testing.T) {
	_, err := ReconstructEntityStateProjection([]ProjectionMutation{{
		Field:    "accumulator.",
		NewValue: map[string]any{"bad": true},
	}})
	if err == nil || !strings.Contains(err.Error(), "accumulator mutation key is required") {
		t.Fatalf("ReconstructEntityStateProjection error = %v", err)
	}
}

func TestReconstructEntityStateProjection_RoundTripsTrackedEntityState(t *testing.T) {
	got, err := ReconstructEntityStateProjection([]ProjectionMutation{
		{Field: "current_state", NewValue: "done"},
		{Field: "status", NewValue: "closed"},
		{Field: "gates.g_done", NewValue: true},
		{Field: "accumulator.evidence", NewValue: map[string]any{"score": float64(2)}},
	})
	if err != nil {
		t.Fatalf("ReconstructEntityStateProjection: %v", err)
	}
	if got.CurrentState != "done" {
		t.Fatalf("CurrentState = %q", got.CurrentState)
	}
	if got.Fields["status"] != "closed" {
		t.Fatalf("Fields = %#v", got.Fields)
	}
	if got.Gates["g_done"] != true {
		t.Fatalf("Gates = %#v", got.Gates)
	}
	acc, _ := got.Accumulator["evidence"].(map[string]any)
	if acc["score"] != float64(2) {
		t.Fatalf("Accumulator = %#v", got.Accumulator)
	}
}

func TestReconstructEntityStateProjection_AppliesNestedFieldMutationsOverTopLevelObjects(t *testing.T) {
	got, err := ReconstructEntityStateProjection([]ProjectionMutation{
		{Field: "metadata", NewValue: map[string]any{"region": "us", "score_band": "low"}},
		{Field: "metadata.region", NewValue: "ca"},
		{Field: "status", NewValue: "open"},
	})
	if err != nil {
		t.Fatalf("ReconstructEntityStateProjection: %v", err)
	}
	metadata, ok := got.Fields["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("Fields = %#v", got.Fields)
	}
	if metadata["region"] != "ca" {
		t.Fatalf("metadata.region = %#v, want ca", metadata["region"])
	}
	if metadata["score_band"] != "low" {
		t.Fatalf("metadata.score_band = %#v, want low", metadata["score_band"])
	}
	if got.Fields["status"] != "open" {
		t.Fatalf("Fields = %#v", got.Fields)
	}
	if _, ok := got.Fields["metadata.region"]; ok {
		t.Fatalf("Fields contains literal dotted key: %#v", got.Fields)
	}
}
