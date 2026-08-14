package mutationlog

import (
	"strings"
	"testing"
)

func TestReconstructEntityStateProjection_FailsOnMalformedGateKey(t *testing.T) {
	_, err := ReconstructEntityStateProjection([]ProjectionMutation{{
		Domain:   DomainGate,
		NewValue: true,
	}})
	if err == nil || !strings.Contains(err.Error(), "gate mutation path is required") {
		t.Fatalf("ReconstructEntityStateProjection error = %v", err)
	}
}

func TestReconstructEntityStateProjection_FailsOnMalformedAccumulatorKey(t *testing.T) {
	_, err := ReconstructEntityStateProjection([]ProjectionMutation{{
		Domain:   DomainAccumulator,
		NewValue: map[string]any{"bad": true},
	}})
	if err == nil || !strings.Contains(err.Error(), "accumulator mutation path is required") {
		t.Fatalf("ReconstructEntityStateProjection error = %v", err)
	}
}

func TestReconstructEntityStateProjection_RoundTripsTrackedEntityState(t *testing.T) {
	got, err := ReconstructEntityStateProjection([]ProjectionMutation{
		{Domain: DomainLifecycleState, NewValue: "done"},
		{Domain: DomainAuthoredField, Path: "status", NewValue: "closed"},
		{Domain: DomainBookkeeping, Path: "activation", NewValue: "platform"},
		{Domain: DomainGate, Path: "g_done", NewValue: true},
		{Domain: DomainAccumulator, Path: "evidence", NewValue: map[string]any{"score": float64(2)}},
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
	if got.Bookkeeping["activation"] != "platform" {
		t.Fatalf("Bookkeeping = %#v", got.Bookkeeping)
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
		{Domain: DomainAuthoredField, Path: "metadata", NewValue: map[string]any{"region": "us", "score_band": "low"}},
		{Domain: DomainAuthoredField, Path: "metadata.region", NewValue: "ca"},
		{Domain: DomainAuthoredField, Path: "status", NewValue: "open"},
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

func TestReconstructEntityStateProjection_DomainNeverComesFromAuthoredPath(t *testing.T) {
	got, err := ReconstructEntityStateProjection([]ProjectionMutation{
		{Domain: DomainLifecycleState, NewValue: "running"},
		{Domain: DomainAuthoredField, Path: "current_state", NewValue: "business-state"},
		{Domain: DomainAuthoredField, Path: "gates.review", NewValue: "business-gate"},
		{Domain: DomainAuthoredField, Path: "accumulator.total", NewValue: 4},
		{Domain: DomainAuthoredField, Path: "bookkeeping.activation", NewValue: "business-activation"},
		{Domain: DomainBookkeeping, Path: "activation", NewValue: "platform-activation"},
	})
	if err != nil {
		t.Fatalf("ReconstructEntityStateProjection: %v", err)
	}
	if got.CurrentState != "running" || got.Fields["current_state"] != "business-state" {
		t.Fatalf("lifecycle/authored collision = %#v", got)
	}
	if _, ok := got.Gates["review"]; ok {
		t.Fatalf("authored gates path changed platform gates: %#v", got.Gates)
	}
	if _, ok := got.Accumulator["total"]; ok {
		t.Fatalf("authored accumulator path changed platform accumulator: %#v", got.Accumulator)
	}
	bookkeeping, _ := got.Fields["bookkeeping"].(map[string]any)
	if bookkeeping["activation"] != "business-activation" || got.Bookkeeping["activation"] != "platform-activation" {
		t.Fatalf("authored/bookkeeping collision = fields:%#v bookkeeping:%#v", got.Fields, got.Bookkeeping)
	}
}
