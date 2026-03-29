package pipeline

import "testing"

func TestWorkflowAppendEvidence_AppendsEntriesForBucket(t *testing.T) {
	bucket := map[string]any{}

	workflowAppendEvidence(bucket, "findings", map[string]any{"finding": "a"})
	workflowAppendEvidence(bucket, "findings", map[string]any{"finding": "b"})

	raw, ok := bucket["findings"].([]any)
	if !ok {
		t.Fatalf("findings bucket type = %T, want []any", bucket["findings"])
	}
	if len(raw) != 2 {
		t.Fatalf("findings bucket len = %d, want 2", len(raw))
	}
	first, ok := raw[0].(map[string]any)
	if !ok || first["finding"] != "a" {
		t.Fatalf("first entry = %#v", raw[0])
	}
	second, ok := raw[1].(map[string]any)
	if !ok || second["finding"] != "b" {
		t.Fatalf("second entry = %#v", raw[1])
	}
}

func TestWorkflowAppendEvidence_PromotesLegacyMapEntry(t *testing.T) {
	bucket := map[string]any{
		"findings": map[string]any{"finding": "legacy"},
	}

	workflowAppendEvidence(bucket, "findings", map[string]any{"finding": "next"})

	raw, ok := bucket["findings"].([]any)
	if !ok {
		t.Fatalf("findings bucket type = %T, want []any", bucket["findings"])
	}
	if len(raw) != 2 {
		t.Fatalf("findings bucket len = %d, want 2", len(raw))
	}
	first, ok := raw[0].(map[string]any)
	if !ok || first["finding"] != "legacy" {
		t.Fatalf("first entry = %#v", raw[0])
	}
	second, ok := raw[1].(map[string]any)
	if !ok || second["finding"] != "next" {
		t.Fatalf("second entry = %#v", raw[1])
	}
}
