package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

func TestServedSetupGeneratedTransitionDigestMatchesFiniteOutput(t *testing.T) {
	raw, err := os.ReadFile("../family145-transitions.json")
	if err != nil {
		t.Fatal(err)
	}
	var rows []transition
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(encoded))
	if len(rows) != 887 || digest != targetDigest {
		t.Fatalf("finite generated transition inventory=%d digest=%s", len(rows), digest)
	}
}
