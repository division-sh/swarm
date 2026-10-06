package main

import (
	"strings"
	"testing"
)

func TestRewrite2566LiteralPreparationPreservesDataAndMetadata(t *testing.T) {
	for _, before := range []string{
		"stages: {start: {initial: true}, done: {terminal: true}}\ndata: {initial: true, terminal: true}\n",
		"stages:\n  start:\n    initial: true\n    description: kept\n    timers: [{after: 1h, emit: tick}]\n  done: {terminal: true}\ndata: {initial: true, terminal: true}\n",
		"stages:\n  start:\n    initial: true\n  done: {terminal: true}\ndata: {initial: true, terminal: true}\n",
	} {
		after, err := prepareStageLiteral(before)
		if err != nil || !strings.Contains(after, "data: {initial: true, terminal: true}") || !strings.Contains(after, "done: {final: true}") {
			t.Fatalf("data/metadata changed: %s, %v", after, err)
		}
		if strings.Contains(before, "description:") && (!strings.Contains(after, "description: kept") || !strings.Contains(after, "timers: [{after: 1h, emit: tick}]")) {
			t.Fatalf("stage metadata lost: %s", after)
		}
		second, err := prepareStageLiteral(after)
		if err != nil || second != after {
			t.Fatalf("preparation not idempotent: %v", err)
		}
	}
}

func TestRewrite2566LiteralPreparationRefusesUnreviewedEntryChange(t *testing.T) {
	if _, err := prepareStageLiteral("stages: {other: {}, selected: {initial: true}}\n"); err == nil {
		t.Fatal("silently selected a different first declaration")
	}
}
