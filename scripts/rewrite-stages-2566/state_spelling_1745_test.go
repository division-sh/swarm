package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestRewrite2566StateSpellingSuccessorsPreserveHistoricalDecisions(t *testing.T) {
	root := canonicalrouting.RepoRoot(t)
	ledger, err := os.ReadFile(filepath.Join(root, "scripts/rewrite-stages-2566/finite-fixtures.json"))
	if err != nil {
		t.Fatal(err)
	}
	if digest(ledger) != "3da9e2f24fb5126eede32087a4fbb2131152bcf4110901fd029c3626bf173b01" || digest(stateSpellingEvidence1745) != "513d1c78f80001fc1526db6b4a432821b988c02c9d2f11030a0076117e853d0e" {
		t.Fatal("historical stage decisions or finite spelling successors changed")
	}
	var original plan
	if err := json.Unmarshal(ledger, &original); err != nil {
		t.Fatal(err)
	}
	var endpoints []stateSpellingEndpoint1745
	if err := json.Unmarshal(stateSpellingEvidence1745, &endpoints); err != nil || len(endpoints) != 6 {
		t.Fatalf("wrong finite endpoint set: %d %v", len(endpoints), err)
	}
	seen := map[string]bool{}
	for _, endpoint := range endpoints {
		if seen[endpoint.File] {
			t.Fatalf("duplicate endpoint %s", endpoint.File)
		}
		seen[endpoint.File] = true
		matches := 0
		for _, c := range original.Changes {
			if c.File != endpoint.File {
				continue
			}
			matches++
			body, err := os.ReadFile(filepath.Join(root, c.File))
			if err != nil {
				t.Fatal(err)
			}
			accepted, err := isStageSpellingEndpoint1745(c, body)
			if err != nil || !accepted {
				t.Fatalf("exact current endpoint refused: %s %v", c.File, err)
			}
			if output, err := pendingChange(root, c, true); err != nil || output != nil {
				t.Fatalf("current endpoint is not idempotent: %s %v", c.File, err)
			}
			for _, coordinate := range []string{"file", "prior", "body"} {
				changed, candidate := c, append([]byte(nil), body...)
				switch coordinate {
				case "file":
					changed.File += ".other"
				case "prior":
					changed.AfterHash = changed.BeforeHash
				case "body":
					candidate = append(candidate, '\n')
				}
				if accepted, err := isStageSpellingEndpoint1745(changed, candidate); err != nil || accepted {
					t.Fatalf("changed %s coordinate accepted: %s %v", coordinate, c.File, err)
				}
			}
		}
		if matches != 1 {
			t.Fatalf("endpoint has %d historical decisions: %s", matches, endpoint.File)
		}
	}
}
