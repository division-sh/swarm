package bootverify

import (
	"context"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestRunGuardTerminationReachabilityThroughSourceAdmission(t *testing.T) {
	for _, tc := range []struct {
		name, target, handler string
		wantUnreachable       bool
	}{
		{"kill without advance", "killed", "      guard: {id: check, check: false, on_fail: kill}\n", false},
		{"kill with success advance", "killed", "      guard: {id: check, check: false, on_fail: kill}\n      advances_to: killed\n", false},
		{"chain", "killed", "      guard:\n        checks:\n          - {id: first, check: true}\n          - {id: second, check: false}\n        on_fail: kill\n", false},
		{"on fail only", "killed", "      guard: {on_fail: kill}\n", true},
		{"empty checks", "killed", "      guard: {checks: [{}], on_fail: kill}\n", true},
		{"policy only", "killed", "      guard: {policy_ref: threshold, on_fail: kill}\n", true},
		{"unnamed", "killed", "      guard: {check: false, on_fail: kill}\n", false},
		{"unnamed chain", "killed", "      guard: {checks: [{check: true}, {check: false}], on_fail: kill}\n", false},
		{"mixed chain", "killed", "      guard: {checks: [{}, {id: first, check: true}, {check: false}, {}], on_fail: kill}\n", false},
		{"no op with ordinary advance", "killed", "      guard: {checks: [{}], on_fail: kill}\n      advances_to: killed\n", false},
		{"no guard", "killed", "      advances_to: ready\n", true},
		{"reject", "killed", "      guard: {id: check, check: false, on_fail: reject}\n", true},
		{"discard", "killed", "      guard: {id: check, check: false, on_fail: discard}\n", true},
		{"wrong case", "Killed", "      guard: {id: check, check: false, on_fail: kill}\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "stages:\n  ready: {}\n  "+tc.target+": {final: true}\n")
			writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "router:\n  execution_type: system_node\n  subscribes_to: [work]\n  event_handlers:\n    work:\n"+tc.handler)
			writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "work:\n")
			writeBootverifyFixtureFile(t, filepath.Join(root, "entities.yaml"), "item: {}\n")
			repo := repoRootForBootverifyTest(t)
			bundle := loadFixtureBundleAt(t, repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
			report := Run(context.Background(), compileBootverifySchemasPreservingPlans(bundle), Options{})
			got := reportContains(report.Errors(), "semantic_drift_unreachable_state", tc.target)
			if got != tc.wantUnreachable {
				t.Fatalf("unreachable=%v want %v: %#v", got, tc.wantUnreachable, report.Errors())
			}
			if !tc.wantUnreachable && len(report.Errors()) != 0 {
				t.Fatalf("earlier boot gates failed: %#v", report.Errors())
			}
		})
	}
}
