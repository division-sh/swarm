package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestActivityIdentityCaptureConsumesAdmittedOwnerAndCoordinates(t *testing.T) {
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	files := map[string]string{
		"schema.yaml": "name: activity-capture\n",
		"events.yaml": "source.requested:\nbranch.requested:\nexplicit.requested:\n",
		"tools.yaml":  "lookup:\n  description: Read a source.\n  handler_type: http\n  effect_class: read_only\n  http: {method: GET, url: https://example.test}\n",
		"nodes.yaml": `worker:
  source.requested:
    activity: {tool: lookup}
  branch.requested:
    rules:
      - id: chosen
        when: true
        activity: {tool: lookup}
      - else: true
  explicit.requested:
    activity: {id: kept, tool: lookup}
`,
	}
	for path, body := range files {
		if err := os.WriteFile(filepath.Join(root, path), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	pins, err := captureActivityIdentities(repo, []string{root})
	if err != nil {
		t.Fatal(err)
	}
	if len(pins) != 3 {
		t.Fatalf("activity membership changed: %v", pins)
	}
	want := map[string]struct {
		id       string
		line     int
		explicit bool
	}{
		"branch.requested":   {"worker_branch_requested_chosen_lookup", 8, false},
		"explicit.requested": {"kept", 11, true},
		"source.requested":   {"worker_source_requested_lookup", 3, false},
	}
	for _, pin := range pins {
		expected, ok := want[pin.Handler]
		if !ok || pin.ResultIdentity.ActivityID != expected.id || pin.ResultIdentity.SuccessEvent != expected.id+".succeeded" || pin.ResultIdentity.FailureEvent != expected.id+".failed" || pin.ResultIdentity.RevisionRequested != expected.id+".revision_requested" || pin.ResultIdentity.Rejected != expected.id+".rejected" || pin.Explicit != expected.explicit || pin.Line != expected.line || pin.Column == 0 || !strings.HasSuffix(pin.File, "nodes.yaml") || pin.Node != "worker" || pin.Flow != "." || pin.BundleHash == "" || pin.Approval {
			t.Fatalf("identity/provenance drift for %s: %+v", pin.Handler, pin)
		}
		delete(want, pin.Handler)
	}
	if len(want) != 0 {
		t.Fatalf("missing activity identities: %v", want)
	}
	if err := os.WriteFile(filepath.Join(root, "nodes.yaml"), []byte("worker:\n  undeclared: {}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if pins, err := captureActivityIdentities(repo, []string{root}); err == nil || pins != nil || !strings.Contains(err.Error(), "undeclared event") {
		t.Fatalf("capture bypassed canonical source admission: pins=%v error=%v", pins, err)
	}
}
