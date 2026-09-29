package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/authoringview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestDescribePayloadlessEventsRenderNoFields(t *testing.T) {
	for _, tc := range []struct {
		name     string
		artifact canonicalrouting.ArtifactID
		flow     string
	}{
		{"root", canonicalrouting.RootIngress, ""},
		{"child", canonicalrouting.ParentConnect, "producer"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := canonicalrouting.CopyExample(t, tc.artifact)
			assertDescribePayloadlessEvent(t, root, tc.flow)
		})
	}
}

func assertDescribePayloadlessEvent(t *testing.T, root, flow string) {
	t.Helper()
	path := filepath.Join(root, flow, "events.yaml")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	writeDescribeTestFile(t, path, string(body)+"\ninvestigation.timed_out:\n")
	var stdout, stderr bytes.Buffer
	code := executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"describe", root}, &stdout, &stderr, defaultRootCommandOptions())
	if code != 0 {
		t.Fatalf("describe code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "investigation.timed_out (no fields)") {
		t.Fatalf("describe omitted payload-less event contract:\n%s", stdout.String())
	}
	stdout.Reset()
	stderr.Reset()
	code = executeRootCommandWithOptions(context.Background(), RepoRoot(), []string{"describe", root, "--json"}, &stdout, &stderr, defaultRootCommandOptions())
	var view authoringview.View
	if err := json.Unmarshal(stdout.Bytes(), &view); code != 0 || err != nil {
		t.Fatalf("describe JSON: code=%d decode=%v stderr=%s", code, err, stderr.String())
	}
	events := view.Root.Events
	if flow != "" {
		events = nil
		for _, child := range view.Flows {
			if child.ID == flow {
				events = child.Events
			}
		}
	}
	for _, event := range events {
		if event.Name == "investigation.timed_out" {
			if event.Fields == nil || len(event.Fields) != 0 {
				t.Fatalf("payload-less field projection=%#v", event.Fields)
			}
			return
		}
	}
	t.Fatal("JSON describe omitted bare event")
}
