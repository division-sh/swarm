package cliapp

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/authoringview"
)

func TestDescribeClockScheduleCompiledProjection(t *testing.T) {
	root := t.TempDir()
	writeDescribeTestFile(t, filepath.Join(root, "schema.yaml"), "stages: []\nschedules:\n  poll: {every: 5m, emit: poll.tick}\n  morning: {cron: '0 9 * * *', emit: poll.tick}\npins:\n  outputs:\n    events: [poll.tick]\n")
	writeDescribeTestFile(t, filepath.Join(root, "events.yaml"), "poll.tick:\n")
	writeDescribeTestFile(t, filepath.Join(root, "worker/schema.yaml"), "stages: []\nschedules:\n  poll: {every: 1m, emit: poll.tick}\n")
	writeDescribeTestFile(t, filepath.Join(root, "worker/events.yaml"), "poll.tick:\n")
	writeDescribeTestFile(t, filepath.Join(root, "worker/nodes.yaml"), "observer:\n  event_handlers:\n    poll.tick: {}\n")
	var jsonOut, stderr bytes.Buffer
	if code := executeRootCommandWithOptions(t.Context(), RepoRoot(), []string{"describe", root, "--json"}, &jsonOut, &stderr, defaultRootCommandOptions()); code != 0 {
		t.Fatalf("describe clock JSON: code=%d stderr=%s", code, stderr.String())
	}
	var view authoringview.View
	if err := json.Unmarshal(jsonOut.Bytes(), &view); err != nil {
		t.Fatal(err)
	}
	if len(view.ClockSchedules) != 3 || view.ClockSchedules[0].FlowID != "." || view.ClockSchedules[0].Name != "morning" || view.ClockSchedules[2].FlowID != "worker" {
		t.Fatalf("describe lost compiled clock ordering: %#v", view.ClockSchedules)
	}
	for _, schedule := range view.ClockSchedules {
		if !strings.HasSuffix(schedule.SourceFile, "schema.yaml") {
			t.Fatalf("missing source provenance: %#v", schedule)
		}
	}
	var textOut bytes.Buffer
	stderr.Reset()
	if code := executeRootCommandWithOptions(t.Context(), RepoRoot(), []string{"describe", root}, &textOut, &stderr, defaultRootCommandOptions()); code != 0 {
		t.Fatal(stderr.String())
	}
	for _, want := range []string{"declared; arms on deployment", ".:poll every=5m emit=poll.tick", "worker:poll every=1m emit=poll.tick", "cron=0 9 * * * UTC"} {
		if !strings.Contains(textOut.String(), want) {
			t.Fatalf("missing %q in %s", want, textOut.String())
		}
	}
	if strings.Contains(textOut.String(), "next_due") || strings.Contains(textOut.String(), "active schedule") {
		t.Fatal("offline describe fabricated live activation")
	}
}
