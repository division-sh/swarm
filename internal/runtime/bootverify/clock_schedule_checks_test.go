package bootverify

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestClockScheduleBareEventAndConsumerLaw(t *testing.T) {
	for _, tc := range []struct {
		name, fields, pins, nodes, want string
	}{
		{name: "root export", pins: "pins:\n  outputs:\n    events: [poll.tick]\n"},
		{name: "actual local handler", nodes: "observer:\n  event_handlers:\n    poll.tick: {}\n"},
		{name: "dangling emission", want: "actual consumer"},
		{name: "input is not consumer", pins: "pins:\n  inputs:\n    events: [poll.tick]\n", want: "actual consumer"},
		{name: "required field", fields: "  reason: text\n", pins: "pins:\n  outputs:\n    events: [poll.tick]\n", want: "bare event"},
		{name: "optional field", fields: "  reason: text?\n", pins: "pins:\n  outputs:\n    events: [poll.tick]\n", want: "bare event"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			for name, content := range map[string]string{
				"schema.yaml": "stages: []\nschedules:\n  poll: {every: 5m, emit: poll.tick}\n" + tc.pins,
				"events.yaml": "poll.tick:\n" + tc.fields,
			} {
				if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if tc.nodes != "" {
				if err := os.WriteFile(filepath.Join(root, "nodes.yaml"), []byte(tc.nodes), 0600); err != nil {
					t.Fatal(err)
				}
			}
			repo := repoRootForBootverifyTest(t)
			bundle := loadFixtureBundleAt(t, repo, root, contracts.DefaultPlatformSpecFile(repo))
			findings := checkClockScheduleValidation(newCheckerContext(context.Background(), semanticview.Wrap(bundle), Options{}))
			if tc.want == "" {
				if len(findings) != 0 {
					t.Fatalf("lawful clock refused: %#v", findings)
				}
				if report := Run(t.Context(), semanticview.Wrap(bundle), Options{Purpose: StructuralValidation}); len(report.Errors()) != 0 {
					t.Fatalf("lawful clock failed full structural verification: %#v", report.Errors())
				}
			} else if len(findings) != 1 || !strings.Contains(findings[0].Message, tc.want) {
				t.Fatalf("missing exact clock refusal %q: %#v", tc.want, findings)
			}
		})
	}
}

func TestClockScheduleRequiresActualConnectionToPrivateConsumer(t *testing.T) {
	for _, connected := range []bool{false, true} {
		t.Run(map[bool]string{false: "unconnected same name", true: "compiled connection"}[connected], func(t *testing.T) {
			root := canonicalrouting.CopyParentConnectClock(t, connected)
			repo := repoRootForBootverifyTest(t)
			bundle := loadFixtureBundleAt(t, repo, root, contracts.DefaultPlatformSpecFile(repo))
			findings := checkClockScheduleValidation(newCheckerContext(t.Context(), semanticview.Wrap(bundle), Options{}))
			if connected && len(findings) != 0 {
				t.Fatalf("real compiled consumer refused: %#v", findings)
			}
			if connected {
				if report := Run(t.Context(), semanticview.Wrap(bundle), Options{Purpose: StructuralValidation}); len(report.Errors()) != 0 {
					t.Fatalf("connected clock failed full structural verification: %#v", report.Errors())
				}
			}
			if !connected && (len(findings) != 1 || !strings.Contains(findings[0].Message, "actual consumer")) {
				t.Fatalf("same-name child manufactured consumer: %#v", findings)
			}
		})
	}
}
