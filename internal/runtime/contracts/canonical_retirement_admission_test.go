package contracts

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestCanonicalRetirementBundleAdmission(t *testing.T) {
	repoRoot := contractRepoRoot(t)
	platformSpec := DefaultPlatformSpecFile(repoRoot)
	baseSchema := "name: retirement-proof\n"
	cases := []struct {
		name      string
		file      string
		canonical string
		retired   string
		want      string
		output    bool
	}{
		{
			name:      "node map key owns identity",
			file:      "nodes.yaml",
			canonical: "worker:\n  event_handlers: {}\n",
			retired:   "worker:\n  id: worker\n  event_handlers: {}\n",
			want:      `node field "id" is not supported`,
		},
		{
			name:      "event payload is not emitter metadata",
			file:      "events.yaml",
			canonical: "work.requested:\n  work_id: text\n",
			retired:   "work.requested:\n  work_id: text\n  emitter: worker\n",
			want:      `event field name "emitter"`,
		},
		{
			name:      "input pin has derived name",
			file:      "schema.yaml",
			canonical: baseSchema + "pins:\n  inputs: [work.requested]\n",
			retired:   baseSchema + "pins:\n  inputs:\n    - name: work_requested\n      event: work.requested\n",
			want:      `event pin field "name" is not supported`,
		},
		{
			name:      "input pin carries are derived",
			file:      "schema.yaml",
			canonical: baseSchema + "pins:\n  inputs: [work.requested]\n",
			retired:   baseSchema + "pins:\n  inputs:\n    - event: work.requested\n      carries: [work_id]\n",
			want:      `event pin field "carries" is not supported`,
		},
		{
			name:      "output pin key is derived",
			file:      "schema.yaml",
			canonical: baseSchema + "pins:\n  outputs: [work.completed]\n",
			retired:   baseSchema + "pins:\n  outputs:\n    - event: work.completed\n      key: work_id\n",
			want:      `must be a scalar text`,
			output:    true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeFixtureFile(t, filepath.Join(root, "schema.yaml"), baseSchema)
			writeFixtureFile(t, filepath.Join(root, "events.yaml"), "work.requested:\n  work_id: text\nwork.completed:\n  work_id: text\n")
			writeFixtureFile(t, filepath.Join(root, tc.file), tc.canonical)
			bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, platformSpec)
			if err != nil {
				t.Fatalf("canonical bundle admission: %v", err)
			}
			switch tc.file {
			case "nodes.yaml":
				if _, ok := bundle.Nodes["worker"]; !ok {
					t.Fatal("canonical node was not admitted")
				}
			case "events.yaml":
				if _, ok := bundle.Events["work.requested"]; !ok {
					t.Fatal("canonical event was not admitted")
				}
			case "schema.yaml":
				if bundle.RootSchema == nil {
					t.Fatal("canonical root schema was not admitted")
				}
				if tc.output {
					if pins := bundle.RootSchema.Pins.Outputs.EventPins; len(pins) != 1 || pins[0].EventType() != "work.completed" {
						t.Fatalf("canonical output pins = %#v", pins)
					}
				} else if pins := bundle.RootSchema.Pins.Inputs.EventPins; len(pins) != 1 || pins[0].EventType() != "work.requested" {
					t.Fatalf("canonical input pins = %#v", pins)
				}
			}
			writeFixtureFile(t, filepath.Join(root, tc.file), tc.retired)
			if _, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, platformSpec); err == nil || !contractErrorContains(err, tc.want) {
				t.Fatalf("retired bundle admission error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestCanonicalConnectBundleAdmission(t *testing.T) {
	repoRoot := contractRepoRoot(t)
	platformSpec := DefaultPlatformSpecFile(repoRoot)
	root := canonicalrouting.CopyExample(t, canonicalrouting.ParentConnect)
	schemaPath := filepath.Join(root, "schema.yaml")
	schema, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, platformSpec)
	if err != nil {
		t.Fatalf("canonical connected bundle admission: %v", err)
	}
	if bundle.RootSchema == nil || len(bundle.RootSchema.Connect) != 2 {
		t.Fatalf("canonical connect rows = %#v", bundle.RootSchema)
	}
	const canonicalRow = "  - event: work.ready\n    from: producer\n    to: consumer\n"
	for _, tc := range []struct {
		name string
		row  string
		want string
	}{
		{
			name: "retired adapter",
			row:  canonicalRow + "    adapter: ready_to_consumer\n",
			want: `connect field "adapter" is not supported`,
		},
		{
			name: "retired endpoint-centric row",
			row:  "  - from: producer.work_ready\n    to: consumer.work_ready\n",
			want: "endpoint-centric connect rows",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			retired := strings.Replace(string(schema), canonicalRow, tc.row, 1)
			if retired == string(schema) {
				t.Fatal("canonical fixture no longer contains the expected connect row")
			}
			writeFixtureFile(t, schemaPath, retired)
			if _, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, platformSpec); err == nil || !contractErrorContains(err, tc.want) {
				t.Fatalf("retired connected bundle admission error = %v, want %q", err, tc.want)
			}
		})
	}
}
