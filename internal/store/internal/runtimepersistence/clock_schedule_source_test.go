package runtimepersistence

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestClockScheduleSourceRoundTripOnBothStores(t *testing.T) {
	for _, tc := range selectedScheduleStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, db, ctx := tc.open(t)
			root := t.TempDir()
			files := map[string]string{
				"schema.yaml":        "# Preserve exact source\nstages: []\nschedules:\n  poll: {every: 5m, emit: poll.tick}\n  morning: {cron: '0 9 * * *', emit: poll.tick}\npins:\n  outputs: [poll.tick]\n",
				"events.yaml":        "poll.tick:\n",
				"worker/schema.yaml": "stages: []\nschedules:\n  poll: {every: 1m, emit: poll.tick}\n",
				"worker/events.yaml": "poll.tick:\n",
				"worker/nodes.yaml":  "observer:\n  event_handlers:\n    poll.tick: {}\n",
			}
			for name, body := range files {
				path := filepath.Join(root, name)
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(body), 0600); err != nil {
					t.Fatal(err)
				}
			}
			repo := pipeline.WorkflowRepoRoot()
			disk, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, root, contracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			catalogStore := store.(selectedSourceArtifactStore)
			if _, err := catalogStore.EnsureSourceArtifact(ctx, disk.SourceArtifact); err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(root); err != nil {
				t.Fatal(err)
			}
			persisted, err := catalogStore.GetSourceArtifact(ctx, disk.SourceArtifact.BundleHash())
			if err != nil {
				t.Fatal(err)
			}
			artifact, err := persisted.Decode()
			if err != nil {
				t.Fatal(err)
			}
			if artifact.BundleHash() != disk.SourceArtifact.BundleHash() || !bytes.Equal(artifact.LogicalBlob(), disk.SourceArtifact.LogicalBlob()) {
				t.Fatal("clock source hash or logical bytes changed")
			}
			for name, body := range files {
				entry, ok := artifact.Entry(name)
				if !ok || !bytes.Equal(entry.Bytes(), []byte(body)) {
					t.Fatalf("clock member %s changed", name)
				}
			}
			retained, err := contracts.LoadWorkflowContractBundleFromArtifact(repo, artifact, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{})
			if err != nil {
				t.Fatal(err)
			}
			original := semanticview.ClockSchedules(semanticview.Wrap(disk))
			reloaded := semanticview.ClockSchedules(semanticview.Wrap(retained))
			if len(original) != 3 || len(reloaded) != len(original) {
				t.Fatalf("retained clock declarations changed: %v / %v", original, reloaded)
			}
			for i := range original {
				if !strings.HasSuffix(reloaded[i].SourceFile, "schema.yaml") {
					t.Fatalf("clock provenance lost: %#v", reloaded[i])
				}
				original[i].SourceFile, reloaded[i].SourceFile = "", ""
			}
			if !reflect.DeepEqual(original, reloaded) {
				t.Fatalf("clock compiled projection changed: %#v / %#v", original, reloaded)
			}
			var timers int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM timers").Scan(&timers); err != nil || timers != 0 {
				t.Fatalf("source admission or offline reconstruction armed clocks: count=%d err=%v", timers, err)
			}
		})
	}
}
