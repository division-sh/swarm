package conformance

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func assertIssue2655ChildCutContents(t *testing.T, ctx context.Context, f *deploymentResourceFixture, sourceRun, childRun string, point runfork.RunForkPoint, marker, feedVersion string) {
	t.Helper()
	snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, f.selected)
	if err != nil {
		t.Fatal(err)
	}
	rows := make(map[string][]map[string]storetest.SelectedForkSnapshotCell, len(snapshot))
	for table, evidence := range snapshot {
		rows[table], err = storetest.DecodeSelectedForkSnapshotRows(evidence)
		if err != nil {
			t.Fatalf("decode %s cut evidence: %v", table, err)
		}
	}
	text := func(row map[string]storetest.SelectedForkSnapshotCell, column string) string {
		t.Helper()
		cell, ok := row[column]
		if !ok {
			t.Fatalf("cut evidence lacks column %s", column)
		}
		value, err := cell.Text()
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	childEvents := map[string]bool{}
	sourceMarker := false
	for _, row := range rows["events"] {
		runID, eventID := text(row, "run_id"), text(row, "event_id")
		if runID == childRun {
			childEvents[eventID] = true
		}
		if runID == sourceRun && eventID == marker {
			sourceMarker = true
		}
	}
	if !sourceMarker {
		t.Fatal("post-cut marker disappeared from the advanced source")
	}
	counts := map[string]int{}
	for table, records := range rows {
		for _, row := range records {
			childOwned := false
			for _, cell := range row {
				if cell.Type != "string" && cell.Type != "[]uint8" {
					continue
				}
				value, err := cell.Text()
				if err != nil {
					t.Fatal(err)
				}
				childOwned = childOwned || value == childRun || childEvents[value]
			}
			if !childOwned {
				continue
			}
			counts[table]++
			for column, cell := range row {
				if cell.Type != "string" && cell.Type != "[]uint8" {
					continue
				}
				value, err := cell.Text()
				if err != nil || strings.Contains(value, marker) {
					t.Fatalf("post-cut marker entered child %s.%s: %s err=%v", table, column, value, err)
				}
			}
		}
	}
	for _, table := range []string{"flow_instances", "entity_state", "run_fork_fact_revisions"} {
		if counts[table] == 0 {
			t.Fatalf("child has no retained at-cut %s evidence", table)
		}
	}
	lineage := false
	for _, row := range rows["runs"] {
		if text(row, "run_id") != childRun {
			continue
		}
		var revision int64
		if err := json.Unmarshal(row["forked_from_revision"].Value, &revision); err != nil {
			t.Fatal(err)
		}
		lineage = text(row, "forked_from_run_id") == sourceRun && text(row, "forked_from_point_kind") == string(point.Kind) &&
			revision == point.Revision && text(row, "forked_from_event_id") == point.EventID
	}
	if !lineage {
		t.Fatalf("child lacks exact at-cut lineage at revision %d", point.Revision)
	}
	if feedVersion != "" {
		retainedFeed := false
		for _, row := range rows["fan_out_intents"] {
			if text(row, "run_id") == childRun && text(row, "origin_kind") == "deployment" && text(row, "source_resource_version_id") == feedVersion {
				retainedFeed = true
			}
		}
		if !retainedFeed {
			t.Fatal("child lost the positive admitted deployment obligation")
		}
	}
}

func issue2655RunPinnedVersion(t *testing.T, f *deploymentResourceFixture, runID string) string {
	t.Helper()
	snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(f.ctx, f.selected)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := storetest.DecodeSelectedForkSnapshotRows(snapshot["resource_version_pins"])
	if err != nil {
		t.Fatal(err)
	}
	var versions []string
	for _, row := range rows {
		id, err := row["run_id"].Text()
		if err != nil {
			t.Fatal(err)
		}
		if id == runID {
			version, err := row["version_id"].Text()
			if err != nil {
				t.Fatal(err)
			}
			versions = append(versions, version)
		}
	}
	if len(versions) != 1 || versions[0] == "" {
		t.Fatalf("source has no exact single admitted pin: %v", versions)
	}
	return versions[0]
}
