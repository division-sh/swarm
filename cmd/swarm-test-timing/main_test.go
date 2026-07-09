package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateShardSnapshotUsesSourceLabel(t *testing.T) {
	dir := t.TempDir()
	packagesPath := filepath.Join(dir, "packages.txt")
	weightsPath := filepath.Join(dir, "weights.json")
	snapshotPath := filepath.Join(dir, "snapshot.json")

	if err := os.WriteFile(packagesPath, []byte("github.com/division-sh/swarm/a\ngithub.com/division-sh/swarm/b\n"), 0o600); err != nil {
		t.Fatalf("write packages: %v", err)
	}
	weights := []byte(`{"Action":"pass","Package":"github.com/division-sh/swarm/a","Elapsed":3}` + "\n" +
		`{"Action":"pass","Package":"github.com/division-sh/swarm/b","Elapsed":1}` + "\n")
	if err := os.WriteFile(weightsPath, weights, 0o600); err != nil {
		t.Fatalf("write weights: %v", err)
	}

	if err := run(runConfig{
		packagesPath:   packagesPath,
		weightsPath:    weightsPath,
		snapshotPath:   snapshotPath,
		sourceLabel:    "github-actions-run-123",
		shardCount:     2,
		maxImbalance:   0.25,
		generateShards: true,
	}); err != nil {
		t.Fatalf("run generate shards: %v", err)
	}

	snapshot, err := readShardSnapshot(snapshotPath)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if snapshot.Source != "github-actions-run-123" {
		t.Fatalf("source = %q, want source label", snapshot.Source)
	}
	if snapshot.ShardCount != 2 || len(snapshot.Shards) != 2 {
		t.Fatalf("snapshot shards = %+v, want 2 shards", snapshot.Shards)
	}
}
