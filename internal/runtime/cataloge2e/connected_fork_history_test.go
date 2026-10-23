package cataloge2e

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"

	"testing"

	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestConnectedForkFutureSuccessArtifactsPreserved(t *testing.T) {
	for name, want := range map[string]string{
		"connected_fork_readiness_success_test.go.txt": "d7c22f96f5c59896ee8c9ac18257975086f8da603bcbd4ca40b9639383fc8392",
		"connected_fork_readiness_fixture.go.txt":      "16e427c354f33b812e5eba9a86602433ece5d756b75c08b354a166762e6e75f4",
	} {
		raw, err := os.ReadFile("testdata/future_capabilities/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if got := fmt.Sprintf("%x", sha256.Sum256(raw)); got != want {
			t.Fatalf("#642 original success oracle %s changed: %s, want %s", name, got, want)
		}
	}
}

// Every application table, column and row participates. Only database-internal
// objects are excluded; the caller must join background writers first.
func snapshotCatalogApplication(t *testing.T, h *runtimeHarness) map[string]storetest.SelectedForkStorageTableSnapshot {
	t.Helper()
	reader, err := h.catalogOperatorEventLister()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(context.Background(), reader)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
