package serveapp

import (
	"context"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/store/storetest"
)

func readServedSourceForkCardinality(reader servedEventCountReader, sourceRun string) (int, error) {
	if reader == nil {
		return 0, fmt.Errorf("source fork count requires the original selected reader")
	}
	var count int
	err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		count, err = storetest.CountSelectedForkRunsForSource(ctx, reader, sourceRun)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func TestServedSourceForkCardinalityRefusesMissingSnapshotOwner(t *testing.T) {
	if count, err := readServedSourceForkCardinality(nil, "missing-source"); err == nil || count != 0 {
		t.Fatalf("missing source fork owner returned provisional evidence: %d %v", count, err)
	}
}
