package serveapp

import (
	"context"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/store/storetest"
)

func readServedRunPhysicalIdentity(reader servedEventCountReader, runID string) (int, error) {
	if reader == nil {
		return 0, fmt.Errorf("served run identity count requires the original selected reader")
	}
	var count int
	err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		count, err = storetest.CountPhysicalRunIdentity(ctx, reader, runID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func TestServedRunIdentityRefusesMissingSnapshotOwner(t *testing.T) {
	if count, err := readServedRunPhysicalIdentity(nil, "missing-run"); err == nil || count != 0 {
		t.Fatalf("missing run identity owner returned provisional evidence: %d %v", count, err)
	}
}
