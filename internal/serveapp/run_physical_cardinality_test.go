package serveapp

import (
	"context"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/store/storetest"
)

func readServedRunFieldRows(reader servedEventCountReader, runID string) (int, error) {
	if reader == nil {
		return 0, fmt.Errorf("served field count requires the original selected reader")
	}
	var count int
	err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		count, err = storetest.CountWorkflowRunFieldRows(ctx, reader, runID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func readServedRunConstructionHeaders(reader servedEventCountReader, runID string) (int, error) {
	if reader == nil {
		return 0, fmt.Errorf("served header count requires the original selected reader")
	}
	var count int
	err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		count, err = storetest.CountRunFlowInstanceStorage(ctx, reader, runID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func TestServedRunPhysicalCountsRefuseMissingSnapshotOwner(t *testing.T) {
	if count, err := readServedRunFieldRows(nil, "missing-run"); err == nil || count != 0 {
		t.Fatalf("missing field owner returned provisional evidence: %d %v", count, err)
	}
	if count, err := readServedRunConstructionHeaders(nil, "missing-run"); err == nil || count != 0 {
		t.Fatalf("missing header owner returned provisional evidence: %d %v", count, err)
	}
}
