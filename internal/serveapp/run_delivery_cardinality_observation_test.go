package serveapp

import (
	"context"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/store/storetest"
)

func readServedRunDeadLetterCount(ctx context.Context, reader servedEventCountReader, runID string) (int, error) {
	if reader == nil {
		return 0, fmt.Errorf("dead-letter count requires the original selected reader")
	}
	var count int
	err := reader.InspectSnapshot(ctx, func(scoped context.Context) error {
		var err error
		count, err = storetest.CountRunDeadLetterDeliveries(scoped, reader, runID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func readServedRunNonterminalDeliveryCount(ctx context.Context, reader servedEventCountReader, runID string) (int, error) {
	if reader == nil {
		return 0, fmt.Errorf("nonterminal count requires the original selected reader")
	}
	var count int
	err := reader.InspectSnapshot(ctx, func(scoped context.Context) error {
		var err error
		count, err = storetest.CountRunNonterminalDeliveryStorage(scoped, reader, runID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func TestServedRunDeliveryCardinalityRefusesMissingSnapshotOwner(t *testing.T) {
	for _, read := range []func(context.Context, servedEventCountReader, string) (int, error){readServedRunDeadLetterCount, readServedRunNonterminalDeliveryCount} {
		if count, err := read(context.Background(), nil, "source-run"); err == nil || count != 0 {
			t.Fatalf("missing reader supplied cardinality evidence: %d %v", count, err)
		}
	}
}
