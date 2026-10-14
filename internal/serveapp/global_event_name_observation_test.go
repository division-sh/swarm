package serveapp

import (
	"context"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/store/storetest"
)

func readServedGlobalEventNameCount(ctx context.Context, reader servedEventCountReader, eventName string) (int, error) {
	if reader == nil {
		return 0, fmt.Errorf("global event-name observation requires the original selected reader")
	}
	var count int
	err := reader.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		count, err = storetest.CountEventNameStorage(ctx, reader, eventName)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func TestServedGlobalEventNameCountRefusesMissingOwner(t *testing.T) {
	if count, err := readServedGlobalEventNameCount(context.Background(), nil, "platform.boot"); count != 0 || err == nil {
		t.Fatalf("missing owner count=%d err=%v", count, err)
	}
}
