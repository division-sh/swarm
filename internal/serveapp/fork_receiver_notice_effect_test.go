package serveapp

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/store/storetest"
)

func readForkReceiverNoticeDomain(t *testing.T, reader receiverProofStateReader, runID string) []string {
	t.Helper()
	out, err := storetest.ReadForkReceiverNoticeStorage(context.Background(), reader, runID)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
