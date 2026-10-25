package apiv1

import (
	"context"
	"github.com/division-sh/swarm/internal/store/storetest"
	"testing"
)

func finiteRunStartDurableCounts(t *testing.T, fixture dataRunLifecycleFixture) storetest.FiniteRunStartStorageCounts {
	t.Helper()
	counts, err := storetest.ReadFiniteRunStartStorageCounts(context.Background(), fixture.primary)
	if err != nil {
		t.Fatal(err)
	}
	return counts
}
