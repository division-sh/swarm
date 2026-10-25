package runforkpersistence_test

import (
	"testing"

	"github.com/google/uuid"
)

func TestRetainedFanOutExecutionRootFollowsExactMaterializationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newSelectedRootedSourceFixture(t, backend)
			child := uuid.NewString()
			fixture.createFork(t, child, fixture.childID, 9, fixture.parent)
			fixture.association(t, child)
		})
	}
}
