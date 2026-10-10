package cataloge2e

import (
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestScatterGatherFrontierAggregatePreservesRefusalsBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := newRuntimeHarnessForBackend(t, filepath.Join(canonicalrouting.RepoRoot(t), "internal/runtime/cataloge2e/testdata/scatter-gather-safety"), backend, true)
			reader, err := h.catalogOperatorEventLister()
			if err != nil {
				t.Fatal(err)
			}
			// Statement-local cases run the exact owner's polling query. They
			// neither mutate runtime rows nor replace final public assertions.
			for _, tc := range []struct {
				name      string
				probe     storetest.CausalDeliveryFrontierProbe
				unsettled int
			}{
				{"complete", storetest.FrontierComplete, 0},
				{"missing", storetest.FrontierMissing, 2},
				{"pending", storetest.FrontierPending, 1},
				{"duplicate-delivery", storetest.FrontierDuplicateDelivery, 1},
				{"dead-lettered", storetest.FrontierDeadLetterDelivery, 1},
			} {
				t.Run(tc.name, func(t *testing.T) {
					frontier, err := storetest.ProbeCausalDeliveryFrontier(h.ctx, reader, tc.probe)
					if err != nil {
						t.Fatal(err)
					}
					observed, unsettled, dead := frontier.Observed, frontier.Unsettled, frontier.DeadLetters
					if observed != 3 || unsettled != tc.unsettled || dead != 2 {
						t.Fatalf("frontier=%d unsettled=%d dead=%d; want 3/%d/2", observed, unsettled, dead, tc.unsettled)
					}
				})
			}
		})
	}
}
