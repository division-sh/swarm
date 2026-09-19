package cataloge2e

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestScatterGatherFrontierAggregatePreservesRefusalsBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			h := newRuntimeHarnessForBackend(t, filepath.Join(canonicalrouting.RepoRoot(t), "internal/runtime/cataloge2e/testdata/scatter-gather-safety"), backend, true)
			// Statement-local tables exercise the actual polling query without
			// corrupting runtime state or replacing the final public assertions.
			prefix := `WITH RECURSIVE events(event_id,run_id,event_name,source_event_id) AS (VALUES
				('root','run','root',''), ('child','run','child','root'), ('grandchild','run','grandchild','child'),
				('log','run','platform.runtime_log','root'), ('log-child','run','hidden','log'),
				('foreign','other','child','root')),
			dead_letters(original_event_id) AS (VALUES ('child'),('child'),('foreign'),('log')),
			event_deliveries(delivery_id,event_id,status) AS (VALUES `
			for _, tc := range []struct {
				name, deliveries string
				unsettled        int
			}{
				{"complete", "('r','root','delivered'),('c','child','delivered'),('g','grandchild','delivered')", 0},
				{"missing", "('r','root','delivered')", 2},
				{"pending", "('r','root','delivered'),('c','child','pending'),('g','grandchild','delivered')", 1},
				{"duplicate-delivery", "('r','root','delivered'),('c','child','delivered'),('c2','child','delivered'),('g','grandchild','delivered')", 1},
				{"dead-lettered", "('r','root','delivered'),('c','child','dead_letter'),('g','grandchild','delivered')", 1},
			} {
				t.Run(tc.name, func(t *testing.T) {
					query := prefix + tc.deliveries + "), " + strings.TrimPrefix(scatterGatherFrontierQuery, "WITH RECURSIVE ")
					var observed, unsettled, dead int
					if err := h.db.QueryRowContext(h.ctx, query, "run", "root").Scan(&observed, &unsettled, &dead); err != nil {
						t.Fatal(err)
					}
					if observed != 3 || unsettled != tc.unsettled || dead != 2 {
						t.Fatalf("frontier=%d unsettled=%d dead=%d; want 3/%d/2", observed, unsettled, dead, tc.unsettled)
					}
				})
			}
		})
	}
}
