package runtimepersistence

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	decisionstore "github.com/division-sh/swarm/internal/store/internal/backend/decisioncard"
)

// A projection counterexample, not a substitute for legal gate creation or
// completion: construct a header, then put a gate only in its canonical bucket.
func TestConstructedHeaderGateSummaryBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			for _, shape := range []string{"fieldless", "zero"} {
				for _, status := range []string{"open", "superseded", "malformed", "unproven_decision"} {
					t.Run(shape+"/"+status, func(t *testing.T) {
						_, child, _ := seedSelectedOrdinaryRootProjectionFixture(t, fixture, backend.name == "postgres", shape)
						ctx, at := testAuthorActivityContext(), time.Now().UTC()
						gate, err := gateruntime.New(child.ForkRunID, child.ForkRunID, child.ForkRunID, ".", "pending", "review", authorActivityTestBundleHash, testGateRoutes(t), "state:pending", at)
						if err != nil {
							t.Fatal(err)
						}
						wantOpen, wantMalformed := 0, 0
						switch status {
						case "open":
							wantOpen = 1
						case "superseded":
							if !gate.Supersede("run_terminal", at) {
								t.Fatal("gate supersession refused")
							}
						case "unproven_decision":
							if err := gate.CommitDecision(child.ForkRunID, at); err != nil {
								t.Fatal(err)
							}
							wantMalformed = 1
						case "malformed":
							wantMalformed = 1
						}
						buckets := map[string]map[string]any{}
						if err := gateruntime.Store(buckets, gate); err != nil {
							t.Fatal(err)
						}
						if status == "malformed" {
							buckets[gateruntime.BucketKey][gate.Key()].(map[string]any)["status"] = "not-a-status"
						}
						raw, err := json.Marshal(buckets)
						if err != nil {
							t.Fatal(err)
						}
						result, err := fixture.db.ExecContext(ctx, `UPDATE flow_instances SET accumulator=$1 WHERE run_id=$2 AND entity_id=$2`, string(raw), child.ForkRunID)
						if err != nil {
							t.Fatal(err)
						}
						if n, err := result.RowsAffected(); err != nil || n != 1 {
							t.Fatalf("header injection: %d %v", n, err)
						}
						dialect := decisionstore.SummaryDialectSQLite
						if backend.name == "postgres" {
							dialect = decisionstore.SummaryDialectPostgres
						}
						summary, err := decisionstore.ReadRunSummary(ctx, fixture.db, dialect, child.ForkRunID)
						if err != nil {
							t.Fatal(err)
						}
						if summary.OpenGateObligations != wantOpen || summary.MalformedObligations != wantMalformed {
							t.Fatalf("header gate summary = %+v; want open=%d malformed=%d", summary, wantOpen, wantMalformed)
						}
					})
				}
			}
		})
	}
}
