package runtimepersistence

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/engine"
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

// Actual constructor/gate admission and the native terminal supersession writer.
// The run-freeze transaction is a component proof, not public fork qualification.
func TestConstructedHeaderGateFreezeBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, fields := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fields=%t", backend.name, fields), func(t *testing.T) {
				f := newForkContentionFixtureForFields(t, backend, fields)
				workflows := f.store.(workflowTestSelectedStore)
				owner := f.state.Identity
				before, found, err := workflows.LoadWorkflowInstance(f.ctx, owner)
				if err != nil || !found {
					t.Fatalf("constructed gate header: found=%t err=%v", found, err)
				}
				dialect := decisionstore.SummaryDialectSQLite
				if backend.name == "postgres" {
					dialect = decisionstore.SummaryDialectPostgres
				}
				assertSummary := func(wantOpen int) {
					t.Helper()
					summary, err := decisionstore.ReadRunSummary(f.ctx, f.db, dialect, f.runID)
					if err != nil || summary.OpenGateObligations != wantOpen || summary.MalformedObligations != 0 {
						t.Fatalf("native gate summary: %+v err=%v; want open=%d", summary, err, wantOpen)
					}
				}
				assertSummary(1)
				if err := freezeDecisionCardRunInTestMutation(f.ctx, workflows, f.runID, time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
				after, found, err := workflows.LoadWorkflowInstance(f.ctx, owner)
				if err != nil || !found || after.Revision != before.Revision+1 || !reflect.DeepEqual(after.Fields, before.Fields) || after.CurrentState != before.CurrentState {
					t.Fatalf("terminal writer changed business fields/stage or lost header CAS: before=%+v after=%+v found=%t err=%v", before, after, found, err)
				}
				carrier, err := engine.StateCarrierFromPersisted(after.Fields, after.Bookkeeping, after.Gates, after.StateBuckets)
				if err != nil {
					t.Fatal(err)
				}
				gate, found, err := gateruntime.Load(carrier.StateBuckets, ".", "review")
				if err != nil || !found || gate.Status != gateruntime.StatusSuperseded || gate.DecisionEventID != f.eventID || gate.CardID != f.cardID {
					t.Fatalf("terminal canonical gate: %+v found=%t err=%v", gate, found, err)
				}
				assertSummary(0)
				if !decisionGateStatusMutationExists(t, f.ctx, f.db, backend.name == "postgres", f.runID, f.entityID, string(gateruntime.StatusSuperseded)) {
					t.Fatal("terminal header CAS omitted its journal")
				}
				var count int
				wantCount := 0
				if fields {
					wantCount = 1
				}
				if err := f.db.QueryRow(`SELECT COUNT(*) FROM entity_state WHERE run_id=$1 AND entity_id=$2`, f.runID, f.entityID).Scan(&count); err != nil || count != wantCount {
					t.Fatalf("optional fields materialized without a declaration: count=%d err=%v", count, err)
				}
			})
		}
	}
}
