package conformance

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestVolumeFanOutServingCardinalityMixedOutputPartitionEquivalenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			// The existing reporter test owns N500. These are the missing boundary
			// and partition comparisons, not another copy of that workload.
			for _, count := range []int{0, 1, 31, 32, 33, 64} {
				t.Run(fmt.Sprintf("N%d", count), func(t *testing.T) {
					f := newSemanticProofFixture(t, backend, true)
					rows := semanticProofRows(count)
					rejected := map[int]bool{}
					for _, ordinal := range []int{1, 31, 32} {
						if ordinal < count {
							rows[ordinal]["gem_score"] = "not-a-number"
							rejected[ordinal] = true
						}
					}
					var baseline []semanticProofOutput
					for _, limit := range []int{32, 16, 1} {
						t.Logf("serving N%d with effective commit cap %d", count, limit)
						release := f.pauseAtEmptyScan(t)
						trigger := f.submit(t, semanticProofPayloadEvent, rows, 75)
						state := "open"
						if count == 0 {
							state = "closed"
						}
						f.waitIntent(t, trigger, 0, state)
						f.probe.mu.Lock()
						f.probe.policies[trigger] = semanticProofPolicy{maxCommit: limit}
						f.probe.mu.Unlock()
						var eager int
						if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM fan_out_outcomes o JOIN event_deliveries d ON d.delivery_id=o.triggering_delivery_id WHERE o.run_id=$1 AND d.event_id=$2`, f.runID, trigger).Scan(&eager); err != nil || eager != 0 {
							t.Fatalf("N%d emitted before serving: %d %v", count, eager, err)
						}
						release()
						// This is semantic partition equivalence, including repeated
						// safe rejections after cap reset, not the 500-item latency gate.
						timeout := 30 * time.Second
						if fanOutRaceBuild && count == 64 && limit == 1 {
							// Gate A 5749347759 changes only this race-mode phase.
							timeout = time.Minute
						}
						started := time.Now()
						waitNotifyAllChildrenRuntimeWithin(t, f.runtime, f.runID, timeout)
						if elapsed := time.Since(started); elapsed >= timeout {
							t.Fatalf("N%d limit%d quiescence took %s, must be below %s", count, limit, elapsed, timeout)
						} else {
							t.Logf("N%d limit%d quiescence=%s bound=%s race=%t", count, limit, elapsed, timeout, fanOutRaceBuild)
						}
						outputs := f.assertOutcomes(t, trigger, rows, 75, rejected)
						if limit == 32 {
							baseline = outputs
						} else if !reflect.DeepEqual(outputs, baseline) {
							t.Fatalf("N%d limit%d changed semantic output/order: got=%+v want=%+v", count, limit, outputs, baseline)
						}
						f.probe.mu.Lock()
						attempts := append([]semanticProofAttempt(nil), f.probe.attempts[trigger]...)
						sqlFaults := append([]semanticProofAttempt(nil), f.probe.sqlFaults[trigger]...)
						outcomeFaults := f.probe.outcomeSQLFaults[trigger]
						f.probe.mu.Unlock()
						var injected []semanticProofAttempt
						for _, attempt := range attempts {
							if attempt.injected {
								injected = append(injected, attempt)
							}
						}
						if !reflect.DeepEqual(sqlFaults, injected) {
							t.Fatalf("N%d limit%d native rollback evidence=%+v, injected attempts=%+v", count, limit, sqlFaults, injected)
						}
						t.Logf("N%d limit%d native rollback faults=%d outcome-only=%d; no attempted event/outcome rows leaked", count, limit, len(sqlFaults), outcomeFaults)
						cursor := 0
						for _, attempt := range attempts {
							if attempt.start != cursor || attempt.count < 1 || attempt.count > 32 {
								t.Fatalf("N%d limit%d unbounded/non-prefix attempt: %+v cursor=%d", count, limit, attempt, cursor)
							}
							if !attempt.injected {
								if attempt.count > limit {
									t.Fatalf("partition limit ignored: %+v limit=%d", attempt, limit)
								}
								cursor += attempt.count
							}
						}
						if cursor != count {
							t.Fatalf("N%d limit%d prefix ledger=%d attempts=%+v", count, limit, cursor, attempts)
						}
					}
				})
			}
		})
	}
}
