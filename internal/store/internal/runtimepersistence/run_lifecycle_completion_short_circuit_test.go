package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/division-sh/swarm/internal/testutil/stagecatalogfixture"
	"github.com/google/uuid"
)

func TestCompletionPendingPipelineDefersOnlyUnusedFanOutFoldBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, session := range []string{"none", "exact_expiry", "foreign_expiry", "malformed_holder"} {
			t.Run(backend+"/"+session, func(t *testing.T) {
				fixture := openRunLifecycleCandidateParityFixture(t, backend)
				ctx := testAuthorActivitySourceArtifactContext()
				fan, eventID, original := seedCompletionShortCircuitFanOut(t, fixture, ctx)
				catalog := completionShortCircuitCatalog()
				selectedNow := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
				expiry := time.Date(2100, 1, 1, 0, 0, 0, 123456000, time.UTC)
				switch session {
				case "exact_expiry":
					if err := insertCompletionBlockerSession(t, fixture, ctx, fan.runID, "worker", expiry); err != nil {
						t.Fatal(err)
					}
				case "malformed_holder":
					if err := insertCompletionBlockerSession(t, fixture, ctx, fan.runID, "worker", nil); err != nil {
						t.Fatal(err)
					}
				}
				if session == "exact_expiry" || session == "foreign_expiry" {
					foreign := seedCompletionBlockerRun(t, fixture, ctx)
					if err := insertCompletionBlockerSession(t, fixture, ctx, foreign, "foreign", expiry.Add(-time.Hour)); err != nil {
						t.Fatal(err)
					}
				}
				full, err := loadCompletionShortCircuitSummaries(ctx, fixture, fan.runID, selectedNow)
				if err != nil || !full.Delivery.Settled() || full.Pipeline.Replayable != 1 || !full.Pipeline.BlocksCompletion() || full.FanOut.Settled != 1 || !full.Entities.ReadyForCompletion() {
					t.Fatalf("fixture lacks exact pending-pipeline gate and otherwise complete owners: %+v err=%v", full, err)
				}
				if session == "exact_expiry" && !full.Sessions.NextExpiry.Equal(expiry) {
					t.Fatalf("session owner expiry=%s want=%s", full.Sessions.NextExpiry, expiry)
				}
				if session == "malformed_holder" && full.Sessions.MalformedLease != 1 {
					t.Fatalf("malformed session did not remain a blocker: %+v", full.Sessions)
				}
				corrupt := corruptCompletionShortCircuitSettlement(t, original)
				writeCompletionShortCircuitSettlement(t, fixture, ctx, eventID, corrupt)
				defer writeCompletionShortCircuitSettlement(t, fixture, ctx, eventID, original)
				_, err = loadCompletionShortCircuitSummaries(ctx, fixture, fan.runID, selectedNow)
				assertJointSourceReadCorrupt(t, eventID, err)
				candidate := requestRunLifecycleCandidateParity(t, fixture, ctx, fan.runID).Candidate
				result, err := fixture.store.ExecuteCompletionCandidate(ctx, candidate, catalog)
				if err != nil {
					t.Fatalf("pending pipeline unnecessarily decoded corrupt fan-out settlement: %v", err)
				}
				assertCompletionShortCircuitRunning(t, fixture, ctx, fan.runID)
				if session == "exact_expiry" {
					if result.Outcome != runtimerunlifecycle.OutcomeRearmAt || !result.Candidate.DueAt.Equal(expiry) || result.Candidate.Revision <= candidate.Revision {
						t.Fatalf("lost exact session rearm: %+v", result)
					}
					if stored := loadRunLifecycleCandidate(t, fixture, ctx, fan.runID); !stored.SameIdentity(result.Candidate) {
						t.Fatalf("rearm differs from persisted candidate: %+v", stored)
					}
				} else {
					if result.Outcome != runtimerunlifecycle.OutcomeAwaitMutation {
						t.Fatalf("pending gate without current-run expiry: %+v", result)
					}
					_, due, revision := loadRunLifecycleCandidateFacts(t, fixture, ctx, fan.runID)
					if due || revision != candidate.Revision {
						t.Fatalf("blocked candidate clearance changed revision: due=%v revision=%d", due, revision)
					}
				}

				// A newer dirty candidate must survive a stale callback even
				// while the irrelevant fan-out bytes remain corrupt.
				current := forceRunLifecycleCandidateParity(t, fixture, ctx, fan.runID, selectedNow.Add(-time.Minute))
				staleResult, err := fixture.store.ExecuteCompletionCandidate(ctx, candidate, catalog)
				if err != nil || staleResult.Outcome != runtimerunlifecycle.OutcomeExactNoop || !loadRunLifecycleCandidate(t, fixture, ctx, fan.runID).SameIdentity(current) {
					t.Fatalf("stale callback lost current dirty generation: %+v err=%v", staleResult, err)
				}

				// Clear the gate through the pipeline owner while its input is
				// valid, then restore corruption before the next candidate.
				writeCompletionShortCircuitSettlement(t, fixture, ctx, eventID, original)
				owner := fixture.store.(pipelineObligationParityStore).PipelineObligations()
				settlePipelineParityEvent(t, ctx, owner, eventID, pipelineobligation.Acknowledged("pipeline_persisted"))
				pipelineSummary, err := owner.SummarizeRun(ctx, fan.runID)
				if err != nil || pipelineSummary.BlocksCompletion() || pipelineSummary.Replayable != 0 {
					t.Fatalf("pipeline gate did not clear authoritatively: %+v err=%v", pipelineSummary, err)
				}
				writeCompletionShortCircuitSettlement(t, fixture, ctx, eventID, corrupt)
				current = requestRunLifecycleCandidateParity(t, fixture, ctx, fan.runID).Candidate
				result, err = fixture.store.ExecuteCompletionCandidate(ctx, current, catalog)
				assertJointSourceReadCorrupt(t, eventID, err)
				if result.Outcome == runtimerunlifecycle.OutcomeTerminallyEligible || !loadRunLifecycleCandidate(t, fixture, ctx, fan.runID).SameIdentity(current) {
					t.Fatalf("corrupt required owner completed or consumed candidate: %+v", result)
				}
				assertCompletionShortCircuitRunning(t, fixture, ctx, fan.runID)
				_, err = loadCompletionShortCircuitSummaries(ctx, fixture, fan.runID, selectedNow)
				assertJointSourceReadCorrupt(t, eventID, err)

				writeCompletionShortCircuitSettlement(t, fixture, ctx, eventID, original)
				if _, err := fixture.db.ExecContext(ctx, `UPDATE agent_sessions SET lease_holder=NULL,lease_expires_at=NULL WHERE run_id=$1`, fan.runID); err != nil {
					t.Fatal(err)
				}
				full, err = loadCompletionShortCircuitSummaries(ctx, fixture, fan.runID, selectedNow)
				if err != nil || !full.Delivery.Settled() || full.Pipeline.BlocksCompletion() || full.FanOut.BlocksCompletion() || full.Barriers.BlocksCompletion() || full.Timers.BlocksCompletion() || full.Sessions.BlocksCompletion() || full.Decisions.BlocksCompletion() || full.Effects.BlocksCompletion() || !full.Entities.ReadyForCompletion() {
					t.Fatalf("all repaired authoritative owners must clear: %+v err=%v", full, err)
				}
				current = requestRunLifecycleCandidateParity(t, fixture, ctx, fan.runID).Candidate
				result, err = fixture.store.ExecuteCompletionCandidate(ctx, current, catalog)
				if err != nil || result.Outcome != runtimerunlifecycle.OutcomeTerminallyEligible {
					t.Fatalf("all owners clear did not complete: %+v err=%v", result, err)
				}
				snapshot, err := fixture.store.LoadRunLifecycleSnapshot(ctx, fan.runID)
				if err != nil || snapshot.Status != "completed" || snapshot.EndedAt == nil {
					t.Fatalf("completion not persisted: %+v err=%v", snapshot, err)
				}
			})
		}
	}
}

func TestCompletionPendingPipelineStillAdvancesFanOutBarriersBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			ctx := testAuthorActivitySourceArtifactContext()
			fan, eventID, original := seedCompletionShortCircuitFanOut(t, fixture, ctx)
			seedFanOutDeliveryBarrier(t, ctx, fixture.db, fan, fan.createdAt)
			writeCompletionShortCircuitSettlement(t, fixture, ctx, eventID, corruptCompletionShortCircuitSettlement(t, original))
			defer writeCompletionShortCircuitSettlement(t, fixture, ctx, eventID, original)
			candidate := requestRunLifecycleCandidateParity(t, fixture, ctx, fan.runID).Candidate
			result, err := fixture.store.ExecuteCompletionCandidate(ctx, candidate, completionShortCircuitCatalog())
			if !errors.Is(err, eventrecord.ErrCorrupt) || !strings.Contains(err.Error(), "advance") || result.Outcome == runtimerunlifecycle.OutcomeTerminallyEligible {
				t.Fatalf("pending pipeline skipped required barrier advance: %+v err=%v", result, err)
			}
			assertFanOutBarrierState(t, ctx, fixture.db, fan.runID, fan.deliveryID, fan.semanticPath, fanoutbarrier.StatusArmed, nil, "")
			assertCompletionShortCircuitRunning(t, fixture, ctx, fan.runID)
			if !loadRunLifecycleCandidate(t, fixture, ctx, fan.runID).SameIdentity(candidate) {
				t.Fatal("failed barrier advance consumed current candidate")
			}
		})
	}
}

func seedCompletionShortCircuitFanOut(t *testing.T, fixture runLifecycleCandidateParityFixture, ctx context.Context) (fanOutOwnerFixture, string, []byte) {
	t.Helper()
	runID := seedCompletionBlockerRun(t, fixture, ctx)
	at := time.Date(2026, 7, 29, 11, 0, 0, 0, time.UTC)
	fan := seedFanOutOwnerChildFixture(t, ctx, fixture.db, fixture.store, fixture.postgres, fanOutOwnerFixture{runID: runID, flowPath: ".", bundleHash: runLifecycleCandidateParityBundleHash}, 1, at)
	// The existing fan-out source fixture already supplies a delivered trigger.
	// Seed its terminal pipeline receipt, leaving only the child publication
	// pending. The child is committed and later settled through real owners.
	query := `INSERT INTO event_receipts (receipt_id,event_id,subscriber_type,subscriber_id,entity_id,flow_instance,outcome,reason_code,failure,side_effects,processed_at) SELECT $1,e.event_id,'platform','pipeline',e.entity_id,e.flow_instance,'success','pipeline_persisted','null','{}',$2 FROM events e WHERE e.event_id=$3`
	if _, err := fixture.db.ExecContext(ctx, query, uuid.NewString(), at, fan.eventID); err != nil {
		t.Fatal(err)
	}
	event := fanOutBarrierChildEvent(t, fan, 0, at.Add(time.Second))
	if err := commitSemanticEventFixture(ctx, fixture.store, event); err != nil {
		t.Fatal(err)
	}
	seedFanOutBarrierOutcomes(t, ctx, fixture.db, fan, []string{event.ID()}, false, at.Add(time.Second))
	var raw []byte
	if err := fixture.db.QueryRowContext(ctx, `SELECT route_settlement FROM events WHERE event_id=$1`, event.ID()).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	return fan, event.ID(), raw
}

func completionShortCircuitCatalog() runtimerunlifecycle.TerminalCatalog {
	return stagecatalogfixture.NewTerminalCatalog(nil, map[string][]string{semanticRunFixtureFlow: {"completed"}})
}

func loadCompletionShortCircuitSummaries(ctx context.Context, fixture runLifecycleCandidateParityFixture, runID string, now time.Time) (runCompletionOwnerSummaries, error) {
	tx, err := fixture.db.BeginTx(ctx, nil)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	defer tx.Rollback()
	if fixture.postgres {
		return fixture.store.(*PostgresStore).runLifecyclePostgresOwner.LoadRunCompletionOwnerSummariesTx(ctx, tx, runID, now, completionShortCircuitCatalog())
	}
	return fixture.store.(*SQLiteRuntimeStore).runLifecycleSQLiteOwner.LoadRunCompletionOwnerSummariesTx(ctx, tx, runID, now, completionShortCircuitCatalog())
}

func corruptCompletionShortCircuitSettlement(t *testing.T, original []byte) []byte {
	t.Helper()
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(original, &wire); err != nil {
		t.Fatal(err)
	}
	wire["unowned_evidence"] = json.RawMessage(`true`)
	raw, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func writeCompletionShortCircuitSettlement(t *testing.T, fixture runLifecycleCandidateParityFixture, ctx context.Context, eventID string, raw []byte) {
	t.Helper()
	query := `UPDATE events SET route_settlement=$1 WHERE event_id=$2`
	if fixture.postgres {
		query = `UPDATE events SET route_settlement=$1::jsonb WHERE event_id=$2::uuid`
	}
	if result, err := fixture.db.ExecContext(ctx, query, string(raw), eventID); err != nil {
		t.Fatal(err)
	} else if count, err := result.RowsAffected(); err != nil || count != 1 {
		t.Fatalf("settlement fixture affected %d rows: %v", count, err)
	}
}

func assertCompletionShortCircuitRunning(t *testing.T, fixture runLifecycleCandidateParityFixture, ctx context.Context, runID string) {
	t.Helper()
	snapshot, err := fixture.store.LoadRunLifecycleSnapshot(ctx, runID)
	if err != nil || snapshot.Status != "running" || snapshot.EndedAt != nil {
		t.Fatalf("blocked/failed candidate mutated lifecycle: %+v err=%v", snapshot, err)
	}
}
