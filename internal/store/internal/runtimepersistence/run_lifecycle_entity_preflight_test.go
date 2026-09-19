package runtimepersistence

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
)

func TestCompletionNonterminalEntityDefersOnlyUnusedFanOutFoldBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, session := range []string{"none", "exact_expiry", "foreign_expiry", "malformed_holder"} {
			t.Run(backend+"/"+session, func(t *testing.T) {
				fixture := openRunLifecycleCandidateParityFixture(t, backend)
				ctx := testAuthorActivitySourceArtifactContext()
				fan, eventID, original := seedCompletionEntityPreflight(t, fixture, ctx)
				catalog := completionShortCircuitCatalog()
				if catalog.Empty() {
					t.Fatal("entity preflight must use the real nonempty terminal catalog")
				}
				now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
				expiry := time.Date(2100, 1, 1, 0, 0, 0, 123456000, time.UTC)
				if session == "exact_expiry" || session == "malformed_holder" {
					var lease any = expiry
					if session == "malformed_holder" {
						lease = nil
					}
					if err := insertCompletionBlockerSession(t, fixture, ctx, fan.runID, "worker", lease); err != nil {
						t.Fatal(err)
					}
				}
				if session == "exact_expiry" || session == "foreign_expiry" {
					foreign := seedCompletionBlockerRun(t, fixture, ctx)
					if err := insertCompletionBlockerSession(t, fixture, ctx, foreign, "foreign", expiry.Add(-time.Hour)); err != nil {
						t.Fatal(err)
					}
				}
				full, err := loadCompletionShortCircuitSummaries(ctx, fixture, fan.runID, now)
				if err != nil || !full.Delivery.Settled() || full.Pipeline.BlocksCompletion() || full.FanOut.BlocksCompletion() || full.Entities.Nonterminal != 1 || full.Entities.Malformed != 0 || full.Entities.ReadyForCompletion() {
					t.Fatalf("expected clear delivery/pipeline with one canonical nonterminal entity: %+v err=%v", full, err)
				}
				if session == "exact_expiry" && !full.Sessions.NextExpiry.Equal(expiry) {
					t.Fatalf("canonical expiry=%s want=%s", full.Sessions.NextExpiry, expiry)
				}
				if session == "malformed_holder" && full.Sessions.MalformedLease != 1 {
					t.Fatalf("malformed lease not represented by session owner: %+v", full.Sessions)
				}
				corrupt := corruptCompletionShortCircuitSettlement(t, original)
				writeCompletionShortCircuitSettlement(t, fixture, ctx, eventID, corrupt)
				defer writeCompletionShortCircuitSettlement(t, fixture, ctx, eventID, original)
				_, err = loadCompletionShortCircuitSummaries(ctx, fixture, fan.runID, now)
				assertJointSourceReadCorrupt(t, eventID, err)
				candidate := requestRunLifecycleCandidateParity(t, fixture, ctx, fan.runID).Candidate
				result, err := fixture.store.ExecuteCompletionCandidate(ctx, candidate, catalog)
				if err != nil {
					t.Fatalf("nonterminal entity unnecessarily folded corrupt fan-out: %v", err)
				}
				assertCompletionShortCircuitRunning(t, fixture, ctx, fan.runID)
				if session == "exact_expiry" {
					if result.Outcome != runtimerunlifecycle.OutcomeRearmAt || !result.Candidate.DueAt.Equal(expiry) || result.Candidate.Revision <= candidate.Revision || !loadRunLifecycleCandidate(t, fixture, ctx, fan.runID).SameIdentity(result.Candidate) {
						t.Fatalf("entity gate lost exact persisted session rearm: %+v", result)
					}
				} else {
					_, due, revision := loadRunLifecycleCandidateFacts(t, fixture, ctx, fan.runID)
					if result.Outcome != runtimerunlifecycle.OutcomeAwaitMutation || due || revision != candidate.Revision {
						t.Fatalf("entity gate should clear only current due marker: result=%+v due=%v revision=%d", result, due, revision)
					}
				}

				current := forceRunLifecycleCandidateParity(t, fixture, ctx, fan.runID, now.Add(-time.Minute))
				if current.Revision <= candidate.Revision {
					t.Fatal("new dirty candidate did not advance the retained revision")
				}
				stale, err := fixture.store.ExecuteCompletionCandidate(ctx, candidate, catalog)
				if err != nil || stale.Outcome != runtimerunlifecycle.OutcomeExactNoop || !loadRunLifecycleCandidate(t, fixture, ctx, fan.runID).SameIdentity(current) {
					t.Fatalf("stale callback consumed newer entity dirty candidate: %+v err=%v", stale, err)
				}

				// Only the entity gate changes. The same corrupt event must now
				// be read by the full owner, even if a session also blocks completion.
				setCompletionEntityPreflightState(t, fixture, ctx, fan.runID, "completed")
				result, err = fixture.store.ExecuteCompletionCandidate(ctx, current, catalog)
				assertJointSourceReadCorrupt(t, eventID, err)
				stage := "summarize sqlite fan-out obligations: "
				if fixture.postgres {
					stage = "summarize fan-out obligations: "
				}
				if !strings.HasPrefix(err.Error(), stage) {
					t.Fatalf("terminal entity did not reach the full fan-out owner: %v", err)
				}
				if result.Outcome == runtimerunlifecycle.OutcomeTerminallyEligible || !loadRunLifecycleCandidate(t, fixture, ctx, fan.runID).SameIdentity(current) {
					t.Fatalf("required fan-out corruption completed or consumed dirty candidate: %+v", result)
				}
				assertCompletionShortCircuitRunning(t, fixture, ctx, fan.runID)

				writeCompletionShortCircuitSettlement(t, fixture, ctx, eventID, original)
				if _, err := fixture.db.ExecContext(ctx, `UPDATE agent_sessions SET lease_holder=NULL,lease_expires_at=NULL WHERE run_id=$1`, fan.runID); err != nil {
					t.Fatal(err)
				}
				full, err = loadCompletionShortCircuitSummaries(ctx, fixture, fan.runID, now)
				if err != nil || !full.Delivery.Settled() || full.Pipeline.BlocksCompletion() || full.FanOut.BlocksCompletion() || full.Barriers.BlocksCompletion() || full.Timers.BlocksCompletion() || full.Sessions.BlocksCompletion() || full.Decisions.BlocksCompletion() || full.Effects.BlocksCompletion() || !full.Entities.ReadyForCompletion() {
					t.Fatalf("repaired full canonical owners not clear: %+v err=%v", full, err)
				}
				result, err = fixture.store.ExecuteCompletionCandidate(ctx, current, catalog)
				if err != nil || result.Outcome != runtimerunlifecycle.OutcomeTerminallyEligible {
					t.Fatalf("same retained candidate did not complete after full owner repair: %+v err=%v", result, err)
				}
				snapshot, err := fixture.store.LoadRunLifecycleSnapshot(ctx, fan.runID)
				if err != nil || snapshot.Status != "completed" || snapshot.EndedAt == nil {
					t.Fatalf("terminal completion not durable: %+v err=%v", snapshot, err)
				}
			})
		}
	}
}

func TestCompletionNonterminalEntityStillAdvancesFanOutBarriersBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			ctx := testAuthorActivitySourceArtifactContext()
			fan, eventID, original := seedCompletionEntityPreflight(t, fixture, ctx)
			seedFanOutDeliveryBarrier(t, ctx, fixture.db, fan, fan.createdAt)
			writeCompletionShortCircuitSettlement(t, fixture, ctx, eventID, corruptCompletionShortCircuitSettlement(t, original))
			defer writeCompletionShortCircuitSettlement(t, fixture, ctx, eventID, original)
			candidate := requestRunLifecycleCandidateParity(t, fixture, ctx, fan.runID).Candidate
			result, err := fixture.store.ExecuteCompletionCandidate(ctx, candidate, completionShortCircuitCatalog())
			if !errors.Is(err, eventrecord.ErrCorrupt) || !strings.Contains(err.Error(), "advance") || result.Outcome == runtimerunlifecycle.OutcomeTerminallyEligible {
				t.Fatalf("nonterminal entity skipped mandatory barrier advance: %+v err=%v", result, err)
			}
			assertFanOutBarrierState(t, ctx, fixture.db, fan.runID, fan.deliveryID, fan.semanticPath, fanoutbarrier.StatusArmed, nil, "")
			assertCompletionShortCircuitRunning(t, fixture, ctx, fan.runID)
			if !loadRunLifecycleCandidate(t, fixture, ctx, fan.runID).SameIdentity(candidate) {
				t.Fatal("failed mandatory barrier advance consumed dirty candidate")
			}
		})
	}
}

func seedCompletionEntityPreflight(t *testing.T, fixture runLifecycleCandidateParityFixture, ctx context.Context) (fanOutOwnerFixture, string, []byte) {
	t.Helper()
	fan, eventID, original := seedCompletionShortCircuitFanOut(t, fixture, ctx)
	owner := fixture.store.(pipelineObligationParityStore).PipelineObligations()
	settlePipelineParityEvent(t, ctx, owner, eventID, pipelineobligation.Acknowledged("pipeline_persisted"))
	setCompletionEntityPreflightState(t, fixture, ctx, fan.runID, "working")
	full, err := loadCompletionShortCircuitSummaries(ctx, fixture, fan.runID, fan.createdAt.Add(time.Minute))
	if err != nil || !full.Delivery.Settled() || full.Pipeline.BlocksCompletion() || full.FanOut.BlocksCompletion() || full.Barriers.BlocksCompletion() || full.Entities.Nonterminal != 1 || full.Entities.Malformed != 0 || full.Entities.ReadyForCompletion() {
		t.Fatalf("entity preflight fixture is not independently blocked by one nonterminal entity: %+v err=%v", full, err)
	}
	return fan, eventID, original
}

func setCompletionEntityPreflightState(t *testing.T, fixture runLifecycleCandidateParityFixture, ctx context.Context, runID, state string) {
	t.Helper()
	result, err := fixture.db.ExecContext(ctx, `UPDATE entity_state SET current_state=$1 WHERE run_id=$2 AND entity_id=$2`, state, runID)
	if err != nil {
		t.Fatal(err)
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		t.Fatalf("entity state fixture affected %d rows: %v", rows, err)
	}
}
