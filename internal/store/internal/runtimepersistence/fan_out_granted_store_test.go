package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

type capturedFanOutTurn struct {
	owner pipeline.FanOutObligationOwner
	key   fanoutobligation.IntentKey
}

type captureFanOutExecutor struct {
	turns  chan capturedFanOutTurn
	errors chan error
	done   chan struct{}
}

func (e *captureFanOutExecutor) ServeFanOutCandidate(ctx context.Context, owner pipeline.FanOutObligationOwner, key fanoutobligation.IntentKey) (pipeline.FanOutTurnResult, error) {
	e.turns <- capturedFanOutTurn{owner: owner, key: key}
	<-ctx.Done()
	close(e.done)
	return pipeline.FanOutTurnResult{}, nil
}

func (e *captureFanOutExecutor) ReportFanOutServingError(_ context.Context, err error) {
	select {
	case e.errors <- err:
	default:
	}
}

// Capture only execution, not admission: selection, retained-session provider,
// grant binding and all mutations below use the production selected-store path.
func newGrantedFanOutProcessForTest(t *testing.T, ctx context.Context, selected selectedFanOutOwner, fixtures []fanOutOwnerFixture) (startupownership.ProcessCapability, []startupownership.LiveGenerationGrant, []*worklifetime.RuntimeOccurrence, agenttopology.SourceSetPlan) {
	t.Helper()
	store := selected.(interface {
		AcquireProcessCapability(context.Context, startupownership.AcquireRequest) (startupownership.ProcessCapability, error)
	})
	request := testStartupAcquireRequest("fan-out-granted-store")
	capability, err := store.AcquireProcessCapability(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := capability.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	sources := make([]agenttopology.SourceCoordinate, len(fixtures))
	for i, fixture := range fixtures {
		sources[i] = agenttopology.SourceCoordinate{BundleHash: fixture.bundleHash}
	}
	plan, err := agenttopology.NewSourceSetPlan(sources, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := capability.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
		t.Fatal(err)
	}
	work := worklifetime.NewProcess()
	t.Cleanup(func() {
		work.Retire()
		deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := work.Join(deadline); err != nil {
			t.Error(err)
		}
	})
	var grants []startupownership.LiveGenerationGrant
	var occurrences []*worklifetime.RuntimeOccurrence
	for _, fixture := range fixtures {
		grant, err := capability.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: fixture.bundleHash, RuntimeInstanceID: request.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := grant.AdmitExecution(ctx); err != nil {
			t.Fatal(err)
		}
		occurrence, err := work.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: request.RuntimeInstanceID, BundleHash: fixture.bundleHash})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := occurrence.RetireAndWait(deadline); err != nil {
				t.Error(err)
			}
		})
		grants = append(grants, grant)
		occurrences = append(occurrences, occurrence)
	}
	return capability, grants, occurrences, plan
}

func grantedFanOutOwnerForTest(t *testing.T, ctx context.Context, selected selectedFanOutOwner, fixture fanOutOwnerFixture) (pipeline.FanOutObligationOwner, startupownership.ProcessCapability, startupownership.LiveGenerationGrant, agenttopology.SourceSetPlan) {
	t.Helper()
	capability, grants, occurrences, plan := newGrantedFanOutProcessForTest(t, ctx, selected, []fanOutOwnerFixture{fixture})
	grant, occurrence := grants[0], occurrences[0]
	executor := &captureFanOutExecutor{turns: make(chan capturedFanOutTurn, 1), errors: make(chan error, 1), done: make(chan struct{})}
	workers := 1
	registration, err := startupownership.StartFanOutServing(ctx, grant, occurrence, &workers, executor)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registration.Close)
	select {
	case turn := <-executor.turns:
		registration.Close()
		<-executor.done
		if turn.key.RunID != fixture.runID || turn.key.TriggeringDeliveryID != fixture.deliveryID {
			t.Fatalf("wrong selected candidate: %+v", turn.key)
		}
		return turn.owner, capability, grant, plan
	case err := <-executor.errors:
		t.Fatalf("granted selection failed: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	return nil, nil, nil, agenttopology.SourceSetPlan{}
}

func TestFanOutGrantedStoreAdmissionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, fence := range []string{"pause", "retired-grant", "source-head", "process-release", "stop", "full-grant"} {
			t.Run(backend+"/"+fence, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 20*time.Second)
				defer cancel()
				raw, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
				fixture := seedFanOutOwnerFixture(t, ctx, db, raw, postgres, 64, time.Now().UTC())
				ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(authorActivityTestRuntimeInstanceID, fixture.bundleHash))
				owner, capability, grant, plan := grantedFanOutOwnerForTest(t, ctx, raw, fixture)
				key := fanoutobligation.IntentKey{RunID: fixture.runID, TriggeringDeliveryID: fixture.deliveryID,
					ElementRef: contracts.FanOutElementRef{FlowPath: fixture.flowPath, Family: "fan_out", SemanticPath: fixture.semanticPath}}
				request := pipeline.FanOutClaimRequest{Owner: "exact-granted-turn", BundleHash: fixture.bundleHash, Now: time.Now().UTC(), Lease: time.Minute}
				if _, _, _, err := owner.ClaimFanOutIntent(ctx, request); err == nil {
					t.Fatal("bound claim accepted no exact candidate")
				}
				request.Candidate = &key
				_, claim, found, err := owner.ClaimFanOutIntent(ctx, request)
				if err != nil || !found {
					t.Fatalf("exact granted claim: found=%v err=%v", found, err)
				}
				if input, err := owner.LoadFanOutEvaluation(ctx, claim); err != nil || len(input.Items) != 32 {
					t.Fatalf("granted immutable source: count=%d err=%v", len(input.Items), err)
				}
				control := raw.(interface {
					PauseRunControlOutcome(context.Context, runcontrol.TransitionRequest) (runcontrol.StoreTransition, error)
					ContinueRunControlOutcome(context.Context, runcontrol.TransitionRequest) (runcontrol.StoreTransition, error)
					StopRunControlOutcome(context.Context, runcontrol.TransitionRequest) (runcontrol.StoreTransition, error)
				})
				transition := runcontrol.TransitionRequest{RunID: fixture.runID, Now: time.Now().UTC(), Reason: "granted-store-test", ControlledBy: "test"}
				switch fence {
				case "pause":
					if outcome, err := control.PauseRunControlOutcome(ctx, transition); err != nil || !outcome.Acknowledged {
						t.Fatalf("pause outcome=%+v err=%v", outcome, err)
					}
					if _, err := owner.LoadFanOutEvaluation(ctx, claim); err != nil {
						t.Fatalf("pause fenced admitted evaluation: %v", err)
					}
					if _, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, 0, 32, time.Now().UTC())); err != nil {
						t.Fatalf("pause fenced admitted publication: %v", err)
					}
					if _, _, found, err := owner.ClaimFanOutIntent(ctx, request); err != nil || found {
						t.Fatalf("pause admitted a new turn: found=%v err=%v", found, err)
					}
					if outcome, err := control.ContinueRunControlOutcome(ctx, transition); err != nil || !outcome.Acknowledged {
						t.Fatalf("continue outcome=%+v err=%v", outcome, err)
					}
					_, successor, found, err := owner.ClaimFanOutIntent(ctx, request)
					if err != nil || !found {
						t.Fatalf("continue did not readmit: %v %v", found, err)
					}
					if _, err := owner.ReleaseFanOutClaim(ctx, claim); !errors.Is(err, fanoutobligation.ErrStaleClaim) {
						t.Fatalf("old release touched successor: %v", err)
					}
					if _, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(successor, 32, 32, time.Now().UTC())); err != nil {
						t.Fatal(err)
					}
					assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 64, 64)
					return
				case "retired-grant":
					if err := grant.Retire(ctx); err != nil {
						t.Fatal(err)
					}
				case "source-head":
					next, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: fixture.bundleHash}, {BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64)}}, nil)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := capability.RestoreSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), ExpectedRevision: plan.Revision, Plan: next}); err != nil {
						t.Fatal(err)
					}
				case "process-release":
					if err := capability.Release(ctx); err != nil {
						t.Fatal(err)
					}
				case "stop":
					if outcome, err := control.StopRunControlOutcome(ctx, transition); err != nil || !outcome.Acknowledged {
						t.Fatalf("stop outcome=%+v err=%v", outcome, err)
					}
				case "full-grant":
					evidence, err := grant.Evidence()
					if err != nil {
						t.Fatal(err)
					}
					mutateFanOutGrantSnapshot(t, ctx, db, postgres, evidence)
				}
				if _, err := owner.LoadFanOutEvaluation(ctx, claim); err == nil {
					t.Error("authority loss allowed evaluation")
				}
				if _, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, 0, 32, time.Now().UTC())); err == nil {
					t.Error("authority loss allowed publication")
				}
				if _, err := owner.ReleaseFanOutRetryable(ctx, pipeline.FanOutRetryableRelease{Claim: claim, Now: time.Now().UTC(), Failure: fanOutRetryFailureForTest()}); err == nil {
					t.Error("authority loss allowed retry")
				}
				if _, err := owner.BlockFanOutClaim(ctx, pipeline.FanOutBlockRequest{Claim: claim, Now: time.Now().UTC(), Failure: failures.Normalize(errors.New("invariant"), "runtime.fan_out", "test")}); err == nil {
					t.Error("authority loss allowed permanent block")
				}
				if _, _, found, err := owner.ClaimFanOutIntent(ctx, request); found || (fence != "stop" && err == nil) {
					t.Errorf("authority loss allowed new claim: found=%v err=%v", found, err)
				}
				assertFanOutCursorAndOutcomeCount(t, ctx, db, fixture, 0, 0)
				if fence == "process-release" {
					if _, err := owner.ReleaseFanOutClaim(ctx, claim); err == nil {
						t.Fatal("cleanup mutated after retained process release")
					}
				} else if fence != "stop" {
					if _, err := owner.ReleaseFanOutClaim(ctx, claim); err != nil {
						t.Fatalf("exact cleanup after authority loss: %v", err)
					}
				}
			})
		}
	}
}

func mutateFanOutGrantSnapshot(t *testing.T, ctx context.Context, db *sql.DB, postgres bool, evidence startupownership.GrantEvidence) {
	t.Helper()
	original, err := canonicaljson.Bytes(evidence)
	if err != nil {
		t.Fatal(err)
	}
	evidence.ProbeSurfaceIDs = []string{"changed-full-evidence"}
	raw, err := canonicaljson.Bytes(evidence)
	if err != nil {
		t.Fatal(err)
	}
	query := `UPDATE runtime_generation_grants SET snapshot=$1 WHERE grant_id=$2 AND state_version=$3`
	if postgres {
		query = `UPDATE runtime_generation_grants SET snapshot=$1::jsonb WHERE grant_id=$2 AND state_version=$3`
	}
	if _, err := db.ExecContext(ctx, query, string(raw), evidence.GrantID, evidence.StateVersion); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := db.ExecContext(context.Background(), query, string(original), evidence.GrantID, evidence.StateVersion); err != nil {
			t.Error(err)
		}
	})
}
