package runtimepersistence

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

// Leave candidates unclaimed: this tests the selected-store selector across
// registration shutdown, not the independent finite-turn mutation protocol.
type sourceLocalSelectorExecutor struct{}

func (sourceLocalSelectorExecutor) ServeFanOutCandidate(context.Context, pipeline.FanOutObligationOwner, fanoutobligation.IntentKey) (pipeline.FanOutTurnResult, error) {
	return pipeline.FanOutTurnResult{}, nil
}

func TestFanOutClosingSourceFailedRetirementPreservesHealthyProgressBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 20*time.Second)
			defer cancel()
			selected, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			idle := seedFanOutOwnerFixture(t, ctx, db, selected, postgres, 0, time.Now().UTC())
			healthy := seedFanOutOwnerFixtureWithArtifact(t, ctx, db, selected, postgres, 1, time.Now().UTC(), storeTestSourceArtifact("closing-source-retirement-healthy-source"))
			process, grants, occurrences, _ := newGrantedFanOutProcessForTest(t, ctx, selected, []fanOutOwnerFixture{idle, healthy})
			workers := 1
			a, err := startupownership.StartFanOutServing(ctx, grants[0], occurrences[0], &workers, sourceLocalSelectorExecutor{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(a.Close)
			a.Close()
			failedCtx, fail := context.WithCancel(ctx)
			fail()
			if err := grants[0].Retire(failedCtx); err == nil {
				t.Fatal("canceled grant retirement unexpectedly succeeded")
			}
			if evidence, err := grants[0].Evidence(); err != nil || evidence.State != startupownership.GrantAdmitted {
				t.Fatalf("failed retirement must retain admitted authority: %+v err=%v", evidence, err)
			}
			if occurrences[0].ActiveCount() != 0 {
				t.Fatal("closing census retained a runtime standing lease")
			}
			read := func(fixture fanOutOwnerFixture) fanoutobligation.RuntimeReadback {
				t.Helper()
				page := fanoutobligation.ListPage{RunID: fixture.runID, Intents: []fanoutobligation.IntentReadback{{
					Key: fanoutobligation.IntentKey{RunID: fixture.runID, TriggeringDeliveryID: fixture.deliveryID,
						ElementRef: contracts.FanOutElementRef{FlowPath: fixture.flowPath, Family: "fan_out", SemanticPath: fixture.semanticPath}},
					BundleHash: fixture.bundleHash,
				}}}
				result, err := startupownership.ObserveFanOutRuntimePage(ctx, process, page)
				if err != nil {
					t.Fatal(err)
				}
				return result.Intents[0].Runtime
			}
			if observed := read(healthy); observed.Reason != "registration_pending" || observed.Eligible != nil {
				t.Fatalf("closing A suppressed never-registered B's admission gap: %+v", observed)
			}
			starting, err := startupownership.RegisterFanOutServing(ctx, grants[1], occurrences[1], &workers)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(starting.Close)
			if observed := read(healthy); observed.Reason != "registration_pending" || observed.Eligible != nil {
				t.Fatalf("starting B without an executor falsely completed the census: %+v", observed)
			}
			starting.Close()
			// Work can arrive from an already-accepted non-fan-out producer while
			// A drains. Its current durable grant must not reopen serving admission.
			closingWork := seedFanOutOwnerIntent(t, ctx, db, idle, 1, time.Now().UTC())
			started := make(chan capturedFanOutTurn, 4)
			completed := make(chan fanoutobligation.Intent, 4)
			failures := make(chan error, 4)
			beforeClaim := make(chan struct{})
			b, err := startupownership.StartFanOutServing(ctx, grants[1], occurrences[1], &workers, &fanOutSelectionExecutor{bundle: healthy.bundleHash, started: started, completed: completed, errors: failures, hold: beforeClaim})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(b.Close)
			select {
			case turn := <-started:
				if turn.key.RunID != healthy.runID {
					t.Fatalf("closing source selected instead of B: %+v", turn.key)
				}
			case err := <-failures:
				t.Fatal(err)
			case <-time.After(5 * time.Second):
				t.Fatal("healthy B did not enter its pre-claim hold")
			}
			if observed := read(healthy); observed.Reason != "eligible" || observed.Eligible == nil || !*observed.Eligible || observed.Validate() != nil {
				t.Fatalf("closing A suppressed healthy B's readback eligibility: %+v", observed)
			}
			select {
			case err := <-failures:
				incident := runtimefailures.Normalize(err, "", "")
				if incident.Detail.Code != "fan_out_claim_opportunity_missed" || incident.Detail.Attributes["run_id"] != healthy.runID || incident.Detail.Attributes["triggering_delivery_id"] != healthy.deliveryID {
					t.Fatalf("closing A hid or misattributed B's D3 opportunity: %+v", incident)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("closing A suppressed B's actual D3 observation")
			}
			close(beforeClaim)
			select {
			case result := <-completed:
				if result.Request.Key.RunID != healthy.runID || result.Cursor != 1 {
					t.Fatalf("wrong source advanced: %+v", result)
				}
			case err := <-failures:
				t.Fatal(err)
			case <-time.After(5 * time.Second):
				t.Fatal("failed retirement of closing A stranded healthy B")
			}
			observed := read(closingWork)
			if observed.Reason != "registration_closing" || observed.Eligible == nil || *observed.Eligible || observed.Validate() != nil {
				t.Fatalf("closing A gained runtime eligibility: %+v", observed)
			}
			if permit, found, err := a.BeginTurn(ctx); err == nil || found || permit != nil {
				if permit != nil {
					permit.Done()
				}
				t.Fatal("closing A admitted another turn")
			}
			scanned := make(chan bool, 1)
			b.SetTestScanObserver(func(_ startupownership.FanOutCandidate, found bool, err error) {
				if err != nil {
					select {
					case failures <- err:
					default:
					}
				}
				select {
				case scanned <- found:
				default:
				}
			})
			b.Wake()
			select {
			case found := <-scanned:
				if found {
					t.Fatal("selector admitted new work under closing A")
				}
			case err := <-failures:
				t.Fatal(err)
			case <-time.After(5 * time.Second):
				t.Fatal("selector did not observe closing-only backlog")
			}
			select {
			case err := <-failures:
				t.Fatal(err)
			default:
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, healthy, 1, 1)
			assertFanOutCursorAndOutcomeCount(t, ctx, db, closingWork, 0, 0)
			var claimGeneration int
			if err := db.QueryRowContext(ctx, `SELECT claim_generation FROM fan_out_intents WHERE run_id=$1 AND semantic_path=$2`, closingWork.runID, closingWork.semanticPath).Scan(&claimGeneration); err != nil || claimGeneration != 0 {
				t.Fatalf("closing A acquired a durable claim: generation=%d err=%v", claimGeneration, err)
			}
		})
	}
}

func (sourceLocalSelectorExecutor) ReportFanOutServingError(context.Context, error) {}

func TestFanOutIdleSourceClosePreservesOtherSourceSelectionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 20*time.Second)
			defer cancel()
			selected, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			idle := seedFanOutOwnerFixture(t, ctx, db, selected, postgres, 0, time.Now().UTC())
			healthy := seedFanOutOwnerFixtureWithArtifact(t, ctx, db, selected, postgres, 1, time.Now().UTC(), storeTestSourceArtifact("idle-source-close-healthy-source"))
			_, grants, occurrences, _ := newGrantedFanOutProcessForTest(t, ctx, selected, []fanOutOwnerFixture{idle, healthy})
			workers := 1
			registrations := make([]*startupownership.FanOutServingRegistration, 2)
			for i := range registrations {
				var err error
				registrations[i], err = startupownership.StartFanOutServing(ctx, grants[i], occurrences[i], &workers, sourceLocalSelectorExecutor{})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(registrations[i].Close)
			}
			type scan struct {
				candidate startupownership.FanOutCandidate
				found     bool
				err       error
			}
			before, after, recovered := make(chan scan, 1), make(chan scan, 1), make(chan scan, 1)
			resume, repair := make(chan struct{}), make(chan struct{})
			var resumeOnce, repairOnce sync.Once
			open := func() { resumeOnce.Do(func() { close(resume) }) }
			finish := func() { repairOnce.Do(func() { close(repair) }) }
			t.Cleanup(func() { open(); finish() })
			phase := 0 // Accessed only by the shared selector's serial scan callback.
			registrations[1].SetTestScanObserver(func(candidate startupownership.FanOutCandidate, found bool, err error) {
				result := scan{candidate, found, err}
				switch phase {
				case 0:
					if !found && err == nil {
						return // A legal admit-before-register startup observation.
					}
					phase++
					before <- result
					select {
					case <-resume:
					case <-ctx.Done():
					}
				case 1:
					phase++
					after <- result
					select {
					case <-repair:
					case <-ctx.Done():
					}
				default:
					select {
					case recovered <- result:
					default:
					}
				}
			})
			await := func(ch <-chan scan) scan {
				t.Helper()
				select {
				case result := <-ch:
					return result
				case <-time.After(5 * time.Second):
					t.Fatal("shared selected-store selector did not report a scan")
					return scan{}
				}
			}
			registrations[1].Wake()
			baseline := await(before)
			if baseline.err != nil || !baseline.found || baseline.candidate.Key.RunID != healthy.runID || baseline.candidate.Key.TriggeringDeliveryID != healthy.deliveryID {
				t.Fatalf("healthy B was not selected before closing idle A: %+v", baseline)
			}
			registrations[0].Close()
			evidence, err := grants[0].Evidence()
			if err != nil || evidence.State != startupownership.GrantAdmitted {
				t.Fatalf("idle A must still own its admitted grant during downstream drain: %+v err=%v", evidence, err)
			}
			var durableState string
			if err := db.QueryRowContext(ctx, `SELECT state FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1`, evidence.GrantID).Scan(&durableState); err != nil || durableState != "admitted" {
				t.Fatalf("idle A durable grant state=%q err=%v", durableState, err)
			}
			open()
			next := await(after)
			if next.err != nil || !next.found || next.candidate != baseline.candidate {
				t.Errorf("closing idle A stranded ready B in the actual selector: before=%+v after=%+v", baseline, next)
			}
			// Positive control isolates the durable census mismatch. It must not
			// replace or relax the failed post-Close assertion above.
			if err := grants[0].Retire(ctx); err != nil {
				t.Fatal(err)
			}
			finish()
			registrations[1].Wake()
			last := await(recovered)
			if last.err != nil || !last.found || last.candidate != baseline.candidate {
				t.Fatalf("healthy B did not resume after only A's grant retired: %+v", last)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, healthy, 0, 0)
			t.Log("actual selector selected B before close and after A grant retirement; B remained unclaimed throughout")
		})
	}
}
