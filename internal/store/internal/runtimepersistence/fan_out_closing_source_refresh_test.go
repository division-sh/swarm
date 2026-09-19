package runtimepersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func TestFanOutClosingSourceHeadRefreshPreservesHealthyProgressBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 20*time.Second)
			defer cancel()
			selected, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			closing := seedFanOutOwnerFixture(t, ctx, db, selected, postgres, 1, time.Now().UTC())
			healthy := seedFanOutOwnerFixtureWithArtifact(t, ctx, db, selected, postgres, 1, time.Now().UTC(), storeTestSourceArtifact("closing-head-refresh-healthy-source"))
			process, grants, occurrences, originalPlan := newGrantedFanOutProcessForTest(t, ctx, selected, []fanOutOwnerFixture{closing, healthy})
			workers := 1
			capture := &captureFanOutExecutor{turns: make(chan capturedFanOutTurn, 1), errors: make(chan error, 8), done: make(chan struct{})}
			a, err := startupownership.StartFanOutServing(ctx, grants[0], occurrences[0], &workers, capture)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(a.Close)
			oldB, err := startupownership.StartFanOutServing(ctx, grants[1], occurrences[1], &workers, sourceLocalSelectorExecutor{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(oldB.Close)
			var oldTurn capturedFanOutTurn
			select {
			case oldTurn = <-capture.turns:
			case err := <-capture.errors:
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			a.Close()
			failedCtx, fail := context.WithCancel(ctx)
			fail()
			if err := grants[0].Retire(failedCtx); err == nil {
				t.Fatal("canceled A retirement unexpectedly succeeded")
			}
			if evidence, err := grants[0].Evidence(); err != nil || evidence.State != startupownership.GrantAdmitted {
				t.Fatalf("failed retirement lost A's admitted evidence: %+v err=%v", evidence, err)
			}
			if occurrences[0].ActiveCount() != 0 {
				t.Fatal("closing A retained an occurrence lease")
			}
			oldB.Close()
			if err := grants[1].Retire(ctx); err != nil {
				t.Fatal(err)
			}
			plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: healthy.bundleHash}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := process.RestoreSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), ExpectedRevision: originalPlan.Revision, Plan: plan}); err != nil {
				t.Fatal(err)
			}
			evidence, err := grants[0].Evidence()
			if err != nil {
				t.Fatal(err)
			}
			currentB, err := process.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: healthy.bundleHash, RuntimeInstanceID: evidence.RuntimeInstanceID, RuntimeGeneration: 2, SourceSetRevision: plan.Revision})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := currentB.MarkProbesSettled(ctx, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := currentB.AdmitExecution(ctx); err != nil {
				t.Fatal(err)
			}
			page := fanoutobligation.ListPage{RunID: healthy.runID, Intents: []fanoutobligation.IntentReadback{{
				Key: fanoutobligation.IntentKey{RunID: healthy.runID, TriggeringDeliveryID: healthy.deliveryID,
					ElementRef: contracts.FanOutElementRef{FlowPath: healthy.flowPath, Family: "fan_out", SemanticPath: healthy.semanticPath}},
				BundleHash: healthy.bundleHash,
			}}}
			read := func() fanoutobligation.RuntimeReadback {
				t.Helper()
				result, err := startupownership.ObserveFanOutRuntimePage(ctx, process, page)
				if err != nil {
					t.Fatal(err)
				}
				return result.Intents[0].Runtime
			}
			if observed := read(); observed.Reason != "registration_pending" || observed.Eligible != nil {
				t.Fatalf("missing current B did not block the census: %+v", observed)
			}
			starting, err := startupownership.RegisterFanOutServing(ctx, currentB, occurrences[1], &workers)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(starting.Close)
			if observed := read(); observed.Reason != "registration_pending" || observed.Eligible != nil {
				t.Fatalf("current B without executor completed the census: %+v", observed)
			}
			starting.Close()
			beforeClaim := make(chan struct{})
			executor := &fanOutSelectionExecutor{bundle: healthy.bundleHash, started: make(chan capturedFanOutTurn, 2), completed: make(chan fanoutobligation.Intent, 2), errors: make(chan error, 8), hold: beforeClaim}
			b, err := startupownership.StartFanOutServing(ctx, currentB, occurrences[1], &workers, executor)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(b.Close)
			select {
			case turn := <-executor.started:
				if turn.key != page.Intents[0].Key {
					t.Fatalf("selected wrong source after refresh: %+v", turn.key)
				}
			case err := <-executor.errors:
				t.Fatal(err)
			case <-time.After(5 * time.Second):
				t.Fatalf("retained off-head closing A stranded current B: readback=%+v", read())
			}
			if observed := read(); observed.Reason != "eligible" || observed.Eligible == nil || !*observed.Eligible || observed.Validate() != nil {
				t.Fatalf("current B lost eligible readback: %+v", observed)
			}
			close(beforeClaim)
			select {
			case intent := <-executor.completed:
				if intent.Request.Key != page.Intents[0].Key || intent.Cursor != 1 {
					t.Fatalf("wrong committed progress: %+v", intent)
				}
			case err := <-executor.errors:
				t.Fatal(err)
			case <-time.After(5 * time.Second):
				t.Fatal("current B did not commit after source refresh")
			}
			b.Close()
			if observed := read(); observed.Eligible == nil || *observed.Eligible || observed.Reason != "registration_closing" {
				t.Fatalf("closed current B gained eligibility: %+v", observed)
			}
			if err := grants[0].ProveCurrent(ctx); err == nil {
				t.Fatal("off-head A retained current execution authority")
			}
			if permit, found, err := a.BeginTurn(ctx); err == nil || found || permit != nil {
				if permit != nil {
					permit.Done()
				}
				t.Fatal("closing off-head A admitted a turn")
			}
			if _, _, found, err := oldTurn.owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "off-head-close-refusal", BundleHash: closing.bundleHash, Candidate: &oldTurn.key, Now: time.Now().UTC(), Lease: time.Minute}); err == nil || found {
				t.Fatalf("old A owner regained a durable claim: found=%v err=%v", found, err)
			}
			assertFanOutCursorAndOutcomeCount(t, ctx, db, healthy, 1, 1)
			assertFanOutCursorAndOutcomeCount(t, ctx, db, closing, 0, 0)
			var generation int
			if err := db.QueryRowContext(ctx, `SELECT claim_generation FROM fan_out_intents WHERE run_id=$1 AND semantic_path=$2`, closing.runID, closing.semanticPath).Scan(&generation); err != nil || generation != 0 {
				t.Fatalf("closing A mutated claim generation: %d err=%v", generation, err)
			}
			// Corrupt only this isolated fixture's retained evidence, then restore
			// it exactly. Off-head must not mean unvalidated or silently omitted.
			var originalSnapshot []byte
			if err := db.QueryRowContext(ctx, `SELECT snapshot FROM runtime_generation_grants WHERE grant_id=$1 AND state_version=$2`, evidence.GrantID, evidence.StateVersion).Scan(&originalSnapshot); err != nil {
				t.Fatal(err)
			}
			for _, hostile := range []struct {
				name   string
				mutate func(*startupownership.GrantEvidence)
			}{
				{"unknown_grant", func(g *startupownership.GrantEvidence) { g.GrantID = uuid.NewString() }},
				{"mismatched_process", func(g *startupownership.GrantEvidence) { g.ProcessOwnerID = "foreign-process" }},
				{"missing_admitted_evidence", func(g *startupownership.GrantEvidence) { g.State = startupownership.GrantRetired }},
			} {
				t.Run(hostile.name, func(t *testing.T) {
					changed := evidence
					hostile.mutate(&changed)
					raw, err := canonicaljson.Bytes(changed)
					if err != nil {
						t.Fatal(err)
					}
					query := `UPDATE runtime_generation_grants SET state=$1,snapshot=$2 WHERE grant_id=$3 AND state_version=$4`
					if _, err := db.ExecContext(ctx, query, string(changed.State), string(raw), evidence.GrantID, evidence.StateVersion); err != nil {
						t.Fatal(err)
					}
					defer func() {
						if _, err := db.ExecContext(ctx, query, string(evidence.State), string(originalSnapshot), evidence.GrantID, evidence.StateVersion); err != nil {
							t.Fatal(err)
						}
					}()
					if result, err := startupownership.ObserveFanOutRuntimePage(ctx, process, page); err == nil {
						t.Fatalf("unvalidated off-head evidence was dropped: %+v", result)
					}
				})
			}
			if observed := read(); observed.Reason != "registration_closing" || observed.Eligible == nil || *observed.Eligible {
				t.Fatalf("restored complete census did not recover: %+v", observed)
			}
			t.Log("validated retained off-head A; current B selected, committed cursor1/outcome1 and remained observable; A authority/claim/turn refused")
		})
	}
}
