package runtimepersistence

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

func TestFanOutReadPaginationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, reopened, db, postgres := newFanOutOwnerPairForTest(t, backend)
			base := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 64, time.Now().UTC())
			for n := 0; n < 4; n++ {
				seedFanOutOwnerIntent(t, ctx, db, base, 1, time.Now().UTC())
			}
			reader := owner.(operatorread.FanOutReader)
			q := fanoutobligation.ListQuery{RunID: base.runID, Limit: 2}
			first, err := reader.ListFanOutIntents(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			if len(first.Intents) != 2 || first.NextCursor == "" || first.ObservedAt.IsZero() || first.RunStatus != "running" {
				t.Fatalf("first=%+v", first)
			}
			for _, row := range first.Intents {
				if row.Runtime != fanoutobligation.UnavailableRuntimeReadback() {
					t.Fatalf("invented runtime facts: %+v", row)
				}
			}
			// Progress and lease updates must not change immutable key ordering.
			_, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "read-test", BundleHash: base.bundleHash, Now: time.Now().UTC(), Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("claim found=%v err=%v", found, err)
			}
			if _, err := owner.CommitFanOutChunk(ctx, rejectedFanOutChunk(claim, 0, 32, time.Now().UTC())); err != nil {
				t.Fatal(err)
			}
			seen := map[fanoutobligation.IntentKey]bool{}
			for _, row := range first.Intents {
				seen[row.Key] = true
			}
			q.Cursor = first.NextCursor
			for q.Cursor != "" {
				page, err := reopened.(operatorread.FanOutReader).ListFanOutIntents(ctx, q)
				if err != nil {
					t.Fatal(err)
				}
				if len(page.Intents) > 2 {
					t.Fatal("page exceeded limit")
				}
				for _, row := range page.Intents {
					if seen[row.Key] {
						t.Fatalf("duplicate %s", row.Key.String())
					}
					seen[row.Key] = true
				}
				q.Cursor = page.NextCursor
			}
			if len(seen) != 5 {
				t.Fatalf("saw %d intents, want 5", len(seen))
			}
			for _, wrong := range []fanoutobligation.ListQuery{
				{RunID: uuid.NewString(), Cursor: first.NextCursor},
				{RunID: base.runID, Cursor: first.NextCursor, Filter: fanoutobligation.ListFilter{Status: fanoutobligation.StatusOpen}},
				{RunID: base.runID, Cursor: first.NextCursor, Filter: fanoutobligation.ListFilter{FlowPath: "other"}},
				{RunID: base.runID, Cursor: "garbage"},
			} {
				if _, err := reader.ListFanOutIntents(ctx, wrong); !errors.Is(err, fanoutobligation.ErrInvalidListCursor) {
					t.Fatalf("cursor accepted: %+v err=%v", wrong, err)
				}
			}
			raw, _ := base64.RawURLEncoding.DecodeString(first.NextCursor)
			var token map[string]any
			if err := json.Unmarshal(raw, &token); err != nil {
				t.Fatal(err)
			}
			token["order"] = "updated_at_desc"
			raw, _ = json.Marshal(token)
			if _, err := reader.ListFanOutIntents(ctx, fanoutobligation.ListQuery{RunID: base.runID, Cursor: base64.RawURLEncoding.EncodeToString(raw)}); !errors.Is(err, fanoutobligation.ErrInvalidListCursor) {
				t.Fatalf("wrong order: %v", err)
			}
			for _, filter := range []fanoutobligation.ListFilter{
				{FlowPath: base.flowPath, SemanticPath: base.semanticPath},
				{TriggeringDeliveryID: base.deliveryID, SemanticPath: base.semanticPath, Status: fanoutobligation.StatusOpen},
			} {
				page, err := reader.ListFanOutIntents(ctx, fanoutobligation.ListQuery{RunID: base.runID, Filter: filter})
				if err != nil || len(page.Intents) != 1 || page.Intents[0].Cursor != 32 || page.NextCursor != "" {
					t.Fatalf("filtered=%+v err=%v", page, err)
				}
			}
			page, err := reader.ListFanOutIntents(ctx, fanoutobligation.ListQuery{RunID: base.runID, Filter: fanoutobligation.ListFilter{Status: fanoutobligation.StatusClosed}})
			if err != nil || page.Intents == nil || len(page.Intents) != 0 {
				t.Fatalf("empty=%+v err=%v", page, err)
			}
			if _, err := reader.ListFanOutIntents(ctx, fanoutobligation.ListQuery{RunID: uuid.NewString()}); !errors.Is(err, operatorread.ErrRunNotFound) {
				t.Fatalf("missing run: %v", err)
			}
			// Corrupt evidence is not silently filtered out or treated as completion.
			if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET capsule='{}' WHERE run_id=$1`, base.runID); err != nil {
				t.Fatal(err)
			}
			if _, err := reader.ListFanOutIntents(ctx, fanoutobligation.ListQuery{RunID: base.runID}); err == nil {
				t.Fatal("malformed capsule accepted")
			}
		})
	}
}

func TestFanOutReadDefaultLimitAndCurrentPagesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			base := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 1, time.Now().UTC())
			for n := 0; n < fanoutobligation.DefaultListLimit+1; n++ {
				seedFanOutOwnerIntent(t, ctx, db, base, 1, time.Now().UTC())
			}
			reader := owner.(operatorread.FanOutReader)
			query := fanoutobligation.ListQuery{RunID: base.runID}
			first, err := reader.ListFanOutIntents(ctx, query)
			if err != nil {
				t.Fatal(err)
			}
			if len(first.Intents) != fanoutobligation.DefaultListLimit || first.NextCursor == "" {
				t.Fatalf("default page count=%d cursor=%q", len(first.Intents), first.NextCursor)
			}
			if err := first.Validate(query); err != nil {
				t.Fatal(err)
			}
			// A newly inserted row after the cursor is visible. A page is not a
			// retained cross-request snapshot; changing page size is allowed.
			newcomer := seedFanOutOwnerIntent(t, ctx, db, base, 1, time.Now().UTC())
			if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET semantic_path=$1 WHERE run_id=$2 AND semantic_path=$3`, `zz.last`, base.runID, newcomer.semanticPath); err != nil {
				t.Fatal(err)
			}
			if _, err := transitionRunForTest(ctx, owner, runlifecycle.ActiveTransitionRequest{RunID: base.runID, State: runlifecycle.StatePaused}); err != nil {
				t.Fatal(err)
			}
			query.Cursor = first.NextCursor
			query.Limit = fanoutobligation.MaxListLimit
			second, err := reader.ListFanOutIntents(ctx, query)
			if err != nil {
				t.Fatal(err)
			}
			if len(second.Intents) != 3 || second.NextCursor != "" || second.RunStatus != "paused" || second.ObservedAt.Before(first.ObservedAt) {
				t.Fatalf("current second page=%+v", second)
			}
			if err := second.Validate(query); err != nil {
				t.Fatal(err)
			}
			if second.Intents[len(second.Intents)-1].Key.ElementRef.SemanticPath != "zz.last" {
				t.Fatal("new arrival not visible after cursor")
			}
			for _, row := range second.Intents {
				if row.Runtime.Eligible != nil {
					t.Fatal("paused run invented execution eligibility")
				}
			}
			if err := owner.CancelRunFanOut(ctx, base.runID, "run_stopped", time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			page, err := reader.ListFanOutIntents(ctx, fanoutobligation.ListQuery{RunID: base.runID, Limit: fanoutobligation.MaxListLimit, Filter: fanoutobligation.ListFilter{Status: fanoutobligation.StatusCanceled}})
			if err != nil || len(page.Intents) != 53 {
				t.Fatalf("canceled page count=%d err=%v", len(page.Intents), err)
			}
			for _, row := range page.Intents {
				if row.Owed != 0 || row.DurableState != "canceled" || row.CancellationReason != "run_stopped" {
					t.Fatalf("canceled=%+v", row)
				}
			}
		})
	}
}

func TestFanOutReadLeaseRetryAndRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, restarted, db, postgres := newFanOutOwnerPairForTest(t, backend)
			base := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 64, time.Now().UTC())
			reader := restarted.(operatorread.FanOutReader)
			_, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "read-lease", BundleHash: base.bundleHash, Now: time.Now().UTC(), Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("claim found=%v err=%v", found, err)
			}
			page, err := reader.ListFanOutIntents(ctx, fanoutobligation.ListQuery{RunID: base.runID})
			if err != nil {
				t.Fatal(err)
			}
			row := page.Intents[0]
			if row.DurableState != "leased" || row.ClaimOwner != claim.Owner || row.ClaimGeneration != claim.Generation || row.LeaseExpiresAt == nil {
				t.Fatalf("lease=%+v", row)
			}
			if _, err := owner.ReleaseFanOutRetryable(ctx, pipeline.FanOutRetryableRelease{Claim: claim, Now: time.Now().UTC(), Failure: fanOutRetryFailureForTest()}); err != nil {
				t.Fatal(err)
			}
			page, err = reader.ListFanOutIntents(ctx, fanoutobligation.ListQuery{RunID: base.runID})
			if err != nil {
				t.Fatal(err)
			}
			row = page.Intents[0]
			if row.DurableState != "retry_wait" || row.Status != fanoutobligation.StatusOpen || row.Retry == nil || row.LeaseExpiresAt != nil || row.NextChunkSize != 16 || row.Owed != 64 || row.Failure != nil {
				t.Fatalf("retry=%+v", row)
			}
			if remaining := time.Until(row.Retry.ReadyAt.Add(time.Millisecond)); remaining > 0 {
				time.Sleep(remaining)
			}
			page, err = reader.ListFanOutIntents(ctx, fanoutobligation.ListQuery{RunID: base.runID})
			if err != nil || page.Intents[0].DurableState != "eligible" || page.Intents[0].Cursor != 0 {
				t.Fatalf("due page=%+v err=%v", page, err)
			}
		})
	}
}

func TestFanOutReadIdentityByteOrderBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			base := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 1, time.Now().UTC())
			paths := []string{"z", "a|b", "A", "\u00e9", "a"}
			for index, path := range paths {
				fixture := base
				if index != 0 {
					fixture = seedFanOutOwnerIntent(t, ctx, db, base, 1, time.Now().UTC())
				}
				if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET semantic_path=$1 WHERE run_id=$2 AND semantic_path=$3`, path, base.runID, fixture.semanticPath); err != nil {
					t.Fatal(err)
				}
			}
			query := fanoutobligation.ListQuery{RunID: base.runID, Limit: 1}
			var got []string
			for {
				page, err := owner.(operatorread.FanOutReader).ListFanOutIntents(ctx, query)
				if err != nil {
					t.Fatal(err)
				}
				if err := page.Validate(query); err != nil {
					t.Fatal(err)
				}
				for _, row := range page.Intents {
					got = append(got, row.Key.ElementRef.SemanticPath)
				}
				if page.NextCursor == "" {
					break
				}
				query.Cursor = page.NextCursor
			}
			sort.Strings(paths)
			if !reflect.DeepEqual(got, paths) {
				t.Fatalf("order=%q want=%q", got, paths)
			}
		})
	}
}

func TestFanOutReadExactRootFilterBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			owner, _, db, postgres := newFanOutOwnerPairForTest(t, backend)
			base := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 1, time.Now().UTC())
			seedFanOutOwnerIntent(t, ctx, db, base, 1, time.Now().UTC())
			if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET flow_path='.' WHERE run_id=$1`, base.runID); err != nil {
				t.Fatal(err)
			}
			descendant := seedFanOutOwnerIntent(t, ctx, db, base, 1, time.Now().UTC())
			if _, err := db.ExecContext(ctx, `UPDATE fan_out_intents SET flow_path='child' WHERE run_id=$1 AND semantic_path=$2`, base.runID, descendant.semanticPath); err != nil {
				t.Fatal(err)
			}
			query := fanoutobligation.ListQuery{RunID: base.runID, Limit: 1, Filter: fanoutobligation.ListFilter{FlowPath: "."}}
			reader := owner.(operatorread.FanOutReader)
			page, err := reader.ListFanOutIntents(ctx, query)
			if err != nil {
				t.Fatal(err)
			}
			if len(page.Intents) != 1 || page.Intents[0].Key.ElementRef.FlowPath != "." || page.NextCursor == "" {
				t.Fatalf("root first=%+v", page)
			}
			query.Cursor = page.NextCursor
			page, err = reader.ListFanOutIntents(ctx, query)
			if err != nil || len(page.Intents) != 1 || page.Intents[0].Key.ElementRef.FlowPath != "." || page.NextCursor != "" {
				t.Fatalf("root second=%+v err=%v", page, err)
			}
			query.Filter.FlowPath = ""
			if _, err := reader.ListFanOutIntents(ctx, query); !errors.Is(err, fanoutobligation.ErrInvalidListCursor) {
				t.Fatalf("root cursor became unfiltered: %v", err)
			}
		})
	}
}
