package runtimepersistence

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

// The connector observes real selected-owner SQL and injects only the boundary
// cut. Provider execution is never invoked by this persistence test.
func TestExternalEffectClosedMutationCancellation(t *testing.T) {
	for _, operation := range []string{"authorize", "response_observed", "owned_response_observed"} {
		for _, cut := range []string{"healthy", "before_admission", "after_write", "lost_commit_ack"} {
			t.Run(operation+"/"+cut, func(t *testing.T) {
				base, store, probe := openPipelineGracefulFixture(t)
				fixture := newCompletionSettlementFixture(t, store, base.db, false)
				ctx, cancel := context.WithCancel(fixture.context)
				defer cancel()
				ctx = withManagedCompletionTestSurface(t, ctx, fixture.authority, "claude_cli")
				var handle *runtimeeffects.Handle
				var err error
				if operation != "authorize" {
					handle, err = beginManagedCompletionForTest(t, ctx, "claude_cli", []byte("closed-mutation"))
					if err != nil {
						t.Fatal(err)
					}
					if err := handle.MarkLaunched(ctx); err != nil {
						t.Fatal(err)
					}
				}
				var cutReached atomic.Bool
				var commits atomic.Int32
				lostAck := errors.New("injected loss after native COMMIT acknowledgement")
				probe.set(func(phase, query string) error {
					if phase == "commit_returned" {
						commits.Add(1)
						if cut == "lost_commit_ack" && cutReached.CompareAndSwap(false, true) {
							return lostAck
						}
					}
					if cut == "after_write" && phase == "exec" && strings.Contains(query, "runtime_external_effect_operations") && cutReached.CompareAndSwap(false, true) {
						cancel()
					}
					return nil
				})
				if cut == "before_admission" {
					cancel()
				}
				if operation == "authorize" {
					handle, err = beginManagedCompletionForTest(t, ctx, "claude_cli", []byte("closed-mutation"))
				} else if operation == "owned_response_observed" {
					err = handle.MarkResponseObserved(ctx, map[string]any{"response_fingerprint": "closed-mutation"})
				} else {
					err = fixture.store.MarkExternalAttemptResponseObserved(ctx, handle.Attempt(), map[string]any{"response_fingerprint": "closed-mutation"}, time.Now().UTC())
				}
				probe.set(nil)
				if cut == "healthy" && err != nil {
					t.Fatalf("healthy mutation: %v", err)
				}
				if operation != "owned_response_observed" && (cut == "before_admission" || cut == "after_write") && !errors.Is(err, context.Canceled) {
					t.Fatalf("operation cancellation lost: %v", err)
				}
				if operation == "owned_response_observed" && cut != "lost_commit_ack" && err != nil {
					t.Fatalf("admitted effect response must survive caller stop: %v", err)
				}
				if cut == "lost_commit_ack" && !errors.Is(err, lostAck) {
					t.Fatalf("commit uncertainty lost: %v", err)
				}
				if (cut == "after_write" || cut == "lost_commit_ack") && !cutReached.Load() {
					t.Fatal("requested boundary was not reached")
				}
				wantCommits := int32(0)
				if cut == "healthy" || cut == "lost_commit_ack" || operation == "owned_response_observed" {
					wantCommits = 1
				}
				if commits.Load() != wantCommits {
					t.Fatalf("commit calls=%d want=%d; mutation must not replay", commits.Load(), wantCommits)
				}
				var operations, attempts, reservations int
				if err := fixture.db.QueryRow(`SELECT (SELECT COUNT(*) FROM runtime_external_effect_operations), (SELECT COUNT(*) FROM runtime_external_effect_attempts), (SELECT COUNT(*) FROM runtime_effect_budget_reservations)`).Scan(&operations, &attempts, &reservations); err != nil {
					t.Fatal(err)
				}
				wantRows := 1
				if operation == "authorize" && wantCommits == 0 {
					wantRows = 0
				}
				if operations != wantRows || attempts != wantRows || reservations != wantRows {
					t.Fatalf("partial or duplicated facts: operations=%d attempts=%d reservations=%d want=%d", operations, attempts, reservations, wantRows)
				}
				if operation != "authorize" {
					wantState := "launched"
					if wantCommits == 1 {
						wantState = "response_observed"
					}
					var operationState, attemptState string
					if err := fixture.db.QueryRow(`SELECT o.state, a.state FROM runtime_external_effect_operations o JOIN runtime_external_effect_attempts a USING (operation_id)`).Scan(&operationState, &attemptState); err != nil {
						t.Fatal(err)
					}
					if operationState != wantState || attemptState != wantState {
						t.Fatalf("partial response observation: operation=%s attempt=%s want=%s", operationState, attemptState, wantState)
					}
				}
			})
		}
	}
}
