package runtimepersistence

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/google/uuid"
)

// These cuts delegate real SQL and COMMIT through the existing driver probe.
// An injected COMMIT-return error models missing acknowledgement, not wire loss.
func TestPostgresWriterMigrationCancellationAndCommitCuts(t *testing.T) {
	for _, owner := range []string{"llm_acquire", "directive_renew", "directive_expiry", "operator_principal"} {
		phases := []string{"healthy", "entry_cancel", "write_cancel", "commit_admitted_cancel", "uncertain_commit"}
		if owner == "llm_acquire" {
			phases = append(phases, "handoff_failure")
		}
		for _, phase := range phases {
			t.Run(owner+"/"+phase, func(t *testing.T) {
				fixture, store, probe := openPipelineGracefulFixture(t)
				base := runtimeeffects.WithDifferentOwner(testAuthorActivityContext(), runtimeeffects.OwnerBuildTestInfrastructure)
				now := time.Now().UTC().Truncate(time.Microsecond)
				var invoke func(context.Context) error
				var changed func() bool
				var needle string
				var lease *runtimesessions.Lease
				var deleted int
				var submits atomic.Int32
				injected := errors.New("injected writer migration cut")
				switch owner {
				case "llm_acquire":
					seedSpecAgent(t, base, store, "a1", "", "")
					seedSpecMemoryRun(t, base, fixture.db)
					identity := specMemoryIdentity("a1", "global")
					var bundleHash string
					if err := fixture.db.QueryRowContext(base, `SELECT bundle_hash FROM runs WHERE run_id=$1::uuid`, identity.RunID).Scan(&bundleHash); err != nil {
						t.Fatal(err)
					}
					sink := &completionHandoffEvidenceProbeSink{submit: func(candidate runtimerunlifecycle.Candidate) error {
						submits.Add(1)
						if candidate.RunID != identity.RunID {
							t.Errorf("foreign handoff: %+v", candidate)
						}
						if phase == "handoff_failure" {
							return injected
						}
						return nil
					}}
					registration, err := store.RegisterCompletionCandidateSink(base, runtimerunlifecycle.CandidateScope{BundleHash: bundleHash}, sink)
					if err != nil {
						t.Fatal(err)
					}
					defer registration.Release()
					needle = "insert into agent_sessions"
					invoke = func(ctx context.Context) error {
						var err error
						lease, _, err = store.AcquireLiveSession(ctx, identity, "writer-cut-owner")
						return err
					}
					changed = func() bool {
						var count int
						if err := fixture.db.QueryRowContext(base, `SELECT COUNT(*) FROM agent_sessions WHERE run_id=$1::uuid AND lease_holder='writer-cut-owner'`, identity.RunID).Scan(&count); err != nil {
							t.Fatal(err)
						}
						return count == 1
					}
				case "directive_renew", "directive_expiry":
					seedDirectiveOperationRun(t, fixture.db, true)
					req := directiveOperationReservationForTest(t, uuid.NewString(), uuid.NewString(), "writer-cut-key", "writer-cut-hash", now)
					reserved, err := store.ReserveDirectiveOperation(base, req)
					if err != nil {
						t.Fatal(err)
					}
					id, ownerID := reserved.Operation.OperationID, uuid.NewString()
					if _, err := admitDirectiveExecutionForTest(base, store, id, ownerID, now, time.Minute); err != nil {
						t.Fatal(err)
					}
					if owner == "directive_renew" {
						needle = "update agent_directive_operations set execution_lease_expires_at"
						invoke = func(ctx context.Context) error {
							return store.RenewDirectiveExecutionLease(ctx, id, ownerID, now.Add(time.Second), 2*time.Minute)
						}
						changed = func() bool {
							var expiry time.Time
							if err := fixture.db.QueryRowContext(base, `SELECT execution_lease_expires_at FROM agent_directive_operations WHERE operation_id=$1::uuid`, id).Scan(&expiry); err != nil {
								t.Fatal(err)
							}
							return expiry.Equal(now.Add(time.Second + 2*time.Minute))
						}
					} else {
						if _, err := store.RecordDirectiveExecuted(base, id, ownerID, directiveOperationResponseForTest(reserved.Operation), now.Add(time.Second)); err != nil {
							t.Fatal(err)
						}
						if _, err := store.FinalizeDirectiveSuccess(base, id, now.Add(2*time.Second), 24*time.Hour); err != nil {
							t.Fatal(err)
						}
						needle = "delete from agent_directive_operations"
						invoke = func(ctx context.Context) error {
							result, err := store.ReconcileDirectiveOperations(ctx, now.Add(25*time.Hour), 24*time.Hour)
							deleted = result.Deleted
							return err
						}
						changed = func() bool {
							var count int
							if err := fixture.db.QueryRowContext(base, `SELECT COUNT(*) FROM agent_directive_operations WHERE operation_id=$1::uuid`, id).Scan(&count); err != nil {
								t.Fatal(err)
							}
							return count == 0
						}
					}
				case "operator_principal":
					needle = "insert into operator_principals"
					invoke = func(ctx context.Context) error {
						_, err := store.EnsureOperatorPrincipal(ctx, now)
						return err
					}
					changed = func() bool {
						var count int
						if err := fixture.db.QueryRowContext(base, `SELECT COUNT(*) FROM operator_principals`).Scan(&count); err != nil {
							t.Fatal(err)
						}
						return count == 1
					}
				}
				ctx, cancel := context.WithCancel(base)
				defer cancel()
				var writes, commits atomic.Int32
				probe.set(func(at, query string) error {
					if at == "exec" && strings.Contains(strings.ToLower(query), needle) {
						writes.Add(1)
						if phase == "write_cancel" {
							cancel()
						}
					}
					if at == "commit_admitted" {
						commits.Add(1)
						if phase == "commit_admitted_cancel" {
							cancel()
						}
					}
					if at == "commit_returned" && phase == "uncertain_commit" {
						return injected
					}
					return nil
				})
				if phase == "entry_cancel" {
					cancel()
				}
				err := invoke(ctx)
				probe.set(nil)
				switch phase {
				case "entry_cancel", "write_cancel":
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("expected cancellation, got %v", err)
					}
				case "uncertain_commit", "handoff_failure":
					if !errors.Is(err, injected) {
						t.Fatalf("lost injected failure: %v", err)
					}
				default:
					if err != nil {
						t.Fatal(err)
					}
				}
				wantCommitted := phase != "entry_cancel" && phase != "write_cancel"
				if changed() != wantCommitted {
					t.Fatalf("durable change disagrees with COMMIT cut; error=%v", err)
				}
				wantWrites, wantCommits := int32(1), int32(0)
				if phase == "entry_cancel" {
					wantWrites = 0
				}
				if wantCommitted {
					wantCommits = 1
				}
				if writes.Load() != wantWrites || commits.Load() != wantCommits {
					t.Fatalf("writes=%d commits=%d, want %d/%d; no replay allowed", writes.Load(), commits.Load(), wantWrites, wantCommits)
				}
				acknowledged := wantCommitted && phase != "uncertain_commit"
				if owner == "llm_acquire" {
					if (lease != nil) != acknowledged || submits.Load() != boolCount(acknowledged) {
						t.Fatalf("lease=%+v handoffs=%d acknowledged=%t", lease, submits.Load(), acknowledged)
					}
				}
				if owner == "directive_expiry" && deleted != int(boolCount(acknowledged)) {
					t.Fatalf("deleted=%d acknowledged=%t", deleted, acknowledged)
				}
				if err := fixture.db.PingContext(base); err != nil {
					t.Fatalf("successor connection unavailable: %v", err)
				}
			})
		}
	}
}

func boolCount(value bool) int32 {
	if value {
		return 1
	}
	return 0
}
