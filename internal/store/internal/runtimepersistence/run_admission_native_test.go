package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	privatemutationlog "github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	sourceartifactstore "github.com/division-sh/swarm/internal/store/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type admissionLifecycleWriter interface {
	RequireActiveSourceTx(context.Context, *sql.Tx, string) (runtimecorrelation.SourceArtifactFact, error)
	TransitionActiveTx(context.Context, *mutationprotocol.Attempt, runtimerunlifecycle.ActiveTransitionRequest) (runtimerunlifecycle.MutationDisposition, error)
	ReviseSourceTx(context.Context, *mutationprotocol.Attempt, runtimerunlifecycle.SourceRevisionRequest) (runtimerunlifecycle.MutationDisposition, error)
	MarkTerminalTx(context.Context, *mutationprotocol.Attempt, runtimerunlifecycle.TerminalRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error)
	CompleteRunTx(context.Context, *mutationprotocol.Attempt, string, time.Time) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error)
	ForkSourceTx(context.Context, *mutationprotocol.Attempt, runtimerunlifecycle.ForkSourceRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error)
}

func selectedAdmissionLifecycleWriter(selected any) admissionLifecycleWriter {
	switch store := selected.(type) {
	case *PostgresStore:
		return store.runLifecyclePostgresOwner
	case *SQLiteRuntimeStore:
		return store.runLifecycleSQLiteOwner
	default:
		panic("unsupported admission fixture store")
	}
}

func TestRunAdmissionCanonicalInvalidationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			writer := selectedAdmissionLifecycleWriter(fixture.store)
			ctx := testAuthorActivitySourceArtifactContext()
			sourceartifactfixture.RequireArtifact(t, ctx, fixture.store, runLifecycleCandidateParityReplacementArtifact)
			for _, change := range []string{"pause", "source-revision", "terminal", "complete", "fork-source", "terminal-uuid-alias", "terminal-unbound-caller"} {
				t.Run(change, func(t *testing.T) {
					if !fixture.postgres && strings.HasSuffix(change, "uuid-alias") {
						return // SQLite's run identity is exact TEXT, not PostgreSQL UUID equivalence.
					}
					runID, childID := uuid.NewString(), uuid.NewString()
					started := time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)
					ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, started)
					if change == "fork-source" {
						ensureRunLifecycleCandidateParityRun(t, fixture, ctx, childID, started)
					}
					rollback := errors.New("rollback after invalidation proof")
					err := runSelectedFixtureMutation(ctx, fixture.store, "canonical admission invalidation", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
						var original runtimecorrelation.SourceArtifactFact
						if err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) (err error) {
							original, err = writer.RequireActiveSourceTx(txctx, tx, runID)
							return err
						}); err != nil {
							return err
						}
						writeID := runID
						if strings.HasSuffix(change, "uuid-alias") {
							writeID = strings.ToUpper(strings.ReplaceAll(runID, "-", ""))
						}
						var err error
						writeCtx := txctx
						if change == "terminal-unbound-caller" {
							writeCtx = ctx
						}
						switch change {
						case "pause":
							_, err = writer.TransitionActiveTx(txctx, attempt, runtimerunlifecycle.ActiveTransitionRequest{RunID: writeID, State: runtimerunlifecycle.StatePaused})
						case "source-revision":
							_, err = writer.ReviseSourceTx(txctx, attempt, runtimerunlifecycle.SourceRevisionRequest{RunID: writeID, Source: mustStoreTestSourceArtifactFact(runLifecycleCandidateParityReplacementHash)})
						case "terminal", "terminal-uuid-alias", "terminal-unbound-caller":
							_, _, err = writer.MarkTerminalTx(writeCtx, attempt, runtimerunlifecycle.TerminalRequest{RunID: writeID, State: runtimerunlifecycle.StateCancelled, EndedAt: started.Add(time.Minute)})
						case "complete":
							_, _, err = writer.CompleteRunTx(txctx, attempt, writeID, started.Add(time.Minute))
						case "fork-source":
							_, _, err = writer.ForkSourceTx(txctx, attempt, runtimerunlifecycle.ForkSourceRequest{RunID: writeID, ContinuedAsRunID: childID, EndedAt: started.Add(time.Minute)})
						}
						if err != nil {
							return err
						}
						if err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
							if _, cached, err := mutationprotocol.CachedActiveRunSource(txctx, tx, runID); err != nil || cached {
								return errors.Join(errors.New("canonical write retained old admission"), err)
							}
							fact, err := writer.RequireActiveSourceTx(txctx, tx, runID)
							if change == "pause" {
								if err != nil || !fact.Matches(original) {
									return errors.Join(errors.New("paused run failed fresh admission"), err)
								}
								return nil
							}
							if strings.HasPrefix(change, "source-revision") {
								if err != nil || fact.BundleHash() != runLifecycleCandidateParityReplacementHash {
									return errors.Join(errors.New("source revision reused stale source"), err)
								}
								return nil
							}
							state := runtimerunlifecycle.StateCancelled
							if change == "complete" {
								state = runtimerunlifecycle.StateCompleted
							} else if change == "fork-source" {
								state = runtimerunlifecycle.StateForked
							}
							return validateInactiveAdmissionRefusal(fact, err, runID, state)
						}); err != nil {
							return err
						}
						return rollback
					})
					if !errors.Is(err, rollback) {
						t.Fatal(err)
					}
					snapshot, err := fixture.store.LoadRunLifecycleSnapshot(ctx, runID)
					if err != nil || snapshot.Status != string(runtimerunlifecycle.StateRunning) {
						t.Fatalf("rollback leaked lifecycle: %+v, %v", snapshot, err)
					}
					fact, err := fixture.store.RequireActiveRunSource(ctx, runID)
					if err != nil || fact.BundleHash() != runLifecycleCandidateParityBundleHash {
						t.Fatalf("rollback leaked source: %+v, %v", fact, err)
					}
				})
			}
		})
	}
}

func validateInactiveAdmissionRefusal(fact runtimecorrelation.SourceArtifactFact, err error, runID string, state runtimerunlifecycle.State) error {
	var inactive *runtimerunlifecycle.RunNotActiveError
	if !errors.As(err, &inactive) || inactive == nil || inactive.RunID != runID || inactive.State != state || fact.Validate() == nil {
		return fmt.Errorf("inactive admission refusal: fact=%q error=%v; want run=%q state=%q and invalid fact", fact.BundleHash(), err, runID, state)
	}
	return nil
}

func TestRunAdmissionMissingRunRefusesBeforeMutationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			writer := selectedAdmissionLifecycleWriter(fixture.store)
			ctx := testAuthorActivitySourceArtifactContext()
			runID := uuid.NewString()
			rollback := errors.New("rollback after missing-run admission")
			err := runSelectedFixtureMutation(ctx, fixture.store, "missing-run admission", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
				return attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
					fact, err := writer.RequireActiveSourceTx(txctx, tx, runID)
					var missing *runtimerunlifecycle.RunNotFoundError
					if !errors.As(err, &missing) || missing == nil || missing.RunID != runID || fact.Validate() == nil {
						return fmt.Errorf("missing active run: fact=%q error=%v; want missing %q and invalid fact", fact.BundleHash(), err, runID)
					}
					if _, cached, err := mutationprotocol.CachedActiveRunSource(txctx, tx, runID); err != nil || cached {
						return errors.Join(errors.New("missing run minted active admission"), err)
					}
					var runs, mutations int
					if err := tx.QueryRowContext(txctx, `SELECT COUNT(*) FROM runs WHERE run_id=$1`, runID).Scan(&runs); err != nil {
						return err
					}
					if err := tx.QueryRowContext(txctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1`, runID).Scan(&mutations); err != nil {
						return err
					}
					if runs != 0 || mutations != 0 {
						return fmt.Errorf("missing-run refusal wrote runs=%d mutations=%d before rollback", runs, mutations)
					}
					return rollback
				})
			})
			if !errors.Is(err, rollback) {
				t.Fatal(err)
			}
		})
	}
}

func TestRunAdmissionWarmArtifactLossRefusesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			writer := selectedAdmissionLifecycleWriter(fixture.store)
			ctx := testAuthorActivitySourceArtifactContext()
			runID := uuid.NewString()
			ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, time.Now().UTC())
			rollback := errors.New("rollback after warmed-artifact refusal")
			err := runSelectedFixtureMutation(ctx, fixture.store, "warmed-artifact refusal", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
				return attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
					original, err := writer.RequireActiveSourceTx(txctx, tx, runID)
					if err != nil || original.BundleHash() != runLifecycleCandidateParityBundleHash {
						return errors.Join(errors.New("failed initial native source admission"), err)
					}
					if err := attempt.RequireActiveRunSourceAdmission(txctx, runID, original); err != nil {
						return err
					}
					if err := sourceartifactstore.DeleteSourceArtifactForFixtureRefusalTx(txctx, tx, backend, original); err != nil {
						return err
					}
					cachedFact, cached, err := mutationprotocol.CachedActiveRunSource(txctx, tx, runID)
					if err != nil || !cached || !cachedFact.Matches(original) {
						return errors.Join(errors.New("artifact fault erased warmed run admission"), err)
					}
					for range 2 {
						fact, err := writer.RequireActiveSourceTx(txctx, tx, runID)
						var unavailable *runtimerunlifecycle.SourceArtifactUnavailableError
						if !errors.As(err, &unavailable) || unavailable == nil || unavailable.BundleHash != original.BundleHash() || unavailable.Cause != "missing_source_artifact" || fact.Validate() == nil {
							return fmt.Errorf("warm admission reused absent artifact: fact=%q error=%v; want unavailable %q and invalid fact", fact.BundleHash(), err, original.BundleHash())
						}
					}
					return rollback
				})
			})
			if !errors.Is(err, rollback) {
				t.Fatal(err)
			}
			fact, err := fixture.store.RequireActiveRunSource(ctx, runID)
			if err != nil || fact.BundleHash() != runLifecycleCandidateParityBundleHash {
				t.Fatalf("rollback did not restore source availability: %q, %v", fact.BundleHash(), err)
			}
		})
	}
}

func TestInactiveAdmissionRefusalRejectsWrongStateIdentityAndLiveFact(t *testing.T) {
	runID := uuid.NewString()
	valid := mustStoreTestSourceArtifactFact(runLifecycleCandidateParityBundleHash)
	for _, probe := range []struct {
		name     string
		fact     runtimecorrelation.SourceArtifactFact
		err      error
		wantPass bool
	}{
		{name: "exact", err: &runtimerunlifecycle.RunNotActiveError{RunID: runID, State: runtimerunlifecycle.StateForked}, wantPass: true},
		{name: "wrong-run", err: &runtimerunlifecycle.RunNotActiveError{RunID: uuid.NewString(), State: runtimerunlifecycle.StateForked}},
		{name: "wrong-state", err: &runtimerunlifecycle.RunNotActiveError{RunID: runID, State: runtimerunlifecycle.StateCancelled}},
		{name: "live-fact", fact: valid, err: &runtimerunlifecycle.RunNotActiveError{RunID: runID, State: runtimerunlifecycle.StateForked}},
		{name: "untyped", err: errors.New("inactive")},
		{name: "success", fact: valid},
	} {
		t.Run(probe.name, func(t *testing.T) {
			if err := validateInactiveAdmissionRefusal(probe.fact, probe.err, runID, runtimerunlifecycle.StateForked); (err == nil) != probe.wantPass {
				t.Fatalf("refusal oracle: %v", err)
			}
		})
	}
}

type sourceFactWithoutMutableAdmission struct {
	fact runtimecorrelation.SourceArtifactFact
}

func (s sourceFactWithoutMutableAdmission) RequireActiveRunSource(context.Context, string) (runtimecorrelation.SourceArtifactFact, error) {
	return s.fact, nil
}

func TestMutationLogCannotBorrowLookupFactBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			ctx := testAuthorActivitySourceArtifactContext()
			runID := uuid.NewString()
			ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, time.Now())
			fact, err := fixture.store.RequireActiveRunSource(ctx, runID)
			if err != nil {
				t.Fatal(err)
			}
			ctx = runtimecorrelation.WithSourceArtifactFact(runtimecorrelation.WithRunID(ctx, runID), fact)
			rollback := errors.New("rollback after lookup-authority refusal")
			err = runSelectedFixtureMutation(ctx, fixture.store, "lookup is not mutable admission", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
				return attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
					record := runtimemutationlog.Record{EntityID: uuid.NewString(), Domain: runtimemutationlog.DomainLifecycleState, NewValue: "active", WriterType: "system_node", WriterID: "admission-proof"}
					owner := sourceFactWithoutMutableAdmission{fact: fact}
					var err error
					if fixture.postgres {
						err = privatemutationlog.Insert(txctx, attempt, owner, record)
					} else {
						err = privatemutationlog.InsertSQLite(txctx, attempt, owner, record)
					}
					if err == nil || !strings.Contains(err.Error(), "was not admitted") {
						return errors.Join(errors.New("lookup fact lent mutation authority"), err)
					}
					var count int
					if err := tx.QueryRowContext(txctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id = $1`, runID).Scan(&count); err != nil {
						return err
					}
					if count != 0 {
						return errors.New("refusal inserted mutation evidence before rollback")
					}
					return rollback
				})
			})
			if !errors.Is(err, rollback) {
				t.Fatal(err)
			}
		})
	}
}

func TestPendingEventDeltaVisibleThroughUnboundTerminalLoanBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			writer := selectedAdmissionLifecycleWriter(fixture.store)
			ctx := testAuthorActivitySourceArtifactContext()
			runID := uuid.NewString()
			started := time.Now().UTC().Round(time.Microsecond)
			ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, started)
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "revision.spelling", "gateway", "counter-visibility", []byte(`{"value":1}`), 0, runID, "", events.EventEnvelope{}, started)
			event, err := eventtest.AdmitPayload(event, "", "revision.spelling")
			if err != nil {
				t.Fatal(err)
			}
			admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
			if err != nil {
				t.Fatal(err)
			}
			record, err := eventrecord.FromAdmitted(admitted, testRouteSettlement(admitted.Event(), nil))
			if err != nil {
				t.Fatal(err)
			}
			rollback := errors.New("rollback after terminal counter visibility")
			err = runSelectedFixtureMutation(ctx, fixture.store, "terminal counter visibility", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
				inserted, err := insertExactSpellingEvent(txctx, fixture.postgres, attempt, record)
				if err != nil || !inserted {
					return errors.Join(errors.New("event was not physically inserted"), err)
				}
				// The original caller has no native attempt key; WithSQL must lend it.
				snapshot, _, err := writer.MarkTerminalTx(ctx, attempt, runtimerunlifecycle.TerminalRequest{RunID: runID, State: runtimerunlifecycle.StateCancelled, EndedAt: started.Add(time.Second)})
				if err != nil {
					return err
				}
				if snapshot.EventCount != 1 {
					return errors.New("terminal result omitted pending physical event delta")
				}
				return rollback
			})
			if !errors.Is(err, rollback) {
				t.Fatal(err)
			}
			snapshot, err := fixture.store.LoadRunLifecycleSnapshot(ctx, runID)
			if err != nil || snapshot.Status != string(runtimerunlifecycle.StateRunning) || snapshot.EventCount != 0 {
				t.Fatalf("rollback leaked terminal state/counter: %+v, %v", snapshot, err)
			}
		})
	}
}
