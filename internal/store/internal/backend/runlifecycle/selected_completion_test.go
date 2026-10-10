package runlifecycle

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
)

const selectedCompletionRunID = "00000000-0000-4000-8000-000000000642"
const selectedCompletionOtherRunID = "00000000-0000-4000-8000-000000000643"
const selectedCompletionBundle = "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type selectedCompletionAuthorityProbe struct {
	calls        int
	err          error
	got          runtimeeffects.Authority
	tx           *sql.Tx
	observations int
	current      bool
	observeErr   error
	observed     runtimeeffects.Authority
	query        sourceadmission.ExecutionQuery
	priorQueries sqlmock.Sqlmock
	priorErr     error
}

func (p *selectedCompletionAuthorityProbe) RequireCurrentExternalEffectAuthorityTx(_ context.Context, tx *sql.Tx, authority runtimeeffects.Authority) error {
	p.calls++
	p.got, p.tx = authority, tx
	if p.priorQueries != nil {
		p.priorErr = p.priorQueries.ExpectationsWereMet()
	}
	return p.err
}

func (p *selectedCompletionAuthorityProbe) ObserveCurrentExternalEffectAuthority(_ context.Context, q sourceadmission.ExecutionQuery, authority runtimeeffects.Authority) (bool, error) {
	p.observations++
	p.observed, p.query = authority, q
	if p.priorQueries != nil {
		p.priorErr = p.priorQueries.ExpectationsWereMet()
	}
	return p.current, p.observeErr
}

func selectedCompletionTransaction(t *testing.T) (*sql.Tx, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		mock.ExpectRollback()
		if err := tx.Rollback(); err != nil {
			t.Error(err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
		_ = db.Close()
	})
	return tx, mock
}

func selectedCompletionCandidate(selected bool) runtimerunlifecycle.Candidate {
	candidate := runtimerunlifecycle.Candidate{
		RunID: selectedCompletionRunID, BundleHash: selectedCompletionBundle,
		Revision: 3, DueAt: time.Unix(200, 0).UTC(),
	}
	if selected {
		candidate.SelectedForkRunID = candidate.RunID
	}
	return candidate
}

func selectedCompletionContext(t *testing.T) (context.Context, runtimeeffects.Authority) {
	t.Helper()
	authority := runtimeeffects.Authority{
		Kind: runtimeeffects.AuthoritySelectedContractFork, ID: selectedCompletionOtherRunID,
		ExecutionOwner: "retained-completion-owner", LeaseExpiresAt: time.Unix(300, 0).UTC(), FenceGeneration: 7,
		ExecutionMode: runtimeeffects.ExecutionModeLive,
		SelectedFork: runtimeeffects.SelectedContractForkAuthority{
			ExecutionID: selectedCompletionOtherRunID, ForkRunID: selectedCompletionRunID, Generation: 2,
			AdmissionFingerprint: "admission", ContainerPlanFingerprint: "container",
			ActorCensusFingerprint: "actors", EffectiveConfigFingerprint: "config",
		},
	}
	if !authority.Valid() {
		t.Fatal("invalid selected completion authority fixture")
	}
	source, err := runtimecorrelation.NewSourceArtifactFact(selectedCompletionBundle)
	if err != nil {
		t.Fatal(err)
	}
	ctx := runtimecorrelation.WithRunID(context.Background(), selectedCompletionRunID)
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, source)
	return runtimeeffects.WithAuthority(ctx, authority), authority
}

func expectSelectedCompletionMembership(mock sqlmock.Sqlmock, postgres, selected bool) {
	rows := sqlmock.NewRows([]string{"fork_run_id"})
	if selected {
		rows.AddRow(selectedCompletionRunID)
	}
	query := `SELECT CAST(fork_run_id AS TEXT) FROM run_fork_selected_contract_bindings WHERE fork_run_id = ?`
	if postgres {
		query = `SELECT fork_run_id::text FROM run_fork_selected_contract_bindings WHERE fork_run_id = $1::uuid`
	}
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(selectedCompletionRunID).WillReturnRows(rows)
}

func expectSelectedCompletionRunLock(mock sqlmock.Sqlmock, postgres bool, state string, revision int64, dueAt, now time.Time) {
	rows := sqlmock.NewRows([]string{"status", "bundle_hash", "completion_revision", "completion_due_at"})
	query := `SELECT LOWER(status), bundle_hash, completion_revision, completion_due_at FROM runs WHERE run_id = ?`
	if postgres {
		query = `SELECT LOWER(status), bundle_hash, completion_revision, completion_due_at, clock_timestamp() FROM runs WHERE run_id = $1::uuid FOR UPDATE`
		rows = sqlmock.NewRows([]string{"status", "bundle_hash", "completion_revision", "completion_due_at", "now"}).
			AddRow(state, selectedCompletionBundle, revision, dueAt, now)
	} else {
		rows.AddRow(state, selectedCompletionBundle, revision, dueAt.Format(time.RFC3339Nano))
	}
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(selectedCompletionRunID).WillReturnRows(rows)
}

func executeSelectedCompletionTest(ctx context.Context, tx *sql.Tx, postgres bool, candidate runtimerunlifecycle.Candidate, owner runAuthorityOwner, now time.Time) (runtimerunlifecycle.CompletionResult, error) {
	if postgres {
		return (&RunLifecyclePostgresOwner{runAuthority: owner}).executeCompletionCandidateTx(ctx, tx, nil, candidate, runtimerunlifecycle.FinalCatalog{})
	}
	return (&RunLifecycleSQLiteOwner{runAuthority: owner, nowFn: func() time.Time { return now }}).
		executeCompletionCandidateTx(ctx, tx, nil, candidate, runtimerunlifecycle.FinalCatalog{})
}

func TestSelectedCompletionBindAuthorityExactlyOnce(t *testing.T) {
	probe := &selectedCompletionAuthorityProbe{}
	postgres, sqlite := &RunLifecyclePostgresOwner{}, &RunLifecycleSQLiteOwner{}
	if err := postgres.BindExecutionAuthority(nil); err == nil {
		t.Fatal("nil PostgreSQL completion authority accepted")
	}
	if err := sqlite.BindExecutionAuthority(nil); err == nil {
		t.Fatal("nil SQLite completion authority accepted")
	}
	if err := postgres.BindExecutionAuthority(probe); err != nil {
		t.Fatal(err)
	}
	if err := sqlite.BindExecutionAuthority(probe); err != nil {
		t.Fatal(err)
	}
	if postgres.BindExecutionAuthority(probe) == nil || sqlite.BindExecutionAuthority(probe) == nil {
		t.Fatal("completion authority replaced after binding")
	}
	if (*RunLifecyclePostgresOwner)(nil).BindExecutionAuthority(probe) == nil || (*RunLifecycleSQLiteOwner)(nil).BindExecutionAuthority(probe) == nil {
		t.Fatal("nil lifecycle owner accepted authority binding")
	}
}

func TestSelectedCompletionRequestDerivesModeFromBindingBothDialects(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		for _, selected := range []bool{true, false} {
			for _, already := range []bool{true, false} {
				t.Run(admissionTestDialect(postgres)+map[bool]string{true: "/selected", false: "/ordinary"}[selected]+map[bool]string{true: "/current", false: "/new"}[already], func(t *testing.T) {
					tx, mock := selectedCompletionTransaction(t)
					now := time.Unix(200, 0).UTC()
					var due driver.Value
					if already {
						due = now
						if !postgres {
							due = now.Format(time.RFC3339Nano)
						}
					}
					if postgres {
						mock.ExpectQuery(`SELECT LOWER\(status\), bundle_hash, completion_due_at, completion_revision, clock_timestamp\(\).*`).WithArgs(selectedCompletionRunID).
							WillReturnRows(sqlmock.NewRows([]string{"state", "bundle", "due", "revision", "now"}).AddRow("running", selectedCompletionBundle, due, 3, now))
					} else {
						mock.ExpectQuery(`SELECT LOWER\(status\), bundle_hash, completion_due_at, completion_revision.*`).WithArgs(selectedCompletionRunID).
							WillReturnRows(sqlmock.NewRows([]string{"state", "bundle", "due", "revision"}).AddRow("running", selectedCompletionBundle, due, 3))
					}
					expectSelectedCompletionMembership(mock, postgres, selected)
					if !already {
						if postgres {
							mock.ExpectQuery(`UPDATE runs`).WithArgs(selectedCompletionRunID, now, "running").
								WillReturnRows(sqlmock.NewRows([]string{"run_id", "bundle", "revision", "due"}).AddRow(selectedCompletionRunID, selectedCompletionBundle, 4, now))
						} else {
							mock.ExpectExec(`UPDATE runs`).WithArgs(now, selectedCompletionRunID, "running").WillReturnResult(sqlmock.NewResult(0, 1))
						}
					}
					var result runtimerunlifecycle.CandidateRequestResult
					var err error
					// Context deliberately contains no selected authority: mode comes only from binding membership.
					if postgres {
						result, err = RequestPostgresCompletionCandidateTx(context.Background(), tx, selectedCompletionRunID, nil)
					} else {
						result, err = RequestSQLiteCompletionCandidateTx(context.Background(), tx, selectedCompletionRunID, nil, now)
					}
					want := selectedCompletionCandidate(selected)
					want.DueAt = now
					if !already {
						want.Revision++
					}
					if err != nil || result.Candidate != want {
						t.Fatalf("binding-derived candidate = %+v, %v; want %+v", result, err, want)
					}
				})
			}
		}
	}
}

func TestSelectedCompletionBindingReadCannotFallBackOnSQLFailure(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		t.Run(admissionTestDialect(postgres), func(t *testing.T) {
			tx, mock := selectedCompletionTransaction(t)
			failure := errors.New("selected bindings table unavailable")
			mock.ExpectQuery(`FROM run_fork_selected_contract_bindings`).WithArgs(selectedCompletionRunID).WillReturnError(failure)
			if mode, err := selectedRunBinding(context.Background(), tx, postgres, selectedCompletionRunID); mode != "" || !errors.Is(err, failure) {
				t.Fatalf("membership error became ordinary completion: mode=%q err=%v", mode, err)
			}
		})
	}
}

func TestSelectedCompletionListingIsExactScopeBothDialects(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		for _, selected := range []bool{true, false} {
			t.Run(admissionTestDialect(postgres)+map[bool]string{true: "/selected", false: "/ordinary"}[selected], func(t *testing.T) {
				tx, mock := selectedCompletionTransaction(t)
				candidate := selectedCompletionCandidate(selected)
				scope := candidate.Scope()
				query, args := completionCandidatesQuery(postgres, scope, runtimerunlifecycle.CandidateCursor{}, 2)
				if !strings.Contains(query, "LEFT JOIN run_fork_selected_contract_bindings") || strings.Contains(query, "origin") {
					t.Fatal("listing mode does not come from canonical binding membership")
				}
				wantArgs := []any{selectedCompletionBundle, "", 2}
				if selected {
					wantArgs = []any{selectedCompletionBundle, "", selectedCompletionRunID, 2}
					if !strings.Contains(query, "AND b.fork_run_id = r.run_id") {
						t.Fatal("selected listing does not require exact binding")
					}
				} else if !strings.Contains(query, "AND b.fork_run_id IS NULL") {
					t.Fatal("ordinary listing admits selected candidates")
				}
				if !reflect.DeepEqual(args, wantArgs) {
					t.Fatalf("listing args=%v want=%v", args, wantArgs)
				}
				values := make([]driver.Value, len(args))
				for i, value := range args {
					values[i] = value
				}
				mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(values...).WillReturnRows(sqlmock.NewRows([]string{"run", "bundle", "revision", "due", "selected_run"}).
					AddRow(candidate.RunID, candidate.BundleHash, candidate.Revision, candidate.DueAt, candidate.SelectedForkRunID))
				page, err := listCompletionCandidatesTx(context.Background(), tx, postgres, scope, runtimerunlifecycle.CandidateCursor{}, 2)
				if err != nil || len(page.Candidates) != 1 || page.Candidates[0] != candidate || !page.Exhausted || page.Next.RunID != candidate.RunID {
					t.Fatalf("listing = %+v, %v", page, err)
				}
			})
		}
	}
}

func TestSelectedCompletionListingDoesNotStampMissingOrForeignMode(t *testing.T) {
	for _, selected := range []bool{true, false} {
		tx, mock := selectedCompletionTransaction(t)
		candidate := selectedCompletionCandidate(!selected)
		scope := selectedCompletionCandidate(selected).Scope()
		query, args := completionCandidatesQuery(false, scope, runtimerunlifecycle.CandidateCursor{}, 2)
		values := make([]driver.Value, len(args))
		for i, value := range args {
			values[i] = value
		}
		mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(values...).WillReturnRows(sqlmock.NewRows([]string{"run", "bundle", "revision", "due", "selected_run"}).
			AddRow(candidate.RunID, candidate.BundleHash, candidate.Revision, candidate.DueAt, candidate.SelectedForkRunID))
		if _, err := listCompletionCandidatesTx(context.Background(), tx, false, scope, runtimerunlifecycle.CandidateCursor{}, 2); err == nil {
			t.Fatal("foreign mode was relabeled from requested scope")
		}
	}
}

func TestSelectedCompletionExecutionRefusesBeforeAnyDueOrLifecycleMutation(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		for _, scenario := range []string{"ordinary_for_selected", "selected_candidate_for_ordinary", "selected_authority_for_ordinary", "invalid_selected_authority_for_ordinary", "missing_authority", "foreign_authority", "wrong_bundle", "missing_owner", "stale", "terminal_stale", "stale_coordinate"} {
			t.Run(admissionTestDialect(postgres)+"/"+scenario, func(t *testing.T) {
				tx, mock := selectedCompletionTransaction(t)
				candidate := selectedCompletionCandidate(true)
				ctx, authority := selectedCompletionContext(t)
				selected, state, revision := true, "running", candidate.Revision
				probe := &selectedCompletionAuthorityProbe{}
				var owner runAuthorityOwner = probe
				switch scenario {
				case "ordinary_for_selected":
					candidate.SelectedForkRunID = ""
					ctx = context.Background()
				case "selected_candidate_for_ordinary":
					selected = false
				case "selected_authority_for_ordinary":
					candidate.SelectedForkRunID = ""
					selected = false
				case "invalid_selected_authority_for_ordinary":
					candidate.SelectedForkRunID = ""
					selected = false
					authority.FenceGeneration = 0
					ctx = runtimeeffects.WithAuthority(ctx, authority)
				case "missing_authority":
					ctx = context.Background()
				case "foreign_authority":
					authority.SelectedFork.ForkRunID = selectedCompletionOtherRunID
					ctx = runtimeeffects.WithAuthority(ctx, authority)
				case "wrong_bundle":
					source, err := runtimecorrelation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("b", 64))
					if err != nil {
						t.Fatal(err)
					}
					ctx = runtimecorrelation.WithSourceArtifactFact(ctx, source)
				case "missing_owner":
					owner = nil
				case "stale", "terminal_stale":
					probe.err = runtimefailures.New(runtimefailures.ClassSupersededGeneration, "external_effect_authority_stale", "external-effects", "check_authority", nil)
					if scenario == "terminal_stale" {
						state = "completed"
					}
				case "stale_coordinate":
					ctx = context.Background()
					revision++
				}
				now := candidate.DueAt.Add(time.Second)
				expectSelectedCompletionRunLock(mock, postgres, state, revision, candidate.DueAt, now)
				expectSelectedCompletionMembership(mock, postgres, selected)
				// No due clear, barrier, schedule, or completion SQL is authorized by this refusal.
				result, err := executeSelectedCompletionTest(ctx, tx, postgres, candidate, owner, now)
				if !errors.Is(err, runtimerunlifecycle.ErrCompletionAuthority) || result.Committed {
					t.Fatalf("completion authority refusal = %+v, %v", result, err)
				}
				if scenario == "stale" || scenario == "terminal_stale" {
					if probe.calls != 1 || probe.tx != tx {
						t.Fatal("stale refusal did not consume the current native authority owner")
					}
					if envelope, typed := runtimefailures.EnvelopeFromError(err); !typed || envelope.Class != runtimefailures.ClassSupersededGeneration {
						t.Fatal("typed stale authority cause was lost")
					}
				} else if probe.calls != 0 {
					t.Fatal("mismatched authority reached the current-fence owner")
				}
			})
		}
	}
}

func TestSelectedCompletionFreshFencePreservesRearmAndOrdinaryBehavior(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		for _, selected := range []bool{true, false} {
			t.Run(admissionTestDialect(postgres)+map[bool]string{true: "/selected", false: "/ordinary"}[selected], func(t *testing.T) {
				tx, mock := selectedCompletionTransaction(t)
				candidate := selectedCompletionCandidate(selected)
				ctx, authority := selectedCompletionContext(t)
				if !selected {
					ctx = context.Background()
				}
				probe := &selectedCompletionAuthorityProbe{}
				now := candidate.DueAt.Add(-time.Second)
				expectSelectedCompletionRunLock(mock, postgres, "running", candidate.Revision, candidate.DueAt, now)
				expectSelectedCompletionMembership(mock, postgres, selected)
				result, err := executeSelectedCompletionTest(ctx, tx, postgres, candidate, probe, now)
				if err != nil || result.Outcome != runtimerunlifecycle.OutcomeRearmAt || result.Candidate != candidate {
					t.Fatalf("unchanged rearm algorithm = %+v, %v", result, err)
				}
				if selected && (probe.calls != 1 || !reflect.DeepEqual(probe.got, authority) || probe.tx != tx) {
					t.Fatal("selected candidate rearmed without exact current authority")
				}
				if !selected && probe.calls != 0 {
					t.Fatal("ordinary candidate borrowed selected completion authority")
				}
			})
		}
	}
}

func TestSelectedCompletionTerminalClearOccursOnlyAfterFreshFence(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		t.Run(admissionTestDialect(postgres), func(t *testing.T) {
			tx, mock := selectedCompletionTransaction(t)
			candidate := selectedCompletionCandidate(true)
			ctx, _ := selectedCompletionContext(t)
			probe := &selectedCompletionAuthorityProbe{}
			now := candidate.DueAt.Add(time.Second)
			expectSelectedCompletionRunLock(mock, postgres, "completed", candidate.Revision, candidate.DueAt, now)
			expectSelectedCompletionMembership(mock, postgres, true)
			mock.ExpectExec(`UPDATE runs SET completion_due_at = NULL`).WithArgs(candidate.RunID, candidate.Revision).WillReturnResult(sqlmock.NewResult(0, 1))
			result, err := executeSelectedCompletionTest(ctx, tx, postgres, candidate, probe, now)
			if err != nil || result.Outcome != runtimerunlifecycle.OutcomeExactNoop || probe.calls != 1 {
				t.Fatalf("terminal due clearing bypassed exact fence: %+v, %v calls=%d", result, err, probe.calls)
			}
		})
	}
}

func TestSelectedCompletionAuthorityPreservesCancellationAndSQLFailures(t *testing.T) {
	ctx, _ := selectedCompletionContext(t)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	candidate := selectedCompletionCandidate(true)
	if err := requireCompletionCandidateAuthorityTx(cancelled, nil, true, candidate, candidate.BundleHash, nil); !errors.Is(err, context.Canceled) || errors.Is(err, runtimerunlifecycle.ErrCompletionAuthority) {
		t.Fatalf("cancellation reclassified as stale authority: %v", err)
	}
	for _, failure := range []error{context.Canceled, context.DeadlineExceeded, errors.New("native fence SQL failed")} {
		tx, mock := selectedCompletionTransaction(t)
		expectSelectedCompletionMembership(mock, true, true)
		probe := &selectedCompletionAuthorityProbe{err: failure}
		if err := requireCompletionCandidateAuthorityTx(ctx, tx, true, candidate, candidate.BundleHash, probe); !errors.Is(err, failure) || errors.Is(err, runtimerunlifecycle.ErrCompletionAuthority) {
			t.Fatalf("native infrastructure/cancellation error reclassified: %v", err)
		}
	}
}
