package runlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
)

type runExecutionTestOwner interface {
	RequireRunExecutionTx(context.Context, *sql.Tx, string) error
	ObserveRunExecution(context.Context, sourceadmission.ExecutionQuery, string) (bool, error)
}

func runExecutionOwner(postgres bool, authority runAuthorityOwner) runExecutionTestOwner {
	if postgres {
		return &RunLifecyclePostgresOwner{runAuthority: authority}
	}
	return &RunLifecycleSQLiteOwner{runAuthority: authority}
}

func runExecutionTransaction(t *testing.T) (*sql.Tx, sqlmock.Sqlmock) {
	t.Helper()
	db, mock := admissionTestSQLMock(t)
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
	})
	return tx, mock
}

func expectRunExecutionSource(mock sqlmock.Sqlmock, postgres, observe bool, state string) {
	rows := sqlmock.NewRows([]string{"status", "bundle_hash"})
	if state != "missing" {
		rows.AddRow(state, selectedCompletionBundle)
	}
	mock.ExpectQuery(admissionTestRunSQL(postgres, observe)).WithArgs(selectedCompletionRunID).WillReturnRows(rows)
	if state == "running" || state == "paused" {
		mock.ExpectQuery(admissionTestArtifactSQL(postgres)).WithArgs(selectedCompletionBundle).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	}
}

func runExecutionBindingSQL(postgres bool) string {
	if postgres {
		return `SELECT fork_run_id::text FROM run_fork_selected_contract_bindings WHERE fork_run_id = $1::uuid`
	}
	return `SELECT CAST(fork_run_id AS TEXT) FROM run_fork_selected_contract_bindings WHERE fork_run_id = ?`
}

func expectRunExecutionBinding(mock sqlmock.Sqlmock, postgres, selected bool) {
	rows := sqlmock.NewRows([]string{"fork_run_id"})
	if selected {
		rows.AddRow(selectedCompletionRunID)
	}
	mock.ExpectQuery(runExecutionBindingSQL(postgres)).WithArgs(selectedCompletionRunID).WillReturnRows(rows)
}

func runExecutionStateSQL(postgres bool) string {
	if postgres {
		return `SELECT status FROM runs WHERE run_id = $1::uuid`
	}
	return `SELECT status FROM runs WHERE run_id = ?`
}

func expectRunExecutionState(mock sqlmock.Sqlmock, postgres bool, state string) {
	mock.ExpectQuery(runExecutionStateSQL(postgres)).WithArgs(selectedCompletionRunID).
		WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow(state))
}

func TestRunExecutionSelectedRefusesBeforeFenceBothDialects(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		for _, observe := range []bool{false, true} {
			for _, scenario := range []string{"paused", "fresh_paused", "missing_context", "missing_authority", "missing_run", "wrong_run", "foreign_authority", "invalid_authority", "missing_source", "wrong_source", "missing_owner"} {
				t.Run(admissionTestDialect(postgres)+map[bool]string{true: "/observe/", false: "/require/"}[observe]+scenario, func(t *testing.T) {
					tx, mock := runExecutionTransaction(t)
					ctx, authority := selectedCompletionContext(t)
					probe := &selectedCompletionAuthorityProbe{current: true}
					var authorityOwner runAuthorityOwner = probe
					sourceState, currentState := "running", "running"
					switch scenario {
					case "paused":
						sourceState, currentState = "paused", "paused"
					case "fresh_paused":
						currentState = "paused"
					case "missing_context":
						ctx = context.Background()
					case "missing_authority":
						ctx = runtimecorrelation.WithSourceArtifactFact(runtimecorrelation.WithRunID(context.Background(), selectedCompletionRunID), admissionTestSource(t, selectedCompletionBundle))
					case "missing_run":
						ctx = runtimeeffects.WithAuthority(runtimecorrelation.WithSourceArtifactFact(context.Background(), admissionTestSource(t, selectedCompletionBundle)), authority)
					case "wrong_run":
						ctx = runtimecorrelation.WithRunID(ctx, selectedCompletionOtherRunID)
					case "foreign_authority":
						authority.SelectedFork.ForkRunID = selectedCompletionOtherRunID
						ctx = runtimeeffects.WithAuthority(ctx, authority)
					case "invalid_authority":
						authority.FenceGeneration = 0
						ctx = runtimeeffects.WithAuthority(ctx, authority)
					case "missing_source":
						ctx = runtimeeffects.WithAuthority(runtimecorrelation.WithRunID(context.Background(), selectedCompletionRunID), authority)
					case "wrong_source":
						ctx = runtimecorrelation.WithSourceArtifactFact(ctx, admissionTestSource(t, admissionTestRevisedBundle))
					case "missing_owner":
						authorityOwner = nil
					}
					expectRunExecutionSource(mock, postgres, observe, sourceState)
					expectRunExecutionBinding(mock, postgres, true)
					expectRunExecutionState(mock, postgres, currentState)
					owner := runExecutionOwner(postgres, authorityOwner)
					if observe {
						if current, err := owner.ObserveRunExecution(ctx, tx, selectedCompletionRunID); current || err != nil {
							t.Fatalf("selected observation did not refuse without authority: current=%t err=%v", current, err)
						}
					} else if err := owner.RequireRunExecutionTx(ctx, tx, selectedCompletionRunID); !errors.Is(err, runtimerunlifecycle.ErrRunExecutionAuthority) {
						t.Fatalf("selected mutable admission did not preserve its authority refusal: %v", err)
					}
					if probe.calls != 0 || probe.observations != 0 {
						t.Fatal("paused or mismatched selected context reached the fresh authority owner")
					}
				})
			}
		}
	}
}

func TestRunExecutionSelectedDelegatesFreshOwnerBothDialects(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		for _, observe := range []bool{false, true} {
			t.Run(admissionTestDialect(postgres)+map[bool]string{true: "/observe", false: "/require"}[observe], func(t *testing.T) {
				tx, mock := runExecutionTransaction(t)
				ctx, authority := selectedCompletionContext(t)
				stale := runtimefailures.New(runtimefailures.ClassSupersededGeneration, "external_effect_authority_stale", "external-effects", "check_authority", nil)
				probe := &selectedCompletionAuthorityProbe{current: true, priorQueries: mock}
				owner := runExecutionOwner(postgres, probe)
				for check := range 2 {
					expectRunExecutionSource(mock, postgres, observe, "running")
					expectRunExecutionBinding(mock, postgres, true)
					expectRunExecutionState(mock, postgres, "running")
					if check == 1 {
						probe.current, probe.err = false, stale
					}
					if observe {
						current, err := owner.ObserveRunExecution(ctx, tx, selectedCompletionRunID)
						if err != nil || current != (check == 0) || probe.observations != check+1 || probe.calls != 0 || probe.query != tx || !reflect.DeepEqual(probe.observed, authority) {
							t.Fatalf("observation reused or mutated a fence: current=%t err=%v probe=%+v", current, err, probe)
						}
					} else {
						err := owner.RequireRunExecutionTx(ctx, tx, selectedCompletionRunID)
						if check == 0 && err != nil || check == 1 && !errors.Is(err, stale) || probe.calls != check+1 || probe.observations != 0 || probe.tx != tx || !reflect.DeepEqual(probe.got, authority) {
							t.Fatalf("mutable admission lost fresh native fence attribution: err=%v probe=%+v", err, probe)
						}
					}
					if probe.priorErr != nil {
						t.Fatalf("authority owner ran before canonical source, artifact, binding, and current status: %v", probe.priorErr)
					}
				}
			})
		}
	}
}

func TestRunExecutionOrdinaryAndGlobalPreservedBothDialects(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		for _, selectedContext := range []bool{false, true} {
			for _, state := range []string{"running", "paused", "global"} {
				t.Run(admissionTestDialect(postgres)+map[bool]string{true: "/selected_context/", false: "/ordinary_context/"}[selectedContext]+state, func(t *testing.T) {
					tx, mock := runExecutionTransaction(t)
					ctx := context.Background()
					if selectedContext {
						ctx, _ = selectedCompletionContext(t)
					}
					probe := &selectedCompletionAuthorityProbe{current: true}
					owner := runExecutionOwner(postgres, probe)
					runID := selectedCompletionRunID
					if state == "global" {
						runID = ""
					} else {
						expectRunExecutionSource(mock, postgres, false, state)
						expectRunExecutionBinding(mock, postgres, false)
					}
					err := owner.RequireRunExecutionTx(ctx, tx, runID)
					if selectedContext && !errors.Is(err, runtimerunlifecycle.ErrRunExecutionAuthority) || !selectedContext && err != nil {
						t.Fatalf("ordinary/global mutable admission changed: selected=%t err=%v", selectedContext, err)
					}
					if state != "global" {
						expectRunExecutionSource(mock, postgres, true, state)
						expectRunExecutionBinding(mock, postgres, false)
					}
					current, err := owner.ObserveRunExecution(ctx, tx, runID)
					if err != nil || current == selectedContext || probe.calls != 0 || probe.observations != 0 {
						t.Fatalf("ordinary/global observation borrowed selected authority: current=%t err=%v probe=%+v", current, err, probe)
					}
				})
			}
		}
	}
}

func TestRunExecutionSourceErrorsKeepModeAndTypedCauseBothDialects(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		for _, selected := range []bool{false, true} {
			for _, state := range []string{"completed", "missing"} {
				t.Run(admissionTestDialect(postgres)+map[bool]string{true: "/selected/", false: "/ordinary/"}[selected]+state, func(t *testing.T) {
					tx, mock := runExecutionTransaction(t)
					ctx := context.Background()
					if selected {
						ctx, _ = selectedCompletionContext(t)
					}
					probe := &selectedCompletionAuthorityProbe{current: true}
					owner := runExecutionOwner(postgres, probe)
					expectRunExecutionSource(mock, postgres, false, state)
					expectRunExecutionBinding(mock, postgres, selected)
					err := owner.RequireRunExecutionTx(ctx, tx, selectedCompletionRunID)
					want := runtimerunlifecycle.ErrRunNotActive
					if state == "missing" {
						want = runtimerunlifecycle.ErrRunNotFound
					}
					if !errors.Is(err, want) || errors.Is(err, runtimerunlifecycle.ErrRunExecutionAuthority) != selected {
						t.Fatalf("source failure lost exact ordinary/selected classification: %v", err)
					}
					if state == "missing" {
						var missing *runtimerunlifecycle.RunNotFoundError
						if !errors.As(err, &missing) || missing.RunID != selectedCompletionRunID {
							t.Fatal("missing run identity was lost")
						}
					} else {
						var inactive *runtimerunlifecycle.RunNotActiveError
						if !errors.As(err, &inactive) || inactive.RunID != selectedCompletionRunID || inactive.State != runtimerunlifecycle.StateCompleted {
							t.Fatal("nonactive run identity or exact state was lost")
						}
					}
					expectRunExecutionSource(mock, postgres, true, state)
					if current, err := owner.ObserveRunExecution(ctx, tx, selectedCompletionRunID); current || err != nil || probe.calls != 0 || probe.observations != 0 {
						t.Fatalf("source observation did not refuse without fence mutation: current=%t err=%v", current, err)
					}
				})
			}
		}
	}
}

func TestRunExecutionSQLAndArtifactRefusalsBothDialects(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		for _, observe := range []bool{false, true} {
			for _, stage := range []string{"source", "artifact", "missing_artifact", "binding", "foreign_binding", "status", "fence"} {
				t.Run(admissionTestDialect(postgres)+map[bool]string{true: "/observe/", false: "/require/"}[observe]+stage, func(t *testing.T) {
					tx, mock := runExecutionTransaction(t)
					ctx, _ := selectedCompletionContext(t)
					failure := errors.New("native " + stage + " unavailable")
					probe := &selectedCompletionAuthorityProbe{}
					if stage == "source" {
						mock.ExpectQuery(admissionTestRunSQL(postgres, observe)).WithArgs(selectedCompletionRunID).WillReturnError(failure)
					} else if stage == "artifact" || stage == "missing_artifact" {
						mock.ExpectQuery(admissionTestRunSQL(postgres, observe)).WithArgs(selectedCompletionRunID).
							WillReturnRows(sqlmock.NewRows([]string{"status", "bundle_hash"}).AddRow("running", selectedCompletionBundle))
						artifact := mock.ExpectQuery(admissionTestArtifactSQL(postgres)).WithArgs(selectedCompletionBundle)
						if stage == "artifact" {
							artifact.WillReturnError(failure)
						} else {
							artifact.WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
						}
					} else {
						expectRunExecutionSource(mock, postgres, observe, "running")
						if stage == "binding" {
							mock.ExpectQuery(runExecutionBindingSQL(postgres)).WithArgs(selectedCompletionRunID).WillReturnError(failure)
						} else if stage == "foreign_binding" {
							mock.ExpectQuery(runExecutionBindingSQL(postgres)).WithArgs(selectedCompletionRunID).
								WillReturnRows(sqlmock.NewRows([]string{"fork_run_id"}).AddRow(selectedCompletionOtherRunID))
						} else {
							expectRunExecutionBinding(mock, postgres, true)
							if stage == "status" {
								mock.ExpectQuery(runExecutionStateSQL(postgres)).WithArgs(selectedCompletionRunID).WillReturnError(failure)
							} else {
								expectRunExecutionState(mock, postgres, "running")
								probe.err, probe.observeErr = failure, failure
							}
						}
					}
					owner := runExecutionOwner(postgres, probe)
					var err error
					if observe {
						var current bool
						current, err = owner.ObserveRunExecution(ctx, tx, selectedCompletionRunID)
						if current || probe.calls != 0 {
							t.Fatal("failed observation authorized or mutated a fence")
						}
					} else {
						err = owner.RequireRunExecutionTx(ctx, tx, selectedCompletionRunID)
					}
					if err == nil || errors.Is(err, runtimerunlifecycle.ErrRunExecutionAuthority) {
						t.Fatalf("native infrastructure/artifact refusal became selected authority refusal: %v", err)
					}
					if stage == "missing_artifact" {
						var unavailable *runtimerunlifecycle.SourceArtifactUnavailableError
						if !errors.As(err, &unavailable) || unavailable.BundleHash != selectedCompletionBundle {
							t.Fatalf("source artifact identity was lost: %v", err)
						}
					} else if stage != "foreign_binding" && !errors.Is(err, failure) {
						t.Fatalf("native error cause was lost: %v", err)
					}
					wantCalls := 0
					if stage == "fence" {
						wantCalls = 1
					}
					if probe.calls+probe.observations != wantCalls {
						t.Fatal("failed canonical source or binding reached the native fence")
					}
				})
			}
		}
	}
}

func TestRunExecutionCancellationPrecedesQueriesAndFenceBothDialects(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		t.Run(admissionTestDialect(postgres), func(t *testing.T) {
			tx, _ := runExecutionTransaction(t)
			base, _ := selectedCompletionContext(t)
			cancelled, cancel := context.WithCancel(base)
			cancel()
			expired, expire := context.WithDeadline(base, time.Unix(1, 0))
			defer expire()
			probe := &selectedCompletionAuthorityProbe{current: true}
			owner := runExecutionOwner(postgres, probe)
			for _, ctx := range []context.Context{cancelled, expired} {
				for _, runID := range []string{selectedCompletionRunID, ""} {
					if err := owner.RequireRunExecutionTx(ctx, tx, runID); !errors.Is(err, ctx.Err()) || errors.Is(err, runtimerunlifecycle.ErrRunExecutionAuthority) {
						t.Fatalf("mutable cancellation reclassified: %v", err)
					}
					if current, err := owner.ObserveRunExecution(ctx, tx, runID); current || !errors.Is(err, ctx.Err()) {
						t.Fatalf("observation cancellation reclassified: current=%t err=%v", current, err)
					}
				}
			}
			if probe.calls != 0 || probe.observations != 0 {
				t.Fatal("canceled work reached execution authority")
			}
		})
	}
}

func TestRunExecutionObservationCannotMintNativeAdmissionBothDialects(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		t.Run(admissionTestDialect(postgres), func(t *testing.T) {
			db, mock := admissionTestSQLMock(t)
			mock.ExpectBegin()
			// Observation cannot mint admission; a cached mutable source cannot replace current execution status.
			expectRunExecutionSource(mock, postgres, true, "running")
			expectRunExecutionBinding(mock, postgres, true)
			expectRunExecutionState(mock, postgres, "running")
			expectRunExecutionSource(mock, postgres, false, "running")
			expectRunExecutionBinding(mock, postgres, true)
			expectRunExecutionState(mock, postgres, "running")
			expectRunExecutionSource(mock, postgres, true, "running")
			expectRunExecutionBinding(mock, postgres, true)
			expectRunExecutionState(mock, postgres, "running")
			mock.ExpectQuery(admissionTestArtifactSQL(postgres)).WithArgs(selectedCompletionBundle).
				WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
			expectRunExecutionBinding(mock, postgres, true)
			expectRunExecutionState(mock, postgres, "paused")
			probe := &selectedCompletionAuthorityProbe{current: true}
			owner := runExecutionOwner(postgres, probe)
			admissionTestNativeAttempt(t, db, mock, postgres, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
				_, authority := selectedCompletionContext(t)
				ctx = runtimeeffects.WithAuthority(runtimecorrelation.WithSourceArtifactFact(runtimecorrelation.WithRunID(ctx, selectedCompletionRunID), admissionTestSource(t, selectedCompletionBundle)), authority)
				return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
					if current, err := owner.ObserveRunExecution(ctx, tx, selectedCompletionRunID); err != nil || !current {
						t.Fatalf("first native observation: current=%t err=%v", current, err)
					}
					if _, cached, err := mutationprotocol.CachedActiveRunSource(ctx, tx, selectedCompletionRunID); err != nil || cached {
						t.Fatalf("observation minted mutable source admission: cached=%t err=%v", cached, err)
					}
					if err := owner.RequireRunExecutionTx(ctx, tx, selectedCompletionRunID); err != nil {
						return err
					}
					if fact, cached, err := mutationprotocol.CachedActiveRunSource(ctx, tx, selectedCompletionRunID); err != nil || !cached || fact.BundleHash() != selectedCompletionBundle {
						t.Fatalf("canonical mutable source was not attributed to native transaction: cached=%t err=%v", cached, err)
					}
					if current, err := owner.ObserveRunExecution(ctx, tx, selectedCompletionRunID); err != nil || !current {
						t.Fatalf("fresh observation with existing admission: current=%t err=%v", current, err)
					}
					if err := owner.RequireRunExecutionTx(ctx, tx, selectedCompletionRunID); !errors.Is(err, runtimerunlifecycle.ErrRunExecutionAuthority) {
						t.Fatalf("cached active source bypassed fresh paused execution status: %v", err)
					}
					if probe.calls != 1 || probe.observations != 2 || probe.tx != tx || probe.query != tx {
						t.Fatal("observation used a mutable fence or lost native transaction attribution")
					}
					return nil
				})
			})
		})
	}
}

func TestRunExecutionRejectsForeignNativeTransactionBothDialects(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		t.Run(admissionTestDialect(postgres), func(t *testing.T) {
			foreign, _ := runExecutionTransaction(t)
			db, mock := admissionTestSQLMock(t)
			mock.ExpectBegin()
			probe := &selectedCompletionAuthorityProbe{current: true}
			owner := runExecutionOwner(postgres, probe)
			admissionTestNativeAttempt(t, db, mock, postgres, nil, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
				_, authority := selectedCompletionContext(t)
				ctx = runtimeeffects.WithAuthority(runtimecorrelation.WithSourceArtifactFact(runtimecorrelation.WithRunID(ctx, selectedCompletionRunID), admissionTestSource(t, selectedCompletionBundle)), authority)
				return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
					if tx == foreign {
						t.Fatal("foreign transaction fixture reused the native frame")
					}
					if err := owner.RequireRunExecutionTx(ctx, foreign, selectedCompletionRunID); err == nil || errors.Is(err, runtimerunlifecycle.ErrRunExecutionAuthority) {
						t.Fatalf("foreign native transaction attribution was accepted or reclassified: %v", err)
					}
					if probe.calls != 0 || probe.observations != 0 {
						t.Fatal("foreign transaction reached selected authority")
					}
					return nil
				})
			})
		})
	}
}

func TestRunExecutionInvalidInputsRefuseBeforeScopeBothDialects(t *testing.T) {
	for _, postgres := range []bool{true, false} {
		for _, runID := range []string{selectedCompletionRunID, ""} {
			for _, scenario := range []string{"nil_lifecycle", "nil_context", "nil_query_or_transaction"} {
				t.Run(admissionTestDialect(postgres)+"/"+runID+"/"+scenario, func(t *testing.T) {
					tx, _ := runExecutionTransaction(t)
					probe := &selectedCompletionAuthorityProbe{current: true}
					owner := runExecutionOwner(postgres, probe)
					ctx := context.Background()
					mutationTx := tx
					var query sourceadmission.ExecutionQuery = tx
					switch scenario {
					case "nil_lifecycle":
						if postgres {
							owner = (*RunLifecyclePostgresOwner)(nil)
						} else {
							owner = (*RunLifecycleSQLiteOwner)(nil)
						}
					case "nil_context":
						ctx = nil
					case "nil_query_or_transaction":
						mutationTx, query = nil, nil
					}
					if err := owner.RequireRunExecutionTx(ctx, mutationTx, runID); err == nil || errors.Is(err, runtimerunlifecycle.ErrRunExecutionAuthority) {
						t.Fatalf("invalid mutable capability became runless permission or selected refusal: %v", err)
					}
					if current, err := owner.ObserveRunExecution(ctx, query, runID); current || err == nil || errors.Is(err, runtimerunlifecycle.ErrRunExecutionAuthority) {
						t.Fatalf("invalid observation capability became runless permission or selected refusal: current=%t err=%v", current, err)
					}
					if probe.calls != 0 || probe.observations != 0 {
						t.Fatal("invalid execution input reached native authority")
					}
				})
			}
		}
	}
}
