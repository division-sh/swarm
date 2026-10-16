package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/decisionpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestSelectedHistoricalSourceAdmissionRequiresCanonicalFreezeLineageBothDialects(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		for _, name := range []string{"completed_successor", "running_successor", "foreign_successor", "crossed_successor_id", "missing_successor", "malformed_successor", "missing_pointer", "wrong_source", "uncommitted_cut", "cancelled_reader"} {
			t.Run(map[bool]string{false: "sqlite", true: "postgres"}[postgres]+"/"+name, func(t *testing.T) {
				tx, mock := startSnapshotTransaction(t)
				at, ended := time.Unix(100, 0).UTC(), time.Unix(200, 0).UTC()
				sourceID, successorID := uuid.NewString(), uuid.NewString()
				origin, err := runlifecycle.ForkMaterializationRunOrigin(sourceID, runfork.RunForkPointDeploymentRevision, 1, "")
				if err != nil {
					t.Fatal(err)
				}
				source := runlifecycle.Snapshot{RunID: sourceID, State: runlifecycle.StateForked, Origin: runlifecycle.DeploymentRunOrigin(), BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), StartedAt: at, EndedAt: &ended, ContinuedAsRunID: successorID}
				successor := runlifecycle.Snapshot{RunID: successorID, State: runlifecycle.StateCompleted, Origin: origin, BundleHash: source.BundleHash, StartedAt: at, EndedAt: &ended}
				readErr := errors.New("original successor missing")
				switch name {
				case "running_successor":
					successor.State, successor.EndedAt = runlifecycle.StateRunning, nil
				case "foreign_successor":
					successor.Origin, err = runlifecycle.ForkMaterializationRunOrigin(uuid.NewString(), runfork.RunForkPointDeploymentRevision, 1, "")
					if err != nil {
						t.Fatal(err)
					}
				case "crossed_successor_id":
					successor.RunID = uuid.NewString()
				case "malformed_successor":
					successor.EndedAt = nil
				case "missing_pointer":
					source.ContinuedAsRunID = ""
				case "wrong_source":
					source.RunID = uuid.NewString()
				case "cancelled_reader":
					readErr = context.Canceled
				}
				beforeSource, beforeSuccessor := source, successor
				if name == "completed_successor" || name == "running_successor" || name == "uncommitted_cut" {
					mock.ExpectQuery(`SELECT origin_kind FROM runs`).WithArgs(sourceID).WillReturnRows(sqlmock.NewRows([]string{"origin_kind"}).AddRow("deployment"))
					rows := sqlmock.NewRows([]string{"revision"})
					if name != "uncommitted_cut" {
						rows.AddRow(int64(1))
					}
					mock.ExpectQuery(`SELECT revision FROM run_fork_revisions`).WithArgs(sourceID, int64(1)).WillReturnRows(rows)
				}
				reads := 0
				got, err := admitSelectedForkSource(t.Context(), tx, sourceID, source, func(_ context.Context, _ *sql.Tx, runID string) (runlifecycle.Snapshot, error) {
					reads++
					if runID != source.ContinuedAsRunID {
						t.Fatalf("reader was given another successor: %q", runID)
					}
					if name == "missing_successor" || name == "cancelled_reader" {
						return runlifecycle.Snapshot{}, readErr
					}
					return successor, nil
				}, postgres)
				positive := name == "completed_successor" || name == "running_successor"
				if positive && (err != nil || got != selectedForkHistoricalSource) || !positive && (err == nil || got != 0) {
					t.Fatalf("admission=%v err=%v positive=%t", got, err, positive)
				}
				if name == "cancelled_reader" && !errors.Is(err, context.Canceled) {
					t.Fatalf("reader cancellation lost: %v", err)
				}
				wantReads := 1
				if name == "missing_pointer" || name == "wrong_source" {
					wantReads = 0
				}
				if reads != wantReads || !reflect.DeepEqual(source, beforeSource) || !reflect.DeepEqual(successor, beforeSuccessor) {
					t.Fatal("admission changed lifecycle facts or bypassed exact successor observation")
				}
			})
		}
	}
}

func TestSelectedUnfrozenSourceAdmissionNeverUsesHistoricalFallback(t *testing.T) {
	for _, state := range []runlifecycle.State{runlifecycle.StateRunning, runlifecycle.StatePaused, runlifecycle.StateCompleted, runlifecycle.StateCancelled} {
		t.Run(string(state), func(t *testing.T) {
			at := time.Unix(100, 0).UTC()
			source := runlifecycle.Snapshot{RunID: uuid.NewString(), State: state, Origin: runlifecycle.ScenarioSetupRunOrigin(), BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), StartedAt: at}
			if state.Terminal() {
				source.EndedAt = &at
			}
			got, err := admitSelectedForkSource(t.Context(), nil, source.RunID, source, func(context.Context, *sql.Tx, string) (runlifecycle.Snapshot, error) {
				t.Fatal("unfrozen source consulted historical lineage")
				return runlifecycle.Snapshot{}, nil
			}, false)
			if got != selectedForkUnfrozenSource || err != nil {
				t.Fatalf("admission=%v err=%v", got, err)
			}
		})
	}
}

func TestSelectedHistoricalSourceAdmissionNativeConservesFreezeBothStores(t *testing.T) {
	for _, dialect := range []string{"sqlite", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			f := openTimerHistoryNativeFixture(t, dialect)
			if owner, ok := f.runs.(*storerunlifecycle.RunLifecyclePostgresOwner); ok {
				decisions, err := decisionpersistence.NewPostgres(f.postgres, f.requireCurrent, owner, owner.CompletionCandidateCoordinator())
				if err != nil {
					t.Fatal(err)
				}
				if err := owner.BindDecisionCards(decisions); err != nil {
					t.Fatal(err)
				}
			} else {
				owner := f.runs.(*storerunlifecycle.RunLifecycleSQLiteOwner)
				decisions, err := decisionpersistence.NewSQLite(f.sqlite, f.requireCurrent, owner, owner.CompletionCandidateCoordinator(), nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := owner.BindDecisionCards(decisions); err != nil {
					t.Fatal(err)
				}
			}
			ctx := authoractivity.WithScope(correlation.WithSourceArtifactFact(t.Context(), sourceartifactfixture.Fact()), authoractivity.BundleScope(uuid.NewString(), sourceartifactfixture.BundleHash))
			at := runlifecycle.CanonicalTimestamp(time.Now().Add(-time.Minute))
			sourceID, firstID, otherID := uuid.NewString(), uuid.NewString(), uuid.NewString()
			for _, id := range []string{sourceID, firstID, otherID} {
				origin := runlifecycle.DeploymentRunOrigin()
				if id != sourceID {
					var err error
					origin, err = runlifecycle.ForkMaterializationRunOrigin(sourceID, runfork.RunForkPointDeploymentRevision, 1, "")
					if err != nil {
						t.Fatal(err)
					}
				}
				if got, err := f.runs.CreateRun(ctx, runlifecycle.CreateRequest{RunID: id, Origin: origin, Source: sourceartifactfixture.Fact(), StartedAt: at}); err != nil || got != runlifecycle.MutationApplied {
					t.Fatalf("create canonical lineage: %s %v", got, err)
				}
			}
			lifecycle := f.runs.(interface {
				ForkRunSource(context.Context, runlifecycle.ForkSourceRequest) (runlifecycle.Snapshot, runlifecycle.MutationDisposition, error)
				CompleteRunTx(context.Context, *mutationprotocol.Attempt, string, time.Time, *storerunlifecycle.ChannelCardChanges) (runlifecycle.Snapshot, runlifecycle.MutationDisposition, error)
			})
			frozen, disposition, err := lifecycle.ForkRunSource(ctx, runlifecycle.ForkSourceRequest{RunID: sourceID, ContinuedAsRunID: firstID, EndedAt: at.Add(time.Second)})
			if err != nil || disposition != runlifecycle.MutationApplied {
				t.Fatalf("canonical initial freeze: %s %v", disposition, err)
			}
			var loadLocked, read runForkLifecycleSnapshotLoader
			var requireActive func(context.Context, *sql.Tx, string) (correlation.SourceArtifactFact, error)
			if owner, ok := f.runs.(*storerunlifecycle.RunLifecyclePostgresOwner); ok {
				loadLocked = func(ctx context.Context, tx *sql.Tx, id string) (runlifecycle.Snapshot, error) {
					return owner.LoadSnapshotTx(ctx, tx, id, true)
				}
				read = func(ctx context.Context, tx *sql.Tx, id string) (runlifecycle.Snapshot, error) {
					return owner.LoadSnapshotTx(ctx, tx, id, false)
				}
				requireActive = owner.RequireActiveSourceTx
			} else {
				owner := f.runs.(*storerunlifecycle.RunLifecycleSQLiteOwner)
				loadLocked, read, requireActive = owner.LoadSnapshotTx, owner.LoadSnapshotTx, owner.RequireActiveSourceTx
			}
			for _, phase := range []string{"running_successor", "completed_successor"} {
				if phase == "completed_successor" {
					complete := func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
						_, disposition, err := lifecycle.CompleteRunTx(ctx, attempt, firstID, at.Add(2*time.Second), nil)
						if err == nil && disposition != runlifecycle.MutationApplied {
							return struct{}{}, errors.New("original successor completion was not applied")
						}
						return struct{}{}, err
					}
					var completion mutationprotocol.Result[struct{}]
					if f.postgres != nil {
						completion = mutationprotocol.RunPostgres(ctx, f.postgres, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, complete)
					} else {
						completion = mutationprotocol.RunSQLite(ctx, f.sqlite, "complete empty native lineage fixture", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, complete)
					}
					if err := completion.Err(); err != nil || !completion.Acknowledged() {
						t.Fatalf("complete original successor: %v", err)
					}
				}
				outcome := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runfork.RunForkReplayResumeAdmission, error) {
					err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
						source, err := loadLocked(ctx, tx, sourceID)
						if err != nil {
							return err
						}
						before, err := read(ctx, tx, firstID)
						if err != nil {
							return err
						}
						got, err := admitSelectedForkSource(ctx, tx, sourceID, source, read, dialect == "postgres")
						if err != nil || got != selectedForkHistoricalSource {
							t.Fatalf("native admission %s: %v %v", phase, got, err)
						}
						if _, err := requireActive(ctx, tx, sourceID); !errors.Is(err, runlifecycle.ErrRunNotActive) {
							t.Fatalf("historical admission allowed source execution: %v", err)
						}
						afterSource, err := read(ctx, tx, sourceID)
						if err != nil || !reflect.DeepEqual(afterSource, frozen) {
							t.Fatalf("source freeze changed: %+v %v", afterSource, err)
						}
						after, err := read(ctx, tx, firstID)
						if err != nil || !reflect.DeepEqual(before, after) {
							t.Fatalf("original successor changed: %+v %v", after, err)
						}
						return nil
					})
					return runfork.RunForkReplayResumeAdmission{}, err
				})
				if err := outcome.Err(); err != nil || !outcome.Acknowledged() {
					t.Fatalf("native historical admission %s: %v", phase, err)
				}
			}
			if _, _, err := lifecycle.ForkRunSource(ctx, runlifecycle.ForkSourceRequest{RunID: sourceID, ContinuedAsRunID: otherID, EndedAt: at.Add(3 * time.Second)}); err == nil {
				t.Fatal("historical admission permitted re-freezing the source into another child")
			}
		})
	}
}
