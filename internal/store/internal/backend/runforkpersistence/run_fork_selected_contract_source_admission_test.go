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

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"

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
