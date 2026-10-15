package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestForkOperationElectionKeepsOriginalRequestBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := forkOperationTestDatabase(t, backend)
			ctx := context.Background()
			request := runfork.ForkOperationRequest{
				OperationID: uuid.NewString(), Actor: "bearer:operator", IdempotencyKey: "elected",
				TransportHash: "sha256:original", SourceRunID: uuid.NewString(), TargetBundleHash: "sha256:bundle",
				ContractSelection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
				ResolvedPoint:     &runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 4},
			}
			childID, bindingID := uuid.NewString(), uuid.NewString()
			withForkOperationTx(t, db, func(tx *sql.Tx) {
				if err := requireFreshForkOperationTx(ctx, tx, &request, backend == "postgres"); err != nil {
					t.Fatalf("fresh invocation: %v", err)
				}
				if _, replay, err := bindForkOperationTx(ctx, tx, request, childID, bindingID, backend == "postgres"); err != nil || replay {
					t.Fatalf("bind winner: replay=%t err=%v", replay, err)
				}
			})
			for _, change := range []string{"same_invocation", "late_winner", "latest_changed", "different_transport", "crossed_invocation", "foreign_source", "independent_key", "keyless", "invalid_invocation"} {
				t.Run(change, func(t *testing.T) {
					candidate := request
					switch change {
					case "late_winner":
						candidate.OperationID = uuid.NewString()
					case "latest_changed":
						candidate.OperationID, candidate.TargetBundleHash = uuid.NewString(), "sha256:later-bundle"
						candidate.ResolvedPoint = &runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 9}
					case "different_transport":
						candidate.OperationID, candidate.TransportHash = uuid.NewString(), "sha256:other-request"
					case "crossed_invocation":
						candidate.TargetBundleHash = "sha256:other-bundle"
					case "foreign_source":
						candidate.OperationID, candidate.SourceRunID = uuid.NewString(), uuid.NewString()
					case "independent_key":
						candidate.OperationID, candidate.IdempotencyKey = uuid.NewString(), "independent"
					case "keyless":
						candidate.OperationID, candidate.IdempotencyKey = uuid.NewString(), ""
					case "invalid_invocation":
						candidate.OperationID = "not-a-uuid"
					}
					withForkOperationTx(t, db, func(tx *sql.Tx) {
						err := requireFreshForkOperationTx(ctx, tx, &candidate, backend == "postgres")
						var elected *selectedForkOperationReplay
						switch change {
						case "same_invocation", "late_winner", "latest_changed":
							if !errors.As(err, &elected) || elected.operationID != request.OperationID || elected.forkRunID != childID {
								t.Fatalf("durable winner lost original identity: replay=%+v err=%v", elected, err)
							}
						case "independent_key", "keyless":
							if err != nil {
								t.Fatalf("independent invocation was treated as replay: %v", err)
							}
						default:
							if err == nil || errors.As(err, &elected) {
								t.Fatalf("invalid request became an election: %v", err)
							}
							if change == "different_transport" {
								var conflict *runfork.ForkOperationKeyConflictError
								if !errors.As(err, &conflict) {
									t.Fatalf("transport mismatch lost typed conflict: %v", err)
								}
							}
						}
						loaded, found, err := loadForkOperationByIDTx(ctx, tx, request.OperationID, backend == "postgres")
						if err != nil || !found || loaded.ForkRunID != childID || loaded.BindingID != bindingID ||
							loaded.Request.TargetBundleHash != request.TargetBundleHash || *loaded.Request.ResolvedPoint != *request.ResolvedPoint || loaded.Status != runfork.ForkOperationMaterialized {
							t.Fatalf("election changed durable winner: record=%+v found=%t err=%v", loaded, found, err)
						}
						if candidate.OperationID != request.OperationID && change != "invalid_invocation" {
							if _, found, err := loadForkOperationByIDTx(ctx, tx, candidate.OperationID, backend == "postgres"); err != nil || found {
								t.Fatalf("losing/independent observation created an operation: found=%t err=%v", found, err)
							}
						}
					})
				})
			}
		})
	}
}

func TestForkOperationContinuationRequiresCleanCallbackAbort(t *testing.T) {
	replay := &selectedForkOperationReplay{operationID: uuid.NewString(), forkRunID: uuid.NewString()}
	independent := errors.New("independent rollback failure")
	for _, test := range []struct {
		name      string
		err       error
		committed bool
		canceled  bool
		callback  *selectedForkOperationReplay
		want      bool
	}{
		{"clean", replay, false, false, replay, true},
		{"wrapped", fmt.Errorf("native abort: %w", replay), false, false, replay, true},
		{"single_branch", errors.Join(replay), false, false, replay, true},
		{"independent_cleanup", errors.Join(replay, independent), false, false, replay, false},
		{"joined_cancellation", errors.Join(replay, context.Canceled), false, false, replay, false},
		{"caller_canceled", replay, false, true, replay, false},
		{"acknowledged", replay, true, false, replay, false},
		{"no_callback_refusal", independent, false, false, nil, false},
		{"unacknowledged_nil", nil, false, false, replay, false},
		{"unrelated_refusal", &selectedForkOperationReplay{}, false, false, replay, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if test.canceled {
				cancel()
			}
			err := selectedForkOperationReplayAfterAbort(ctx, test.committed, test.err, test.callback)
			var continuation *runfork.ForkOperationReplayRequiredError
			if errors.As(err, &continuation) != test.want {
				t.Fatalf("continuation disposition=%+v err=%v want=%t", continuation, err, test.want)
			}
			if test.want {
				if continuation.OperationID != replay.operationID || continuation.ForkRunID != replay.forkRunID {
					t.Fatalf("continuation lost exact operation/child: %+v", continuation)
				}
			} else if err != test.err {
				t.Fatalf("refusal replaced independent error: got=%v want=%v", err, test.err)
			}
		})
	}
}
